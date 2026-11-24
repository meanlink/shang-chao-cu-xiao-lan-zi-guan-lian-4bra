"""Logic tests for per-store rule withdrawal.

The sandbox has no FastAPI/PyMySQL/MySQL, so this harness injects tiny stubs and
an in-memory table engine, then drives the real service functions to verify
state transitions and the withdrawal / receipt-booking race branches.
Run: python3 tests/test_withdrawal_logic.py
"""
import copy
import re
import sys
import types
from contextlib import contextmanager
from datetime import date
from types import SimpleNamespace

# --- stub third-party modules -------------------------------------------------
fastapi = types.ModuleType('fastapi')


class HTTPException(Exception):
    def __init__(self, status_code, detail):
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


fastapi.HTTPException = HTTPException
sys.modules['fastapi'] = fastapi

pymysql = types.ModuleType('pymysql')
pymysql_cursors = types.ModuleType('pymysql.cursors')
pymysql_cursors.DictCursor = object
pymysql_cursors_module = pymysql_cursors
pymysql.cursors = pymysql_cursors
sys.modules['pymysql.cursors'] = pymysql_cursors
pymysql_err = types.ModuleType('pymysql.err')


class IntegrityError(Exception):
    def __init__(self, args):
        self.args = (args,)


class IntegrityError(Exception):
    def __init__(self, args):
        self.args = (args,)


pymysql_err.IntegrityError = IntegrityError
pymysql.err = pymysql_err
sys.modules['pymysql'] = pymysql
sys.modules['pymysql.err'] = pymysql_err

sys.path.insert(0, '/workspace/backend')

from src.db import transaction  # noqa: E402,F401  (ensures src package imports)
from src import db  # noqa: E402
from src.constants.enums import PromotionStatus as Plan  # noqa: E402
from src.services import promotion, basket_receipts, rule_withdrawal  # noqa: E402

# --- in-memory database -------------------------------------------------------
TABLES = {}


def reset_db():
    TABLES.clear()
    TABLES.update({
        'users': [], 'stores': [], 'product_families': [], 'products': [],
        'association_rules': [], 'promotion_plans': [], 'promotion_settlements': [],
        'basket_offers': [], 'basket_offer_requirements': [], 'basket_receipts': [],
        'basket_receipt_lines': [], 'rule_store_withdrawals': [], 'audit_logs': [],
    })


class FakeCursor:
    def __init__(self, conn=None):
        self.conn = conn
        self.lastrowid = None
        self._rows = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def executemany(self, sql, seq):
        for args in seq:
            self.execute(sql, args)

    def execute(self, sql, args=()):
        self._rows = []
        s = ' '.join(sql.split())
        args = tuple(args)
        m = re.match(r'INSERT INTO (\w+)\s*\(([^)]+)\)\s*VALUES', s)
        if m:
            table, cols = m.group(1), [c.strip() for c in m.group(2).split(',')]
            if self.conn:
                self.conn._ensure_snapshot(table)
            rows = TABLES[table]
            new_id = max((r['id'] for r in rows), default=0) + 1
            row = {'id': new_id}
            if table == 'promotion_plans':
                row['status'] = Plan.planned
            row.update(dict(zip(cols, args)))
            if table == 'rule_store_withdrawals' and any(
                    r['rule_id'] == row['rule_id'] and r['store_id'] == row['store_id'] for r in rows):
                raise IntegrityError(1062)
            rows.append(row)
            self.lastrowid = new_id
            return
        m = re.match(r'UPDATE (\w+) SET (.+?) WHERE (.+)$', s)
        if m:
            table, sets, where = m.group(1), m.group(2), m.group(3)
            if self.conn:
                self.conn._ensure_snapshot(table)
            set_cols = re.findall(r'(\w+)\s*=\s*%s', sets)
            assignments = dict(zip(set_cols, args[:len(set_cols)]))
            where_args = args[len(set_cols):]
            for func_col in re.findall(r'(\w+)\s*=\s*[A-Za-z_]+\([^)]*\)', sets):
                assignments.setdefault(func_col, 'UTC_TIMESTAMP')
            for row in TABLES[table]:
                if _match_where(row, where, where_args):
                    row.update(assignments)
            return
        if s.startswith('DELETE FROM'):
            table = re.search(r'FROM (\w+)', s).group(1)
            if self.conn: self.conn._ensure_snapshot(table)
            where = s.split(' WHERE ', 1)[1]
            TABLES[table] = [r for r in TABLES[table] if not _match_where(r, where, args)]
            return
        self._rows = _select(s, args)

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)


def _value(row, col):
    return row.get(col.split('.')[-1])


def _match_where(row, where, args):
    """Evaluate the simple WHERE shapes the service layer emits."""
    where = re.sub(r' FOR (UPDATE|SHARE)$', '', where)
    idx = 0
    m = re.search(r'(\w+(?:\.\w+)?)\s+IN \(([^)]+)\)', where)
    if m:
        n = m.group(2).count('%s')
        values = args[idx:idx + n]
        idx += n
        if _value(row, m.group(1)) not in values:
            return False
        where = (where[:m.start()] + ' ' + where[m.end():]).replace('AND  AND', 'AND').strip(' AND ')
    for col in re.findall(r'(\w+(?:\.\w+)?)\s*=\s*%s', where):
        if _value(row, col) != args[idx]:
            return False
        idx += 1
    null_match = re.search(r'(\w+)\s+IS NULL', where)
    if null_match and row.get(null_match.group(1)) is not None:
        return False
    return True


def _select(sql, args):
    if 'FROM products p JOIN product_families' in sql:
        sku_filter = re.search(r'p\.sku IN \(([^)]+)\)', sql)
        skus = set(args[:sql.count('%s')]) if sku_filter else None
        id_filter = re.search(r'p\.id=%s', sql)
        wanted_id = args[0] if id_filter else None
        out = []
        for p in TABLES['products']:
            if wanted_id is not None and p['id'] != wanted_id:
                continue
            if p['status'] != 'active' or (skus and p['sku'] not in skus):
                continue
            fam = next(f for f in TABLES['product_families'] if f['id'] == p['family_id'])
            if not fam['active']:
                continue
            if 'p.price' in sql:
                out.append({'id': p['id'], 'price': p['price'], 'family': fam['name']})
            else:
                out.append({'id': p['id'], 'sku': p['sku'], 'name': p['name'],
                            'family_id': p['family_id'], 'family': fam['name']})
        return out
    if 'FROM basket_offer_requirements q JOIN product_families' in sql:
        plan_id = args[0]
        return [{'id': f['id'], 'name': f['name'], 'minimum_units': q['minimum_units']}
                for q in TABLES['basket_offer_requirements'] if q['plan_id'] == plan_id
                for f in TABLES['product_families'] if f['id'] == q['family_id']]
    m = re.match(r'SELECT (.+?) FROM (\w+)(?:\s+\w+)?(?: WHERE (.+))?$', ' '.join(sql.split()))
    if not m:
        raise AssertionError(f'unhandled SQL: {sql}')
    cols, table, where = m.group(1), m.group(2), m.group(3)
    rows = TABLES[table]
    if where:
        where_clean = re.sub(r' FOR (UPDATE|SHARE)$', '', where)
        placeholder_cols = re.findall(r'(\w+(?:\.\w+)?)\s*=\s*%s', where_clean)
        literal_in = [(c.split('.')[-1], re.findall(r"'([^']+)'", body))
                      for c, body in re.findall(r'(\w+(?:\.\w+)?)\s+IN \(([^)]+)\)', where_clean)
                      if '%s' not in body]
        filtered = []
        for row in rows:
            ok = all(_value(row, col) == arg for col, arg in zip(placeholder_cols, args))
            for col, allowed in literal_in:
                ok = ok and row.get(col) in allowed
            null_match = re.search(r'(\w+)\s+IS NULL', where_clean)
            if null_match and row.get(null_match.group(1)) is not None:
                ok = False
            if ok:
                filtered.append(row)
        rows = filtered
    if cols == '*':
        return [dict(r) for r in rows]
    if 'COALESCE(SUM' in cols:
        col = re.search(r'SUM\((\w+)\)', cols).group(1)
        return [{'units': sum(r[col] for r in rows)}]
    if cols.startswith('COUNT('):
        return [{'c': len(rows)}]
    keys = [c.split()[-1].split('.')[-1] for c in cols.split(',')]
    return [{k: r.get(k) for k in keys} for r in rows]


class FakeConn:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def begin(self):
        # Row-level snapshot of tables this transaction actually writes to,
        # mirroring InnoDB: rollback never touches unrelated committed rows.
        self._snapshot = None
        self._written = set()

    def _ensure_snapshot(self, table):
        if self._snapshot is None:
            self._snapshot = {}
        if table not in self._snapshot and TABLES.get(table) and 'id' in TABLES[table][0]:
            self._snapshot[table] = {row['id']: copy.deepcopy(row) for row in TABLES[table]}
        self._written.add(table)

    def commit(self):
        self._snapshot = None

    def rollback(self):
        if not self._snapshot:
            return
        for name, before in self._snapshot.items():
            current = {row['id']: row for row in TABLES[name]}
            TABLES[name][:] = [copy.deepcopy(before[rid]) for rid in before]
            for rid, row in current.items():
                if rid not in before:
                    TABLES[name].append(row)
        self._snapshot = None

    def cursor(self):
        return FakeCursor(self)


pymysql.connect = lambda **cfg: FakeConn()


@contextmanager
def fake_transaction():
    cur = FakeCursor()
    yield cur


db.fetch_all = lambda sql, args=(): _select_from_conn(sql, args)
db.fetch_one = lambda sql, args=(): (_select_from_conn(sql, args) or [None])[0]
db.execute = lambda sql, args=(): FakeCursor().execute(sql, args) or None


def _select_from_conn(sql, args):
    cur = FakeCursor()
    cur.execute(sql, args)
    return cur.fetchall()


# --- fixtures -----------------------------------------------------------------
MANAGER = {'id': 1, 'role': 'manager', 'display_name': '周宁'}
OPERATOR = {'id': 3, 'role': 'store_operator', 'display_name': '陈杰'}
TODAY = date(2026, 11, 24)


def seed():
    reset_db()
    TABLES['users'] += [
        {'id': 1, 'email': 'm@x', 'display_name': '周宁', 'role': 'manager'},
        {'id': 3, 'email': 's@x', 'display_name': '陈杰', 'role': 'store_operator'},
    ]
    TABLES['stores'] += [
        {'id': 1, 'code': 'BJ-CY', 'name': '朝阳望京店', 'active': 1},
        {'id': 2, 'code': 'SH-XH', 'name': '徐汇滨江店', 'active': 1},
    ]
    TABLES['product_families'] += [
        {'id': 1, 'name': '咖啡与冲调', 'active': 1},
        {'id': 2, 'name': '乳品', 'active': 1},
        {'id': 3, 'name': '早餐烘焙', 'active': 1},
    ]
    TABLES['products'] += [
        {'id': 1, 'sku': 'CF-001', 'name': '咖啡豆', 'family_id': 1, 'price': 59, 'status': 'active'},
        {'id': 2, 'sku': 'ML-001', 'name': '牛奶', 'family_id': 2, 'price': 12.5, 'status': 'active'},
        {'id': 3, 'sku': 'BK-002', 'name': '早餐饼', 'family_id': 3, 'price': 22, 'status': 'active'},
    ]
    TABLES['association_rules'] = [
        {'id': 10, 'antecedent': '咖啡与冲调 + 乳品', 'consequent': '早餐烘焙',
         'support': 0.25, 'store_consistent': 1, 'status': 'published'},
    ]


def add_plan(plan_id, store_id, status):
    TABLES['promotion_plans'].append({
        'id': plan_id, 'rule_id': 10, 'store_id': store_id, 'product_id': 3,
        'operator_id': 3, 'created_by': 1, 'starts_on': TODAY, 'ends_on': TODAY,
        'list_price': 22, 'unit_discount': 3, 'unit_cap': 12, 'status': status,
    })


def receipt_payload(receipt_no):
    return SimpleNamespace(receipt_no=receipt_no, lines=[
        SimpleNamespace(sku='CF-001', quantity=2),
        SimpleNamespace(sku='ML-001', quantity=1),
        SimpleNamespace(sku='BK-002', quantity=3),
    ])


def enable_basket_terms(plan_id):
    TABLES['basket_offers'].append({'plan_id': plan_id, 'per_receipt_cap': 2, 'configured_by': 1})
    TABLES['basket_offer_requirements'] += [
        {'plan_id': plan_id, 'family_id': 1, 'minimum_units': 2},
        {'plan_id': plan_id, 'family_id': 2, 'minimum_units': 1},
    ]


def expect_409(label, fn):
    try:
        fn()
    except HTTPException as exc:
        assert exc.status_code == 409, f'{label}: expected 409 got {exc.status_code}'
        return exc.detail
    raise AssertionError(f'{label}: expected HTTPException')


PASS, FAIL = 0, 0


def check(label, condition):
    global PASS, FAIL
    assert condition, label
    PASS += 1
    print(f'  ✓ {label}')


# --- tests --------------------------------------------------------------------
def test_withdrawal_cancels_unopened_keeps_opened():
    print('撤回：未开档取消、已开档保留，其他门店不受影响')
    seed()
    add_plan(101, 1, Plan.planned)
    add_plan(102, 1, Plan.active)
    add_plan(103, 2, Plan.planned)
    result = rule_withdrawal.withdraw_rule(
        10, SimpleNamespace(store_id=1, reason='规则口径有误，经理按门店撤回'), MANAGER)
    check('返回取消的未开档档期', result['cancelled_plan_ids'] == [101])
    check('返回保留的已开档档期', result['kept_plan_ids'] == [102])
    by_id = {p['id']: p for p in TABLES['promotion_plans']}
    check('未开档档期已取消并让出时段', by_id[101]['status'] == Plan.cancelled)
    check('已开档档期仍可作废小票和对账', by_id[102]['status'] == Plan.active)
    check('其他门店档期照常', by_id[103]['status'] == Plan.planned)
    check('撤回留痕一条', len(TABLES['rule_store_withdrawals']) == 1)


def test_double_withdrawal_rejected():
    print('重复撤回被拒绝')
    seed()
    add_plan(201, 1, Plan.planned)
    rule_withdrawal.withdraw_rule(10, SimpleNamespace(store_id=1, reason='首次撤回'), MANAGER)
    detail = expect_409('重复撤回', lambda: rule_withdrawal.withdraw_rule(
        10, SimpleNamespace(store_id=1, reason='再次撤回'), MANAGER))
    check('提示不能重复撤回', '重复撤回' in detail)


def test_no_new_schedule_or_activation_after_withdrawal():
    print('撤回后该店不能再排期、不能开档')
    seed()
    add_plan(301, 1, Plan.planned)
    rule_withdrawal.withdraw_rule(10, SimpleNamespace(store_id=1, reason='撤回'), MANAGER)
    plan_in = SimpleNamespace(
        rule_id=10, store_id=1, product_id=3, operator_id=3,
        starts_on=TODAY, ends_on=TODAY, unit_discount=3, unit_cap=5)
    detail = expect_409('撤回门店排期', lambda: promotion.create_plan(plan_in, MANAGER))
    check('排期提示已撤回', '撤回' in detail)
    plan_in.store_id = 2
    created = promotion.create_plan(plan_in, MANAGER)
    check('其他门店仍可排期', created['status'] == Plan.planned)
    # 并发边界：档期已排期但撤回事务取消动作未覆盖时，开档仍被撤回开关拦下
    add_plan(902, 1, Plan.planned)
    detail = expect_409('撤回门店开档', lambda: promotion.activate_plan(902, OPERATOR))
    check('开档提示已撤回', '撤回' in detail)


def test_receipts_before_valid_after_refused_and_replay_idempotent():
    print('并发口径：撤回前的票有效，撤回后新票拒收，同票重试幂等回放')
    seed()
    add_plan(401, 1, Plan.active)
    enable_basket_terms(401)
    # 1) 撤回前办成的小票有效
    first = basket_receipts.record(401, receipt_payload('UI-CY-100'), OPERATOR)
    check('撤回前登记成功', first['replayed'] is False and first['rewarded_units'] == 2)
    # 2) 经理撤回（已开档档期保留）
    rule_withdrawal.withdraw_rule(10, SimpleNamespace(store_id=1, reason='规则有误'), MANAGER)
    # 3) 撤回后的新票拒收（quote 与 record 都拦）
    expect_409('撤回后核定新票', lambda: basket_receipts.preview(401, receipt_payload('UI-CY-101'), OPERATOR))
    detail = expect_409('撤回后登记新票', lambda: basket_receipts.record(401, receipt_payload('UI-CY-101'), OPERATOR))
    check('停收提示保留已登记小票权益', '停止登记新小票' in detail and '仍有效' in detail)
    # 4) 同票重试不重复核销，返回原结果
    replay = basket_receipts.record(401, receipt_payload('UI-CY-100'), OPERATOR)
    check('同票重试命中幂等回放', replay['replayed'] is True and replay['id'] == first['id'])
    check('没有产生第二条流水', len(TABLES['basket_receipts']) == 1)
    # 5) 已登记小票保留原立减资格
    stored = TABLES['basket_receipts'][0]
    check('原立减金额保留', float(stored['discount_amount']) == 6.0)


def test_void_still_allowed_and_returned_settlement_still_blocks():
    print('已开档可作废小票；对账退回后仍停收新票')
    seed()
    add_plan(501, 1, Plan.active)
    enable_basket_terms(501)
    booked = basket_receipts.record(501, receipt_payload('UI-CY-200'), OPERATOR)
    rule_withdrawal.withdraw_rule(10, SimpleNamespace(store_id=1, reason='规则有误'), MANAGER)
    # 已开档档期仍可作废小票
    basket_receipts.void(501, booked['id'], SimpleNamespace(reason='收银已撤销'), OPERATOR)
    row = next(r for r in TABLES['basket_receipts'] if r['id'] == booked['id'])
    check('撤回后小票仍可作废并保留说明', row['voided_at'] is not None and row['void_reason'] == '收银已撤销')
    # 模拟对账退回：计划回到 active，但停收开关依旧生效
    plan = next(p for p in TABLES['promotion_plans'] if p['id'] == 501)
    plan['status'] = Plan.settling
    plan['status'] = Plan.active
    expect_409('退回后新票仍拒收', lambda: basket_receipts.record(501, receipt_payload('UI-CY-201'), OPERATOR))
    check('已登记小票数据未被破坏', len(TABLES['basket_receipts']) == 1)


def test_other_store_receipt_flow_unaffected():
    print('其他门店小票登记完全不受影响')
    seed()
    add_plan(601, 1, Plan.active)
    add_plan(602, 2, Plan.active)
    enable_basket_terms(601)
    enable_basket_terms(602)
    rule_withdrawal.withdraw_rule(10, SimpleNamespace(store_id=1, reason='撤回朝阳店'), MANAGER)
    booked = basket_receipts.record(602, receipt_payload('UI-XH-300'), OPERATOR)
    check('徐汇店新票照常登记', booked['replayed'] is False and booked['rewarded_units'] == 2)
    expect_409('朝阳店新票拒收', lambda: basket_receipts.record(601, receipt_payload('UI-CY-301'), OPERATOR))


if __name__ == '__main__':
    for fn in [
        test_withdrawal_cancels_unopened_keeps_opened,
        test_double_withdrawal_rejected,
        test_no_new_schedule_or_activation_after_withdrawal,
        test_receipts_before_valid_after_refused_and_replay_idempotent,
        test_void_still_allowed_and_returned_settlement_still_blocks,
        test_other_store_receipt_flow_unaffected,
    ]:
        fn()
    print(f'\nALL {PASS} ASSERTIONS PASSED')
