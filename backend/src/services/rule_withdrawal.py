"""Withdraw a published bundling rule for a single store.

The rule itself stays ``published`` for other stores. A ``rule_store_withdrawals``
row is the per-store stop switch: new scheduling, activation and receipt booking
for that rule/store pair are refused, while receipts already booked keep their
original discount and stay eligible for reconciliation.
"""

from fastapi import HTTPException
from pymysql.err import IntegrityError

from ..constants.enums import PromotionStatus
from ..db import fetch_all, transaction
from .promotion import audit, one


def store_withdrawn(cur, rule_id, store_id):
    """Locking read so concurrent withdrawal and receipt booking serialize."""
    return one(cur, """SELECT id, reason FROM rule_store_withdrawals
                       WHERE rule_id=%s AND store_id=%s FOR UPDATE""",
               (rule_id, store_id))


def withdraw_rule(rule_id, data, user):
    try:
        with transaction() as cur:
            rule = one(cur, 'SELECT id, status, antecedent, consequent FROM association_rules WHERE id=%s FOR UPDATE',
                       (rule_id,))
            if not rule:
                raise HTTPException(404, '规则不存在')
            if rule['status'] != 'published':
                raise HTTPException(409, '只有已发布的规则可以按门店撤回')
            store = one(cur, 'SELECT id, name, active FROM stores WHERE id=%s FOR UPDATE', (data.store_id,))
            if not store or not store['active']:
                raise HTTPException(422, '请选择启用门店')
            if store_withdrawn(cur, rule_id, data.store_id):
                raise HTTPException(409, '该门店已撤回这条规则，不能重复撤回')
            cur.execute('''INSERT INTO rule_store_withdrawals (rule_id, store_id, reason, withdrawn_by)
                           VALUES (%s, %s, %s, %s)''',
                        (rule_id, data.store_id, data.reason, user['id']))
            withdrawal_id = cur.lastrowid
            # Lock every plan of this rule/store so an in-flight activation or
            # receipt booking commits before (and therefore observes) the switch.
            cur.execute('''SELECT id, status FROM promotion_plans
                           WHERE rule_id=%s AND store_id=%s FOR UPDATE''',
                        (rule_id, data.store_id))
            plans = cur.fetchall()
            cancelled = [plan['id'] for plan in plans if plan['status'] == PromotionStatus.planned]
            if cancelled:
                placeholders = ','.join(['%s'] * len(cancelled))
                cur.execute(f'''UPDATE promotion_plans SET status=%s
                                WHERE id IN ({placeholders})''',
                            (PromotionStatus.cancelled, *cancelled))
                for plan_id in cancelled:
                    audit(cur, user, 'cancel_promotion', 'promotion_plan', plan_id,
                          f'规则 #{rule_id} 按门店撤回，未开档档期自动取消并让出原时段')
            kept = [plan['id'] for plan in plans if plan['status'] != PromotionStatus.planned]
            audit(cur, user, 'withdraw_rule_store', 'rule_store_withdrawal', withdrawal_id,
                  f"规则 #{rule_id}（{rule['antecedent']} → {rule['consequent']}）在 {store['name']} 撤回："
                  f"{data.reason}；取消未开档 {len(cancelled)} 个，已开档保留 {len(kept)} 个"[:500])
    except IntegrityError as exc:
        if exc.args[0] == 1062 and 'uq_rule_store_withdrawal' in str(exc):
            raise HTTPException(409, '该门店已撤回这条规则，不能重复撤回') from exc
        raise
    return {
        'id': withdrawal_id, 'rule_id': rule_id, 'store_id': data.store_id,
        'cancelled_plan_ids': cancelled, 'kept_plan_ids': kept,
    }


def list_withdrawals():
    return fetch_all("""
        SELECT w.id, w.rule_id, w.store_id, w.reason, w.withdrawn_at, w.withdrawn_by,
               r.antecedent, r.consequent, s.code store_code, s.name store_name,
               u.display_name withdrawn_by_name,
               SUM(p.status IN ('planned','active','settling','closed')) affected_plans,
               SUM(p.status='cancelled') cancelled_plans,
               SUM(p.status IN ('active','settling')) open_plans
        FROM rule_store_withdrawals w
        JOIN association_rules r ON r.id = w.rule_id
        JOIN stores s ON s.id = w.store_id
        JOIN users u ON u.id = w.withdrawn_by
        LEFT JOIN promotion_plans p ON p.rule_id = w.rule_id AND p.store_id = w.store_id
        GROUP BY w.id, w.rule_id, w.store_id, w.reason, w.withdrawn_at, w.withdrawn_by,
                 r.antecedent, r.consequent, s.code, s.name, u.display_name
        ORDER BY w.withdrawn_at DESC, w.id DESC
    """)


def withdrawal_detail(withdrawal_id):
    with transaction() as cur:
        cur.execute("""SELECT w.*, r.antecedent, r.consequent, s.code store_code, s.name store_name
                       FROM rule_store_withdrawals w
                       JOIN association_rules r ON r.id = w.rule_id
                       JOIN stores s ON s.id = w.store_id WHERE w.id=%s""", (withdrawal_id,))
        withdrawal = cur.fetchone()
        if not withdrawal:
            raise HTTPException(404, '撤回记录不存在')
        cur.execute("""SELECT p.id, p.status, p.starts_on, p.ends_on, p.unit_cap, p.unit_discount,
                              sku.sku, sku.name product_name, u.display_name operator_name
                       FROM promotion_plans p JOIN products sku ON sku.id=p.product_id
                       JOIN users u ON u.id=p.operator_id
                       WHERE p.rule_id=%s AND p.store_id=%s ORDER BY p.id""",
                      (withdrawal['rule_id'], withdrawal['store_id']))
        withdrawal['plans'] = cur.fetchall()
        return withdrawal
