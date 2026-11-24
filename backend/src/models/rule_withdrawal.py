"""Per-store withdrawal of a published association rule.

A withdrawal is scoped to one rule and one store: other stores keep executing
the rule. Unopened plans are cancelled inside the same transaction; opened
plans stay open for receipt voiding and reconciliation, while new receipt
booking is refused (including after a settlement is returned).
"""

RULE_WITHDRAWAL_SCHEMA = [
    """CREATE TABLE IF NOT EXISTS rule_store_withdrawals (
        id INT AUTO_INCREMENT PRIMARY KEY,
        rule_id INT NOT NULL,
        store_id INT NOT NULL,
        reason VARCHAR(240) NOT NULL,
        withdrawn_by INT NOT NULL,
        withdrawn_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (rule_id) REFERENCES association_rules(id),
        FOREIGN KEY (store_id) REFERENCES stores(id),
        FOREIGN KEY (withdrawn_by) REFERENCES users(id),
        UNIQUE KEY uq_rule_store_withdrawal (rule_id, store_id)
    )""",
]
