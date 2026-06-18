"""Business-rule (semantic) validation for the Silver layer.

The schema registry enforces *structural* validity — right types, required fields
present, unique keys. But data can be structurally perfect and still impossible:
an order marked ``delivered`` with no delivery date, a delivery dated *before* the
purchase, a negative price. Those are **semantic** violations, and they are
exactly the kind of issue real-world data (including the real Olist dataset) ships
with while passing every type check.

Each rule returns a boolean mask where ``True`` marks an **invalid** row. Silver
applies these after type casting; any row that trips a rule is quarantined to
``data/_rejected/`` with the rule's reason — never silently kept or dropped.

Policy note (defensible in interviews): we quarantine semantically inconsistent
rows for investigation rather than guessing a correction. Downstream, the item-
and payment-grain facts inner-join orders, so the children of a quarantined order
are consistently excluded from revenue rather than counted against an order we
could not trust.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import pandas as pd


@dataclass(frozen=True)
class BusinessRule:
    name: str
    reason: str
    # df -> boolean Series; True where the row VIOLATES the rule.
    invalid_mask: Callable[[pd.DataFrame], pd.Series]


# Rules are keyed by table. Each predicate guards its own nulls so that only
# genuine violations (not missing data) are flagged.
BUSINESS_RULES: dict[str, list[BusinessRule]] = {
    "orders": [
        BusinessRule(
            "delivered_without_delivery_date",
            "order_status is 'delivered' but order_delivered_customer_date is null",
            lambda df: (df["order_status"] == "delivered")
            & df["order_delivered_customer_date"].isna(),
        ),
        BusinessRule(
            "delivery_before_purchase",
            "order_delivered_customer_date precedes order_purchase_timestamp",
            lambda df: df["order_delivered_customer_date"].notna()
            & (df["order_delivered_customer_date"] < df["order_purchase_timestamp"]),
        ),
        BusinessRule(
            "estimated_before_purchase",
            "order_estimated_delivery_date precedes order_purchase_timestamp",
            lambda df: df["order_estimated_delivery_date"].notna()
            & (df["order_estimated_delivery_date"] < df["order_purchase_timestamp"]),
        ),
    ],
    "order_items": [
        BusinessRule(
            "non_positive_price",
            "price is not greater than 0",
            lambda df: df["price"].notna() & (df["price"] <= 0),
        ),
        BusinessRule(
            "negative_freight",
            "freight_value is negative",
            lambda df: df["freight_value"].notna() & (df["freight_value"] < 0),
        ),
    ],
    "order_payments": [
        BusinessRule(
            "negative_payment",
            "payment_value is negative",
            lambda df: df["payment_value"].notna() & (df["payment_value"] < 0),
        ),
    ],
}


def rules_for(table: str) -> list[BusinessRule]:
    return BUSINESS_RULES.get(table, [])
