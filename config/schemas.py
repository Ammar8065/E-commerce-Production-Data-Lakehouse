"""Schema registry for the Olist dataset.

This is the contract every layer enforces:

* Bronze validates that the *expected columns exist* on read (fail loudly if the
  source drifts) but otherwise keeps data as-is, as strings, plus audit columns.
* Silver casts to ``dtypes``, applies ``rename``, enforces ``not_null`` and
  ``primary_key`` (dedup + reject), and parses ``date_columns``.

Keeping all of this declarative means adding a table or tightening a constraint
is a data change, not a code change — and it documents the warehouse in one file.
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class TableSchema:
    name: str                                  # logical table name (used for paths)
    source_file: str                           # CSV filename in data/raw
    dtypes: dict[str, str]                      # column -> pandas target dtype
    primary_key: list[str] = field(default_factory=list)
    not_null: list[str] = field(default_factory=list)
    date_columns: list[str] = field(default_factory=list)
    rename: dict[str, str] = field(default_factory=dict)   # raw name -> clean name
    partition_by: list[str] = field(default_factory=list)  # Silver partition cols

    @property
    def expected_columns(self) -> list[str]:
        """Columns the raw CSV must contain (the keys of ``dtypes``)."""
        return list(self.dtypes.keys())

    def silver_name(self, raw_col: str) -> str:
        return self.rename.get(raw_col, raw_col)


# IDs in Olist are 32-char hashes -> keep as strings, never numeric.
# Zip prefixes are stored as strings to preserve leading zeros.

SCHEMAS: dict[str, TableSchema] = {
    "orders": TableSchema(
        name="orders",
        source_file="olist_orders_dataset.csv",
        dtypes={
            "order_id": "string",
            "customer_id": "string",
            "order_status": "string",
            "order_purchase_timestamp": "datetime64[ns]",
            "order_approved_at": "datetime64[ns]",
            "order_delivered_carrier_date": "datetime64[ns]",
            "order_delivered_customer_date": "datetime64[ns]",
            "order_estimated_delivery_date": "datetime64[ns]",
        },
        primary_key=["order_id"],
        not_null=["order_id", "customer_id", "order_status",
                  "order_purchase_timestamp"],
        date_columns=[
            "order_purchase_timestamp", "order_approved_at",
            "order_delivered_carrier_date", "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ],
        partition_by=["order_purchase_year_month"],
    ),
    "customers": TableSchema(
        name="customers",
        source_file="olist_customers_dataset.csv",
        dtypes={
            "customer_id": "string",
            "customer_unique_id": "string",
            "customer_zip_code_prefix": "string",
            "customer_city": "string",
            "customer_state": "string",
        },
        primary_key=["customer_id"],
        not_null=["customer_id", "customer_unique_id"],
    ),
    "order_items": TableSchema(
        name="order_items",
        source_file="olist_order_items_dataset.csv",
        dtypes={
            "order_id": "string",
            "order_item_id": "Int64",
            "product_id": "string",
            "seller_id": "string",
            "shipping_limit_date": "datetime64[ns]",
            "price": "float64",
            "freight_value": "float64",
        },
        primary_key=["order_id", "order_item_id"],
        not_null=["order_id", "order_item_id", "product_id", "seller_id", "price"],
        date_columns=["shipping_limit_date"],
    ),
    "order_payments": TableSchema(
        name="order_payments",
        source_file="olist_order_payments_dataset.csv",
        dtypes={
            "order_id": "string",
            "payment_sequential": "Int64",
            "payment_type": "string",
            "payment_installments": "Int64",
            "payment_value": "float64",
        },
        primary_key=["order_id", "payment_sequential"],
        not_null=["order_id", "payment_type", "payment_value"],
    ),
    "order_reviews": TableSchema(
        name="order_reviews",
        source_file="olist_order_reviews_dataset.csv",
        dtypes={
            "review_id": "string",
            "order_id": "string",
            "review_score": "Int64",
            "review_comment_title": "string",
            "review_comment_message": "string",
            "review_creation_date": "datetime64[ns]",
            "review_answer_timestamp": "datetime64[ns]",
        },
        # review_id is NOT unique on its own in Olist; the grain is review+order.
        primary_key=["review_id", "order_id"],
        not_null=["review_id", "order_id", "review_score"],
        date_columns=["review_creation_date", "review_answer_timestamp"],
    ),
    "products": TableSchema(
        name="products",
        source_file="olist_products_dataset.csv",
        dtypes={
            "product_id": "string",
            "product_category_name": "string",
            "product_name_lenght": "Int64",          # sic: misspelled in source
            "product_description_lenght": "Int64",    # sic
            "product_photos_qty": "Int64",
            "product_weight_g": "float64",
            "product_length_cm": "float64",
            "product_height_cm": "float64",
            "product_width_cm": "float64",
        },
        primary_key=["product_id"],
        not_null=["product_id"],
        rename={
            "product_name_lenght": "product_name_length",
            "product_description_lenght": "product_description_length",
        },
    ),
    "sellers": TableSchema(
        name="sellers",
        source_file="olist_sellers_dataset.csv",
        dtypes={
            "seller_id": "string",
            "seller_zip_code_prefix": "string",
            "seller_city": "string",
            "seller_state": "string",
        },
        primary_key=["seller_id"],
        not_null=["seller_id"],
    ),
    "geolocation": TableSchema(
        name="geolocation",
        source_file="olist_geolocation_dataset.csv",
        dtypes={
            "geolocation_zip_code_prefix": "string",
            "geolocation_lat": "float64",
            "geolocation_lng": "float64",
            "geolocation_city": "string",
            "geolocation_state": "string",
        },
        # No natural key: many lat/lng rows share a zip prefix. Dedup on the
        # full row (handled in Silver via primary_key = all columns).
        primary_key=[
            "geolocation_zip_code_prefix", "geolocation_lat",
            "geolocation_lng", "geolocation_city", "geolocation_state",
        ],
        not_null=["geolocation_zip_code_prefix", "geolocation_lat",
                  "geolocation_lng"],
    ),
    "category_translation": TableSchema(
        name="category_translation",
        source_file="product_category_name_translation.csv",
        dtypes={
            "product_category_name": "string",
            "product_category_name_english": "string",
        },
        primary_key=["product_category_name"],
        not_null=["product_category_name", "product_category_name_english"],
    ),
}


def get_schema(table: str) -> TableSchema:
    try:
        return SCHEMAS[table]
    except KeyError:
        raise KeyError(
            f"Unknown table '{table}'. Known tables: {sorted(SCHEMAS)}"
        ) from None


def all_tables() -> list[str]:
    return list(SCHEMAS.keys())
