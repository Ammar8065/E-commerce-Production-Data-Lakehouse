"""Generate small synthetic Olist-shaped CSVs into data/raw for local testing.

Why this exists: it lets you (and anyone reviewing the repo) run the full
pipeline end-to-end without Kaggle credentials, and — importantly — it plants
*deliberately broken* rows so the rejection / quarantine logic actually fires.
Swap in the real Kaggle download whenever you want; the schemas are identical.

The planted problems (so you can verify they land in _rejected):
  * orders:        one row with a null required field, one with a bad timestamp,
                   plus a LATE-ARRIVING order (purchased long ago).
  * order_items:   one row with a non-numeric price (uncastable required field).
  * customers:     a duplicate customer_id with conflicting data (PK conflict).
  * geolocation:   exact-duplicate rows (should be deduped, NOT quarantined).
"""
from __future__ import annotations

import csv

from config import settings


def _write(filename: str, header: list[str], rows: list[list[str]]) -> None:
    settings.ensure_dirs()
    path = settings.RAW_DIR / filename
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    print(f"wrote {len(rows):>3} rows -> {path}")


def main() -> None:
    # ── orders ────────────────────────────────────────────────────────────────
    _write(
        "olist_orders_dataset.csv",
        ["order_id", "customer_id", "order_status", "order_purchase_timestamp",
         "order_approved_at", "order_delivered_carrier_date",
         "order_delivered_customer_date", "order_estimated_delivery_date"],
        [
            ["o1", "c1", "delivered", "2017-10-02 10:56:33", "2017-10-02 11:07:15",
             "2017-10-04 19:55:00", "2017-10-10 21:25:13", "2017-10-18 00:00:00"],
            ["o2", "c2", "delivered", "2018-07-24 20:41:37", "2018-07-26 03:24:27",
             "2018-07-26 14:31:00", "2018-08-07 15:27:45", "2018-08-13 00:00:00"],
            # LATE-ARRIVING: purchased back in 2016, "landed" in this load.
            ["o3", "c3", "delivered", "2016-10-03 09:00:00", "2016-10-03 09:30:00",
             "2016-10-05 12:00:00", "2016-10-12 18:00:00", "2016-10-20 00:00:00"],
            # BAD: null required order_status -> should be rejected.
            ["o4", "c4", "", "2017-11-01 08:00:00", "", "", "", "2017-11-15 00:00:00"],
            # BAD: unparseable purchase timestamp -> rejected (required, uncastable).
            ["o5", "c5", "shipped", "not-a-date", "", "", "", "2017-12-01 00:00:00"],
            # ORPHAN: customer c99 does not exist in the customers table. Passes
            # Silver (customer_id is present) but has no dim_customers match, so
            # fct_orders flags is_orphaned_customer and the relationships test WARNs.
            ["o6", "c99", "delivered", "2018-01-05 14:00:00", "2018-01-05 15:00:00",
             "2018-01-07 10:00:00", "2018-01-12 16:00:00", "2018-01-20 00:00:00"],
        ],
    )

    # ── customers (o-prefixed ids reuse the same letters for joinability) ─────
    _write(
        "olist_customers_dataset.csv",
        ["customer_id", "customer_unique_id", "customer_zip_code_prefix",
         "customer_city", "customer_state"],
        [
            ["c1", "u1", "01001", "sao paulo", "SP"],
            ["c2", "u2", "20040", "rio de janeiro", "RJ"],
            ["c3", "u3", "30110", "belo horizonte", "MG"],
            ["c4", "u4", "40010", "salvador", "BA"],
            ["c5", "u5", "50030", "recife", "PE"],
            # PK CONFLICT: same customer_id c1, different city -> loser quarantined.
            ["c1", "u1", "01001", "campinas", "SP"],
        ],
    )

    # ── order_items ───────────────────────────────────────────────────────────
    _write(
        "olist_order_items_dataset.csv",
        ["order_id", "order_item_id", "product_id", "seller_id",
         "shipping_limit_date", "price", "freight_value"],
        [
            ["o1", "1", "p1", "s1", "2017-10-06 11:07:15", "58.90", "13.29"],
            ["o2", "1", "p2", "s2", "2018-07-30 03:24:27", "239.90", "19.93"],
            ["o3", "1", "p1", "s1", "2016-10-09 09:30:00", "49.00", "8.72"],
            # BAD: non-numeric price (required) -> rejected. Note: we use "free"
            # rather than "N/A"/"null", because pandas reads those as null on CSV
            # load, which would make this a *null* rejection instead of the
            # *uncastable* one we want to demonstrate here.
            ["o1", "2", "p3", "s3", "2017-10-06 11:07:15", "free", "5.00"],
            # item for the orphaned order o6, so the orphan carries real revenue.
            ["o6", "1", "p2", "s2", "2018-01-10 14:00:00", "120.00", "15.00"],
        ],
    )

    # ── order_payments ────────────────────────────────────────────────────────
    _write(
        "olist_order_payments_dataset.csv",
        ["order_id", "payment_sequential", "payment_type",
         "payment_installments", "payment_value"],
        [
            ["o1", "1", "credit_card", "2", "72.19"],
            ["o2", "1", "boleto", "1", "259.83"],
            ["o3", "1", "credit_card", "1", "57.72"],
        ],
    )

    # ── order_reviews ─────────────────────────────────────────────────────────
    _write(
        "olist_order_reviews_dataset.csv",
        ["review_id", "order_id", "review_score", "review_comment_title",
         "review_comment_message", "review_creation_date", "review_answer_timestamp"],
        [
            ["r1", "o1", "5", "", "Otimo", "2017-10-11 00:00:00", "2017-10-12 03:00:00"],
            ["r2", "o2", "4", "Bom", "Recomendo", "2018-08-08 00:00:00", "2018-08-09 10:00:00"],
            ["r3", "o3", "3", "", "", "2016-10-13 00:00:00", "2016-10-14 11:00:00"],
        ],
    )

    # ── products (note the source's misspelled *_lenght columns) ──────────────
    _write(
        "olist_products_dataset.csv",
        ["product_id", "product_category_name", "product_name_lenght",
         "product_description_lenght", "product_photos_qty", "product_weight_g",
         "product_length_cm", "product_height_cm", "product_width_cm"],
        [
            ["p1", "cama_mesa_banho", "40", "287", "1", "500", "20", "10", "15"],
            ["p2", "informatica_acessorios", "55", "1200", "3", "1800", "30", "20", "25"],
            # optional weight is garbage -> coerced to null, row KEPT (not rejected).
            ["p3", "telefonia", "33", "450", "2", "heavy", "12", "5", "8"],
        ],
    )

    # ── sellers ───────────────────────────────────────────────────────────────
    _write(
        "olist_sellers_dataset.csv",
        ["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"],
        [
            ["s1", "13023", "campinas", "SP"],
            ["s2", "01310", "sao paulo", "SP"],
            ["s3", "80010", "curitiba", "PR"],
        ],
    )

    # ── geolocation (with EXACT duplicates -> deduped, not rejected) ──────────
    _write(
        "olist_geolocation_dataset.csv",
        ["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng",
         "geolocation_city", "geolocation_state"],
        [
            ["01001", "-23.5505", "-46.6333", "sao paulo", "SP"],
            ["01001", "-23.5505", "-46.6333", "sao paulo", "SP"],   # exact dup
            ["20040", "-22.9068", "-43.1729", "rio de janeiro", "RJ"],
            ["30110", "-19.9167", "-43.9345", "belo horizonte", "MG"],
        ],
    )

    # ── category translation ──────────────────────────────────────────────────
    _write(
        "product_category_name_translation.csv",
        ["product_category_name", "product_category_name_english"],
        [
            ["cama_mesa_banho", "bed_bath_table"],
            ["informatica_acessorios", "computers_accessories"],
            ["telefonia", "telephony"],
        ],
    )


if __name__ == "__main__":
    main()
