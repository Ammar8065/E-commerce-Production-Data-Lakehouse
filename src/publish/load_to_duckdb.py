"""Publish step: load Silver Delta tables into the DuckDB lakehouse warehouse.

Why this step exists (an honest engineering decision):
    Our Silver layer is Delta Lake on disk — that is the lake / source of truth.
    The Gold layer is built with dbt, and dbt needs a SQL engine to transform in.
    The cleanest "pure lakehouse" path would be for dbt's engine (DuckDB) to read
    the Delta tables directly via the `delta` extension (`delta_scan`). In DuckDB
    1.0 that extension is still immature and failed to read our partitioned tables,
    so instead we *materialize* Silver into a DuckDB database (``lakehouse.duckdb``)
    and let dbt build Gold on top of plain DuckDB tables.

    This is a normal, legitimate lakehouse shape: **Delta = lake storage, DuckDB =
    query/serving engine.** In production you'd instead point dbt-spark/Databricks
    or `delta_scan` straight at the Delta tables and drop this copy step. The
    transformation logic in dbt does not change either way.

Idempotent: every table is written with CREATE OR REPLACE, so re-running just
refreshes the warehouse from the current Silver state.
"""
from __future__ import annotations

import duckdb
from deltalake import DeltaTable

from config import settings
from config.schemas import all_tables
from src.common.delta_io import table_exists
from src.common.logging_setup import get_logger

log = get_logger("publish")

SILVER_SCHEMA = "silver"


def publish_silver() -> None:
    """Materialize every existing Silver Delta table into lakehouse.duckdb."""
    settings.ensure_dirs()
    con = duckdb.connect(str(settings.LAKEHOUSE_DB))
    try:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}")
        published = 0
        for table in all_tables():
            spath = str(settings.silver_path(table))
            if not table_exists(spath):
                log.warning("skip '%s': no Silver table (run silver first)", table)
                continue
            df = DeltaTable(spath).to_pandas()  # noqa: F841 — used by DuckDB below
            con.execute(
                f"CREATE OR REPLACE TABLE {SILVER_SCHEMA}.{table} AS SELECT * FROM df"
            )
            log.info("published silver.%s (%d rows)", table, len(df))
            published += 1
        log.info("Publish complete: %d Silver table(s) -> %s",
                 published, settings.LAKEHOUSE_DB.name)
    finally:
        con.close()


if __name__ == "__main__":
    publish_silver()
