"""Pipeline entry point.

Phase 4 will wrap these stages in Prefect for scheduling/retries/UI. Until then
this is a plain, explicit orchestrator — the right amount of machinery for a
batch job you trigger by hand, and it keeps the stages obvious.

Stages, in order:
    download -> bronze -> silver -> publish -> dbt        (`all` runs bronze..dbt)

Usage (from the project root, with the venv active):
    python -m scripts.run_pipeline all                 # bronze + silver + publish + dbt
    python -m scripts.run_pipeline bronze              # ingest only
    python -m scripts.run_pipeline silver              # clean only
    python -m scripts.run_pipeline publish             # Silver Delta -> lakehouse.duckdb
    python -m scripts.run_pipeline dbt                 # build Gold marts + run tests
    python -m scripts.run_pipeline download            # fetch from Kaggle first

    python -m scripts.run_pipeline bronze --table orders   # restrict to one table
"""
from __future__ import annotations

import argparse

from config import settings
from src.common.dbt_runner import run_dbt
from src.common.logging_setup import get_logger
from src.common.metadata import init_metadata
from src.ingest import bronze
from src.publish.load_to_duckdb import publish_silver
from src.transform import silver

log = get_logger("runner")


def main() -> None:
    parser = argparse.ArgumentParser(description="Olist lakehouse pipeline runner")
    parser.add_argument(
        "stage",
        choices=["download", "bronze", "silver", "publish", "dbt", "all"],
        help="which stage to run",
    )
    parser.add_argument(
        "--table",
        help="restrict bronze/silver to a single table (default: all tables)",
        default=None,
    )
    args = parser.parse_args()

    settings.ensure_dirs()
    init_metadata()

    if args.stage == "download":
        from src.download_data import download
        download()
        return

    if args.stage in ("bronze", "all"):
        log.info("=== STAGE: Bronze ingestion ===")
        bronze.ingest_table(args.table) if args.table else bronze.ingest_all()

    if args.stage in ("silver", "all"):
        log.info("=== STAGE: Silver transform ===")
        silver.transform_table(args.table) if args.table else silver.transform_all()

    if args.stage in ("publish", "all"):
        log.info("=== STAGE: Publish Silver -> DuckDB ===")
        publish_silver()

    if args.stage in ("dbt", "all"):
        log.info("=== STAGE: dbt build (Gold marts + tests) ===")
        run_dbt("build")

    log.info("Done. Metadata: %s | Lakehouse: %s",
             settings.METADATA_DB.name, settings.LAKEHOUSE_DB.name)


if __name__ == "__main__":
    main()
