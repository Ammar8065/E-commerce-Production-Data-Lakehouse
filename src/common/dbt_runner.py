"""Run dbt in-process via its official ``dbtRunner`` API.

Factored out so both the plain CLI runner (scripts/run_pipeline.py) and the
Prefect flow can build the Gold layer the same way. We point dbt at the shared
lakehouse DuckDB file by exporting LAKEHOUSE_DB (read by profiles.yml).
"""
from __future__ import annotations

import os

from config import settings
from src.common.logging_setup import get_logger

log = get_logger("dbt")

DBT_DIR = str(settings.PROJECT_ROOT / "dbt")


def run_dbt(command: str = "build") -> None:
    from dbt.cli.main import dbtRunner  # heavy import; keep it lazy

    os.environ["LAKEHOUSE_DB"] = str(settings.LAKEHOUSE_DB.resolve())
    log.info("running `dbt %s` (project=%s)", command, DBT_DIR)
    res = dbtRunner().invoke(
        [command, "--project-dir", DBT_DIR, "--profiles-dir", DBT_DIR]
    )
    if not res.success:
        raise RuntimeError(f"dbt {command} failed: {res.exception or 'see logs above'}")
