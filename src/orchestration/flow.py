"""Prefect orchestration for the Olist lakehouse (Phase 4).

scripts/run_pipeline.py is the plain, dependency-free way to run the pipeline.
This module is the *orchestrated* path: the same stages wrapped as Prefect tasks
so you get **automatic retries, a task-level run graph, and a UI/scheduling story**
for free — the things you'd want the moment this runs unattended.

Why Prefect over Airflow (per the project plan): far less to stand up for a solo
batch job, Pythonic flows/tasks, and a good visual UI for a portfolio. Why not
just the plain runner: no retries, no per-task observability, no scheduling.

⚠️  ENVIRONMENT NOTE (a real, defensible decision):
    Prefect and dbt have *conflicting transitive pins* (on `pydantic` and
    `griffe`), so they cannot share one virtualenv without breaking each other.
    The professional pattern is to run the orchestrator in its **own isolated
    environment / container** that shells out to the pipeline — orchestrators
    routinely live apart from the tools they invoke for exactly this reason.
    This repo's main `.venv` therefore installs the *transformation* stack
    (dbt/deltalake/duckdb/streamlit); install Prefect separately to run this flow:

        python -m venv .venv-prefect
        .venv-prefect\\Scripts\\pip install prefect
        .venv-prefect\\Scripts\\python -m src.orchestration.flow

    See docs/DESIGN_DECISIONS.md (§ orchestration) for the full rationale.

Design notes:
  * Bronze and Silver are one task *per table* — so the Prefect graph shows each
    table, and a single table's transient failure retries without re-running the
    others.
  * Tasks are called directly (not ``.submit()``), which runs them sequentially
    in declaration order. That enforces the only ordering that matters here:
    all Bronze -> all Silver -> publish -> dbt.

Run it:
    python -m src.orchestration.flow
    # or serve/schedule it with Prefect:  prefect deployment ...
"""
from __future__ import annotations

try:
    from prefect import flow, task
except ModuleNotFoundError as e:  # pragma: no cover - clear guidance, not a crash
    raise ModuleNotFoundError(
        "Prefect is not installed in this environment by design (it conflicts "
        "with the dbt dependency stack). Run this flow from a separate venv with "
        "`pip install prefect`. See the module docstring and DESIGN_DECISIONS.md."
    ) from e

from config.schemas import all_tables
from src.common.dbt_runner import run_dbt
from src.common.metadata import init_metadata
from src.ingest import bronze
from src.publish.load_to_duckdb import publish_silver
from src.transform import silver


@task(retries=2, retry_delay_seconds=10, name="bronze-ingest")
def bronze_task(table: str) -> None:
    bronze.ingest_table(table)


@task(retries=1, retry_delay_seconds=10, name="silver-transform")
def silver_task(table: str) -> None:
    silver.transform_table(table)


@task(name="publish-to-duckdb")
def publish_task() -> None:
    publish_silver()


@task(retries=1, retry_delay_seconds=15, name="dbt-build")
def dbt_task() -> None:
    run_dbt("build")


@flow(name="olist-lakehouse")
def olist_pipeline(tables: list[str] | None = None) -> None:
    """Bronze -> Silver -> publish -> dbt, with per-table retries."""
    init_metadata()
    tables = tables or all_tables()

    for t in tables:
        bronze_task(t)
    for t in tables:
        silver_task(t)
    publish_task()
    dbt_task()


if __name__ == "__main__":
    olist_pipeline()
