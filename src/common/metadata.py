"""Pipeline run observability — the metadata store.

Every run of every layer writes one row to ``pipeline_runs`` and (optionally) a
set of per-column null-rate rows to ``column_metrics``. This is the thing the
plan calls out as the differentiator: we treat *operational* data about the
pipeline as a first-class product, not an afterthought buried in log files.

Why DuckDB for this (and not Postgres / a CSV / the Delta tables themselves):
  * It is an embedded, single-file OLAP database — zero server to run, perfect
    for a local lakehouse, and it speaks real SQL so the Streamlit health page
    in Phase 4 can just query it.
  * It is the same engine we use to process the data, so there is one fewer
    moving part to learn and operate.

Usage (context-manager guarantees a row is always written, success OR failure):

    with RunLogger("bronze", "orders", source_file="olist_orders_dataset.csv") as run:
        df = ...                       # do the work
        run.rows_in = len(raw)
        run.rows_out = len(df)
        run.rows_rejected = 0
        run.log_column_metrics(df)     # optional

If the body raises, the run is recorded as ``failed`` with the traceback, then
the exception is re-raised so the orchestrator still sees the failure.
"""
from __future__ import annotations

import traceback
import uuid
from datetime import datetime
from types import TracebackType

import duckdb
import pandas as pd

from config import settings
from src.common.logging_setup import get_logger

log = get_logger("metadata")

_RUNS_DDL = """
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id           VARCHAR PRIMARY KEY,
    pipeline         VARCHAR,        -- bronze | silver | ...
    table_name       VARCHAR,
    source_file      VARCHAR,
    status           VARCHAR,        -- running | success | failed
    started_at       TIMESTAMP,
    ended_at         TIMESTAMP,
    duration_sec     DOUBLE,
    rows_in          BIGINT,         -- rows read from the upstream layer
    rows_out         BIGINT,         -- rows written to this layer
    rows_rejected    BIGINT,         -- rows quarantined this run
    row_count_delta  BIGINT,         -- rows_out minus previous successful run
    error_message    VARCHAR,
    created_at       TIMESTAMP
);
"""

_METRICS_DDL = """
CREATE TABLE IF NOT EXISTS column_metrics (
    run_id      VARCHAR,
    pipeline    VARCHAR,
    table_name  VARCHAR,
    column_name VARCHAR,
    total_rows  BIGINT,
    null_count  BIGINT,
    null_rate   DOUBLE,
    created_at  TIMESTAMP
);
"""


def _connect() -> duckdb.DuckDBPyConnection:
    settings.ensure_dirs()
    return duckdb.connect(str(settings.METADATA_DB))


def init_metadata() -> None:
    """Create the metadata tables if they do not exist. Safe to call repeatedly."""
    with _connect() as con:
        con.execute(_RUNS_DDL)
        con.execute(_METRICS_DDL)


class RunLogger:
    """Context manager that records exactly one ``pipeline_runs`` row per run."""

    def __init__(self, pipeline: str, table_name: str, source_file: str | None = None):
        self.run_id = str(uuid.uuid4())
        self.pipeline = pipeline
        self.table_name = table_name
        self.source_file = source_file

        self.started_at: datetime | None = None
        self.rows_in: int | None = None
        self.rows_out: int | None = None
        self.rows_rejected: int = 0

        self._metrics: pd.DataFrame | None = None

    # -- lifecycle -----------------------------------------------------------
    def __enter__(self) -> "RunLogger":
        init_metadata()
        self.started_at = datetime.now()
        with _connect() as con:
            con.execute(
                "INSERT INTO pipeline_runs (run_id, pipeline, table_name, "
                "source_file, status, started_at, created_at) VALUES "
                "(?, ?, ?, ?, 'running', ?, ?)",
                [self.run_id, self.pipeline, self.table_name, self.source_file,
                 self.started_at, datetime.now()],
            )
        log.info("[%s/%s] run started (run_id=%s)",
                 self.pipeline, self.table_name, self.run_id[:8])
        return self

    def __exit__(self, exc_type, exc: BaseException | None,
                 tb: TracebackType | None) -> bool:
        ended_at = datetime.now()
        duration = (ended_at - self.started_at).total_seconds() if self.started_at else None

        if exc_type is not None:
            status = "failed"
            error_message = "".join(traceback.format_exception(exc_type, exc, tb))[:4000]
            log.error("[%s/%s] run FAILED: %s", self.pipeline, self.table_name, exc)
        else:
            status = "success"
            error_message = None

        delta = self._row_count_delta()
        with _connect() as con:
            con.execute(
                "UPDATE pipeline_runs SET status=?, ended_at=?, duration_sec=?, "
                "rows_in=?, rows_out=?, rows_rejected=?, row_count_delta=?, "
                "error_message=? WHERE run_id=?",
                [status, ended_at, duration, self.rows_in, self.rows_out,
                 self.rows_rejected, delta, error_message, self.run_id],
            )
            self._flush_metrics(con)

        if status == "success":
            log.info(
                "[%s/%s] run OK in %.2fs | in=%s out=%s rejected=%s delta=%s",
                self.pipeline, self.table_name, duration or 0.0,
                self.rows_in, self.rows_out, self.rows_rejected, delta,
            )
        # Return False -> do not suppress the exception; the orchestrator must see it.
        return False

    # -- metrics -------------------------------------------------------------
    def log_column_metrics(self, df: pd.DataFrame) -> None:
        """Capture null counts/rates for every column in ``df`` for this run.

        A sudden jump in a column's null rate is one of the earliest signals
        that an upstream source changed; storing it per run makes that trend
        queryable instead of invisible.
        """
        total = len(df)
        rows = []
        now = datetime.now()
        for col in df.columns:
            null_count = int(df[col].isna().sum())
            rows.append({
                "run_id": self.run_id,
                "pipeline": self.pipeline,
                "table_name": self.table_name,
                "column_name": col,
                "total_rows": total,
                "null_count": null_count,
                "null_rate": (null_count / total) if total else 0.0,
                "created_at": now,
            })
        self._metrics = pd.DataFrame(rows)

    def _flush_metrics(self, con: duckdb.DuckDBPyConnection) -> None:
        if self._metrics is None or self._metrics.empty:
            return
        metrics = self._metrics  # noqa: F841 — referenced by name in DuckDB SQL
        con.execute(
            "INSERT INTO column_metrics SELECT run_id, pipeline, table_name, "
            "column_name, total_rows, null_count, null_rate, created_at FROM metrics"
        )

    # -- helpers -------------------------------------------------------------
    def _row_count_delta(self) -> int | None:
        """rows_out of this run minus rows_out of the previous *successful* run."""
        if self.rows_out is None:
            return None
        with _connect() as con:
            prev = con.execute(
                "SELECT rows_out FROM pipeline_runs WHERE pipeline=? AND "
                "table_name=? AND status='success' AND run_id != ? AND "
                "rows_out IS NOT NULL ORDER BY started_at DESC LIMIT 1",
                [self.pipeline, self.table_name, self.run_id],
            ).fetchone()
        if not prev:
            return None
        return int(self.rows_out) - int(prev[0])
