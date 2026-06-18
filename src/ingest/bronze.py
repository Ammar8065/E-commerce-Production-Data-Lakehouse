"""Phase 1 — Bronze ingestion: CSV -> Delta, faithfully, with an audit trail.

Design rules for Bronze (deliberately conservative):

  * **Keep raw data raw.** Every value is read as a *string*; we do no type
    casting, no null handling, no dedup here. Bronze is the immutable landing
    zone — if Silver's cleaning logic has a bug, we can re-derive everything from
    Bronze without re-downloading. Typing too early throws away information
    (e.g. a malformed number silently becomes NaN and you can't tell why).
  * **Add an audit trail, never mutate.** Four columns are appended so every row
    can be traced back to where/when it came from:
        _ingested_at  — wall-clock time of this ingestion
        _source_file  — which CSV the row came from
        _row_hash     — md5 of the raw values, for change detection / dedup
        _ingest_date  — partition key (date the row landed)
  * **Partition by ingest date** so a day's load is a self-contained unit.
  * **Idempotent per source file.** Re-ingesting a file deletes its previous
    Bronze rows first, then appends — re-running never duplicates data.

Edge cases handled (all required by the plan):
  * source file missing      -> skip table with a warning (no failed run)
  * source file empty        -> record a 0-row run, write nothing
  * expected column missing  -> raise (fail loudly); run recorded as 'failed'
"""
from __future__ import annotations

import hashlib
from datetime import datetime

import pandas as pd
from deltalake import DeltaTable, write_deltalake

from config import settings
from config.schemas import TableSchema, all_tables, get_schema
from src.common.delta_io import table_exists
from src.common.logging_setup import get_logger
from src.common.metadata import RunLogger

log = get_logger("bronze")

PIPELINE = "bronze"


def ingest_table(table: str) -> None:
    """Ingest one table's CSV into its Bronze Delta table."""
    schema = get_schema(table)
    src = settings.RAW_DIR / schema.source_file

    if not src.exists():
        log.warning("skip '%s': source file not found at %s", table, src)
        return

    with RunLogger(PIPELINE, table, schema.source_file) as run:
        # Read everything as string — Bronze does not interpret types.
        try:
            raw = pd.read_csv(src, dtype="string", keep_default_na=True)
        except pd.errors.EmptyDataError:
            log.warning("'%s': source file is empty (no header/rows); nothing ingested", table)
            run.rows_in = 0
            run.rows_out = 0
            return

        run.rows_in = len(raw)
        _validate_columns(schema, list(raw.columns))  # fail loudly on drift

        if raw.empty:
            log.warning("'%s': header present but 0 data rows; nothing written", table)
            run.rows_out = 0
            run.log_column_metrics(raw)
            return

        enriched = _add_audit_columns(raw, schema.source_file)
        _write_bronze(table, enriched, schema.source_file)

        run.rows_out = len(enriched)
        run.log_column_metrics(raw)  # null rates on the *raw* columns only


def ingest_all() -> None:
    """Ingest every table in the schema registry. One failure does not stop the rest."""
    settings.ensure_dirs()
    failures = []
    for table in all_tables():
        try:
            ingest_table(table)
        except Exception as e:  # noqa: BLE001 — keep ingesting other tables
            failures.append((table, str(e)))
            log.error("'%s' ingestion failed; continuing with remaining tables", table)
    if failures:
        log.error("Bronze finished with %d failed table(s): %s",
                  len(failures), ", ".join(t for t, _ in failures))
    else:
        log.info("Bronze ingestion complete for all tables.")


# ── internals ────────────────────────────────────────────────────────────────
def _validate_columns(schema: TableSchema, present: list[str]) -> None:
    """Fail loudly if any expected column is absent; warn on unexpected extras."""
    present_set = set(present)
    missing = [c for c in schema.expected_columns if c not in present_set]
    if missing:
        raise ValueError(
            f"Schema drift in '{schema.name}': missing expected column(s) {missing}. "
            f"Got columns: {present}"
        )
    extra = [c for c in present if c not in schema.dtypes]
    if extra:
        log.warning("'%s': unexpected extra column(s) kept in Bronze: %s",
                    schema.name, extra)


def _add_audit_columns(raw: pd.DataFrame, source_file: str) -> pd.DataFrame:
    now = datetime.now()
    df = raw.copy()
    df["_row_hash"] = _hash_rows(raw)
    df["_ingested_at"] = now
    df["_source_file"] = pd.array([source_file] * len(df), dtype="string")
    df["_ingest_date"] = pd.array([now.strftime("%Y-%m-%d")] * len(df), dtype="string")
    return df


def _hash_rows(df: pd.DataFrame) -> pd.Series:
    """md5 over the raw values of each row (vectorized concat, then hash).

    Lets Silver detect exact-duplicate rows and lets us tell whether a re-ingested
    file actually changed. Unit separator (\\x1f) avoids accidental collisions
    between e.g. ('a','bc') and ('ab','c').
    """
    filled = [df[c].fillna("") for c in df.columns]
    joined = filled[0].str.cat(filled[1:], sep="\x1f")
    return joined.map(lambda s: hashlib.md5(s.encode("utf-8")).hexdigest())


def _write_bronze(table: str, df: pd.DataFrame, source_file: str) -> None:
    """Write to Delta, idempotently per source file (delete-then-append)."""
    path = str(settings.bronze_path(table))
    safe_file = source_file.replace("'", "''")

    if table_exists(path):
        # Remove any prior rows from this exact source file, then append fresh.
        DeltaTable(path).delete(predicate=f"_source_file = '{safe_file}'")
        write_deltalake(path, df, mode="append")
        log.info("'%s': appended %d rows to existing Bronze table", table, len(df))
    else:
        write_deltalake(path, df, mode="overwrite", partition_by=["_ingest_date"])
        log.info("'%s': created Bronze table with %d rows", table, len(df))


if __name__ == "__main__":
    ingest_all()
