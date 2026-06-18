"""Phase 2 — Silver: clean, typed, deduplicated data you'd trust for analysis.

What Silver does (and *why*, because each step is a defensible decision):

  1. **Validate schema on read.** If Bronze is missing a column the schema
     expects, we raise instead of producing a half-correct table. Failing loudly
     beats silently shipping wrong numbers downstream.
  2. **Cast types.** Bronze is all strings; here we coerce to the real types.
     A value that *cannot* be cast is treated differently depending on whether
     the column is required (see step 4).
  3. **Drop exact duplicates** (identical ``_row_hash``). These come from
     re-ingesting the same file and are *expected*, not errors — so we remove
     them silently but count them. (Olist's geolocation table is full of these.)
  4. **Reject, don't drop.** Rows that violate the contract are written to a
     ``_rejected`` quarantine with a ``_reject_reason`` column, never discarded.
     A row is rejected when:
        - a *required* column holds a value that cannot be cast to its type, or
        - a *required* / primary-key column is null, or
        - it is a primary-key *conflict* (same key, different data — we keep the
          most recently ingested and quarantine the losers).
     Bad values in *optional* columns are coerced to null (and counted), not
     rejected — losing the whole row over a malformed optional field is too harsh.
  5. **Rename source typos** (e.g. ``product_name_lenght`` -> ``..._length``).
  6. **Full-refresh overwrite** of the Silver Delta table. Re-running Silver is
     idempotent — it always reproduces the same result from Bronze, so reruns
     never duplicate rows. This also transparently handles **late-arriving
     orders**: an order that landed today but was purchased months ago simply
     reappears in its correct historical month partition on the next refresh.
"""
from __future__ import annotations

import shutil
from datetime import datetime

import pandas as pd
from deltalake import DeltaTable, write_deltalake

from config import settings
from config.schemas import TableSchema, all_tables, get_schema
from src.common.delta_io import table_exists
from src.common.logging_setup import get_logger
from src.common.metadata import RunLogger
from src.transform.business_rules import rules_for

log = get_logger("silver")

PIPELINE = "silver"


def transform_table(table: str) -> None:
    """Clean one Bronze table into its Silver Delta table."""
    schema = get_schema(table)
    bpath = str(settings.bronze_path(table))

    if not table_exists(bpath):
        log.warning("skip '%s': no Bronze table at %s (run ingestion first)", table, bpath)
        return

    with RunLogger(PIPELINE, table) as run:
        bronze = DeltaTable(bpath).to_pandas()
        run.rows_in = len(bronze)
        _validate_columns(schema, list(bronze.columns))

        # 3) exact-duplicate removal (expected from re-ingestion; not a rejection)
        before = len(bronze)
        bronze = bronze.drop_duplicates(subset=["_row_hash"], keep="first")
        exact_dups = before - len(bronze)

        # 2 + 4) cast types and assign rejection reasons
        typed, reason = _cast_and_flag(bronze, schema)
        # 4a) semantic business rules (only on rows still structurally valid)
        reason = _apply_business_rules(typed, reason, table)
        valid = typed[reason.isna()].copy()
        rejected = typed[reason.notna()].copy()
        rejected["_reject_reason"] = reason[reason.notna()]

        # 4b) primary-key conflicts among survivors -> quarantine the losers
        valid, conflicts = _resolve_pk_conflicts(valid, schema)
        if not conflicts.empty:
            rejected = pd.concat([rejected, conflicts], ignore_index=False)

        # 5) rename source typo columns
        valid = valid.rename(columns=schema.rename)

        # 6) derive partition column(s) for the Silver table
        valid = _add_partition_columns(valid, schema)

        _write_silver(table, valid, schema)
        _write_rejected(table, rejected)

        run.rows_out = len(valid)
        run.rows_rejected = len(rejected)
        run.log_column_metrics(valid)

        log.info(
            "'%s': bronze=%d exact_dups=%d -> silver=%d rejected=%d",
            table, before, exact_dups, len(valid), len(rejected),
        )


def transform_all() -> None:
    """Clean every table. One failure does not stop the others."""
    settings.ensure_dirs()
    failures = []
    for table in all_tables():
        try:
            transform_table(table)
        except Exception as e:  # noqa: BLE001
            failures.append((table, str(e)))
            log.error("'%s' transform failed; continuing", table)
    if failures:
        log.error("Silver finished with %d failed table(s): %s",
                  len(failures), ", ".join(t for t, _ in failures))
    else:
        log.info("Silver transform complete for all tables.")


# ── internals ────────────────────────────────────────────────────────────────
def _validate_columns(schema: TableSchema, present: list[str]) -> None:
    missing = [c for c in schema.expected_columns if c not in set(present)]
    if missing:
        raise ValueError(
            f"'{schema.name}': Bronze is missing expected column(s) {missing}. "
            "Refusing to build Silver from an incomplete source."
        )


def _cast_series(series: pd.Series, dtype: str) -> pd.Series:
    """Coerce a string column to its target type; un-castable values become null."""
    if dtype.startswith("datetime"):
        return pd.to_datetime(series, errors="coerce")
    if dtype == "Int64":
        return pd.to_numeric(series, errors="coerce").astype("Int64")
    if dtype == "float64":
        return pd.to_numeric(series, errors="coerce").astype("float64")
    if dtype == "string":
        return series.astype("string")
    return series.astype(dtype)


def _cast_and_flag(df: pd.DataFrame, schema: TableSchema) -> tuple[pd.DataFrame, pd.Series]:
    """Return (typed_df, reason_series). reason is <NA> for rows that pass."""
    out = df.copy()
    reason = pd.Series(pd.NA, index=out.index, dtype="string")
    required = set(schema.not_null) | set(schema.primary_key)

    for col, dtype in schema.dtypes.items():
        raw = out[col]
        casted = _cast_series(raw, dtype)
        out[col] = casted
        if col in required:
            # had a value but it didn't survive casting -> malformed required field
            bad = reason.isna() & raw.notna() & casted.isna()
            reason[bad] = f"uncastable value in required column '{col}'"
        else:
            lost = int((raw.notna() & casted.isna()).sum())
            if lost:
                log.warning("'%s.%s': %d optional value(s) coerced to null (bad format)",
                            schema.name, col, lost)

    # genuine nulls in required / primary-key columns
    for col in required:
        bad = reason.isna() & out[col].isna()
        reason[bad] = f"null in required column '{col}'"

    return out, reason


def _apply_business_rules(df: pd.DataFrame, reason: pd.Series, table: str) -> pd.Series:
    """Flag semantically-invalid rows (impossible/inconsistent) for quarantine.

    Runs only on rows that are still valid (reason is <NA>); the first rule a row
    trips wins, so the reason points at the most specific problem found.
    """
    for rule in rules_for(table):
        violates = rule.invalid_mask(df).fillna(False)
        bad = reason.isna() & violates
        n = int(bad.sum())
        if n:
            reason[bad] = f"business rule [{rule.name}]: {rule.reason}"
            log.warning("'%s': %d row(s) violate business rule '%s'", table, n, rule.name)
    return reason


def _resolve_pk_conflicts(valid: pd.DataFrame, schema: TableSchema) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Keep the most recently ingested row per primary key; quarantine the rest.

    Exact duplicates are already gone, so anything caught here is a *real*
    conflict: the same key with differing data. We keep the latest by
    ``_ingested_at`` (last writer wins) and quarantine the older versions so the
    decision is auditable rather than silent.
    """
    if not schema.primary_key:
        return valid, valid.iloc[0:0].assign(_reject_reason=pd.Series(dtype="string"))

    ordered = valid.sort_values("_ingested_at")  # ascending: keep='last' = newest
    dup_mask = ordered.duplicated(subset=schema.primary_key, keep="last")
    conflicts = ordered[dup_mask].copy()
    if not conflicts.empty:
        conflicts["_reject_reason"] = "primary-key conflict (kept latest by _ingested_at)"
    kept = ordered[~dup_mask]
    return kept, conflicts


def _add_partition_columns(df: pd.DataFrame, schema: TableSchema) -> pd.DataFrame:
    """Derive any columns named in ``schema.partition_by`` that aren't real fields."""
    if "order_purchase_year_month" in schema.partition_by:
        # YYYY-MM bucket; survivors always have a valid purchase timestamp.
        df = df.copy()
        df["order_purchase_year_month"] = (
            df["order_purchase_timestamp"].dt.strftime("%Y-%m").astype("string")
        )
    return df


def _write_silver(table: str, df: pd.DataFrame, schema: TableSchema) -> None:
    """Full-refresh overwrite -> idempotent. Partitioned where the schema says so."""
    path = str(settings.silver_path(table))
    partition_by = schema.partition_by or None
    write_deltalake(
        path, df, mode="overwrite",
        partition_by=partition_by,
        schema_mode="overwrite",  # allow the typed schema to replace prior runs'
    )
    log.info("'%s': wrote %d rows to Silver%s", table, len(df),
             f" (partitioned by {partition_by})" if partition_by else "")


def _write_rejected(table: str, rejected: pd.DataFrame) -> None:
    """Overwrite the table's quarantine with this run's rejects (idempotent).

    Parquet, not Delta: the reject log is a simple debugging artifact, and a
    single overwritten file always reflects the latest run rather than piling up.
    """
    dest = settings.rejected_path(table, layer="silver")
    if rejected.empty:
        if dest.exists():
            shutil.rmtree(dest)  # last run had rejects, this one is clean
        return
    dest.mkdir(parents=True, exist_ok=True)
    out = rejected.copy()
    out["_rejected_at"] = datetime.now()
    out.to_parquet(dest / "rejected.parquet", index=False)
    log.warning("'%s': quarantined %d row(s) -> %s", table, len(out), dest)


if __name__ == "__main__":
    transform_all()
