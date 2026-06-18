"""Central path + environment configuration.

Everything that touches the filesystem imports from here so that the layout is
defined in exactly one place. Paths are resolved relative to the project root
(the parent of this file's parent), which keeps the pipeline runnable from any
working directory.
"""
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

# Load a local .env if present (Kaggle creds, overrides). Never committed.
load_dotenv()

# ── Roots ────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[1]

DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"              # Kaggle CSVs land here untouched
BRONZE_DIR = DATA_DIR / "bronze"        # raw -> Delta, audit columns added
SILVER_DIR = DATA_DIR / "silver"        # typed, cleaned, deduplicated Delta
REJECTED_DIR = DATA_DIR / "_rejected"   # quarantined rows + reason

WAREHOUSE_DIR = PROJECT_ROOT / "warehouse"
METADATA_DB = WAREHOUSE_DIR / "metadata.duckdb"   # pipeline_runs, column_metrics
LAKEHOUSE_DB = WAREHOUSE_DIR / "lakehouse.duckdb"  # Silver published here; dbt builds Gold

# ── Dataset ──────────────────────────────────────────────────────────────────
# Olist Brazilian e-commerce public dataset.
KAGGLE_DATASET = os.getenv("KAGGLE_DATASET", "olistbr/brazilian-ecommerce")


def ensure_dirs() -> None:
    """Create every directory the pipeline writes to. Idempotent."""
    for d in (RAW_DIR, BRONZE_DIR, SILVER_DIR, REJECTED_DIR, WAREHOUSE_DIR):
        d.mkdir(parents=True, exist_ok=True)


def bronze_path(table: str) -> Path:
    return BRONZE_DIR / table


def silver_path(table: str) -> Path:
    return SILVER_DIR / table


def rejected_path(table: str, layer: str) -> Path:
    """Quarantine location for a table, namespaced by the layer that rejected it."""
    return REJECTED_DIR / layer / table
