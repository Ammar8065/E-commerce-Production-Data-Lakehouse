"""Download the Olist dataset from Kaggle into ``data/raw``.

Authentication (pick one):
  * Put ``kaggle.json`` at ``%USERPROFILE%\\.kaggle\\kaggle.json`` (Kaggle ->
    Account -> Create New API Token downloads this file), OR
  * Set ``KAGGLE_USERNAME`` and ``KAGGLE_KEY`` in a local ``.env`` file.

Run:
    python -m src.download_data

Why a script and not a manual download: a pipeline that can (re)acquire its own
source data is reproducible. Anyone cloning the repo runs one command instead of
hunting for the right Kaggle page, and the exact dataset slug is pinned in config.
"""
from __future__ import annotations

import sys

from config import settings
from config.schemas import SCHEMAS
from src.common.logging_setup import get_logger

log = get_logger("download")


def download() -> None:
    settings.ensure_dirs()

    # Imported lazily: the kaggle package authenticates on import and will raise
    # immediately if creds are missing, which we want to catch with a clear message.
    try:
        from kaggle.api.kaggle_api_extended import KaggleApi
    except OSError as e:
        log.error("Kaggle auth failed on import: %s", e)
        _print_auth_help()
        sys.exit(1)

    api = KaggleApi()
    try:
        api.authenticate()
    except Exception as e:  # noqa: BLE001 — surface any auth problem clearly
        log.error("Kaggle authentication failed: %s", e)
        _print_auth_help()
        sys.exit(1)

    log.info("Downloading '%s' -> %s", settings.KAGGLE_DATASET, settings.RAW_DIR)
    api.dataset_download_files(
        settings.KAGGLE_DATASET,
        path=str(settings.RAW_DIR),
        unzip=True,
        quiet=False,
    )

    _verify_expected_files()


def _verify_expected_files() -> None:
    """Confirm every CSV the schema registry expects actually landed."""
    missing = []
    for schema in SCHEMAS.values():
        if not (settings.RAW_DIR / schema.source_file).exists():
            missing.append(schema.source_file)

    if missing:
        log.warning("Download finished but %d expected file(s) are missing:", len(missing))
        for f in missing:
            log.warning("  - %s", f)
        log.warning("Bronze ingestion will skip any table whose source file is absent.")
    else:
        log.info("All %d expected source files are present in data/raw.", len(SCHEMAS))


def _print_auth_help() -> None:
    log.error(
        "Set up Kaggle credentials, then re-run:\n"
        "  Option A: place kaggle.json at %%USERPROFILE%%\\.kaggle\\kaggle.json\n"
        "  Option B: add KAGGLE_USERNAME and KAGGLE_KEY to a .env file in the project root"
    )


if __name__ == "__main__":
    download()
