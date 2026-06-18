"""Small Delta Lake helpers shared by the Bronze and Silver layers.

``deltalake`` moved ``DeltaTable.is_deltatable`` around between releases, so we
check for a table by looking for its transaction log (``_delta_log``) directory.
That is the on-disk marker of a Delta table and is stable across versions.
"""
from __future__ import annotations

from pathlib import Path


def table_exists(path: str | Path) -> bool:
    """True if ``path`` is an initialized Delta table (has a ``_delta_log``)."""
    return (Path(path) / "_delta_log").exists()
