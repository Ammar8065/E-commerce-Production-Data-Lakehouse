"""Consistent logging across every pipeline module.

Why a tiny wrapper instead of ``print``: a real pipeline needs timestamps,
levels, and the module name on every line so that when a 2 a.m. run fails you
can read the log top-to-bottom and know *where* and *when* it broke. We keep it
stdlib-only (no extra dependency) and configure the root logger once.
"""
from __future__ import annotations

import logging
import sys

_CONFIGURED = False


def get_logger(name: str) -> logging.Logger:
    global _CONFIGURED
    if not _CONFIGURED:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter(
                fmt="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        root = logging.getLogger()
        root.setLevel(logging.INFO)
        root.addHandler(handler)
        _CONFIGURED = True
    return logging.getLogger(name)
