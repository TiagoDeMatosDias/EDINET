"""Filesystem roots for runtime state and generated artifacts.

Every root defaults to its historical location inside the project and can be
redirected with an environment variable. Tests use the overrides to keep all
writes inside a temporary directory, away from operator-owned data.
"""

from __future__ import annotations

import os
from pathlib import Path

from src.paths import app_dir

PROJECT_ROOT = app_dir()


def _root(env_name: str, default: Path) -> Path:
    value = os.getenv(env_name, "").strip()
    return Path(value).expanduser().resolve(strict=False) if value else default


def state_dir() -> Path:
    """Mutable application state (saved screens, uploads, job workspaces)."""
    return _root("EDINET_STATE_DIR", PROJECT_ROOT / "config" / "state")


def backtest_root() -> Path:
    """Saved backtest result directories."""
    return _root("EDINET_BACKTEST_DIR", PROJECT_ROOT / "data" / "Backtests")


def report_root() -> Path:
    """Generated report archives."""
    return _root("EDINET_REPORT_DIR", PROJECT_ROOT / "data" / "reports")
