"""Managed paths for the filing archive and rebuildable catalog."""

from __future__ import annotations

import os
from pathlib import Path

from src.orchestrator.common.db_config import get_filings_db

from .catalog import FilingCatalog

PROJECT_ROOT = Path(__file__).resolve().parents[2]
FILINGS_DB_PATH = Path(os.getenv("EDINET_FILINGS_DB") or get_filings_db()).expanduser()
ARCHIVE_ROOT = Path(
    os.getenv("EDINET_FILINGS_ARCHIVE", str(PROJECT_ROOT / "data" / "filings" / "archive"))
).expanduser()
catalog = FilingCatalog(FILINGS_DB_PATH)
