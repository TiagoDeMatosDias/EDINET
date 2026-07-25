"""Managed paths for the rebuildable filing catalog (database-only storage)."""

from __future__ import annotations

import os
from pathlib import Path

from src.orchestrator.common.db_config import get_filings_db

from .catalog import FilingCatalog

FILINGS_DB_PATH = Path(os.getenv("EDINET_FILINGS_DB") or get_filings_db()).expanduser()
catalog = FilingCatalog(FILINGS_DB_PATH)
