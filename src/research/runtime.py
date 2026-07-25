"""Managed research-state database path."""

from __future__ import annotations

import os
from pathlib import Path

from src.orchestrator.common.db_config import get_research_db

from .storage import ResearchStore

RESEARCH_DB_PATH = Path(os.getenv("EDINET_RESEARCH_DB") or get_research_db()).expanduser()
store = ResearchStore(RESEARCH_DB_PATH)
