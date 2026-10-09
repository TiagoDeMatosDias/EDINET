"""Managed research-state database path."""

from __future__ import annotations

from pathlib import Path

from src.orchestrator.common.db_config import get_app_db

from .storage import ResearchStore

RESEARCH_DB_PATH = Path(get_app_db())
store = ResearchStore(RESEARCH_DB_PATH)
