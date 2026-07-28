"""Startup initialization for every database used by the application."""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

from src.orchestrator.common.db_config import (
    get_auth_db,
    get_db1,
    get_db2,
    get_db3,
    get_filings_db,
    get_pipeline_jobs_db,
    get_research_db,
)
from src.orchestrator.common.sqlite import (
    DEFAULT_BUSY_TIMEOUT_MS,
    connect_write,
    initialize_managed_database,
)

logger = logging.getLogger(__name__)


def _path(value: str | Path) -> Path:
    """Normalize a configured database path without requiring it to exist."""
    return Path(value).expanduser().resolve(strict=False)


def _touch_sqlite_database(path: str | Path, *, busy_timeout_ms: int) -> Path:
    """Create an empty SQLite database and apply the common journal policy."""
    normalized = _path(path)
    normalized.parent.mkdir(parents=True, exist_ok=True)
    conn = connect_write(normalized, busy_timeout_ms=busy_timeout_ms)
    try:
        initialize_managed_database(conn)
        conn.commit()
    finally:
        conn.close()
    return normalized


def _configured_path(
    explicit: str | Path | None,
    environment_name: str | None,
    fallback: str,
) -> Path:
    if explicit is not None:
        return _path(explicit)
    if environment_name:
        configured = os.getenv(environment_name)
        if configured:
            return _path(configured)
    return _path(fallback)


def ensure_application_databases(
    *,
    settings: Any | None = None,
    db1_path: str | Path | None = None,
    db2_path: str | Path | None = None,
    db3_path: str | Path | None = None,
    auth_db_path: str | Path | None = None,
    research_db_path: str | Path | None = None,
    jobs_db_path: str | Path | None = None,
    filings_db_path: str | Path | None = None,
    busy_timeout_ms: int | None = None,
) -> dict[str, Path]:
    """Ensure all configured application databases and schemas exist.

    DB1 (``Base.db``) and DB2 (``Standardized.db``) intentionally receive no
    fixed schema because their tables are created by the pipeline according to
    the imported source data.  They are still created here so a clean install
    has the complete database layout before the first pipeline run.
    """
    effective_busy_timeout = int(
        busy_timeout_ms
        if busy_timeout_ms is not None
        else getattr(settings, "sqlite_busy_timeout_ms", DEFAULT_BUSY_TIMEOUT_MS)
    )

    paths = {
        "db1": _configured_path(db1_path, None, get_db1()),
        "db2": _configured_path(db2_path, None, get_db2()),
        "db3": _configured_path(db3_path, None, get_db3()),
        "auth": _configured_path(
            auth_db_path,
            None,
            str(getattr(settings, "auth_db_path", get_auth_db())),
        ),
        "research": _configured_path(
            research_db_path,
            "EDINET_RESEARCH_DB",
            get_research_db(),
        ),
        "pipeline_jobs": _configured_path(
            jobs_db_path,
            None,
            get_pipeline_jobs_db(),
        ),
        "filings": _configured_path(
            filings_db_path,
            "EDINET_FILINGS_DB",
            get_filings_db(),
        ),
    }

    # These stores are rebuildable or pipeline-owned, so only their files are
    # created here; their table schemas are materialized by pipeline steps.
    _touch_sqlite_database(paths["db1"], busy_timeout_ms=effective_busy_timeout)
    _touch_sqlite_database(paths["db2"], busy_timeout_ms=effective_busy_timeout)

    # Portfolio has a stable, versioned schema and must be ready immediately.
    from src.portfolio.schema import create_tables

    create_tables(str(paths["db3"]))

    # These stores own their schemas and migrations.  Calling their idempotent
    # initializers here makes startup self-sufficient even when the runtime
    # modules are imported through a different entry point.
    from src.auth.storage import AuthStore
    from src.filings.catalog import FilingCatalog
    from src.pipeline_jobs.store import JobStore
    from src.research.storage import ResearchStore

    AuthStore(paths["auth"], busy_timeout_ms=effective_busy_timeout)
    ResearchStore(paths["research"], busy_timeout_ms=effective_busy_timeout)
    JobStore(paths["pipeline_jobs"], busy_timeout_ms=effective_busy_timeout)
    FilingCatalog(paths["filings"], busy_timeout_ms=effective_busy_timeout)

    logger.info(
        "Application databases are ready: %s",
        ", ".join(f"{name}={path}" for name, path in paths.items()),
    )
    return paths
