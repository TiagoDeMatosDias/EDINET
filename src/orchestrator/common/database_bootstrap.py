"""Startup initialization for the application's four databases."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from src.orchestrator.common.db_config import (
    get_app_db,
    get_chat_db,
    get_filings_db,
    get_market_db,
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


def ensure_application_databases(
    *,
    settings: Any | None = None,
    app_db_path: str | Path | None = None,
    chat_db_path: str | Path | None = None,
    market_db_path: str | Path | None = None,
    filings_db_path: str | Path | None = None,
    busy_timeout_ms: int | None = None,
) -> dict[str, Path]:
    """Create any missing database and every managed schema.

    The market database's statement, price, and ratio tables are created by
    the pipeline according to the imported data; only its bond tables have a
    fixed schema. The file itself is still created here so a clean install has
    the complete layout before the first pipeline run.
    """
    timeout = int(
        busy_timeout_ms
        if busy_timeout_ms is not None
        else getattr(settings, "sqlite_busy_timeout_ms", DEFAULT_BUSY_TIMEOUT_MS)
    )
    paths = {
        "app": _path(app_db_path or get_app_db()),
        "chat": _path(chat_db_path or get_chat_db()),
        "market": _path(market_db_path or get_market_db()),
        "filings": _path(filings_db_path or get_filings_db()),
    }
    for path in paths.values():
        path.parent.mkdir(parents=True, exist_ok=True)

    from src.auth.storage import AuthStore
    from src.bonds.store import ensure_bond_tables
    from src.chat.storage import ChatStore
    from src.filings.catalog import FilingCatalog
    from src.pipeline_jobs.store import JobStore
    from src.portfolio.schema import create_tables as create_portfolio_tables
    from src.research.storage import ResearchStore
    from src.settings.store import SettingsStore

    # app.db: each component owns its tables and migrations; their
    # initializers are idempotent.
    SettingsStore(paths["app"], busy_timeout_ms=timeout)
    AuthStore(paths["app"], busy_timeout_ms=timeout)
    ResearchStore(paths["app"], busy_timeout_ms=timeout)
    JobStore(paths["app"], busy_timeout_ms=timeout)
    create_portfolio_tables(str(paths["app"]))

    ChatStore(paths["chat"], busy_timeout_ms=timeout)

    conn = connect_write(paths["market"], busy_timeout_ms=timeout)
    try:
        initialize_managed_database(conn)
        conn.commit()
    finally:
        conn.close()
    ensure_bond_tables(paths["market"])

    FilingCatalog(paths["filings"], busy_timeout_ms=timeout)

    logger.info(
        "Application databases are ready: %s",
        ", ".join(f"{name}={path}" for name, path in paths.items()),
    )
    return paths
