"""Startup initialization for the application's databases.

The server creates its own databases (``ensure_server_databases``); workers and
the data steps create the market and filings databases (``ensure_data_databases``).
``ensure_application_databases`` does both for tests and tools.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from src import paths as app_paths
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


def _timeout(settings: Any | None, busy_timeout_ms: int | None) -> int:
    return int(
        busy_timeout_ms
        if busy_timeout_ms is not None
        else getattr(settings, "sqlite_busy_timeout_ms", DEFAULT_BUSY_TIMEOUT_MS)
    )


def _ensure_server(
    paths: dict[str, Path],
    timeout: int,
) -> None:
    from src.auth.storage import AuthStore
    from src.backtesting.catalog import BacktestCatalog
    from src.chat.storage import ChatStore
    from src.jobs.legacy import import_legacy_pipeline_jobs
    from src.jobs.registry import WorkerRegistry
    from src.jobs.store import JobsStore
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
    WorkerRegistry(paths["app"], busy_timeout_ms=timeout)
    create_portfolio_tables(str(paths["app"]))
    BacktestCatalog(paths["app"], busy_timeout_ms=timeout)

    ChatStore(paths["chat"], busy_timeout_ms=timeout)

    # jobs.db: queue, worker sessions, and (once) the old pipeline history.
    jobs = JobsStore(paths["jobs"], busy_timeout_ms=timeout)
    import_legacy_pipeline_jobs(jobs, paths["app"])


def ensure_server_databases(
    *,
    settings: Any | None = None,
    busy_timeout_ms: int | None = None,
) -> dict[str, Path]:
    """Create the databases the server owns: app.db, chat.db, and jobs.db.

    Never opens market.db or filings.db; those belong to workers.
    """
    timeout = _timeout(settings, busy_timeout_ms)
    paths = {
        "app": _path(get_app_db()),
        "chat": _path(get_chat_db()),
        "jobs": _path(app_paths.jobs_db_path()),
    }
    for path in paths.values():
        path.parent.mkdir(parents=True, exist_ok=True)
    _ensure_server(paths, timeout)
    logger.info(
        "Server databases are ready: %s",
        ", ".join(f"{name}={path}" for name, path in paths.items()),
    )
    return paths


def ensure_data_databases(
    *,
    market_db_path: str | Path | None = None,
    filings_db_path: str | Path | None = None,
    busy_timeout_ms: int | None = None,
) -> dict[str, Path]:
    """Create market.db and filings.db and their fixed-schema tables.

    The market database's statement, price, and ratio tables are created by
    the pipeline according to the imported data; only its bond and ownership
    tables have a fixed schema. The file itself is still created here so a clean
    install has the complete layout before the first pipeline run. The logs of
    filings the bond and ownership steps have read are kept in filings.db.
    """
    from src.bonds.store import ensure_bond_tables
    from src.filings.catalog import FilingCatalog
    from src.ownership.store import ensure_ownership_tables

    timeout = _timeout(None, busy_timeout_ms)
    paths = {
        "market": _path(market_db_path or get_market_db()),
        "filings": _path(filings_db_path or get_filings_db()),
    }
    for path in paths.values():
        path.parent.mkdir(parents=True, exist_ok=True)

    conn = connect_write(paths["market"], busy_timeout_ms=timeout)
    try:
        initialize_managed_database(conn)
        conn.commit()
    finally:
        conn.close()
    FilingCatalog(paths["filings"], busy_timeout_ms=timeout)

    ensure_bond_tables(paths["market"], paths["filings"])
    ensure_ownership_tables(paths["market"], paths["filings"])

    logger.info(
        "Data databases are ready: %s",
        ", ".join(f"{name}={path}" for name, path in paths.items()),
    )
    return paths


def ensure_application_databases(
    *,
    settings: Any | None = None,
    app_db_path: str | Path | None = None,
    chat_db_path: str | Path | None = None,
    market_db_path: str | Path | None = None,
    filings_db_path: str | Path | None = None,
    busy_timeout_ms: int | None = None,
) -> dict[str, Path]:
    """Create any missing database and every managed schema (server and data)."""
    timeout = _timeout(settings, busy_timeout_ms)
    server_paths = {
        "app": _path(app_db_path or get_app_db()),
        "chat": _path(chat_db_path or get_chat_db()),
        "jobs": _path(app_paths.jobs_db_path()),
    }
    for path in server_paths.values():
        path.parent.mkdir(parents=True, exist_ok=True)
    _ensure_server(server_paths, timeout)
    data_paths = ensure_data_databases(
        market_db_path=market_db_path,
        filings_db_path=filings_db_path,
        busy_timeout_ms=timeout,
    )
    # jobs.db sits at its fixed place in the data folder and is not part of the
    # returned mapping, which keeps the four databases this function always returned.
    paths = {"app": server_paths["app"], "chat": server_paths["chat"], **data_paths}
    logger.info(
        "Application databases are ready: %s",
        ", ".join(f"{name}={path}" for name, path in paths.items()),
    )
    return paths
