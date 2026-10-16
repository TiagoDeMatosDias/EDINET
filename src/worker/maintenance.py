"""Schema, index and checkpoint upkeep for the market and filings databases.

Only workers write these databases, so the work the server used to do inside
requests (screening preparation, indexes) happens here: once at worker start and
again after a job that wrote data. Every step is best effort; tables may be
missing on a fresh install and one failure must not block the others.
"""

from __future__ import annotations

import logging
import sqlite3
from contextlib import closing
from typing import Callable

from src.orchestrator.common.db_config import get_filings_db, get_market_db

logger = logging.getLogger(__name__)

BUSY_TIMEOUT_MS = 30_000


_NO_DATA_YET = ("no such table", "Required table not found")


def _step(results: dict[str, str], name: str, action: Callable[[], object]) -> None:
    try:
        action()
    except Exception as exc:  # noqa: BLE001 - best effort; log and carry on
        if any(marker in str(exc) for marker in _NO_DATA_YET):
            # A fresh install: the pipeline has not filled the tables this step needs.
            results[name] = f"skipped: {exc}"
            logger.info("Maintenance step %s skipped until the data exists: %s", name, exc)
            return
        results[name] = f"{type(exc).__name__}: {exc}"
        logger.warning("Maintenance step %s failed: %s", name, exc, exc_info=True)
    else:
        results[name] = "ok"


def _refresh_market(market_db: str, results: dict[str, str]) -> None:
    def indexes() -> None:
        from src.security_analysis.security_analysis import ensure_security_analysis_indexes

        ensure_security_analysis_indexes(market_db)

    def screening() -> None:
        from src.screening.screening import _prepare_screening_database

        _prepare_screening_database(market_db)

    _step(results, "screening_prep", screening)
    _step(results, "security_analysis_indexes", indexes)


def startup() -> dict[str, str]:
    """Create missing schemas, then run the market refresh. Returns each step's outcome."""
    results: dict[str, str] = {}
    market_db, filings_db = get_market_db(), get_filings_db()

    def schemas() -> None:
        from src.orchestrator.common.database_bootstrap import ensure_data_databases

        ensure_data_databases(
            market_db_path=market_db, filings_db_path=filings_db, busy_timeout_ms=BUSY_TIMEOUT_MS
        )

    _step(results, "schemas", schemas)
    _refresh_market(market_db, results)
    logger.info("Worker startup maintenance: %s", results)
    return results


def checkpoint(path: str) -> None:
    """``PRAGMA wal_checkpoint(PASSIVE)``: never waits for readers (unlike TRUNCATE/RESTART)."""
    with closing(sqlite3.connect(path, timeout=BUSY_TIMEOUT_MS / 1000)) as conn:
        conn.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchall()


def checkpoint_filings(path: str) -> None:
    """Passive checkpoints of the filing index and every yearly filing file."""
    from src.filings.maintenance import checkpoint_all
    from src.filings.storage import FilingStorage

    checkpoint_all(FilingStorage(path), "PASSIVE")


def after_write(writes: frozenset[str]) -> dict[str, str]:
    """Maintenance after a job that wrote the named databases (``market`` and/or ``filings``)."""
    results: dict[str, str] = {}
    databases = {"market": get_market_db(), "filings": get_filings_db()}
    if "market" in writes:
        _refresh_market(databases["market"], results)
    for name in sorted(writes):
        path = databases.get(name)
        if path:
            action = checkpoint_filings if name == "filings" else checkpoint
            _step(results, f"checkpoint_{name}", lambda path=path, action=action: action(path))  # type: ignore[misc]
    logger.info("Maintenance after %s writes: %s", "+".join(sorted(writes)), results)
    return results
