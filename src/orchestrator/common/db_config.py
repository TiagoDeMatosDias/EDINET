"""Locations of the application's four databases.

``app.db``     irreplaceable: settings, secrets, accounts, research, pipeline
               jobs, and portfolio. Back up this file.
``chat.db``    chat channels and messages, kept apart because it grows with use.
``market.db``  rebuildable market data: the EDINET document index
               (``DocumentList``), taxonomy, company info, statements, ratios,
               rolling metrics, prices, splits, and bonds.
``filings.db`` rebuildable filing archive: provider ZIPs, XBRL facts, and
               translations. Kept apart because of its size.

``app.db`` and ``chat.db`` always live in the data folder (see ``src.paths``).
``market.db`` and ``filings.db`` default to it; the ``storage.market_db_path``
and ``storage.filings_db_path`` settings move them, for example to a larger
disk. Those settings are read from ``app.db`` once per process.

Until the databases of the old layout have been migrated (see
``migrate_layout``), every lookup raises ``LegacyLayoutError`` rather than
return a path at which an empty database would be created beside the real one.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from src import paths

_layout_checked = False


def _require_migrated_layout() -> None:
    global _layout_checked
    if _layout_checked:
        return
    from .migrate_layout import LegacyLayoutError, pending_databases

    pending = pending_databases()
    if pending:
        raise LegacyLayoutError(
            "The databases still use the old layout ("
            + ", ".join(str(path) for path in pending)
            + "). Stop every server and pipeline, then run `python main.py migrate` "
            "(or start the application with main.py, which migrates first)."
        )
    _layout_checked = True


def get_app_db() -> str:
    """Return the absolute path to the irreplaceable application database."""
    _require_migrated_layout()
    return str(paths.app_db_path())


def get_chat_db() -> str:
    """Return the absolute path to the chat database."""
    _require_migrated_layout()
    return str(paths.chat_db_path())


def get_market_db() -> str:
    """Return the absolute path to the rebuildable market database."""
    _require_migrated_layout()
    return _configured_path("storage.market_db_path", paths.default_market_db_path())


def get_filings_db() -> str:
    """Return the absolute path to the rebuildable filing archive."""
    _require_migrated_layout()
    return _configured_path("storage.filings_db_path", paths.default_filings_db_path())


def _configured_path(key: str, default: Path) -> str:
    configured = _storage_setting(str(paths.app_db_path()), key)
    if not configured:
        return str(default)
    candidate = Path(configured).expanduser()
    if not candidate.is_absolute():
        candidate = paths.data_dir() / candidate
    return str(candidate.resolve(strict=False))


@lru_cache(maxsize=None)
def _storage_setting(app_db: str, key: str) -> str:
    from src.settings.store import read_stored_value

    value = read_stored_value(Path(app_db), key)
    return str(value).strip() if value else ""


def reload() -> None:
    """Forget cached lookups so the next one re-reads the disk and ``app.db``."""
    global _layout_checked
    _layout_checked = False
    _storage_setting.cache_clear()
