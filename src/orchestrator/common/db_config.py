"""Hardcoded database path configuration for orchestrator steps.

DB1 contains: DocumentList, financialData_full (raw ingested data).
DB2 contains: all other tables (Taxonomy, CompanyInfo, FinancialStatements,
              IncomeStatement, BalanceSheet, CashflowStatement, ShareMetrics,
              ratio tables, rolling tables, Stock_Prices).

Edit ``config/database_paths.json`` to point to the right databases.
"""

import json
import os
import sys

_CONFIG_DIR_NAME = "config"
_CONFIG_FILE_NAME = "database_paths.json"


def _find_project_root() -> str:
    """Return the project root directory.

    - PyInstaller frozen exe: the folder that contains the exe.
    - Plain Python script: walks up from this module to find the repo root
      (identified by ``config/`` and ``src/orchestrator/`` directories).
    """
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)

    current = os.path.dirname(os.path.abspath(__file__))
    for _ in range(5):
        parent = os.path.dirname(current)
        if parent == current:
            break
        current = parent
        if os.path.isdir(os.path.join(current, _CONFIG_DIR_NAME)) and os.path.isdir(
            os.path.join(current, "src", "orchestrator")
        ):
            return current
    # Fallback: three levels up from src/orchestrator/common/
    return os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "..")
    )


_PROJECT_ROOT = _find_project_root()
_CONFIG_PATH = os.path.join(_PROJECT_ROOT, _CONFIG_DIR_NAME, _CONFIG_FILE_NAME)

_cache: dict[str, str] | None = None


def _load_config() -> dict[str, str]:
    global _cache
    if _cache is not None:
        return _cache
    with open(_CONFIG_PATH, "r", encoding="utf-8") as f:
        _cache = json.load(f)
    return _cache


def _resolve(path: str) -> str:
    if os.path.isabs(path):
        return path
    return os.path.abspath(os.path.join(_PROJECT_ROOT, path))


def reload() -> None:
    """Clear the cached config so the next access re-reads from disk."""
    global _cache
    _cache = None


def get_db1() -> str:
    """Return the absolute path to DB1 (raw data: DocumentList, financialData_full)."""
    cfg = _load_config()
    return _resolve(cfg.get("db1", "data/databases/Base.db"))


def get_db2() -> str:
    """Return the absolute path to DB2 (standardized data: all other tables)."""
    cfg = _load_config()
    return _resolve(cfg.get("db2", "data/databases/Standardized.db"))


def get_db3() -> str:
    """Return the absolute path to DB3 (portfolio module data)."""
    cfg = _load_config()
    return _resolve(cfg.get("db3", "data/databases/Portfolio.db"))


def resolve_db_path(db_value: str | None) -> str | None:
    """Resolve a user-provided database identifier into a filesystem path.

    Behaviour:
    - If ``db_value`` is falsy, return it unchanged.
    - If ``db_value`` is an absolute path, return it unchanged.
    - If ``db_value`` contains a path separator, return its absolute path.
    - Otherwise treat ``db_value`` as a filename and attempt to locate it
      relative to the ``db2`` path. If ``db2`` points to a file, the
      filename's dirname is used; if it points to a directory that exists,
      that directory is used. Fallback: resolve relative to cwd.
    """
    if not db_value:
        return db_value
    raw = str(db_value).strip().strip("'\"")
    if os.path.isabs(raw):
        return raw
    if ("/" in raw) or ("\\" in raw):
        return os.path.abspath(raw)
    # Bare filename: try to derive a directory from db2
    try:
        db2 = get_db2()
    except Exception:
        db2 = None
    if db2:
        if os.path.isdir(db2):
            return os.path.abspath(os.path.join(db2, raw))
        base = os.path.dirname(db2) or os.getcwd()
        return os.path.abspath(os.path.join(base, raw))
    return os.path.abspath(raw)
