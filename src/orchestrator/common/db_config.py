"""Hardcoded database path configuration for orchestrator steps.

DB1 contains: DocumentList, financialData_full (raw ingested data).
DB2 contains: all other tables (Taxonomy, CompanyInfo, FinancialStatements,
              IncomeStatement, BalanceSheet, CashflowStatement, ShareMetrics,
              ratio tables, rolling tables, Stock_Prices).

Edit ``config/database_paths.json`` to point to the right databases.
"""

import json
import os

_CONFIG_DIR_NAME = "config"
_CONFIG_FILE_NAME = "database_paths.json"


def _find_project_root() -> str:
    """Walk up from this module's directory to find the project root.

    The project root is identified by the presence of ``config/`` and
    ``src/orchestrator/`` directories.
    """
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
