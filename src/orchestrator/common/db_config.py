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
from dataclasses import dataclass

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


@dataclass(frozen=True)
class DatabaseSpec:
    """One application database.

    ``request_selectable`` marks the analytical databases an API request may
    name as its data source. Every other store (credentials, per-user state,
    job history, ...) is private: it is refused as a request-selected path even
    inside an explicitly allowed data root, so a store added here later is
    private unless it opts in.
    """

    key: str
    default_path: str
    env_override: str | None = None
    request_selectable: bool = False


DATABASES: tuple[DatabaseSpec, ...] = (
    DatabaseSpec("db1", "data/databases/Base.db", request_selectable=True),
    DatabaseSpec("db2", "data/databases/Standardized.db", request_selectable=True),
    DatabaseSpec("db3", "data/databases/Portfolio.db"),
    DatabaseSpec("auth_db", "data/databases/auth.db", env_override="EDINET_AUTH_DB"),
    DatabaseSpec("research_db", "data/databases/research.db", env_override="EDINET_RESEARCH_DB"),
    DatabaseSpec("pipeline_jobs_db", "data/databases/pipeline_jobs.db"),
    DatabaseSpec("filings_db", "data/databases/Filings.db", env_override="EDINET_FILINGS_DB"),
)
_DATABASES_BY_KEY = {spec.key: spec for spec in DATABASES}


def database_path(key: str) -> str:
    """Return the configured absolute path for one application database."""
    spec = _DATABASES_BY_KEY[key]
    return _resolve(_load_config().get(key, spec.default_path))


def database_locations(spec: DatabaseSpec) -> list[str]:
    """Every path the database may be opened from: configured and env override."""
    locations = [database_path(spec.key)]
    override = os.getenv(spec.env_override) if spec.env_override else None
    if override:
        locations.append(os.path.abspath(os.path.expanduser(override)))
    return locations


def get_db1() -> str:
    """Return the absolute path to DB1 (raw data: DocumentList, financialData_full)."""
    return database_path("db1")


def get_db2() -> str:
    """Return the absolute path to DB2 (standardized data: all other tables)."""
    return database_path("db2")


def get_db3() -> str:
    """Return the absolute path to DB3 (portfolio module data)."""
    return database_path("db3")


def get_auth_db() -> str:
    """Return the absolute path to the non-rebuildable authentication database."""
    return database_path("auth_db")


def get_research_db() -> str:
    """Return the absolute path to the owner-scoped research-state database."""
    return database_path("research_db")


def get_pipeline_jobs_db() -> str:
    """Return the absolute path to the durable pipeline-jobs database."""
    return database_path("pipeline_jobs_db")


def get_filings_db() -> str:
    """Return the absolute path to the rebuildable filing catalog database."""
    return database_path("filings_db")


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
