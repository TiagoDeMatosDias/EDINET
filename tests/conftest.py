import os
import shutil
import sqlite3
import tempfile
from pathlib import Path

import pytest

from tests.factories import create_market_database, sample_ibkr_xml, write_sample_ibkr_xml

# The shared FastAPI app is imported while tests are collected. Keep that app
# deterministic and disconnected from all operator-owned databases and build
# artifacts. Tests that exercise authentication construct an explicit
# account-mode app instead.
_TEST_RUNTIME_DIR = Path(tempfile.mkdtemp(prefix="edinet-pytest-")).resolve()
_TEST_DATABASE_DIR = _TEST_RUNTIME_DIR / "databases"
_TEST_DATABASE_DIR.mkdir(parents=True)
_TEST_FRONTEND_DIST = _TEST_RUNTIME_DIR / "frontend-dist"
(_TEST_FRONTEND_DIST / "app-assets").mkdir(parents=True)
(_TEST_FRONTEND_DIST / "index.html").write_text(
    "<!doctype html><html><head><title>Shade Research</title></head>"
    '<body><div id="root"></div><script type="module" '
    'src="/app-assets/test-app.js"></script></body></html>',
    encoding="utf-8",
)
(_TEST_FRONTEND_DIST / "app-assets" / "test-app.js").write_text(
    "window.__EDINET_TEST_APP__ = true;\n",
    encoding="utf-8",
)

_TEST_DB2 = create_market_database(_TEST_DATABASE_DIR / "Standardized.db")
_TEST_DATABASE_PATHS = {
    "db1": str(_TEST_DATABASE_DIR / "Base.db"),
    "db2": str(_TEST_DB2),
    "db3": str(_TEST_DATABASE_DIR / "Portfolio.db"),
    "auth_db": str(_TEST_DATABASE_DIR / "auth.db"),
    "research_db": str(_TEST_DATABASE_DIR / "research.db"),
    "pipeline_jobs_db": str(_TEST_DATABASE_DIR / "pipeline_jobs.db"),
    "filings_db": str(_TEST_DATABASE_DIR / "Filings.db"),
}
with sqlite3.connect(_TEST_DATABASE_PATHS["db1"]):
    pass

os.environ["EDINET_AUTH_MODE"] = "disabled"
os.environ["EDINET_AUTH_DB"] = _TEST_DATABASE_PATHS["auth_db"]
os.environ["EDINET_ALLOWED_DATA_ROOTS"] = str(_TEST_DATABASE_DIR)
os.environ["EDINET_FRONTEND_DIST"] = str(_TEST_FRONTEND_DIST)

# db_config intentionally reads one project-level JSON file in production.
# Supplying its cache before test collection gives every imported module the
# same isolated runtime paths without modifying the operator's configuration.
from src.orchestrator.common import db_config  # noqa: E402

db_config._cache = dict(_TEST_DATABASE_PATHS)


def pytest_sessionfinish(session, exitstatus):
    del session, exitstatus
    shutil.rmtree(_TEST_RUNTIME_DIR, ignore_errors=True)


@pytest.fixture(scope="session")
def market_db_path() -> str:
    """Path to the suite's deterministic standardized market database."""
    return str(_TEST_DB2)


@pytest.fixture
def sample_ibkr_file(tmp_path: Path) -> Path:
    """A synthetic IBKR FlexQuery export with trades, cash, and a spinoff."""
    return write_sample_ibkr_xml(tmp_path / "portfolio.xml")


@pytest.fixture
def sample_ibkr_content() -> str:
    return sample_ibkr_xml()


@pytest.fixture(scope="session")
def populated_db3(market_db_path: str):
    """Portfolio database populated exclusively from synthetic test data."""
    from src.portfolio.ibkr_parser import normalize_entries, parse_ibkr_xml
    from src.portfolio.portfolio_state import build_portfolio_state
    from src.portfolio.schema import create_tables
    from src.portfolio.transactions import insert_entries

    path = str(_TEST_RUNTIME_DIR / "populated-portfolio.db")
    create_tables(path)
    entries = normalize_entries(parse_ibkr_xml(sample_ibkr_xml()))
    insert_entries(path, entries, source_file="synthetic.xml")
    build_portfolio_state(
        path,
        db2_path=market_db_path,
        end_date="2024-01-20",
        base_currency="EUR",
    )
    return path
