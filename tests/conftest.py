import os
import tempfile
import pytest


@pytest.fixture(scope="session")
def populated_db3():
    """Session-scoped full IBKR dataset used by multiple tests.

    This mirrors the in-file fixture previously defined inside
    `tests/test_portfolio_additional.py::TestFullIntegration` but exposes
    it at module/session scope so other test classes can reuse it.
    """
    from src.portfolio.schema import create_tables
    from src.portfolio.ibkr_parser import parse_ibkr_xml_file, normalize_entries
    from src.portfolio.transactions import insert_entries
    from src.portfolio.portfolio_state import build_portfolio_state

    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    create_tables(path)

    ibkr_dir = os.path.join(os.path.dirname(__file__), "..", "data", "ibkr")
    for year in ["2020", "2021", "2022", "2023", "2024", "2025"]:
        fpath = os.path.join(ibkr_dir, f"{year}.xml")
        result = parse_ibkr_xml_file(fpath)
        entries = normalize_entries(result)
        insert_entries(path, entries, source_file=f"{year}.xml")

    build_portfolio_state(path, base_currency="EUR")
    yield path
    try:
        os.unlink(path)
    except OSError:
        pass
