"""Tests for src/portfolio/portfolio_state.py — walk-forward reconstruction."""

import os
import sqlite3
import tempfile
import pytest
from datetime import date, timedelta

from src.portfolio.schema import create_tables
from src.portfolio.ibkr_parser import parse_ibkr_xml_file, normalize_entries
from src.portfolio.transactions import insert_entries
from src.portfolio.portfolio_state import (
    build_portfolio_state,
    get_daily_values,
    get_current_holdings,
    get_holdings_at_date,
)
from src.orchestrator.common.db_config import get_db2

IBKR_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "data", "ibkr"
)


class TestBuildPortfolioState:
    """Integration tests: parse XML → insert → build state."""

    @pytest.fixture
    def db3_path(self):
        fd, path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        create_tables(path)
        yield path
        try:
            os.unlink(path)
        except OSError:
            pass

    @pytest.fixture
    def db2_conn(self):
        """Use the real db2 database for price lookups."""
        return sqlite3.connect(get_db2())

    def _load_year(self, db3_path, year):
        """Parse XML, insert, return entries."""
        fpath = os.path.join(IBKR_DIR, f"{year}.xml")
        result = parse_ibkr_xml_file(fpath)
        entries = normalize_entries(result)
        insert_entries(db3_path, entries, source_file=f"{year}.xml")
        return entries

    def test_build_from_2024(self, db3_path):
        """Build portfolio state from 2024 data (single year)."""
        self._load_year(db3_path, "2024")
        result = build_portfolio_state(db3_path, base_currency="EUR")
        assert result["daily_rows"] > 0
        assert result["holdings_count"] > 0

    def test_build_from_all_years(self, db3_path):
        """Build from all 6 years of data."""
        for year in ["2020", "2021", "2022", "2023", "2024", "2025"]:
            self._load_year(db3_path, year)
        result = build_portfolio_state(db3_path, base_currency="EUR")
        assert result["daily_rows"] > 100, f"Only {result['daily_rows']} daily rows"
        assert result["holdings_count"] > 0

    def test_daily_values_returned(self, db3_path):
        self._load_year(db3_path, "2024")
        build_portfolio_state(db3_path, base_currency="EUR")
        daily = get_daily_values(db3_path)
        assert len(daily) > 0
        assert "total_value" in daily[0]
        assert "cash_balance" in daily[0]

    def test_current_holdings(self, db3_path):
        self._load_year(db3_path, "2024")
        build_portfolio_state(db3_path, base_currency="EUR")
        holdings = get_current_holdings(db3_path)
        assert len(holdings) > 0
        for h in holdings:
            assert h["symbol"]
            assert h["asset_category"]
            assert "quantity" in h

    def test_holdings_at_date(self, db3_path):
        self._load_year(db3_path, "2024")
        build_portfolio_state(db3_path, base_currency="EUR")
        snap = get_holdings_at_date(db3_path, "2024-06-15")
        assert len(snap) > 0

    def test_dividend_income_recorded(self, db3_path):
        """Dividend entries should produce positive dividend_income in Portfolio_Daily."""
        self._load_year(db3_path, "2024")
        build_portfolio_state(db3_path, base_currency="EUR")
        daily = get_daily_values(db3_path)
        total_divs = sum(d.get("dividend_income", 0) or 0 for d in daily)
        assert total_divs > 0, "Expected positive dividend income"

    def test_idempotent_rebuild(self, db3_path):
        """Calling build twice should produce the same row counts."""
        self._load_year(db3_path, "2024")
        result1 = build_portfolio_state(db3_path, base_currency="EUR")
        result2 = build_portfolio_state(db3_path, base_currency="EUR")
        assert result1["daily_rows"] == result2["daily_rows"]
        assert result1["holdings_count"] == result2["holdings_count"]

    def test_spinoff_creates_new_position(self, db3_path):
        """Loading 2024 should create SOLV position from MMM spinoff."""
        for year in ["2020", "2021", "2022", "2023", "2024"]:
            self._load_year(db3_path, year)
        build_portfolio_state(db3_path, base_currency="EUR")
        # Spinoff shares may have been sold by end of data;
        # check holdings shortly after the spinoff date
        snap = get_holdings_at_date(db3_path, "2024-04-15")
        symbols = {h["symbol"] for h in snap if h["quantity"] > 0}
        # SOLV spinoff from MMM was 2024-03-29, ONL spinoff from O was 2021-11-12
        spinoff_symbols = {"SOLV", "ONL"}
        found = symbols & spinoff_symbols
        assert found, f"Expected at least one spinoff symbol in holdings, got: {symbols}"
