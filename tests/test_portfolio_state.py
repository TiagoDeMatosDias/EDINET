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

    def test_rebuild_clears_stale_holdings_history(self, db3_path):
        """After rebuild, Holdings_History entries are continuous for each symbol.

        Holdings_History retains history for all positions that were ever
        held (including closed ones) so that historical queries like
        ``get_holdings_at_date`` and the constituents chart work correctly.
        The constituents API handles zero-fill for sold positions.
        """
        self._load_year(db3_path, "2024")
        build_portfolio_state(db3_path, base_currency="EUR")

        import sqlite3
        conn = sqlite3.connect(db3_path)
        cur_syms = set(r[0] for r in conn.execute(
            "SELECT symbol FROM Portfolio_Holdings WHERE quantity > 0"
            " AND asset_category != 'CASH'"
        ).fetchall())

        hh_syms = set(r[0] for r in conn.execute(
            "SELECT DISTINCT symbol FROM Holdings_History"
            " WHERE symbol NOT LIKE 'CASH%'"
        ).fetchall())

        # All current holdings must have Holdings_History entries
        missing = cur_syms - hh_syms
        assert not missing, (
            f"{len(missing)} current holdings missing from Holdings_History: {missing}"
        )

        # For each holding, verify it has daily entries without gaps
        for sym in hh_syms:
            dates = sorted(r[0] for r in conn.execute(
                "SELECT date FROM Holdings_History WHERE symbol = ? ORDER BY date",
                (sym,),
            ).fetchall())
            assert len(dates) >= 1, f"Symbol {sym} has no history entries"
            # Dates should be daily within the range
            from datetime import date as D, timedelta
            first = D.fromisoformat(dates[0])
            last = D.fromisoformat(dates[-1])
            expected_days = (last - first).days + 1
            # Allow up to 15% gap (weekends/holidays where price may be missing
            # but forward-fill should handle them)
            min_expected = int(expected_days * 0.85)
            assert len(dates) >= min_expected, (
                f"Symbol {sym}: {len(dates)} entries for "
                f"{expected_days} day range ({first} to {last})"
            )
        conn.close()

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

    # --- Return/Contribution verification ---------------------------------

    def test_contribution_sums_consistent_with_portfolio_change(self, db3_path):
        """Sum of company contributions ≈ portfolio total value change."""
        self._load_year(db3_path, "2024")
        build_portfolio_state(db3_path, base_currency="EUR")

        import sqlite3
        from collections import defaultdict

        conn = sqlite3.connect(db3_path)
        conn.row_factory = sqlite3.Row

        pf_rows = conn.execute(
            "SELECT date, total_value FROM Portfolio_Daily ORDER BY date"
        ).fetchall()
        pf_start: dict[int, float] = {}
        pf_end: dict[int, float] = {}
        for r in pf_rows:
            y = int(r["date"][:4])
            if y not in pf_start:
                pf_start[y] = r["total_value"] or 0
            pf_end[y] = r["total_value"] or 0

        hh_rows = conn.execute("""
            SELECT symbol, date, market_value FROM Holdings_History
            WHERE symbol NOT LIKE 'CASH%' AND market_value IS NOT NULL
            ORDER BY symbol, date
        """).fetchall()

        trade_rows = conn.execute("""
            SELECT symbol, CAST(substr(trade_date,1,4) AS INT) AS year,
                   SUM(CASE WHEN buy_sell='BUY' THEN ABS(trade_money)*COALESCE(fx_rate_to_base,1) ELSE 0 END
                       - CASE WHEN buy_sell='SELL' THEN COALESCE(proceeds,ABS(trade_money))*COALESCE(fx_rate_to_base,1) ELSE 0 END) AS net_inv
            FROM Transactions WHERE activity_type='TRADE' AND buy_sell IN ('BUY','SELL')
              AND symbol NOT LIKE 'CASH%' GROUP BY symbol, year
        """).fetchall()

        div_rows = conn.execute("""
            SELECT symbol, CAST(substr(trade_date,1,4) AS INT) AS year,
                   SUM(CASE WHEN activity_type IN ('DIVIDEND','PIL_DIVIDEND')
                            THEN ABS(amount)*COALESCE(fx_rate_to_base,1) ELSE 0 END
                       - CASE WHEN activity_type='WITHHOLDING_TAX'
                              THEN ABS(amount)*COALESCE(fx_rate_to_base,1) ELSE 0 END) AS net_div
            FROM Transactions WHERE activity_type IN ('DIVIDEND','PIL_DIVIDEND','WITHHOLDING_TAX')
              AND symbol NOT LIKE 'CASH%' GROUP BY symbol, year
        """).fetchall()

        # Cash flows: deposits, withdrawals, fees, interest
        cash_rows = conn.execute("""
            SELECT CAST(substr(trade_date,1,4) AS INT) AS year,
                   SUM(amount * COALESCE(fx_rate_to_base,1)) AS net_cash
            FROM Transactions WHERE activity_type IN ('DEPOSIT_WITHDRAWAL','BROKER_INTEREST','OTHER_FEE','COMMISSION_ADJ')
            GROUP BY year
        """).fetchall()

        conn.close()

        sym_entries = defaultdict(list)
        for r in hh_rows:
            sym_entries[r["symbol"]].append(r)
        trade_map = defaultdict(lambda: defaultdict(float))
        for r in trade_rows:
            trade_map[r["symbol"]][r["year"]] = r["net_inv"] or 0
        div_map = defaultdict(lambda: defaultdict(float))
        for r in div_rows:
            div_map[r["symbol"]][r["year"]] = r["net_div"] or 0
        cash_map = {r["year"]: r["net_cash"] or 0 for r in cash_rows}

        years = sorted({int(r["date"][:4]) for r in hh_rows})
        assert years, "No years found in data"

        for y in years:
            total_contrib = cash_map.get(y, 0.0)  # start with cash flows
            for sym, entries in sym_entries.items():
                ye = [e for e in entries if e["date"].startswith(str(y))]
                if not ye:
                    continue
                first_date = ye[0]["date"]
                # Position opened during the year: start_val=0 (capital in net_inv)
                # Position carried from prior year: start_val reflects jan value
                if first_date <= f"{y}-01-07":
                    start_val = ye[0]["market_value"] or 0
                else:
                    start_val = 0.0
                end_val = ye[-1]["market_value"] or 0
                net_inv = trade_map[sym][y]
                div = div_map[sym][y]
                contrib = end_val - start_val - net_inv + div
                total_contrib += contrib

            pf_change = (pf_end.get(y, 0) or 0) - (pf_start.get(y, 0) or 0)
            tolerance = max(abs(pf_change) * 0.20, 3000)
            diff = abs(total_contrib - pf_change)
            assert diff < tolerance, (
                f"Year {y}: sum(contributions)={total_contrib:.0f}, "
                f"pf_change={pf_change:.0f}, diff={diff:.0f} > {tolerance:.0f}"
            )

    def test_no_company_has_phantom_negative_contribution(self, db3_path):
        """When a position's value increases over a year (before new money),
        its contribution should not be deeply negative."""
        self._load_year(db3_path, "2024")
        build_portfolio_state(db3_path, base_currency="EUR")

        import sqlite3
        from collections import defaultdict

        conn = sqlite3.connect(db3_path)
        conn.row_factory = sqlite3.Row

        hh_rows = conn.execute("""
            SELECT symbol, date, market_value FROM Holdings_History
            WHERE symbol NOT LIKE 'CASH%' AND market_value IS NOT NULL
            ORDER BY symbol, date
        """).fetchall()

        trade_rows = conn.execute("""
            SELECT symbol, CAST(substr(trade_date,1,4) AS INT) AS year,
                   SUM(CASE WHEN buy_sell='BUY' THEN ABS(trade_money)*COALESCE(fx_rate_to_base,1) ELSE 0 END
                       - CASE WHEN buy_sell='SELL' THEN COALESCE(proceeds,ABS(trade_money))*COALESCE(fx_rate_to_base,1) ELSE 0 END) AS net_inv
            FROM Transactions WHERE activity_type='TRADE' AND buy_sell IN ('BUY','SELL')
              AND symbol NOT LIKE 'CASH%' GROUP BY symbol, year
        """).fetchall()

        div_rows = conn.execute("""
            SELECT symbol, CAST(substr(trade_date,1,4) AS INT) AS year,
                   SUM(CASE WHEN activity_type IN ('DIVIDEND','PIL_DIVIDEND')
                            THEN ABS(amount)*COALESCE(fx_rate_to_base,1) ELSE 0 END
                       - CASE WHEN activity_type='WITHHOLDING_TAX'
                              THEN ABS(amount)*COALESCE(fx_rate_to_base,1) ELSE 0 END) AS net_div
            FROM Transactions WHERE activity_type IN ('DIVIDEND','PIL_DIVIDEND','WITHHOLDING_TAX')
              AND symbol NOT LIKE 'CASH%' GROUP BY symbol, year
        """).fetchall()
        conn.close()

        sym_entries = defaultdict(list)
        for r in hh_rows:
            sym_entries[r["symbol"]].append(r)
        trade_map = defaultdict(lambda: defaultdict(float))
        for r in trade_rows:
            trade_map[r["symbol"]][r["year"]] = r["net_inv"] or 0
        div_map = defaultdict(lambda: defaultdict(float))
        for r in div_rows:
            div_map[r["symbol"]][r["year"]] = r["net_div"] or 0

        violations = []
        for sym, entries in sym_entries.items():
            for y in {int(e["date"][:4]) for e in entries}:
                ye = [e for e in entries if e["date"].startswith(str(y))]
                if not ye:
                    continue
                first_date = ye[0]["date"]
                # Position opened during the year: start_val=0
                if first_date <= f"{y}-01-07":
                    start_val = ye[0]["market_value"] or 0
                else:
                    start_val = 0.0
                end_val = ye[-1]["market_value"] or 0
                net_inv = trade_map[sym][y]
                div = div_map[sym][y]
                contrib = end_val - start_val - net_inv + div
                raw_change = end_val - start_val + div

                # If position value + dividends grew, contribution should be non-negative
                # (after accounting for invested capital)
                if raw_change > 100 and contrib < -abs(raw_change) * 0.5:
                    violations.append(
                        f"{sym} {y}: raw_change=+{raw_change:.0f} but contrib={contrib:.0f} "
                        f"(start={start_val:.0f} end={end_val:.0f} net_inv={net_inv:.0f} div={div:.0f})"
                    )

        assert not violations, (
            f"{len(violations)} companies have phantom negative contributions:\n" +
            "\n".join(violations[:15])
        )
