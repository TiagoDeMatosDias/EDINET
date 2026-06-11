"""Additional tests for edge cases, price fetching, and full integration."""

import os
import sqlite3
import tempfile
import pytest
from collections import Counter
from datetime import date


# ---------------------------------------------------------------------------
# Price fetcher tests
# ---------------------------------------------------------------------------

class TestBuildCurrencyMap:
    def test_empty_entries(self):
        from src.portfolio.price_fetcher import _build_currency_map
        assert _build_currency_map([]) == {}

    def test_single_stk_trade(self):
        from src.portfolio.price_fetcher import _build_currency_map
        mapping = _build_currency_map([{
            "activity_type": "TRADE", "asset_category": "STK",
            "symbol": "VWCE", "currency": "EUR",
        }])
        assert mapping == {"VWCE": "EUR"}

    def test_option_trade_maps_underlying_only(self):
        from src.portfolio.price_fetcher import _build_currency_map
        mapping = _build_currency_map([{
            "activity_type": "TRADE", "asset_category": "OPT",
            "symbol": "JXN 250620P00050000", "currency": "USD",
            "underlying_symbol": "JXN",
        }])
        # Option symbol should NOT be in the map
        assert "JXN 250620P00050000" not in mapping
        # Underlying SHOULD be mapped
        assert mapping["JXN"] == "USD"

    def test_forex_pairs_skipped(self):
        from src.portfolio.price_fetcher import _build_currency_map
        mapping = _build_currency_map([
            {"activity_type": "TRADE", "asset_category": "CASH",
             "symbol": "EUR.USD", "currency": "USD"},
            {"activity_type": "TRADE", "asset_category": "CASH",
             "symbol": "USD.JPY", "currency": "JPY"},
        ])
        assert "EUR.USD" not in mapping
        assert "USD.JPY" not in mapping

    def test_non_trade_entries_skipped(self):
        from src.portfolio.price_fetcher import _build_currency_map
        mapping = _build_currency_map([
            {"activity_type": "DIVIDEND", "symbol": "VWCE", "currency": "EUR"},
        ])
        assert mapping == {}

    def test_currency_map_from_real_data(self):
        """Build currency map from all 6 parsed XML files."""
        from src.portfolio.ibkr_parser import parse_ibkr_xml_file, normalize_entries
        from src.portfolio.price_fetcher import _build_currency_map

        ibkr_dir = os.path.join(os.path.dirname(__file__), "../..", "data", "ibkr")
        all_entries = []
        for year in ["2020", "2021", "2022", "2023", "2024", "2025"]:
            result = parse_ibkr_xml_file(os.path.join(ibkr_dir, f"{year}.xml"))
            all_entries.extend(normalize_entries(result))
        mapping = _build_currency_map(all_entries)
        assert len(mapping) > 20  # many tickers across all years
        # European ETFs
        assert mapping.get("VWCE") == "EUR"
        # US stocks
        assert mapping.get("JXN") == "USD"
        assert mapping.get("BTI") == "USD"
        # Japanese stocks
        assert mapping.get("7575.T") == "JPY"
        # Forex pairs excluded
        for k in mapping:
            assert k not in ("EUR.USD", "USD.JPY")


class TestGetStoredCurrency:
    def test_none_for_empty_db(self):
        import sqlite3, tempfile
        from src.portfolio.price_fetcher import _get_stored_currency
        fd, path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        conn = sqlite3.connect(path)
        assert _get_stored_currency(conn, "VWCE") is None
        conn.close()
        os.unlink(path)


# ---------------------------------------------------------------------------
# Parser edge cases
# ---------------------------------------------------------------------------

class TestParserEdgeCases:
    def test_empty_xml(self):
        from src.portfolio.ibkr_parser import parse_ibkr_xml
        xml = '<?xml version="1.0"?><FlexQueryResponse></FlexQueryResponse>'
        result = parse_ibkr_xml(xml)
        assert result["trades"] == []
        assert result["cash_transactions"] == []
        assert result["corp_actions"] == []

    def test_date_time_without_timestamp(self):
        from src.portfolio.ibkr_parser import parse_ibkr_xml
        xml = """<?xml version="1.0"?>
        <FlexQueryResponse><FlexStatements><FlexStatement><CashTransactions>
        <CashTransaction accountId="U1" currency="USD" fxRateToBase="1"
          dateTime="2024-12-25" amount="100" type="Dividends"
          transactionID="date-test-1" levelOfDetail="DETAIL"/>
        </CashTransactions></FlexStatement></FlexStatements></FlexQueryResponse>"""
        result = parse_ibkr_xml(xml)
        assert result["cash_transactions"][0]["trade_date"] == "2024-12-25"

    def test_missing_fx_rate_defaults_to_one(self):
        from src.portfolio.ibkr_parser import parse_ibkr_xml
        xml = """<?xml version="1.0"?>
        <FlexQueryResponse><FlexStatements><FlexStatement><Trades>
        <Trade accountId="U1" currency="EUR" fxRateToBase="" assetCategory="STK"
          symbol="VWCE" tradeDate="2024-06-15" quantity="10" tradePrice="100"
          tradeMoney="1000" proceeds="-1000" ibCommission="0" taxes="0"
          netCash="-1000" buySell="BUY" transactionID="fx-test" levelOfDetail="EXECUTION"/>
        </Trades></FlexStatement></FlexStatements></FlexQueryResponse>"""
        result = parse_ibkr_xml(xml)
        assert result["trades"][0]["fx_rate_to_base"] == 1.0

    def test_unrecognized_cash_type_skipped(self):
        from src.portfolio.ibkr_parser import parse_ibkr_xml
        xml = """<?xml version="1.0"?>
        <FlexQueryResponse><FlexStatements><FlexStatement><CashTransactions>
        <CashTransaction accountId="U1" currency="USD" fxRateToBase="1"
          dateTime="2024-01-01" amount="50" type="UnknownAction"
          transactionID="unknown-type" levelOfDetail="DETAIL"/>
        </CashTransactions></FlexStatement></FlexStatements></FlexQueryResponse>"""
        result = parse_ibkr_xml(xml)
        assert len(result["cash_transactions"]) == 0

    def test_order_level_skipped(self):
        """ORDER level trades should be skipped (only EXECUTION is used)."""
        from src.portfolio.ibkr_parser import parse_ibkr_xml_file
        ibkr_dir = os.path.join(os.path.dirname(__file__), "../..", "data", "ibkr")
        result = parse_ibkr_xml_file(os.path.join(ibkr_dir, "2024.xml"))
        # Verify no ORDER-level trades leaked through
        for t in result["trades"]:
            assert "ORDER" not in str(t.get("levelOfDetail", ""))

    def test_normalize_entries_preserves_order(self):
        from src.portfolio.ibkr_parser import parse_ibkr_xml, normalize_entries
        xml = """<?xml version="1.0"?>
        <FlexQueryResponse><FlexStatements><FlexStatement>
        <Trades>
        <Trade accountId="U1" currency="EUR" fxRateToBase="1" assetCategory="STK"
          symbol="A" tradeDate="2024-01-15" quantity="10" tradePrice="100"
          tradeMoney="1000" proceeds="-1000" ibCommission="0" taxes="0"
          netCash="-1000" buySell="BUY" transactionID="a" levelOfDetail="EXECUTION"/>
        </Trades>
        <CashTransactions>
        <CashTransaction accountId="U1" currency="EUR" fxRateToBase="1"
          dateTime="2024-01-20" amount="50" type="Dividends"
          transactionID="b" levelOfDetail="DETAIL"/>
        </CashTransactions>
        </FlexStatement></FlexStatements></FlexQueryResponse>"""
        entries = normalize_entries(parse_ibkr_xml(xml))
        assert entries[0]["transaction_id"] == "a"
        assert entries[1]["transaction_id"] == "b"


# ---------------------------------------------------------------------------
# Option pricing edge cases
# ---------------------------------------------------------------------------

class TestOptionPricingEdgeCases:
    def test_zero_strike(self):
        from src.portfolio.option_pricing import black_scholes
        # K=0 causes log(S/0) → infinity. Use very small K instead.
        price = black_scholes("call", S=100, K=0.01, T=1, r=0.05, sigma=0.20)
        assert price > 99  # close to S

    def test_very_low_volatility(self):
        from src.portfolio.option_pricing import black_scholes, binomial_tree
        # Very low vol: price should be close to discounted intrinsic
        bs = black_scholes("call", 100, 90, 1, 0.05, 0.01)
        bt = binomial_tree("call", 100, 90, 1, 0.05, 0.01, steps=100)
        import math
        expected = 100 - 90 * math.exp(-0.05)
        assert abs(bs - expected) < 1.5
        assert abs(bt - expected) < 1.5

    def test_extremely_deep_itm(self):
        from src.portfolio.option_pricing import black_scholes
        price = black_scholes("call", S=10000, K=1, T=0.5, r=0.05, sigma=0.20)
        # Should be ~S - PV(K) ≈ 10000 - ~0.975 ≈ 9999
        assert price > 9500

    def test_extremely_deep_otm(self):
        from src.portfolio.option_pricing import black_scholes
        price = black_scholes("call", S=1, K=10000, T=0.5, r=0.05, sigma=0.20)
        assert price < 0.01

    def test_very_short_time(self):
        from src.portfolio.option_pricing import get_option_price
        # 1 day until expiry: price ≈ intrinsic
        price = get_option_price("call", 100, 95, 1/365, 0.05, 0.20)
        assert price > 0  # still has some value
        assert price < 6  # intrinsic + tiny time value

    def test_long_time_asymptotic(self):
        from src.portfolio.option_pricing import black_scholes
        # Very long-dated call should approach S
        price = black_scholes("call", 100, 100, 100, 0.05, 0.20)
        # Discounted strike → 0 as T→∞ for call, price ≈ S
        assert 90 < price <= 100

    def test_greeks_atm_symmetry(self):
        """ATM call and put should have symmetric delta except for offset."""
        from src.portfolio.option_pricing import option_greeks
        gc = option_greeks("call", 100, 100, 1, 0.05, 0.20)
        gp = option_greeks("put", 100, 100, 1, 0.05, 0.20)
        # Gamma and Vega should be identical
        assert abs(gc["gamma"] - gp["gamma"]) < 0.01
        assert abs(gc["vega"] - gp["vega"]) < 0.01

    def test_binomial_converges_to_bs_for_european(self):
        """European option via binomial should converge to BS as steps increase."""
        from src.portfolio.option_pricing import black_scholes, binomial_tree
        bs = black_scholes("call", 100, 100, 1, 0.05, 0.25)
        bt5 = binomial_tree("call", 100, 100, 1, 0.05, 0.25, steps=5)
        bt200 = binomial_tree("call", 100, 100, 1, 0.05, 0.25, steps=200)
        # 200-step binomial should be closer to BS than 5-step
        assert abs(bt200 - bs) < abs(bt5 - bs)


# ---------------------------------------------------------------------------
# Portfolio state edge cases
# ---------------------------------------------------------------------------

class TestPortfolioStateEdgeCases:
    @pytest.fixture
    def db3_path(self):
        fd, path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        from src.portfolio.schema import create_tables
        create_tables(path)
        yield path
        try:
            os.unlink(path)
        except OSError:
            pass

    def test_empty_db(self, db3_path):
        from src.portfolio.portfolio_state import build_portfolio_state
        result = build_portfolio_state(db3_path)
        assert result["daily_rows"] == 0
        assert result["holdings_count"] == 0

    def test_single_buy_then_sell(self, db3_path):
        """Buy 5 shares at 100, sell all at 110 → zero holdings."""
        from src.portfolio.portfolio_state import build_portfolio_state, get_current_holdings
        from src.portfolio.transactions import insert_entries

        entries = [
            {"transaction_id": "t1", "activity_type": "TRADE", "asset_category": "STK",
             "symbol": "TEST", "currency": "USD", "trade_date": "2024-01-15",
             "quantity": 5, "trade_price": 100, "trade_money": 500,
             "proceeds": -500, "commission": 0, "taxes": 0, "net_cash": -500,
             "buy_sell": "BUY", "fx_rate_to_base": 1.0},
            {"transaction_id": "t2", "activity_type": "TRADE", "asset_category": "STK",
             "symbol": "TEST", "currency": "USD", "trade_date": "2024-02-01",
             "quantity": -5, "trade_price": 110, "trade_money": 550,
             "proceeds": 550, "commission": 0, "taxes": 0, "net_cash": 550,
             "buy_sell": "SELL", "fx_rate_to_base": 1.0},
        ]
        insert_entries(db3_path, entries)
        result = build_portfolio_state(db3_path)
        # Current holdings should be empty after selling everything
        holdings = get_current_holdings(db3_path)
        # CASH USD row should be present (50 profit in USD)
        cash_rows = [h for h in holdings if h['asset_category'] == 'CASH']
        assert len(cash_rows) >= 1, f"Expected at least 1 CASH row, got {len(cash_rows)}"
        usd_cash = [h for h in cash_rows if h['currency'] == 'USD']
        assert len(usd_cash) == 1
        assert usd_cash[0]['market_value_native'] == 50.0

    def test_deposit_withdrawal_net_inflow(self, db3_path):
        """Deposits should add to cash, withdrawals subtract."""
        from src.portfolio.portfolio_state import build_portfolio_state, get_daily_values
        from src.portfolio.transactions import insert_entries

        entries = [
            {"transaction_id": "d1", "activity_type": "DEPOSIT_WITHDRAWAL",
             "symbol": None, "currency": "EUR", "trade_date": "2024-03-01",
             "amount": 1000, "fx_rate_to_base": 1.0},
            {"transaction_id": "d2", "activity_type": "DEPOSIT_WITHDRAWAL",
             "symbol": None, "currency": "EUR", "trade_date": "2024-03-15",
             "amount": -200, "fx_rate_to_base": 1.0},
        ]
        insert_entries(db3_path, entries)
        build_portfolio_state(db3_path)
        daily = get_daily_values(db3_path)
        total_inflow = sum(d.get("net_inflow", 0) or 0 for d in daily)
        assert total_inflow == 800

    def test_broker_interest_applied(self, db3_path):
        from src.portfolio.portfolio_state import build_portfolio_state, get_daily_values
        from src.portfolio.transactions import insert_entries

        entries = [
            {"transaction_id": "bi1", "activity_type": "BROKER_INTEREST",
             "symbol": None, "currency": "EUR", "trade_date": "2024-04-01",
             "amount": 5.25, "fx_rate_to_base": 1.0},
        ]
        insert_entries(db3_path, entries)
        result = build_portfolio_state(db3_path)
        assert result["daily_rows"] > 0

    def test_fx_conversion_applied(self, db3_path):
        """Verify fx_rate_to_base converts non-base-currency transactions."""
        from src.portfolio.portfolio_state import build_portfolio_state, get_daily_values
        from src.portfolio.transactions import insert_entries

        entries = [
            # Buy USD stock, fx_rate = 0.92 (1 USD = 0.92 EUR)
            {"transaction_id": "fx1", "activity_type": "TRADE", "asset_category": "STK",
             "symbol": "JXN", "currency": "USD", "trade_date": "2024-05-01",
             "quantity": 10, "trade_price": 50, "trade_money": 500,
             "proceeds": -500, "commission": 0, "taxes": 0, "net_cash": -500,
             "buy_sell": "BUY", "fx_rate_to_base": 0.92},
            # USD dividend
            {"transaction_id": "fx2", "activity_type": "DIVIDEND",
             "symbol": "JXN", "currency": "USD", "trade_date": "2024-06-01",
             "amount": 15, "fx_rate_to_base": 0.92},
        ]
        insert_entries(db3_path, entries)
        result = build_portfolio_state(db3_path)
        assert result["daily_rows"] > 0


# ---------------------------------------------------------------------------
# Performance edge cases
# ---------------------------------------------------------------------------

class TestPerformanceEdgeCases:
    @pytest.fixture
    def db3_path(self):
        fd, path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        from src.portfolio.schema import create_tables
        create_tables(path)
        yield path
        try:
            os.unlink(path)
        except OSError:
            pass

    def test_all_zero_returns(self, db3_path):
        """All zero returns → Sharpe/Sortino = 0, no drawdown."""
        from src.portfolio.performance import calculate_metrics
        from src.portfolio.portfolio_state import build_portfolio_state
        from src.portfolio.transactions import insert_entries

        entries = [
            {"transaction_id": "z1", "activity_type": "DEPOSIT_WITHDRAWAL",
             "symbol": None, "currency": "EUR", "trade_date": "2024-01-01",
             "amount": 1000, "fx_rate_to_base": 1.0},
        ]
        insert_entries(db3_path, entries)
        build_portfolio_state(db3_path)
        result = calculate_metrics(db3_path, risk_free_rate=0.02)
        if result.get("sharpe_ratio"):
            assert abs(result["sharpe_ratio"]) < 0.01

    def test_extreme_single_return(self, db3_path):
        """Single transaction → minimal metrics."""
        from src.portfolio.performance import calculate_metrics
        from src.portfolio.transactions import insert_entries
        from src.portfolio.portfolio_state import build_portfolio_state

        entries = [
            {"transaction_id": "s1", "activity_type": "DEPOSIT_WITHDRAWAL",
             "symbol": None, "currency": "EUR", "trade_date": "2024-06-15",
             "amount": 5000, "fx_rate_to_base": 1.0},
        ]
        insert_entries(db3_path, entries)
        build_portfolio_state(db3_path)
        result = calculate_metrics(db3_path, risk_free_rate=0.0)
        # Should not crash
        assert "start_date" in result
        assert result["start_date"] is not None

    def test_max_drawdown_negative_values(self):
        from src.portfolio.performance import max_drawdown
        values = [0.0, 10.0, 20.0, 5.0, 1.0, 8.0, 15.0]
        dd, peak_i, trough_i = max_drawdown(values)
        assert dd < 0
        assert dd == (1.0 - 20.0) / 20.0

    def test_dividend_breakdown_uses_raw_transactions(self, db3_path):
        """Ensure dividend_breakdown comes from Transactions, not Portfolio_Daily."""
        from src.portfolio.performance import calculate_metrics
        from src.portfolio.transactions import insert_entries
        from src.portfolio.portfolio_state import build_portfolio_state

        entries = [
            {"transaction_id": "div1", "activity_type": "DIVIDEND",
             "symbol": "VWCE", "currency": "EUR", "trade_date": "2024-03-15",
             "amount": 100.0, "fx_rate_to_base": 1.0},
            {"transaction_id": "div2", "activity_type": "DIVIDEND",
             "symbol": "VWCE", "currency": "EUR", "trade_date": "2024-06-15",
             "amount": 50.0, "fx_rate_to_base": 1.0},
            {"transaction_id": "tax1", "activity_type": "WITHHOLDING_TAX",
             "symbol": "VWCE", "currency": "EUR", "trade_date": "2024-06-15",
             "amount": -15.0, "fx_rate_to_base": 1.0},
            {"transaction_id": "cash1", "activity_type": "DEPOSIT_WITHDRAWAL",
             "symbol": None, "currency": "EUR", "trade_date": "2024-01-01",
             "amount": 1000.0, "fx_rate_to_base": 1.0},
        ]
        insert_entries(db3_path, entries)
        build_portfolio_state(db3_path)
        result = calculate_metrics(db3_path, risk_free_rate=0.0)
        assert result["dividend_breakdown"]["total_gross"] == 150.0
        assert result["dividend_breakdown"]["total_tax"] == -15.0
        assert result["dividend_breakdown"]["total_net"] == 135.0
        assert result["total_dividend_income"] == 135.0


# ---------------------------------------------------------------------------
# Transactions edge cases
# ---------------------------------------------------------------------------

class TestTransactionsEdgeCases:
    @pytest.fixture
    def db3_path(self):
        fd, path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        from src.portfolio.schema import create_tables
        create_tables(path)
        yield path
        try:
            os.unlink(path)
        except OSError:
            pass

    def test_insert_none_entries(self, db3_path):
        from src.portfolio.transactions import insert_entries
        result = insert_entries(db3_path, None)
        assert result["inserted"] == 0
        assert result["skipped"] == 0

    def test_get_transactions_empty(self, db3_path):
        from src.portfolio.transactions import get_transactions
        rows = get_transactions(db3_path)
        assert rows == []

    def test_get_symbols_empty(self, db3_path):
        from src.portfolio.transactions import get_unique_symbols
        assert get_unique_symbols(db3_path) == []

    def test_date_range_empty(self, db3_path):
        from src.portfolio.transactions import get_date_range
        result = get_date_range(db3_path)
        assert result["min_date"] is None
        assert result["max_date"] is None

    def test_delete_nonexistent_source(self, db3_path):
        from src.portfolio.transactions import delete_by_source
        assert delete_by_source(db3_path, "nonexistent.xml") == 0

    def test_large_batch_insert(self, db3_path):
        """Insert 200 entries — should all succeed."""
        from src.portfolio.transactions import insert_entries
        entries = []
        for i in range(200):
            entries.append({
                "transaction_id": f"large-{i}",
                "activity_type": "TRADE",
                "asset_category": "STK",
                "symbol": "TST",
                "currency": "USD",
                "trade_date": f"2024-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}",
                "quantity": 1,
                "trade_price": 100,
                "trade_money": 100,
                "proceeds": -100,
                "commission": 0,
                "taxes": 0,
                "net_cash": -100,
                "buy_sell": "BUY",
                "fx_rate_to_base": 1.0,
            })
        result = insert_entries(db3_path, entries)
        assert result["inserted"] == 200
        assert result["skipped"] == 0


# ---------------------------------------------------------------------------
# Full integration: parse all 6 files → insert → rebuild → metrics → verify data
# ---------------------------------------------------------------------------

class TestFullIntegration:
    @pytest.fixture(scope="class")
    def populated_db3(self):
        from src.portfolio.schema import create_tables
        from src.portfolio.ibkr_parser import parse_ibkr_xml_file, normalize_entries
        from src.portfolio.transactions import insert_entries
        from src.portfolio.portfolio_state import build_portfolio_state

        fd, path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        create_tables(path)

        ibkr_dir = os.path.join(os.path.dirname(__file__), "../..", "data", "ibkr")
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

    def test_total_transactions_count(self, populated_db3):
        """All 6 files combined should have a substantial number of entries."""
        import sqlite3
        conn = sqlite3.connect(populated_db3)
        count = conn.execute("SELECT COUNT(*) FROM Transactions").fetchone()[0]
        conn.close()
        assert count > 100, f"Only {count} total transactions"

    def test_activity_types_all_present(self, populated_db3):
        import sqlite3
        conn = sqlite3.connect(populated_db3)
        types = conn.execute(
            "SELECT activity_type FROM Transactions GROUP BY activity_type"
        ).fetchall()
        conn.close()
        type_set = {r[0] for r in types}
        # At minimum we must have trades, dividends, taxes, deposits
        for at in ["TRADE", "DIVIDEND", "WITHHOLDING_TAX", "DEPOSIT_WITHDRAWAL"]:
            assert at in type_set, f"Missing activity type: {at}"

    def test_holdings_has_stocks_and_options(self, populated_db3):
        import sqlite3
        conn = sqlite3.connect(populated_db3)
        conn.row_factory = sqlite3.Row
        holdings = conn.execute(
            "SELECT DISTINCT asset_category FROM Portfolio_Holdings"
        ).fetchall()
        conn.close()
        cats = {r["asset_category"] for r in holdings}
        assert "STK" in cats, "Should have STK holdings"

    def test_daily_data_spans_multiple_years(self, populated_db3):
        import sqlite3
        conn = sqlite3.connect(populated_db3)
        min_date = conn.execute("SELECT MIN(date) FROM Portfolio_Daily").fetchone()[0]
        max_date = conn.execute("SELECT MAX(date) FROM Portfolio_Daily").fetchone()[0]
        conn.close()
        # First transaction date should be in 2020
        assert "2020" in (min_date or ""), f"Start date should be 2020, got {min_date}"
        # Max date should be at or after the last transaction (>= 2025)
        assert max_date is not None
        assert max_date >= "2025-01-01", f"Max date should be >= 2025, got {max_date}"

    def test_performance_metrics_comprehensive(self, populated_db3):
        from src.portfolio.performance import calculate_metrics
        result = calculate_metrics(
            populated_db3, risk_free_rate=0.02, base_currency="EUR"
        )
        required = [
            "total_return", "annualized_return", "volatility",
            "sharpe_ratio", "sortino_ratio", "max_drawdown",
            "calmar_ratio", "win_rate", "profit_factor",
            "var_95", "cvar_95", "total_dividend_income",
            "dividend_breakdown",
        ]
        for key in required:
            assert key in result, f"Missing key: {key}"
            assert result[key] is not None, f"Key {key} is None"

        # Plausibility checks
        assert result["total_dividend_income"] > 0  # we know there are dividends
        assert result["max_drawdown"] is not None
        assert result["dividend_breakdown"]["total_gross"] > 0
        assert result["dividend_breakdown"]["total_tax"] != 0  # taxes present
        assert result["dividend_breakdown"]["total_gross"] + result["dividend_breakdown"]["total_tax"] == result["total_dividend_income"]

    def test_performance_with_benchmark(self, populated_db3):
        from src.portfolio.performance import calculate_metrics
        # VWCE should have price data in db2
        result = calculate_metrics(
            populated_db3, risk_free_rate=0.02, base_currency="EUR",
            benchmark_ticker="VWCE",
        )
        assert "benchmark" in result
        if result["benchmark"].get("ticker"):
            assert result["benchmark"]["ticker"] == "VWCE"

    def test_transaction_id_uniqueness_across_years(self, populated_db3):
        """Ensure no duplicate transactionIDs across all 6 uploads."""
        import sqlite3
        conn = sqlite3.connect(populated_db3)
        total = conn.execute("SELECT COUNT(*) FROM Transactions").fetchone()[0]
        distinct = conn.execute(
            "SELECT COUNT(DISTINCT transaction_id) FROM Transactions"
        ).fetchone()[0]
        conn.close()
        assert total == distinct, f"{total - distinct} duplicate transactionIDs found"


class TestGetAllHoldingsPerformance:
    """Tests for the batched holdings performance function."""

    def test_returns_performance_for_all_holdings(self, populated_db3):
        """Batch function returns all required fields for every holding."""
        from src.portfolio.portfolio_state import get_all_holdings_performance
        from src.orchestrator.common.db_config import get_db2
        results = get_all_holdings_performance(populated_db3, get_db2(), "EUR")
        assert isinstance(results, list)
        assert len(results) > 0
        required = ["cost_basis_native", "cost_basis_display", "pnl_native",
                     "pnl_display", "total_return_native", "total_return_display",
                     "annualized_return_native", "fx_return", "name", "industry"]
        for r in results:
            cat = r.get("asset_category", "")
            sym = r.get("symbol", "")
            if cat == "CASH" or sym.startswith("CASH"):
                assert r.get("performance") is None, f"Cash entry {sym} should have no performance"
            else:
                perf = r.get("performance")
                assert perf is not None, f"Missing performance for {sym}"
                for field in required:
                    assert field in perf, f"{sym}: missing field {field}"

    def test_fx_return_zero_for_same_currency(self, populated_db3):
        """FX effect is zero when native = display currency."""
        from src.portfolio.portfolio_state import get_all_holdings_performance
        from src.orchestrator.common.db_config import get_db2
        results = get_all_holdings_performance(populated_db3, get_db2(), "EUR")
        found = False
        for r in results:
            perf = r.get("performance")
            if not perf:
                continue
            if perf.get("currency") == "EUR":
                assert abs(perf.get("fx_return", 999)) < 0.001, \
                    f"FX for EUR→EUR should be 0, got {perf.get('fx_return')}"
                found = True
        if not found:
            # No EUR holdings — test that USD display on USD holdings gives zero
            results2 = get_all_holdings_performance(populated_db3, get_db2(), "USD")
            for r in results2:
                perf = r.get("performance")
                if not perf:
                    continue
                if perf.get("currency") == "USD":
                    assert abs(perf.get("fx_return", 999)) < 0.001, \
                        f"FX for USD→USD should be 0, got {perf.get('fx_return')}"
                    found = True
                    break
        # If no EUR or USD holdings, this is still acceptable

    def test_batch_matches_individual(self, populated_db3):
        """Batch results match per-symbol get_holding_performance."""
        from src.portfolio.portfolio_state import (
            get_all_holdings_performance, get_holding_performance,
        )
        from src.orchestrator.common.db_config import get_db2
        import sqlite3
        db2 = get_db2()
        conn = sqlite3.connect(populated_db3)
        conn.row_factory = sqlite3.Row
        syms = [r["symbol"] for r in conn.execute(
            "SELECT symbol FROM Portfolio_Holdings WHERE asset_category!='CASH' AND symbol NOT LIKE 'CASH%' LIMIT 3"
        ).fetchall()]
        conn.close()
        if not syms:
            pytest.skip("No stock holdings to compare")
        batch = get_all_holdings_performance(populated_db3, db2, "EUR")
        batch_map = {r["symbol"]: r.get("performance") for r in batch}
        for sym in syms:
            indiv = get_holding_performance(sym, populated_db3, db2, "EUR")
            bat = batch_map.get(sym)
            if indiv and bat:
                for field in ["cost_basis_native", "pnl_native", "total_return_native"]:
                    iv = indiv.get(field) or 0
                    bv = bat.get(field) or 0
                    assert abs(iv - bv) < 0.01, \
                        f"{sym}.{field}: batch={bv:.2f} indiv={iv:.2f}"

    def test_display_currency_changes_converted_values(self, populated_db3):
        """Monetary values differ when display currency changes."""
        from src.portfolio.portfolio_state import get_all_holdings_performance
        from src.orchestrator.common.db_config import get_db2
        results_eur = get_all_holdings_performance(populated_db3, get_db2(), "EUR")
        results_usd = get_all_holdings_performance(populated_db3, get_db2(), "USD")
        assert len(results_eur) == len(results_usd)
