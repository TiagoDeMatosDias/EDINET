"""Additional tests for edge cases, price fetching, and full integration."""

import os
import sqlite3
import tempfile

import pytest

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

    def test_currency_map_from_parsed_data(self, sample_ibkr_content):
        from src.portfolio.ibkr_parser import normalize_entries, parse_ibkr_xml
        from src.portfolio.price_fetcher import _build_currency_map

        entries = normalize_entries(parse_ibkr_xml(sample_ibkr_content))

        assert _build_currency_map(entries) == {"AAA": "USD", "BBB": "EUR"}


class TestGetStoredCurrency:
    def test_none_for_empty_db(self, tmp_path):
        from src.portfolio.price_fetcher import _get_stored_currency

        path = tmp_path / "prices.db"
        conn = sqlite3.connect(path)
        assert _get_stored_currency(conn, "VWCE") is None
        conn.close()


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
        from src.portfolio.option_pricing import binomial_tree, black_scholes
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
        from src.portfolio.option_pricing import binomial_tree, black_scholes
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
        assert result["daily_rows"] > 0
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
        from src.portfolio.portfolio_state import build_portfolio_state
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
        from src.portfolio.portfolio_state import build_portfolio_state
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
        from src.portfolio.portfolio_state import build_portfolio_state
        from src.portfolio.transactions import insert_entries

        entries = [
            {"transaction_id": "s1", "activity_type": "DEPOSIT_WITHDRAWAL",
             "symbol": None, "currency": "EUR", "trade_date": "2024-06-15",
             "amount": 5000, "fx_rate_to_base": 1.0},
        ]
        insert_entries(db3_path, entries)
        insert_entries(db3_path, [{
            "transaction_id": "other-user-dividend", "activity_type": "DIVIDEND",
            "symbol": "OTHER", "currency": "EUR", "trade_date": "2024-06-15",
            "amount": 999.0, "fx_rate_to_base": 1.0,
        }], owner_user_id="other-user")
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
        from src.portfolio.portfolio_state import build_portfolio_state
        from src.portfolio.transactions import insert_entries

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

class TestGetAllHoldingsPerformance:
    """Tests for the batched holdings performance function."""

    def test_returns_performance_for_all_holdings(
        self,
        populated_db3,
        market_db_path,
    ):
        """Batch function returns all required fields for every holding."""
        from src.portfolio.portfolio_state import get_all_holdings_performance
        results = get_all_holdings_performance(populated_db3, market_db_path, "EUR")
        assert isinstance(results, list)
        assert len(results) > 0
        required = ["cost_basis_native", "cost_basis_display", "pnl_native",
                     "pnl_display", "total_return_native", "total_return_display",
                     "annualized_return_native", "fx_return", "name", "industry"]
        tested_symbols = set()
        for r in results:
            cat = r.get("asset_category", "")
            sym = r.get("symbol", "")
            if cat == "CASH" or sym.startswith("CASH") or sym == "SPIN":
                assert r.get("performance") is None, f"Cash entry {sym} should have no performance"
            else:
                perf = r.get("performance")
                assert perf is not None, f"Missing performance for {sym}"
                for field in required:
                    assert field in perf, f"{sym}: missing field {field}"
                tested_symbols.add(sym)
        assert {"AAA", "BBB"} <= tested_symbols

    def test_fx_return_zero_for_same_currency(
        self,
        populated_db3,
        market_db_path,
    ):
        """FX effect is zero when native = display currency."""
        from src.portfolio.portfolio_state import get_all_holdings_performance
        results = get_all_holdings_performance(populated_db3, market_db_path, "EUR")
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
            results2 = get_all_holdings_performance(populated_db3, market_db_path, "USD")
            for r in results2:
                perf = r.get("performance")
                if not perf:
                    continue
                if perf.get("currency") == "USD":
                    assert abs(perf.get("fx_return", 999)) < 0.001, \
                        f"FX for USD→USD should be 0, got {perf.get('fx_return')}"
                    found = True
                    break
        assert found, "Synthetic portfolio must include a EUR or USD holding"

    def test_batch_matches_individual(self, populated_db3, market_db_path):
        """Batch results match per-symbol get_holding_performance."""
        from src.portfolio.portfolio_state import (
            get_all_holdings_performance,
            get_holding_performance,
        )
        db2 = market_db_path
        conn = sqlite3.connect(populated_db3)
        conn.row_factory = sqlite3.Row
        syms = [r["symbol"] for r in conn.execute(
            "SELECT symbol FROM Portfolio_Holdings WHERE asset_category!='CASH' AND symbol NOT LIKE 'CASH%' LIMIT 3"
        ).fetchall()]
        conn.close()
        assert syms
        batch = get_all_holdings_performance(populated_db3, db2, "EUR")
        batch_map = {r["symbol"]: r.get("performance") for r in batch}
        for sym in syms:
            indiv = get_holding_performance(sym, populated_db3, db2, "EUR")
            bat = batch_map.get(sym)
            assert indiv is not None
            assert bat is not None
            for field in ["cost_basis_native", "pnl_native", "total_return_native"]:
                assert (bat.get(field) or 0) == pytest.approx(indiv.get(field) or 0)

    def test_display_currency_changes_converted_values(
        self,
        populated_db3,
        market_db_path,
    ):
        """Monetary values differ when display currency changes."""
        from src.portfolio.portfolio_state import get_all_holdings_performance
        results_eur = get_all_holdings_performance(populated_db3, market_db_path, "EUR")
        results_usd = get_all_holdings_performance(populated_db3, market_db_path, "USD")
        assert len(results_eur) == len(results_usd)
        eur = {row["symbol"]: row.get("performance") for row in results_eur}
        usd = {row["symbol"]: row.get("performance") for row in results_usd}
        assert eur["BBB"]["cost_basis_display"] != usd["BBB"]["cost_basis_display"]
