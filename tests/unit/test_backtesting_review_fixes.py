"""Regression tests for the backtesting review: each test pins one bug that was found and fixed."""

from __future__ import annotations

import sqlite3

import pandas as pd
import pytest

from src.backtesting.backtesting import (
    _build_portfolios,
    _build_rolling_aggregate,
    _discover_screening_periods,
    run_backtest_web,
)
from src.orchestrator.common.backtesting import (
    build_daily_portfolio_tracker,
    get_dividend_data,
    get_portfolio_benchmark_returns,
    get_portfolio_prices,
)


def _prices(rows):
    frame = pd.DataFrame(rows, columns=["Date", "Ticker", "Price"])
    frame["Date"] = pd.to_datetime(frame["Date"])
    return frame


def test_a_holding_that_starts_trading_later_keeps_its_capital_as_cash():
    # BBB has no price for the first two days. It used to be skipped while its
    # half of the capital still counted, so the portfolio "lost" 50% on day one.
    prices = _prices([
        ("2024-01-01", "AAA", 100.0), ("2024-01-02", "AAA", 100.0), ("2024-01-03", "AAA", 100.0), ("2024-01-04", "AAA", 100.0),
        ("2024-01-03", "BBB", 50.0), ("2024-01-04", "BBB", 50.0),
    ])
    tracker = build_daily_portfolio_tracker(prices, {"AAA": 0.5, "BBB": 0.5}, initial_capital=1000.0)
    daily = tracker["daily"]
    assert daily["portfolio_total"].tolist() == pytest.approx([1000.0, 1000.0, 1000.0])
    assert tracker["metrics"]["total_return"] == pytest.approx(0.0)
    assert daily["shares_BBB"].iloc[-1] == pytest.approx(10.0)
    assert any("BBB" in note and "2024-01-03" in note for note in tracker["notes"])


def test_a_holding_with_no_usable_price_stays_in_cash():
    prices = _prices([("2024-01-01", "AAA", 100.0), ("2024-01-02", "AAA", 110.0), ("2024-01-01", "BBB", 0.0), ("2024-01-02", "BBB", 0.0)])
    tracker = build_daily_portfolio_tracker(prices, {"AAA": 0.5, "BBB": 0.5}, initial_capital=1000.0)
    assert tracker["metrics"]["total_return"] == pytest.approx(0.05)


def test_dividends_for_periods_before_a_late_purchase_are_not_credited():
    prices = _prices([("2024-01-01", "AAA", 100.0), ("2024-03-01", "AAA", 100.0), ("2024-03-01", "BBB", 100.0), ("2024-06-01", "AAA", 100.0), ("2024-06-01", "BBB", 100.0)])
    dividends = pd.DataFrame({"Ticker": ["BBB", "BBB"], "periodEnd": pd.to_datetime(["2024-01-31", "2024-05-31"]), "PerShare_Dividends": [5.0, 2.0]})
    tracker = build_daily_portfolio_tracker(prices, {"AAA": 0.5, "BBB": 0.5}, dividends, initial_capital=1000.0)
    # 5 shares of BBB; only the May dividend falls after the March purchase.
    assert tracker["metrics"]["portfolio_dividend_return"] == pytest.approx(5 * 2.0 / 1000.0)


@pytest.fixture
def market(tmp_path):
    path = tmp_path / "market.db"
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE Stock_Prices (Date TEXT, Ticker TEXT, Currency TEXT, Price REAL);
        CREATE TABLE CompanyInfo (Company_Code TEXT, Company_Ticker TEXT, Company_Name TEXT);
        CREATE TABLE FinancialStatements (docID TEXT, Company_Code TEXT, periodEnd TEXT);
        CREATE TABLE ShareMetrics (docID TEXT, "Dividend paid per share" REAL);
        """
    )
    conn.executemany("INSERT INTO Stock_Prices VALUES (?, ?, 'JPY', ?)", [
        ("2024-01-04", "72030", 2500.0), ("2024-01-05", "72030", 2550.0), ("2024-06-28", "72030", 3000.0),
        ("2024-01-04", "TPX", 2400.0), ("2024-01-05", "TPX", 2450.0), ("2024-06-28", "TPX", 2700.0),
    ])
    conn.execute("INSERT INTO CompanyInfo VALUES ('E02144', '72030', 'Toyota')")
    conn.execute("INSERT INTO FinancialStatements VALUES ('D1', 'E02144', '2024-03-31')")
    conn.execute("INSERT INTO ShareMetrics VALUES ('D1', 60.0)")
    conn.commit()
    conn.close()
    return str(path)


def test_four_digit_tokyo_codes_find_prices_and_dividends_stored_with_the_check_digit(market):
    prices = get_portfolio_prices(market, "Stock_Prices", ["7203"], "2024-01-01", "2024-12-31")
    assert set(prices["Ticker"]) == {"7203"} and len(prices) == 3
    dividends = get_dividend_data(market, "ShareMetrics", "CompanyInfo", ["7203"], "2024-01-01", "2024-12-31")
    assert dividends[["Ticker", "PerShare_Dividends"]].values.tolist() == [["7203", 60.0]]


def test_a_benchmark_without_prices_is_reported_not_silently_dropped(market):
    result = run_backtest_web(market, {"7203": {"mode": "weight", "value": 1.0}}, "2024-01-01", "2024-12-31", benchmark_ticker="^TPX", initial_capital=1_000_000)
    assert result["metrics"]["benchmark_total_return"] is None
    assert any("'^TPX' has no stored prices" in warning for warning in result["warnings"])
    with_benchmark = run_backtest_web(market, {"7203": {"mode": "weight", "value": 1.0}}, "2024-01-01", "2024-12-31", benchmark_ticker="TPX", initial_capital=1_000_000)
    assert with_benchmark["metrics"]["benchmark_total_return"] == pytest.approx(2700 / 2400 - 1)


def test_the_portfolio_benchmark_reads_one_account_and_carries_fx_over_weekends(tmp_path, monkeypatch):
    db3 = tmp_path / "portfolio.db"
    conn = sqlite3.connect(db3)
    conn.execute("CREATE TABLE Portfolio_Daily (date TEXT, owner_user_id TEXT, total_value REAL, net_inflow REAL)")
    # Friday, Saturday, Monday for alice; bob's much larger values must not mix in.
    conn.executemany("INSERT INTO Portfolio_Daily VALUES (?, ?, ?, 0)", [
        ("2024-01-05", "alice", 100.0), ("2024-01-06", "alice", 100.0), ("2024-01-08", "alice", 110.0),
        ("2024-01-05", "bob", 9999.0), ("2024-01-06", "bob", 1.0), ("2024-01-08", "bob", 5.0),
    ])
    conn.commit()
    conn.close()
    monkeypatch.setattr("src.portfolio.currency.get_fx_series", lambda *_args, **_kwargs: {"2024-01-05": 160.0, "2024-01-08": 160.0})
    frame = get_portfolio_benchmark_returns(str(db3), "2024-01-01", "2024-01-31", "JPY", "unused", owner_user_id="alice")
    # Saturday has no FX row: it keeps Friday's rate instead of 1.0.
    assert frame["benchmark_return"].tolist() == pytest.approx([0.0, 0.10])


def test_information_ratio_uses_the_annualised_active_return(market):
    result = run_backtest_web(market, {"7203": {"mode": "weight", "value": 1.0}}, "2024-01-01", "2024-12-31", benchmark_ticker="TPX", initial_capital=1_000_000)
    metrics = result["metrics"]
    assert metrics["tracking_error"] > 0
    assert abs(metrics["information_ratio"]) < 50


def test_market_cap_weights_cover_only_the_selected_companies():
    screen = pd.DataFrame({"Company_Ticker": ["A", "B", "C"], "LatestPrice": [10.0, 10.0, 10.0], "SharesOutstanding": [1.0, 3.0, 1000.0]})
    portfolios, _ = _build_portfolios(["A", "B"], ["market_cap"], screen_df=screen, shares_outstanding_col="SharesOutstanding")
    assert portfolios["market_cap"] == {"A": {"mode": "weight", "value": 0.25}, "B": {"mode": "weight", "value": 0.75}}


def test_quarterly_screens_follow_the_calendar_even_with_missing_months(tmp_path):
    path = tmp_path / "fs.db"
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE FinancialStatements (periodEnd TEXT)")
    # No period ends in February or May.
    conn.executemany("INSERT INTO FinancialStatements VALUES (?)", [(f"2024-{month:02d}-28",) for month in (1, 3, 4, 6, 7, 8, 9, 10)])
    conn.commit()
    conn.close()
    assert _discover_screening_periods(str(path), "quarterly") == ["2024-01-01", "2024-04-01", "2024-07-01", "2024-10-01"]


def _run(total, start, end, **extra):
    metrics = {"total_return": total, "annualized_return": total, "sharpe_ratio": 1.0, "max_drawdown": -0.1, "start_date": start, "end_date": end, "benchmark_total_return": 0.0, **extra}
    return {"metrics": metrics, **{key: value for key, value in extra.items() if key in ("truncated", "no_data")}}


def test_truncated_and_empty_runs_stay_out_of_the_statistics():
    results = [
        {"period": "2020-01-01", "ticker_count": 3, "backtests": {"equal": {"1yr": _run(0.10, "2020-01-02", "2021-01-01")}}},
        {"period": "2024-06-01", "ticker_count": 3, "backtests": {"equal": {"1yr": {**_run(0.02, "2024-06-03", "2024-08-01"), "truncated": True}}}},
        {"period": "2024-07-01", "ticker_count": 3, "backtests": {"equal": {"1yr": {"metrics": {"total_return": 0.0, "no_data": True}, "no_data": True}}}},
    ]
    aggregate = _build_rolling_aggregate(results, ["1yr"], ["equal"], ["2020-01-01", "2024-06-01", "2024-07-01"], "TPX")
    assert aggregate["stats"]["total_return"]["mean"] == pytest.approx(0.10)
    assert (aggregate["complete_backtests"], aggregate["truncated"], aggregate["no_data"]) == (1, 1, 1)
    assert aggregate["benchmark_comparison"]["win_rate"] == 1.0
    assert aggregate["by_weighting"]["equal"]["1yr"]["count"] == 1


def test_csv_weights_may_be_percentages(monkeypatch):
    from src.backtesting import backtesting as bt

    seen = []

    def fake_run(**kwargs):
        seen.append(kwargs["portfolio"])
        metrics = {"total_return": 0.1, "annualized_return": 0.1, "portfolio_price_return": 0.1, "portfolio_dividend_return": 0.0,
                   "sharpe_ratio": 1.0, "max_drawdown": -0.05, "volatility": 0.1, "start_date": "2020-01-06", "end_date": "2020-12-30"}
        return {"metrics": metrics, "warnings": []}

    monkeypatch.setattr(bt, "run_backtest_web", fake_run)
    bt.run_backtest_set_web("unused.db", "Year,Tickers,Type,Amount\n2020,7203,weight,60\n2020,6758,weight,40\n2021,7203,shares,100\n", durations=["1yr"])
    assert seen[0] == {"7203": {"mode": "weight", "value": 0.6}, "6758": {"mode": "weight", "value": 0.4}}
    assert seen[1] == {"7203": {"mode": "shares", "value": 100.0}}


def test_only_tradeable_matches_take_the_top_slots():
    from src.backtesting.backtesting import _investable_tickers

    screen = pd.DataFrame({
        "Company_Ticker": ["", "59220", None, "88910", "13010", "59220"],
        "LatestPrice": [None, 6879.0, 100.0, 490.0, 1200.0, 6879.0],
        "PriceDate": [None, "2020-07-01", "2020-07-01", "2020-07-01", "2019-03-01", "2020-07-01"],
    })
    # No ticker, no price, and a price last seen over a year earlier are all skipped.
    assert _investable_tickers(screen, "Company_Ticker", "2020-07-01", 2) == (["59220", "88910"], 3)
