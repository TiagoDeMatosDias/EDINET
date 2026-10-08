"""Accuracy review: dividends on the price basis, and yearly figures that add up."""

from __future__ import annotations

import sqlite3

import pandas as pd
import pytest

from src.backtesting.backtesting import _build_portfolios
from src.orchestrator.common.backtesting import build_daily_portfolio_tracker, get_dividend_data
from src.orchestrator.common.corporate_actions import (
    AnnualFacts,
    SplitEvent,
    adjust_payments_for_splits,
    dividend_payments,
    infer_share_count_splits,
    share_count_basis_factors,
    split_factor_at,
    standard_split_multiplier,
)

T = pd.Timestamp


def _prices(rows):
    frame = pd.DataFrame(rows, columns=["Date", "Ticker", "Price"])
    frame["Date"] = pd.to_datetime(frame["Date"])
    return frame


def test_share_counts_reveal_standard_splits_only():
    assert standard_split_multiplier(5.0) == 5.0
    assert standard_split_multiplier(1.985) == 2.0  # a 2-for-1 split with a small buyback
    assert standard_split_multiplier(0.1) == 0.1  # a 10-to-1 consolidation
    assert standard_split_multiplier(1.21) is None  # a small issue is not a 6-for-5 split
    assert standard_split_multiplier(1.2) == 1.2
    assert standard_split_multiplier(1.7) is None
    events = infer_share_count_splits([(T("2020-03-31"), 1_000.0), (T("2021-03-31"), 1_002.0), (T("2022-03-31"), 5_010.0)])
    assert events == [SplitEvent(T("2021-03-31"), T("2022-03-31"), 5.0, "annual reports")]


def test_a_split_with_cancellations_needs_book_value_to_confirm_it():
    # Sony 2025: 5-for-1 with cancelled treasury shares, a share-count ratio of 4.88.
    split = [AnnualFacts(T("2024-03-31"), 1_261_231_889, 2661.69), AnnualFacts(T("2025-03-31"), 6_149_810_645, 540.61)]
    assert [event.multiplier for event in infer_share_count_splits(split)] == [5.0]
    # The same ratio from a merger leaves book value per share where it was.
    merger = [AnnualFacts(T("2024-03-31"), 1_000_000, 2000.0), AnnualFacts(T("2025-03-31"), 4_880_000, 2100.0)]
    assert infer_share_count_splits(merger) == []
    # Without per-share evidence a near miss is not trusted.
    assert infer_share_count_splits([AnnualFacts(T("2024-03-31"), 1_000_000), AnnualFacts(T("2025-03-31"), 4_880_000)]) == []


def test_annual_dividends_become_interim_and_final_payments():
    rows = pd.DataFrame({"Ticker": ["A", "B"], "periodEnd": pd.to_datetime(["2022-03-31", "2022-12-31"]), "PerShare_Dividends": [148.0, 30.0], "Interim_Dividends": [120.0, 0.0]})
    payments = dividend_payments(rows)
    assert payments[["Ticker", "periodEnd", "PerShare_Dividends", "payment"]].values.tolist() == [
        ["A", T("2021-09-30"), 120.0, "interim"],
        ["A", T("2022-03-31"), 28.0, "final"],
        ["B", T("2022-12-31"), 30.0, "final"],
    ]


def test_payments_before_a_split_are_divided_by_it():
    # Toyota: a 5-for-1 split between the 2021 and 2022 year ends, the
    # interim of 2022 still paid on the old shares.
    rows = pd.DataFrame({
        "Ticker": ["72030"] * 3,
        "periodEnd": pd.to_datetime(["2021-03-31", "2022-03-31", "2023-03-31"]),
        "PerShare_Dividends": [240.0, 148.0, 60.0],
        "Interim_Dividends": [105.0, 120.0, 25.0],
    })
    events = {"72030": [SplitEvent(T("2021-03-31"), T("2022-03-31"), 5.0, "annual reports")]}
    adjusted = adjust_payments_for_splits(dividend_payments(rows), events)
    assert adjusted["PerShare_Dividends"].round(6).tolist() == [21.0, 27.0, 24.0, 28.0, 25.0, 35.0]
    assert adjusted["reported_per_share"].tolist() == [105.0, 135.0, 120.0, 28.0, 25.0, 35.0]
    # Prices still on the raw basis around the split keep the dividends as paid.
    raw = adjust_payments_for_splits(dividend_payments(rows), events, raw_events={("72030", T("2022-03-31"))})
    assert raw["PerShare_Dividends"].tolist() == raw["reported_per_share"].tolist()


def test_an_exactly_dated_split_divides_only_earlier_payments():
    payments = pd.DataFrame({"Ticker": ["X", "X"], "periodEnd": pd.to_datetime(["2024-03-31", "2024-09-30"]), "PerShare_Dividends": [40.0, 10.0], "fiscal_period_end": pd.to_datetime(["2024-03-31", "2025-03-31"]), "payment": ["final", "interim"]})
    events = {"X": [SplitEvent(T("2024-06-30"), T("2024-07-01"), 4.0, "Stock_Splits")]}
    assert adjust_payments_for_splits(payments, events)["PerShare_Dividends"].tolist() == [10.0, 10.0]
    assert split_factor_at(events["X"], T("2024-03-31")) == 0.25
    assert split_factor_at(events["X"], T("2024-12-31")) == 1.0


@pytest.fixture
def split_market(tmp_path):
    """A company that split 5-for-1 in its 2022 fiscal year, with adjusted prices."""
    path = tmp_path / "market.db"
    conn = sqlite3.connect(path)
    conn.executescript("""
        CREATE TABLE CompanyInfo (Company_Code TEXT, Company_Ticker TEXT);
        CREATE TABLE FinancialStatements (docID TEXT PRIMARY KEY, Company_Code TEXT, docTypeCode TEXT, submitDateTime TEXT, periodEnd TEXT);
        CREATE TABLE ShareMetrics (docID TEXT PRIMARY KEY, "Dividend paid per share" REAL, "Interim dividend paid per share" REAL,
            "Number of issued shares as of fiscal year end" REAL, "Net assets per share" REAL);
        CREATE TABLE Stock_Prices (Date TEXT, Ticker TEXT, Currency TEXT, Price REAL, Adjusted_Price REAL);
        INSERT INTO CompanyInfo VALUES ('E1', '72030');
        INSERT INTO FinancialStatements VALUES ('D20', 'E1', '030000', '2020-06-20 10:00', '2020-03-31'),
            ('D21', 'E1', '030000', '2021-06-20 10:00', '2021-03-31'), ('D22', 'E1', '030000', '2022-06-20 10:00', '2022-03-31');
        INSERT INTO ShareMetrics VALUES ('D20', 220, 100, 1000, 4000), ('D21', 240, 105, 1000, 4400), ('D22', 148, 120, 5000, 1000);
    """)
    days = pd.bdate_range("2020-04-01", "2022-06-30")
    conn.executemany("INSERT INTO Stock_Prices VALUES (?, '72030', 'JPY', 1000.0, 1000.0)", [(day.strftime("%Y-%m-%d"),) for day in days])
    conn.commit()
    conn.close()
    return str(path)


def test_dividends_come_back_on_the_adjusted_share_basis(split_market):
    dividends = get_dividend_data(split_market, "ShareMetrics", "CompanyInfo", ["72030"], "2020-04-01", "2022-06-30")
    assert dividends[["periodEnd", "PerShare_Dividends", "payment"]].values.tolist() == [
        [T("2020-09-30"), 21.0, "interim"],
        [T("2021-03-31"), 27.0, "final"],
        [T("2021-09-30"), 24.0, "interim"],
        [T("2022-03-31"), 28.0, "final"],
    ]
    assert dividends["split_factor"].tolist() == [0.2, 0.2, 0.2, 1.0]


def test_market_caps_use_the_share_count_basis(split_market):
    conn = sqlite3.connect(split_market)
    try:
        assert share_count_basis_factors(conn, ["72030"], "2021-09-01") == {"72030": 0.2}
        assert share_count_basis_factors(conn, ["72030"], "2022-09-01") == {}
    finally:
        conn.close()
    screen = pd.DataFrame({"Ticker": ["72030", "BIG"], "LatestPrice": [1000.0, 1000.0], "Shares": [1000.0, 5000.0]})
    portfolios, _ = _build_portfolios(["72030", "BIG"], ["market_cap"], screen_df=screen, shares_outstanding_col="Shares", ticker_col="Ticker", share_basis_factors={"72030": 0.2})
    weights = {ticker: spec["value"] for ticker, spec in portfolios["market_cap"].items()}
    # 1,000 shares at ¥5,000 as traded is the same size as 5,000 at ¥1,000.
    assert weights == pytest.approx({"72030": 0.5, "BIG": 0.5})


def test_yearly_rows_run_from_the_previous_close_and_compound_to_the_total():
    prices = _prices([
        ("2022-12-28", "A", 100.0), ("2022-12-30", "A", 110.0),
        ("2023-01-04", "A", 121.0), ("2023-12-29", "A", 121.0),
        ("2022-12-28", "B", 100.0), ("2022-12-30", "B", 100.0),
        ("2023-01-04", "B", 100.0), ("2023-12-29", "B", 90.0),
    ])
    tracker = build_daily_portfolio_tracker(prices, {"A": 0.5, "B": 0.5}, initial_capital=1000.0)
    rows = {(row["Year"], row["Ticker"]): row for row in tracker["per_company_per_year"].to_dict("records")}
    assert rows[(2023, "A")]["Start_Price"] == 110.0  # the 2022 close, not the first 2023 price
    assert rows[(2023, "A")]["Start_Date"] == "2022-12-30"
    assert rows[(2023, "A")]["Price_Return_Pct"] == pytest.approx(0.10)
    # Contributions use the weights after 2022's drift: A is 550 of 1,050.
    assert rows[(2023, "A")]["Weighted_Return"] == pytest.approx(550 / 1050 * 0.10)
    assert rows[(2023, "B")]["Weighted_Return"] == pytest.approx(500 / 1050 * -0.10)
    by_year = {}
    for (year, _ticker), row in rows.items():
        by_year[year] = by_year.get(year, 0.0) + row["Weighted_Return"]
    growth = (1 + by_year[2022]) * (1 + by_year[2023])
    assert growth - 1 == pytest.approx(tracker["metrics"]["total_return"])
    assert tracker["metrics"]["start_date"] == "2022-12-28"


def test_yearly_dividends_match_what_the_daily_series_credited():
    prices = _prices([("2023-01-04", "A", 100.0), ("2023-06-01", "A", 100.0), ("2023-12-29", "A", 100.0), ("2023-01-04", "B", 100.0), ("2023-12-29", "B", 100.0)])
    # Both of A's payments fall in 2023; the yearly row reports exactly what was credited.
    dividends = pd.DataFrame({"Ticker": ["A", "A"], "periodEnd": pd.to_datetime(["2023-03-31", "2023-09-30"]), "PerShare_Dividends": [2.0, 3.0]})
    tracker = build_daily_portfolio_tracker(prices, {"A": 1.0}, dividends, initial_capital=1000.0)
    row = next(row for row in tracker["per_company_per_year"].to_dict("records") if row["Ticker"] == "A")
    assert row["Dividend_Per_Share"] == 5.0
    assert row["Total_Dividends_Received"] == pytest.approx(tracker["daily"]["dividend_event"].sum())


def test_a_split_restated_before_the_share_count_moves_is_found():
    # Shin-Etsu: 5-for-1 effective 1 April 2023 with cancellations (ratio 4.94);
    # the 2023 report already restated book value per share.
    history = [
        AnnualFacts(T("2022-03-31"), 416_662_793, 8007.24),
        AnnualFacts(T("2023-03-31"), 404_824_593, 1918.37),
        AnnualFacts(T("2024-03-31"), 1_900_000_000, 2133.17),  # a near miss: 4.69
    ]
    assert [(event.after, event.multiplier) for event in infer_share_count_splits(history)] == [(T("2023-03-31"), 5.0)]


def test_per_share_figures_follow_the_filing_basis(split_market):
    from src.orchestrator.common.corporate_actions import filing_basis_factors

    conn = sqlite3.connect(split_market)
    try:
        factors = {doc: (restated, fiscal) for doc, restated, fiscal in filing_basis_factors(conn)}
    finally:
        conn.close()
    # Reports before the split are on the old shares; the split year's report is not.
    assert factors == {"D20": (0.2, 0.2), "D21": (0.2, 0.2)}


def test_screens_compare_prices_with_per_share_figures_on_one_basis(split_market):
    from src.screening.screening import build_screening_query, install_share_basis

    criteria = [{
        "comparison_mode": "full_expression",
        "left_side": [{"type": "column", "table": "Stock_Prices", "column": "Price"}, {"type": "op", "op": "/"}, {"type": "column", "table": "ShareMetrics", "column": "Net assets per share"}],
        "operator": "<=", "right_side": [{"type": "value", "value": 1}],
    }]
    sql, params = build_screening_query(criteria, ["CompanyInfo.Company_Ticker", "ShareMetrics.Net assets per share"], screening_date="2021-09-01", use_adjusted_price=True)
    assert "temp.share_basis" in sql and sql.count("COALESCE(sb.[restated], 1.0)") >= 2
    conn = sqlite3.connect(split_market)
    try:
        install_share_basis(conn, split_market)
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()
    # Adjusted price 1,000 against book value 4,400 per old share is P/B 0.23
    # unadjusted; on one basis (880 per new share) it is 1.14 and fails.
    assert rows == []
