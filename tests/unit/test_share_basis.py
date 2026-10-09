"""Per-share figures on one share basis: rolling windows, history, and the overview."""

from __future__ import annotations

import json
import math
import sqlite3
from unittest.mock import patch

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from src.orchestrator.common.corporate_actions import load_split_events
from src.orchestrator.generate_rolling_metrics import service as rolling
from src.security_analysis import get_security_statements
from src.web_app.server import app

client = TestClient(app)

EPS = "Basic earnings (loss) per share"
DPS = "Dividend paid per share"
INTERIM = "Interim dividend paid per share"
YEAR_END_SHARES = "Number of issued shares as of fiscal year end"
FILING_SHARES = "Number of issued shares as of filing date"

# A March year end and a 5-for-1 split on 1 October 2021, inside the year to
# March 2022: earnings per share grew 10 a year on today's shares (100 to
# 150), and as filed read 500 … 650, then 140. The 2022 dividend is 130 paid
# before the split and 27 after (53 on today's shares).
YEARS = [
    # docID, year end, filed, EPS, DPS, interim, year-end shares, filing-date shares, sales per share
    ("D18", "2018-03-31", "2018-06-25", 500.0, 200.0, 100.0, 1000.0, 1000.0, 4000.0),
    ("D19", "2019-03-31", "2019-06-25", 550.0, 220.0, 110.0, 1000.0, 1000.0, 4200.0),
    ("D20", "2020-03-31", "2020-06-25", 600.0, 240.0, 120.0, 1000.0, 1000.0, 4400.0),
    ("D21", "2021-03-31", "2021-06-25", 650.0, 250.0, 125.0, 1000.0, 1000.0, 4600.0),
    ("D22", "2022-03-31", "2022-06-25", 140.0, 157.0, 130.0, 5000.0, 5000.0, 960.0),
    ("D23", "2023-03-31", "2023-06-25", 150.0, 56.0, 28.0, 5000.0, 5000.0, 1000.0),
]


@pytest.fixture
def split_db(tmp_path):
    path = str(tmp_path / "split.db")
    conn = sqlite3.connect(path)
    conn.executescript(f"""
        CREATE TABLE CompanyInfo (Company_Code TEXT PRIMARY KEY, Company_Name TEXT, [Submitter Name] TEXT,
            Company_Industry TEXT, Company_Ticker TEXT, Listed TEXT);
        CREATE TABLE FinancialStatements (docID TEXT PRIMARY KEY, Company_Code TEXT, docTypeCode TEXT,
            submitDateTime TEXT, periodEnd TEXT);
        CREATE TABLE ShareMetrics (docID TEXT PRIMARY KEY, [{EPS}] REAL, [{DPS}] REAL, [{INTERIM}] REAL,
            [{YEAR_END_SHARES}] REAL, [{FILING_SHARES}] REAL, [Net assets per share] REAL, [Number of employees] REAL);
        CREATE TABLE PerShare_Metrics (docID TEXT PRIMARY KEY, [Sales Per Share] REAL);
        CREATE TABLE IncomeStatement (docID TEXT PRIMARY KEY, [Net sales] REAL);
        CREATE TABLE Stock_Prices (Date TEXT, Ticker TEXT, Currency TEXT, Price REAL, PRIMARY KEY (Date, Ticker));
        CREATE TABLE Stock_Splits (id INTEGER PRIMARY KEY, ticker TEXT, split_date TEXT, ratio_from REAL, ratio_to REAL,
            confirmation TEXT, superseded_by INTEGER);
        INSERT INTO CompanyInfo VALUES ('E1', 'Split Motor', 'Split Motor', 'Autos', '72030', 'TSE');
        INSERT INTO Stock_Prices VALUES ('2023-09-29', '72030', 'JPY', 1500.0);
    """)
    for doc, end, filed, eps, dps, interim, year_end, filing, sales in YEARS:
        conn.execute("INSERT INTO FinancialStatements VALUES (?, 'E1', '030000', ?, ?)", (doc, f"{filed} 09:00", end))
        conn.execute("INSERT INTO ShareMetrics VALUES (?, ?, ?, ?, ?, ?, NULL, 100)", (doc, eps, dps, interim, year_end, filing))
        conn.execute("INSERT INTO PerShare_Metrics VALUES (?, ?)", (doc, sales))
        conn.execute("INSERT INTO IncomeStatement VALUES (?, ?)", (doc, sales * year_end))
    conn.commit()
    conn.close()
    return path


def _missing(value) -> bool:
    return value is None or math.isnan(value)


def _run_rolling(path, tmp_path, spec):
    config = tmp_path / "rolling_metrics.json"
    config.write_text(json.dumps(spec), encoding="utf-8")
    with patch.object(rolling, "ROLLING_METRICS_CONFIG_PATH", str(config)):
        rolling.generate_rolling_metrics(source_database=path, target_database=path, overwrite=True)
    conn = sqlite3.connect(path)
    try:
        return {table: pd.read_sql_query(f'SELECT * FROM "{table}"', conn).set_index("docID") for table in (rolling.rolling_table_name(name) for name in spec)}
    finally:
        conn.close()


def test_rolling_figures_read_through_a_split(split_db, tmp_path):
    tables = _run_rolling(split_db, tmp_path, {"ShareMetrics": [EPS, DPS, YEAR_END_SHARES], "PerShare_Metrics": ["Sales Per Share"]})
    shares = tables["ShareMetrics_Rolling"]
    # Growth compares the years on today's shares: 100 → 150 over five years.
    assert shares.loc["D23", f"{EPS}_Growth_5_Year"] == pytest.approx(1.5 ** (1 / 5) - 1)
    assert shares.loc["D22", f"{EPS}_Growth_2_Year"] == pytest.approx(math.sqrt(140 / 120) - 1)
    # Averages are stored on each filing's own basis, like the figures they average.
    assert shares.loc["D23", f"{EPS}_Average_3_Year"] == pytest.approx(140.0)
    assert shares.loc["D21", f"{EPS}_Average_3_Year"] == pytest.approx(600.0)
    # The split year's dividend counts as 53 (130 before the split, 27 after).
    assert shares.loc["D22", f"{DPS}_Average_2_Year"] == pytest.approx((50 + 53) / 2)
    assert shares.loc["D23", f"{DPS}_Growth_2_Year"] == pytest.approx(math.sqrt(56 / 50) - 1)
    # The share count did not grow: the split is not an issue of shares.
    assert shares.loc["D23", f"{YEAR_END_SHARES}_Growth_5_Year"] == pytest.approx(0.0)
    per_share = tables["PerShare_Metrics_Rolling"]
    assert per_share.loc["D23", "Sales Per Share_Growth_3_Year"] == pytest.approx((1000 / 880) ** (1 / 3) - 1)


def test_a_multi_year_figure_needs_every_year_of_its_window(split_db, tmp_path):
    conn = sqlite3.connect(split_db)
    conn.execute("DELETE FROM FinancialStatements WHERE docID = 'D20'")
    conn.commit()
    conn.close()
    shares = _run_rolling(split_db, tmp_path, {"ShareMetrics": [EPS]})["ShareMetrics_Rolling"]
    # Four years of history is not a five-year average.
    assert _missing(shares.loc["D21", f"{EPS}_Average_5_Year"])
    assert _missing(shares.loc["D18", f"{EPS}_Average_2_Year"])
    # With the 2020 report missing, 2019-2021 is not three years of figures …
    assert _missing(shares.loc["D21", f"{EPS}_Average_3_Year"])
    assert shares.loc["D22", f"{EPS}_Average_2_Year"] == pytest.approx(135.0)
    # … but growth still compares two year ends three years apart.
    assert shares.loc["D22", f"{EPS}_Growth_3_Year"] == pytest.approx((140 / 110) ** (1 / 3) - 1)


def test_history_reads_per_share_lines_on_todays_shares(split_db):
    history = get_security_statements(split_db, "E1", periods=10, statement_sources={"share": "ShareMetrics", "income": "IncomeStatement"})
    rows = {row["field"]: row for row in history["ShareMetrics"]}
    assert rows[EPS]["values"] == pytest.approx([100.0, 110.0, 120.0, 130.0, 140.0, 150.0])
    assert rows[EPS]["reported_values"] == [500.0, 550.0, 600.0, 650.0, 140.0, 150.0]
    assert rows[DPS]["values"][4] == pytest.approx(53.0)
    assert rows[FILING_SHARES]["values"] == pytest.approx([5000.0] * 6)
    assert "reported_values" not in rows["Number of employees"]
    assert all("reported_values" not in row for row in history["IncomeStatement"])
    assert history["share_basis"]["splits"] == [
        {"date": "2022-03-31", "after": "2021-03-31", "multiplier": 5.0, "source": "share counts"},
    ]


def test_the_history_api_returns_both_bases(split_db, monkeypatch):
    import src.web_app.api.security_analysis as api

    monkeypatch.setattr(api, "get_db2", lambda: split_db)
    data = client.get("/api/security/history", params={"company_code": "E1", "periods": 10}).json()
    eps = next(metric for metric in data["tables"]["ShareMetrics"]["metrics"] if metric["field"] == EPS)
    assert eps["values"][0] == pytest.approx(100.0)
    assert eps["reported_values"][0] == 500.0
    assert data["share_basis"]["splits"][0]["multiplier"] == 5.0


def test_valuations_use_the_latest_filing_on_todays_shares(split_db, monkeypatch):
    import src.web_app.api.security_analysis as api

    # A 2-for-1 split traded ex on 2 October 2023, after the latest report.
    conn = sqlite3.connect(split_db)
    conn.execute("INSERT INTO Stock_Splits VALUES (1, '72030', '2023-10-02', 1, 2, 'confirmed', NULL)")
    conn.commit()
    conn.close()
    monkeypatch.setattr(api, "get_db2", lambda: split_db)
    metrics = client.get("/api/security/overview", params={"company_code": "E1"}).json()["metrics"]
    assert metrics["PERatio"] == pytest.approx(1500.0 / 75.0)
    assert metrics["SharesOutstanding"] == pytest.approx(10_000.0)


def test_squeeze_outs_and_duplicate_records_are_not_splits(split_db):
    conn = sqlite3.connect(split_db)
    conn.executemany("INSERT INTO Stock_Splits VALUES (?, '72030', ?, ?, ?, 'confirmed', NULL)", [
        (1, "2023-10-02", 1, 2),
        (2, "2023-10-05", 1, 2),  # the same split recorded again
        (3, "2025-09-29", 20_000_000, 1),  # a squeeze-out before delisting
    ])
    try:
        events = load_split_events(conn, ["72030"])["72030"]
    finally:
        conn.close()
    assert [(event.until.strftime("%Y-%m-%d"), event.multiplier) for event in events] == [("2022-03-31", 5.0), ("2023-10-02", 2.0)]


def test_a_trust_bank_reads_its_own_reports(split_db, tmp_path):
    # Reports filed for the bank's trusts share its EDINET code.
    conn = sqlite3.connect(split_db)
    for index, end in enumerate(["2021-09-30", "2022-09-30", "2023-01-15"]):
        conn.execute("INSERT INTO FinancialStatements VALUES (?, 'E1', '07A000', '2023-01-01 09:00', ?)", (f"T{index}", end))
        conn.execute("INSERT INTO IncomeStatement VALUES (?, 1.0)", (f"T{index}",))
    conn.commit()
    conn.close()
    history = get_security_statements(split_db, "E1", periods=10, statement_sources={"income": "IncomeStatement"})
    assert history["periods"] == [end for _doc, end, *_ in YEARS]
    income = _run_rolling(split_db, tmp_path, {"IncomeStatement": ["Net sales"]})["IncomeStatement_Rolling"]
    assert "T0" not in income.index
    assert income.loc["D23", "Net sales_Average_2_Year"] == pytest.approx((4_800_000 + 5_000_000) / 2)


def test_a_split_between_the_year_end_and_the_report_shows_in_the_report(split_db):
    from src.orchestrator.common.corporate_actions import filing_basis_factors

    # A 2-for-1 split in April 2023: the June 2023 report counts 5,000 shares
    # at the March year end and 10,000 at filing, with EPS already restated.
    conn = sqlite3.connect(split_db)
    conn.execute(f"UPDATE ShareMetrics SET [{FILING_SHARES}] = 10000 WHERE docID = 'D23'")
    conn.commit()
    try:
        events = load_split_events(conn, ["72030"])["72030"]
        basis = {row.doc_id: row for row in filing_basis_factors(conn)}
        assert [(event.after.strftime("%Y-%m-%d"), event.until.strftime("%Y-%m-%d"), event.multiplier, event.source) for event in events][-1] == (
            "2023-03-31", "2023-06-25", 2.0, "filing date count",
        )
        assert (basis["D23"].restated, basis["D23"].fiscal) == (1.0, 0.5)
        # The next year-end count shows the same split; it is not counted twice.
        conn.execute("INSERT INTO FinancialStatements VALUES ('D24', 'E1', '030000', '2024-06-25 09:00', '2024-03-31')")
        conn.execute("INSERT INTO ShareMetrics VALUES ('D24', 80, 30, 15, 10000, 10000, NULL, 100)")
        conn.commit()
        assert [event.multiplier for event in load_split_events(conn, ["72030"])["72030"]] == [5.0, 2.0]
    finally:
        conn.close()


def test_a_consolidation_in_a_merger_year_is_confirmed_by_the_stored_prices():
    from src.orchestrator.common.corporate_actions import AnnualFacts, infer_share_count_splits

    T = pd.Timestamp
    # 10-to-1 with a merger issue: 8.07 times fewer shares. The stored prices
    # were multiplied by ten before it: stored ÷ traded fell from 1.86 to 0.19.
    merged = [AnnualFacts(T("2017-03-31"), 377_544_000, 215.03, price_factor=541.1 / 290.3), AnnualFacts(T("2018-03-31"), 46_805_000, 2707.51, price_factor=651.9 / 3428.7)]
    assert [event.multiplier for event in infer_share_count_splits(merged)] == [0.1]
    # Without the prices, a count 24 % off the ratio is not a consolidation.
    unpriced = [AnnualFacts(T("2017-03-31"), 377_544_000, 215.03), AnnualFacts(T("2018-03-31"), 46_805_000, 2707.51)]
    assert infer_share_count_splits(unpriced) == []


def test_a_count_that_moved_by_a_split_ratio_without_the_price_is_no_split():
    from src.orchestrator.common.corporate_actions import (
        AnnualFacts,
        SplitEvent,
        _price_denies,
        _traded_price,
    )

    T = pd.Timestamp
    event = SplitEvent(T("2020-06-30"), T("2021-06-30"), 2.0, "annual reports")
    # 2.08 times the shares in a loss year with no P/E either side (an issue
    # at a low price): the reports a year further out show stored and traded
    # prices still together, so the provider adjusted for no split.
    issued = [
        AnnualFacts(T("2019-06-30"), 13_518_600, 702.38, price_factor=0.84),
        AnnualFacts(T("2020-06-30"), 13_601_000, 319.92, net_assets=4_404_183_000),
        AnnualFacts(T("2021-06-30"), 28_306_000, 115.83, net_assets=3_278_730_000),
        AnnualFacts(T("2022-06-30"), 28_306_000, 127.08, price_factor=0.95),
    ]
    assert _price_denies(event, issued)
    # Another split between those reports moves the price too: no verdict.
    assert not _price_denies(event, issued, [SplitEvent(T("2021-12-27"), T("2021-12-28"), 3.0, "Stock_Splits")])
    # A 2-for-1 split the provider adjusted for: stored over traded doubles.
    split = [
        AnnualFacts(T("2020-06-30"), 13_601_000, 319.92, price_factor=0.5, net_assets=4_351_000_000),
        AnnualFacts(T("2021-06-30"), 27_202_000, 160.0, price_factor=1.0),
    ]
    assert not _price_denies(event, split)
    # Effective after the 2020 year end and restated in that report already
    # (book value per share on twice the year-end shares): its price is no
    # evidence, and the 2019 report's is on the old basis.
    early = [
        AnnualFacts(T("2019-06-30"), 13_601_000, 640.0, price_factor=0.5),
        AnnualFacts(T("2020-06-30"), 13_601_000, 160.0, price_factor=1.0, net_assets=4_351_000_000),
        AnnualFacts(T("2021-06-30"), 27_202_000, 170.0, price_factor=1.0),
    ]
    assert not _price_denies(event, early)
    # Without prices on both sides the share counts decide, as before.
    assert not _price_denies(event, [AnnualFacts(f.period_end, f.shares) for f in issued])
    # A loss year gives a negative P/E and EPS; their product is the price.
    assert _traded_price(-1.9, -45.37) == pytest.approx(86.2, abs=0.01)
    assert _traded_price(6.3, -12.89) is None


def test_a_filing_count_and_the_recorded_split_it_shows_are_one_split():
    from src.orchestrator.common.corporate_actions import SplitEvent, merge_split_events

    T = pd.Timestamp
    # The filing-date count of a report filed 28 June shows a 5-for-1 split
    # recorded at 23 June, within the record-date allowance.
    filing = [SplitEvent(T("2021-03-31"), T("2021-06-28"), 5.0, "filing date count")]
    recorded = [SplitEvent(T("2021-06-22"), T("2021-06-23"), 5.0, "Stock_Splits")]
    assert [e.multiplier for e in merge_split_events(recorded, [], filing)] == [5.0]
    # A report filed 24 March restated its book value for a 2-for-1 split
    # taking effect on 29 March: one split, already in that report.
    filing = [SplitEvent(T("2016-12-31"), T("2017-03-24"), 2.0, "filing date count")]
    recorded = [SplitEvent(T("2017-03-28"), T("2017-03-29"), 2.0, "Stock_Splits")]
    assert merge_split_events(recorded, [], filing) == filing


def test_a_heuristic_record_weeks_from_the_providers_is_the_same_split():
    from src.utilities.price_provenance import distinct_split_records

    records = [("2026-02-13", 3.0, "price_heuristic"), ("2026-03-30", 3.0, "provider")]
    assert distinct_split_records(records) == [("2026-03-30", 3.0, "provider")]
    # Days apart, the earlier date (the price's step) is kept.
    assert distinct_split_records([("2026-03-02", 5.0, "price_heuristic"), ("2026-03-05", 5.0, "provider")]) == [("2026-03-02", 5.0, "price_heuristic")]
    # Two provider splits of one ratio a quarter apart are two splits.
    assert len(distinct_split_records([("2026-01-05", 2.0, "provider"), ("2026-03-30", 2.0, "provider")])) == 2


def test_revenue_ratios_read_the_larger_of_net_sales_and_operating_revenue(tmp_path):
    from src.orchestrator.generate_ratios.generate_ratios import generate_ratios

    path = str(tmp_path / "ratios.db")
    conn = sqlite3.connect(path)
    conn.executescript("""
        CREATE TABLE FinancialStatements (docID TEXT PRIMARY KEY, Company_Code TEXT, periodEnd TEXT);
        CREATE TABLE IncomeStatement (docID TEXT PRIMARY KEY, "Net sales" REAL, "Operating Revenue - Operating revenue" REAL,
            "Cost of sales" REAL, "Profit (loss)" REAL, "Profit (loss) before income taxes" REAL,
            "Selling, general and administrative expenses" REAL, "Non-operating expenses - Interest expenses" REAL,
            "Ordinary Income - Ordinary income" REAL, "Ordinary Expenses - Operating expenses" REAL);
        CREATE TABLE BalanceSheet (docID TEXT PRIMARY KEY, "Assets" REAL, "Net assets" REAL, "Current assets" REAL,
            "Current liabilities" REAL, "Assets - Intangible assets" REAL, "Property, plant and equipment" REAL);
        CREATE TABLE CashflowStatement (docID TEXT PRIMARY KEY, "Depreciation" REAL);
        CREATE TABLE ShareMetrics (docID TEXT PRIMARY KEY, "Number of issued shares as of fiscal year end" REAL);
        INSERT INTO FinancialStatements VALUES ('HOLD', 'E1', '2026-03-31'), ('RAIL', 'E2', '2026-03-31'), ('BANK', 'E3', '2026-03-31'), ('SHOP', 'E4', '2016-05-31');
        INSERT INTO IncomeStatement VALUES ('HOLD', 164, 802, 32, 600, 620, 100, 4, NULL, NULL), ('RAIL', NULL, 900, NULL, 90, 120, NULL, 9, NULL, NULL),
            ('BANK', NULL, 16.6, NULL, 42, 60, 1.4, NULL, 260, 199), ('SHOP', NULL, NULL, NULL, 1.9, 2.0, NULL, NULL, 1.98, NULL);
        INSERT INTO BalanceSheet VALUES ('HOLD', 4000, 3000, 500, 250, 40, 100), ('RAIL', 3000, 1000, 300, 400, 30, 2000),
            ('BANK', 9000, 500, NULL, NULL, 5, 50), ('SHOP', 50, 20, 30, 10, 0, 5);
        INSERT INTO CashflowStatement VALUES ('HOLD', 20), ('RAIL', 90), ('BANK', 3), ('SHOP', 0.1);
        INSERT INTO ShareMetrics VALUES ('HOLD', 10), ('RAIL', 100), ('BANK', 100), ('SHOP', 10);
    """)
    conn.commit()
    conn.close()
    generate_ratios(database=path, overwrite=True)
    conn = sqlite3.connect(path)
    try:
        ratios = pd.read_sql_query("SELECT * FROM Financial_Ratios", conn).set_index("docID")
        per_share = pd.read_sql_query("SELECT * FROM PerShare_Metrics", conn).set_index("docID")
    finally:
        conn.close()
    assert ratios.loc["HOLD", "Net Margin"] == pytest.approx(600 / 802)
    assert ratios.loc["HOLD", "Gross Margin"] == pytest.approx((164 - 32) / 164)
    assert ratios.loc["RAIL", "Net Margin"] == pytest.approx(90 / 900)
    assert ratios.loc["RAIL", "Depreciation Margin"] == pytest.approx(90 / 900)
    assert ratios.loc["RAIL", "Interest expenses Margin"] == pytest.approx(9 / 900)
    assert ratios.loc["HOLD", "G&A Margin"] == pytest.approx(100 / 802)
    assert ratios.loc["HOLD", "Intangibles on Assets"] == pytest.approx(40 / 4000)
    assert per_share.loc["RAIL", "Sales Per Share"] == pytest.approx(9.0)
    # A bank's revenue is its ordinary revenue, not its holding company's own
    # operating revenue; without ordinary expenses that label is profit.
    assert ratios.loc["BANK", "Net Margin"] == pytest.approx(42 / 260)
    assert pd.isna(ratios.loc["SHOP", "Net Margin"])


def test_a_rolling_metric_can_read_a_line_filed_under_several_names(split_db, tmp_path):
    conn = sqlite3.connect(split_db)
    conn.execute('ALTER TABLE IncomeStatement ADD COLUMN "Operating Revenue - Operating revenue" REAL')
    conn.execute("UPDATE IncomeStatement SET [Net sales] = NULL, [Operating Revenue - Operating revenue] = 1000 WHERE docID IN ('D22', 'D23')")
    conn.commit()
    conn.close()
    spec = {"IncomeStatement": [{"name": "Revenue", "columns": ["Net sales", "Operating Revenue - Operating revenue"], "aggregation": "max"}]}
    income = _run_rolling(split_db, tmp_path, spec)["IncomeStatement_Rolling"]
    assert income.loc["D23", "Revenue_Average_2_Year"] == pytest.approx(1000.0)
    assert income.loc["D22", "Revenue_Growth_2_Year"] == pytest.approx(math.sqrt(1000 / (4400 * 1000)) - 1)


def test_a_split_already_in_the_first_reports_price_was_restated_in_it(tmp_path):
    from src.orchestrator.common.corporate_actions import filing_basis_factors

    # 10-for-1 effective 1 April 2017: the March 2017 count is the old one,
    # but the June 2017 report already restated EPS and book value, and the
    # year-end price (P/E × EPS = ¥1,844) is the new one. No earlier report
    # shows the fall in book value per share; the prices do.
    path = str(tmp_path / "early.db")
    conn = sqlite3.connect(path)
    conn.executescript(f"""
        CREATE TABLE CompanyInfo (Company_Code TEXT, Company_Ticker TEXT);
        CREATE TABLE FinancialStatements (docID TEXT PRIMARY KEY, Company_Code TEXT, docTypeCode TEXT, submitDateTime TEXT, periodEnd TEXT);
        CREATE TABLE ShareMetrics (docID TEXT PRIMARY KEY, [Total number of issued shares] REAL, [Net assets per share] REAL,
            [{EPS}] REAL, [Price-earnings ratio] REAL, [{DPS}] REAL);
        CREATE TABLE Stock_Prices (Date TEXT, Ticker TEXT, Currency TEXT, Price REAL);
        INSERT INTO CompanyInfo VALUES ('E1', '88760');
        INSERT INTO FinancialStatements VALUES ('F17', 'E1', '030000', '2017-06-26 09:00', '2017-03-31'), ('F18', 'E1', '030000', '2018-06-27 09:00', '2018-03-31');
        INSERT INTO ShareMetrics VALUES ('F17', 15295120, 262.22, 100.0, 18.44, 184), ('F18', 152951200, 283.54, 110.0, 26.8, 22);
    """)
    days = pd.bdate_range("2017-03-01", "2018-04-30")
    conn.executemany("INSERT INTO Stock_Prices VALUES (?, '88760', 'JPY', ?)", [(day.strftime("%Y-%m-%d"), 1844.0 + index * 4) for index, day in enumerate(days)])
    conn.commit()
    try:
        basis = {row.doc_id: row for row in filing_basis_factors(conn)}
    finally:
        conn.close()
    assert (basis["F17"].restated, basis["F17"].fiscal) == (1.0, pytest.approx(0.1))
