"""Tests for the security analysis module."""

from __future__ import annotations

import sqlite3

import pandas as pd
import pytest

from src.security_analysis import (
    ensure_security_analysis_indexes,
    get_security_overview,
    get_security_peers,
    get_security_price_history,
    get_security_ratios,
    get_security_statements,
    search_securities,
    update_security_price,
)


def _create_security_db(path: str) -> str:
    conn = sqlite3.connect(path)
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE CompanyInfo (
            EdinetCode TEXT PRIMARY KEY,
            Company_Name TEXT,
            [Submitter Name] TEXT,
            Company_Industry TEXT,
            Company_Ticker TEXT,
            Listed TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE FinancialStatements (
            edinetCode TEXT,
            docID TEXT UNIQUE,
            periodEnd TEXT,
            SharesOutstanding REAL,
            SharePrice REAL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IncomeStatement (
            docID TEXT UNIQUE,
            netSales REAL,
            grossProfit REAL,
            operatingIncome REAL,
            netIncome REAL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE BalanceSheet (
            docID TEXT UNIQUE,
            cash REAL,
            currentAssets REAL,
            totalAssets REAL,
            shareholdersEquity REAL,
            currentLiabilities REAL,
            TotalLiabilities REAL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE CashflowStatement (
            docID TEXT UNIQUE,
            operatingCashflow REAL,
            investmentCashflow REAL,
            financingCashflow REAL,
            capex REAL,
            dividends REAL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE PerShare (
            docID TEXT UNIQUE,
            EPS REAL,
            BookValue REAL,
            Dividends REAL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE Valuation (
            docID TEXT UNIQUE,
            PERatio REAL,
            PriceToBook REAL,
            DividendsYield REAL,
            MarketCap REAL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE Quality (
            docID TEXT UNIQUE,
            ReturnOnEquity REAL,
            DebtToEquity REAL,
            CurrentRatio REAL,
            GrossMargin REAL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE Stock_Prices (
            Date TEXT,
            Ticker TEXT,
            Currency TEXT,
            Price REAL
        )
        """
    )

    companies = [
        ("E00001", "Alpha Corp", "Alpha Corp Holdings", "Industrial", "1001", "JPX Prime"),
        ("E00002", "Beta Works", "Beta Works KK", "Industrial", "1002", "JPX Prime"),
        ("E00003", "Gamma Retail", "Gamma Retail KK", "Retail", "1003", "JPX Standard"),
        ("E00004", None, "Delta Seeds KK", "Industrial", "1004", "JPX Growth"),
    ]
    cur.executemany("INSERT INTO CompanyInfo VALUES (?, ?, ?, ?, ?, ?)", companies)

    filings = [
        ("E00001", "DOC_A_2023", "2023-03-31", 100_000_000, 900.0),
        ("E00001", "DOC_A_2024", "2024-03-31", 100_000_000, 1_000.0),
        ("E00002", "DOC_B_2024", "2024-03-31", 80_000_000, 850.0),
        ("E00003", "DOC_C_2024", "2024-03-31", 50_000_000, 650.0),
        ("E00004", "DOC_D_2024", "2024-03-31", 75_000_000, 920.0),
    ]
    cur.executemany("INSERT INTO FinancialStatements VALUES (?, ?, ?, ?, ?)", filings)

    cur.executemany(
        "INSERT INTO IncomeStatement VALUES (?, ?, ?, ?, ?)",
        [
            ("DOC_A_2023", 9_000_000_000, 3_200_000_000, 1_100_000_000, 900_000_000),
            ("DOC_A_2024", 10_000_000_000, 3_500_000_000, 1_300_000_000, 1_000_000_000),
            ("DOC_B_2024", 8_000_000_000, 2_900_000_000, 1_050_000_000, 810_000_000),
            ("DOC_C_2024", 6_500_000_000, 2_100_000_000, 500_000_000, 320_000_000),
            ("DOC_D_2024", 7_500_000_000, 2_700_000_000, 980_000_000, 790_000_000),
        ],
    )
    cur.executemany(
        "INSERT INTO BalanceSheet VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            ("DOC_A_2023", 1_000_000_000, 3_000_000_000, 12_000_000_000, 6_000_000_000, 1_700_000_000, 6_000_000_000),
            ("DOC_A_2024", 1_100_000_000, 3_200_000_000, 13_000_000_000, 6_500_000_000, 1_800_000_000, 6_500_000_000),
            ("DOC_B_2024", 900_000_000, 2_500_000_000, 10_500_000_000, 5_000_000_000, 1_500_000_000, 5_500_000_000),
            ("DOC_C_2024", 500_000_000, 1_700_000_000, 8_000_000_000, 3_000_000_000, 1_400_000_000, 5_000_000_000),
            ("DOC_D_2024", 950_000_000, 2_600_000_000, 10_900_000_000, 5_200_000_000, 1_450_000_000, 5_700_000_000),
        ],
    )
    cur.executemany(
        "INSERT INTO CashflowStatement VALUES (?, ?, ?, ?, ?, ?)",
        [
            ("DOC_A_2023", 1_500_000_000, -600_000_000, -300_000_000, -250_000_000, 18.0),
            ("DOC_A_2024", 1_700_000_000, -650_000_000, -350_000_000, -300_000_000, 20.0),
            ("DOC_B_2024", 1_300_000_000, -500_000_000, -200_000_000, -180_000_000, 15.0),
            ("DOC_C_2024", 850_000_000, -430_000_000, -120_000_000, -120_000_000, 8.0),
            ("DOC_D_2024", 1_240_000_000, -480_000_000, -210_000_000, -170_000_000, 14.0),
        ],
    )
    cur.executemany(
        "INSERT INTO PerShare VALUES (?, ?, ?, ?)",
        [
            ("DOC_A_2023", 9.0, 58.0, 18.0),
            ("DOC_A_2024", 10.0, 65.0, 20.0),
            ("DOC_B_2024", 10.125, 62.5, 15.0),
            ("DOC_C_2024", 6.4, 60.0, 8.0),
            ("DOC_D_2024", 10.5333, 69.3333, 14.0),
        ],
    )
    cur.executemany(
        "INSERT INTO Valuation VALUES (?, ?, ?, ?, ?)",
        [
            ("DOC_A_2023", 100.0, 15.5, 0.020, 90_000_000_000),
            ("DOC_A_2024", None, None, None, None),
            ("DOC_B_2024", 84.0, 13.6, 0.018, 68_000_000_000),
            ("DOC_C_2024", 101.0, 10.8, 0.012, 32_000_000_000),
            ("DOC_D_2024", 87.3, 13.3, 0.015, 69_000_000_000),
        ],
    )
    cur.executemany(
        "INSERT INTO Quality VALUES (?, ?, ?, ?, ?)",
        [
            ("DOC_A_2023", 0.150, 0.70, 1.76, 0.355),
            ("DOC_A_2024", 0.154, 0.68, 1.78, 0.350),
            ("DOC_B_2024", 0.162, 0.72, 1.67, 0.362),
            ("DOC_C_2024", 0.107, 1.10, 1.21, 0.323),
            ("DOC_D_2024", 0.152, 0.71, 1.79, 0.360),
        ],
    )

    prices = [
        ("2023-12-31", "1001", "JPY", 930.0),
        ("2024-11-01", "1001", "JPY", 1_050.0),
        ("2024-12-31", "1001", "JPY", 1_100.0),
        ("2023-12-31", "1002", "JPY", 790.0),
        ("2024-12-31", "1002", "JPY", 870.0),
        ("2023-12-31", "1003", "JPY", 700.0),
        ("2024-12-31", "1003", "JPY", 640.0),
        ("2023-12-31", "1004", "JPY", 840.0),
        ("2024-12-31", "1004", "JPY", 920.0),
    ]
    cur.executemany("INSERT INTO Stock_Prices VALUES (?, ?, ?, ?)", prices)

    conn.commit()
    conn.close()
    return path


@pytest.fixture
def security_db(tmp_path):
    db_path = str(tmp_path / "security.db")
    _create_security_db(db_path)
    return db_path


def test_search_securities_matches_name_and_ticker(security_db):
    results = search_securities(security_db, "alpha 1001")
    assert results
    assert results[0]["edinet_code"] == "E00001"
    assert results[0]["company_name"] == "Alpha Corp"
    assert results[0]["ticker"] == "1001"


def test_search_securities_uses_submitter_name_fallback(security_db):
    results = search_securities(security_db, "delta 1004")
    assert results
    assert results[0]["edinet_code"] == "E00004"
    assert results[0]["company_name"] == "Delta Seeds KK"


def test_ensure_security_analysis_indexes_creates_expected_indexes(security_db):
    result = ensure_security_analysis_indexes(security_db)
    assert result["ok"] is True

    conn = sqlite3.connect(security_db)
    try:
        company_indexes = {row[1] for row in conn.execute("PRAGMA index_list([CompanyInfo])")}
        fs_indexes = {row[1] for row in conn.execute("PRAGMA index_list([FinancialStatements])")}
        price_indexes = {row[1] for row in conn.execute("PRAGMA index_list([Stock_Prices])")}
    finally:
        conn.close()

    assert "idx_sa_company_edinet" in company_indexes
    assert "idx_sa_company_ticker" in company_indexes
    assert "idx_sa_company_industry" in company_indexes
    assert "idx_sa_fs_edinet_period" in fs_indexes
    assert "idx_sa_prices_ticker_date" in price_indexes


def test_get_security_overview_uses_ratio_fallbacks(security_db):
    overview = get_security_overview(security_db, "E00001")
    valuation = overview["valuation_latest"]
    assert overview["metadata"]["last_financial_period_end"] == "2024-03-31"
    assert pytest.approx(valuation["PERatio"], rel=1e-4) == 110.0
    assert pytest.approx(valuation["PriceToBook"], rel=1e-4) == (1100.0 / 65.0)
    assert pytest.approx(valuation["DividendsYield"], rel=1e-4) == (20.0 / 1100.0)


def test_get_security_ratios_returns_quality_metrics(security_db):
    ratios = get_security_ratios(security_db, "E00002")
    assert pytest.approx(ratios["PERatio"], rel=1e-4) == 84.0
    assert pytest.approx(ratios["ReturnOnEquity"], rel=1e-4) == 0.162


def test_get_security_statements_returns_ordered_periods(security_db):
    statements = get_security_statements(security_db, "E00001", periods=4)
    assert statements["periods"] == ["2023-03-31", "2024-03-31"]
    income_rows = {row["field"]: row for row in statements["income_statement"]}
    assert income_rows["netSales"]["values"] == [9_000_000_000.0, 10_000_000_000.0]


def test_get_security_price_history_returns_sorted_rows(security_db):
    history = get_security_price_history(security_db, "1001")
    assert [row["trade_date"] for row in history] == ["2023-12-31", "2024-11-01", "2024-12-31"]


def test_get_security_peers_uses_same_industry(security_db):
    peers = get_security_peers(security_db, "E00001")
    assert len(peers) == 2
    assert [peer["edinet_code"] for peer in peers] == ["E00004", "E00002"]
    assert peers[0]["company_name"] == "Delta Seeds KK"


def test_get_security_peers_handles_missing_company_names(security_db):
    peers = get_security_peers(security_db, "E00001")
    peer_codes = {peer["edinet_code"] for peer in peers}
    assert peer_codes == {"E00002", "E00004"}
    sparse_peer = next(peer for peer in peers if peer["edinet_code"] == "E00004")
    assert sparse_peer["company_name"] == "Delta Seeds KK"


def test_update_security_price_updates_single_ticker(security_db, monkeypatch):
    def _fake_load_ticker_data(ticker, prices_table, conn):
        assert ticker == "1002"
        conn.execute(
            f"INSERT INTO {prices_table} (Date, Ticker, Currency, Price) VALUES (?, ?, ?, ?)",
            ("2025-01-02", ticker, "JPY", 880.0),
        )
        return True

    monkeypatch.setattr("src.security_analysis.load_ticker_data", _fake_load_ticker_data)

    result = update_security_price(security_db, "1002")
    assert result["ok"] is True
    assert result["rows_inserted"] == 1

    conn = sqlite3.connect(security_db)
    try:
        row = conn.execute(
            "SELECT Price FROM Stock_Prices WHERE Ticker = ? AND Date = ?",
            ("1002", "2025-01-02"),
        ).fetchone()
        assert row == (880.0,)
    finally:
        conn.close()