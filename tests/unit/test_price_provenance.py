"""Regression coverage for row-level price provenance and split read models."""

import sqlite3

import pandas as pd

from src.portfolio.portfolio_state import (
    _apply_split_actions,
    _get_adjusted_price,
    _get_as_traded_price,
)
from src.portfolio.split_schema import ensure_split_tables
from src.utilities.price_provenance import refresh_split_adjusted_prices
from src.utilities.stock_prices import _append_price_rows, _create_prices_table


def _price_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    _create_prices_table(conn, "Stock_Prices")
    ensure_split_tables(None, conn=conn)
    return conn


def test_new_rows_record_source_and_basis() -> None:
    conn = _price_db()
    frame = pd.DataFrame({"Date": ["2024-01-02"], "Close": [100.0]})
    frame.attrs.update({
        "provider": "Yahoo Finance chart",
        "provider_symbol": "7203.T",
        "price_basis": "raw",
    })

    _append_price_rows(conn, "Stock_Prices", "72030", frame, "JPY")
    row = conn.execute(
        "SELECT Price, Price_Basis, Provider, Source_Id, Source_Revision, "
        "Retrieved_At FROM Stock_Prices"
    ).fetchone()

    assert row["Price"] == 100.0
    assert row["Price_Basis"] == "raw"
    assert row["Provider"] == "Yahoo Finance chart"
    assert row["Source_Id"] == "Yahoo Finance chart:7203.T:2024-01-02"
    assert row["Source_Revision"] == "chart-csv-v1"
    assert row["Retrieved_At"]


def test_adjusted_provider_rows_record_split_multiplier() -> None:
    conn = _price_db()
    frame = pd.DataFrame({"Date": ["2024-06-14", "2024-06-17"], "Close": [50.0, 50.0]})
    _append_price_rows(
        conn,
        "Stock_Prices",
        "X",
        frame,
        "JPY",
        provider="Yahoo Finance chart",
        provider_symbol="X.T",
        price_basis="adjusted",
        split_events=[{"split_date": "2024-06-15", "ratio_from": 1, "ratio_to": 2}],
    )
    conn.execute(
        "INSERT INTO Stock_Splits(ticker, split_date, ratio_from, ratio_to, "
        "confirmation, price_basis) VALUES ('X', '2024-06-15', 1, 2, 'confirmed', 'raw')"
    )
    refresh_split_adjusted_prices(conn, ticker="X")

    rows = conn.execute(
        "SELECT Price, Price_Basis, Adjustment_Factor, Adjusted_Price "
        "FROM Stock_Prices ORDER BY Date"
    ).fetchall()
    assert rows[0][0] == 50.0 and rows[0][1] == "adjusted"
    assert rows[0][2] == 0.5 and rows[0][3] == 50.0
    assert rows[1][2] == 1.0 and rows[1][3] == 50.0


def test_split_read_model_never_rewrites_raw_price() -> None:
    conn = _price_db()
    conn.execute(
        "INSERT INTO Stock_Prices(Date, Ticker, Currency, Price, Price_Basis) "
        "VALUES ('2024-06-14', 'X', 'JPY', 100, 'raw')"
    )
    conn.execute(
        "INSERT INTO Stock_Prices(Date, Ticker, Currency, Price, Price_Basis) "
        "VALUES ('2024-06-17', 'X', 'JPY', 50, 'raw')"
    )
    conn.execute(
        "INSERT INTO Stock_Splits(ticker, split_date, ratio_from, ratio_to, "
        "confirmation, price_basis) VALUES ('X', '2024-06-15', 1, 2, 'confirmed', 'raw')"
    )
    conn.commit()

    assert refresh_split_adjusted_prices(conn, ticker="X") == 2
    rows = conn.execute(
        "SELECT Price, Split_Adjustment_Factor, Adjusted_Price "
        "FROM Stock_Prices ORDER BY Date"
    ).fetchall()
    assert [row[0] for row in rows] == [100.0, 50.0]
    assert [row[1] for row in rows] == [0.5, 1.0]
    assert [row[2] for row in rows] == [50.0, 50.0]
    assert _get_adjusted_price(conn, "X", "2024-06-14") == 50.0


def test_duplicate_legacy_split_rows_are_applied_once() -> None:
    conn = _price_db()
    conn.execute("DROP INDEX idx_stock_splits_ticker_date")
    conn.execute(
        "INSERT INTO Stock_Prices(Date, Ticker, Currency, Price, Price_Basis) "
        "VALUES ('2024-06-14', 'X', 'JPY', 100, 'raw')"
    )
    conn.executemany(
        "INSERT INTO Stock_Splits(ticker, split_date, ratio_from, ratio_to, "
        "detection_method, confirmation, price_basis) VALUES (?, ?, ?, ?, ?, 'confirmed', 'raw')",
        [
            ("X", "2024-06-15", 1, 2, "price_heuristic"),
            ("X", "2024-06-15", 1, 2, "provider"),
        ],
    )
    conn.commit()

    refresh_split_adjusted_prices(conn, ticker="X")
    row = conn.execute(
        "SELECT Split_Adjustment_Factor, Adjusted_Price FROM Stock_Prices"
    ).fetchone()
    assert row[0] == 0.5
    assert row[1] == 50.0
    assert _get_adjusted_price(conn, "X", "2024-06-14") == 50.0


def test_unknown_basis_is_not_silently_split_adjusted() -> None:
    conn = _price_db()
    conn.execute(
        "INSERT INTO Stock_Prices(Date, Ticker, Currency, Price, Price_Basis) "
        "VALUES ('2024-06-14', 'X', 'JPY', 100, 'unknown')"
    )
    conn.execute(
        "INSERT INTO Stock_Splits(ticker, split_date, ratio_from, ratio_to, "
        "confirmation, price_basis) VALUES ('X', '2024-06-15', 1, 2, 'confirmed', 'raw')"
    )
    conn.commit()

    assert _get_adjusted_price(conn, "X", "2024-06-14") == 100.0
    refresh_split_adjusted_prices(conn, ticker="X")
    row = conn.execute(
        "SELECT Split_Adjustment_Factor, Adjusted_Price FROM Stock_Prices"
    ).fetchone()
    assert row[0] is None and row[1] is None


def test_adjusted_provider_quote_is_restored_only_for_ledger_valuation() -> None:
    conn = _price_db()
    conn.executemany(
        "INSERT INTO Stock_Prices(Date, Ticker, Currency, Price, Price_Basis) "
        "VALUES (?, 'X', 'JPY', ?, 'adjusted')",
        [("2024-06-14", 50.0), ("2024-06-17", 50.0)],
    )
    conn.execute(
        "INSERT INTO Stock_Splits(ticker, split_date, ratio_from, ratio_to, "
        "confirmation, price_basis) VALUES ('X', '2024-06-15', 1, 2, 'confirmed', 'raw')"
    )
    conn.commit()

    # The source quote remains the current-share (split-adjusted) value for
    # analytics, while the portfolio ledger gets the inverse factor before it
    # applies its quantity action on the split date.
    assert _get_adjusted_price(conn, "X", "2024-06-14") == 50.0
    assert _get_as_traded_price(conn, "X", "2024-06-14") == 100.0
    assert _get_as_traded_price(conn, "X", "2024-06-17") == 50.0


def test_split_action_updates_stock_quantity_and_cost_basis() -> None:
    holdings = {
        ("X", "STK"): {
            "symbol": "X",
            "asset_category": "STK",
            "quantity": 10.0,
            "total_cost": 100.0,
            "avg_cost": 10.0,
            "market_price": 100.0,
            "market_value": 1000.0,
            "is_option": False,
        }
    }

    _apply_split_actions(holdings, {"X": [("2024-06-15", 2.0)]}, "2024-06-15")
    holding = holdings[("X", "STK")]
    assert holding["quantity"] == 20.0
    assert holding["total_cost"] == 100.0
    assert holding["avg_cost"] == 5.0
    assert holding["market_price"] is None
