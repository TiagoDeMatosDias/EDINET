"""Stored prices carry the currency they are quoted in, whatever the caller assumed."""

from __future__ import annotations

import sqlite3
from unittest.mock import patch

import pandas as pd
import pytest

from src.utilities.stock_prices import (
    _apply_reported_currency,
    _create_prices_table,
    _parse_yahoo_chart_payload,
    load_ticker_data,
    repair_price_currency_labels,
    resolve_price_currency,
)


def _prices() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    _create_prices_table(conn, "Stock_Prices")
    return conn


def _insert(conn: sqlite3.Connection, rows: list[tuple[str, str, str, float, str | None]]) -> None:
    conn.executemany(
        "INSERT INTO Stock_Prices (Date, Ticker, Currency, Price, Provider) VALUES (?, ?, ?, ?, ?)",
        rows,
    )


def test_currency_priority_and_defaults() -> None:
    assert resolve_price_currency("CSPX", reported="USD", requested="EUR", stored="EUR") == "USD"
    assert resolve_price_currency("VWCE", stored="EUR", requested="JPY") == "EUR"
    assert resolve_price_currency("AFL", requested="usd") == "USD"
    # Only Tokyo codes default to yen.
    assert resolve_price_currency("59840") == "JPY"
    assert resolve_price_currency("VWCE") == "USD"


def test_yahoo_reports_the_listing_currency_and_pence_become_pounds() -> None:
    payload = {"chart": {"result": [{
        "meta": {"currency": "GBp", "dataGranularity": "1d"},
        "timestamp": [1704182400],
        "indicators": {"quote": [{"close": [1250.0]}]},
    }]}}
    raw, _events = _parse_yahoo_chart_payload(payload)
    assert raw.attrs["currency"] == "GBp"
    normalized = _apply_reported_currency(pd.DataFrame({"Date": ["2024-01-02"], "Close": [1250.0]}), raw)
    assert normalized.attrs["currency"] == "GBP"
    assert normalized["Close"].tolist() == [12.5]


def test_reported_currency_is_stored_and_relabels_the_same_listing() -> None:
    conn = _prices()
    # Older rows from the London USD line, stored under the broker's EUR.
    _insert(conn, [("2024-01-02", "CSPX", "EUR", 480.0, "Yahoo Finance chart (as CSPX.L)")])
    history = pd.DataFrame({"Close": [500.0]}, index=pd.to_datetime(["2024-03-01"]))
    history.index.name = "Date"
    history.attrs["currency"] = "USD"
    failure = RuntimeError("not listed")
    with patch("src.utilities.stock_prices._fetch_stooq_history", side_effect=failure), \
         patch("src.utilities.stock_prices._fetch_yahoo_history", side_effect=lambda symbol, start_date=None: (history, []) if symbol == "CSPX.L" else (_ for _ in ()).throw(failure)):
        assert load_ticker_data("CSPX", "Stock_Prices", conn, currency="EUR")
    rows = conn.execute("SELECT Date, Currency, Price FROM Stock_Prices WHERE Ticker = 'CSPX' ORDER BY Date").fetchall()
    assert rows == [("2024-01-02", "USD", 480.0), ("2024-03-01", "USD", 500.0)]


def test_continuing_prices_mislabelled_as_yen_take_the_earlier_currency() -> None:
    conn = _prices()
    _insert(conn, [
        ("2026-05-20", "VWCE", "EUR", 159.0, None),
        ("2026-05-21", "VWCE", "EUR", 159.8, None),
        ("2026-05-22", "VWCE", "JPY", 161.6, "Yahoo Finance chart (as VWCE.DE)"),
        ("2026-05-25", "VWCE", "JPY", 162.8, "Yahoo Finance chart (as VWCE.DE)"),
    ])
    assert repair_price_currency_labels(conn, "Stock_Prices", "VWCE") == 2
    assert {row[0] for row in conn.execute("SELECT Currency FROM Stock_Prices WHERE Ticker = 'VWCE'")} == {"EUR"}
    # A Tokyo code is genuinely priced in yen and is left alone.
    _insert(conn, [("2026-05-20", "72030", "USD", 20.0, None), ("2026-05-21", "72030", "JPY", 21.0, None)])
    assert repair_price_currency_labels(conn, "Stock_Prices", "72030") == 0


def test_listing_lookup_relabels_rows_and_keeps_existing_dates() -> None:
    conn = _prices()
    _insert(conn, [
        ("2024-01-02", "CSPX", "EUR", 480.0, "Yahoo Finance chart (as CSPX.L)"),
        ("2024-01-03", "CSPX", "EUR", 481.0, "Yahoo Finance chart (as CSPX.L)"),
        ("2024-01-03", "CSPX", "USD", 481.5, "Yahoo Finance chart (as CSPX.L)"),
    ])
    changed = repair_price_currency_labels(conn, "Stock_Prices", "CSPX", listing_currency=lambda symbol: "USD" if symbol == "CSPX.L" else None)
    assert changed == 1
    assert conn.execute("SELECT Date, Currency, Price FROM Stock_Prices ORDER BY Date").fetchall() == [
        ("2024-01-02", "USD", 480.0),
        ("2024-01-03", "USD", 481.5),
    ]


def test_a_failed_listing_lookup_changes_nothing() -> None:
    conn = _prices()
    _insert(conn, [("2024-01-02", "CSPX", "EUR", 480.0, "Yahoo Finance chart (as CSPX.L)")])

    def offline(_symbol: str) -> str:
        raise RuntimeError("offline")

    assert repair_price_currency_labels(conn, "Stock_Prices", "CSPX", listing_currency=offline) == 0
    assert conn.execute("SELECT Currency FROM Stock_Prices").fetchone()[0] == "EUR"


@pytest.mark.parametrize("reported", [None, ""])
def test_frames_without_a_report_keep_their_currency(reported) -> None:
    raw = pd.DataFrame()
    if reported is not None:
        raw.attrs["currency"] = reported
    frame = pd.DataFrame({"Date": ["2024-01-02"], "Close": [1.0]})
    assert "currency" not in _apply_reported_currency(frame, raw).attrs
