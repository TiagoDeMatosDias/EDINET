"""Valuing a ledger from stored prices: ticker forms, listing currencies, splits, and FX."""

from __future__ import annotations

import sqlite3
from datetime import date, timedelta
from pathlib import Path

import pytest

from src.portfolio.data_quality import portfolio_data_quality, sparse_price_years
from src.portfolio.market_data import MarketData, is_share_split, price_ticker_candidates
from src.portfolio.portfolio_state import (
    build_portfolio_state,
    get_current_holdings,
    get_daily_values,
)
from src.portfolio.schema import create_tables
from src.portfolio.split_schema import ensure_split_tables
from src.portfolio.transactions import insert_entries
from src.utilities.stock_prices import _create_prices_table


def _weekdays(start: str, end: str):
    day = date.fromisoformat(start)
    while day <= date.fromisoformat(end):
        if day.weekday() < 5:
            yield day.isoformat()
        day += timedelta(days=1)


def _market(path: Path, prices: list[tuple[str, str, str, float]], fx: dict[str, float] | None = None) -> str:
    conn = sqlite3.connect(path)
    _create_prices_table(conn, "Stock_Prices")
    ensure_split_tables(None, conn=conn)
    conn.executemany("INSERT INTO Stock_Prices (Date, Ticker, Currency, Price) VALUES (?, ?, ?, ?)", prices)
    for currency, rate in (fx or {"USD": 1.10, "JPY": 160.0}).items():
        conn.executemany(
            "INSERT INTO Stock_Prices (Date, Ticker, Currency, Price) VALUES (?, 'EUR', ?, ?)",
            [(day, currency, rate) for day in _weekdays("2024-01-01", "2024-03-31")],
        )
    conn.commit()
    conn.close()
    return str(path)


def _trade(transaction_id: str, symbol: str, currency: str, day: str, quantity: float, price: float, side: str = "BUY") -> dict:
    sign = -1 if side == "BUY" else 1
    return {
        "transaction_id": transaction_id, "activity_type": "TRADE", "asset_category": "STK",
        "symbol": symbol, "currency": currency, "trade_date": day, "quantity": quantity if side == "BUY" else -quantity,
        "trade_price": price, "trade_money": quantity * price * (1 if side == "BUY" else -1),
        "net_cash": sign * quantity * price, "commission": 0, "buy_sell": side, "fx_rate_to_base": 1.0,
    }


def _deposit(transaction_id: str, day: str, amount: float, currency: str = "EUR") -> dict:
    return {"transaction_id": transaction_id, "activity_type": "DEPOSIT_WITHDRAWAL", "currency": currency, "trade_date": day, "amount": amount, "fx_rate_to_base": 1.0}


def _ledger(path: Path, entries: list[dict]) -> str:
    create_tables(str(path))
    insert_entries(str(path), entries, source_file="test.xml")
    return str(path)


def test_ticker_forms_and_share_split_rule() -> None:
    assert price_ticker_candidates("5984.T")[:2] == ["5984.T", "59840"]
    assert price_ticker_candidates("AFL") == ["AFL"]
    assert is_share_split(1, 2) and is_share_split(10, 1) and is_share_split(20, 21)
    # Yahoo's spinoff adjustments (3M/Solventum, Realty Income/Orion) are not share splits.
    assert not is_share_split(1000, 1196)
    assert not is_share_split(1000, 1032)


def test_quotes_from_another_listing_are_converted_to_the_holding_currency(tmp_path: Path) -> None:
    db2 = _market(tmp_path / "market.db", [("2024-01-02", "CSPX", "USD", 550.0)])
    conn = sqlite3.connect(db2)
    try:
        price, price_date, series = MarketData(conn).price("CSPX", "EUR", "2024-01-05")
    finally:
        conn.close()
    assert price == pytest.approx(550.0 / 1.10)
    assert price_date == "2024-01-02"
    assert series.source_currency == "USD"


def test_tokyo_holdings_use_the_stored_five_character_code(tmp_path: Path) -> None:
    db2 = _market(tmp_path / "market.db", [(day, "59840", "JPY", 800.0 + index) for index, day in enumerate(_weekdays("2024-01-02", "2024-01-31"))])
    db3 = _ledger(tmp_path / "portfolio.db", [
        _deposit("d1", "2024-01-02", 1_000_000, "JPY"),
        _trade("t1", "5984.T", "JPY", "2024-01-03", 100, 790.0),
    ])
    build_portfolio_state(db3, db2, end_date="2024-01-31")
    holding = next(row for row in get_current_holdings(db3) if row["symbol"] == "5984.T")
    assert holding["price_ticker"] == "59840"
    assert holding["price_source"] == "market"
    assert holding["market_price"] == pytest.approx(800.0 + 21)  # the 22nd weekday close
    assert holding["fx_rate"] == pytest.approx(1 / 160.0)


def test_spinoff_price_adjustments_do_not_create_phantom_shares(tmp_path: Path) -> None:
    db2 = _market(tmp_path / "market.db", [(day, "MMM", "USD", 100.0) for day in _weekdays("2024-01-02", "2024-02-29")])
    conn = sqlite3.connect(db2)
    conn.execute(
        "INSERT INTO Stock_Splits (ticker, split_date, ratio_from, ratio_to, confirmation, price_basis) "
        "VALUES ('MMM', '2024-02-01', 1000, 1196, 'confirmed', 'raw')"
    )
    conn.commit()
    conn.close()
    db3 = _ledger(tmp_path / "portfolio.db", [
        _deposit("d1", "2024-01-02", 10_000, "USD"),
        _trade("t1", "MMM", "USD", "2024-01-03", 20, 100.0),
        _trade("t2", "MMM", "USD", "2024-02-15", 20, 100.0, side="SELL"),
    ])
    build_portfolio_state(db3, db2, end_date="2024-02-29")
    assert not [row for row in get_current_holdings(db3) if row["symbol"] == "MMM"]


def test_foreign_cash_is_revalued_at_each_days_rate(tmp_path: Path) -> None:
    db2 = sqlite3.connect(tmp_path / "market.db")
    _create_prices_table(db2, "Stock_Prices")
    ensure_split_tables(None, conn=db2)
    db2.executemany(
        "INSERT INTO Stock_Prices (Date, Ticker, Currency, Price) VALUES (?, 'EUR', 'USD', ?)",
        [("2024-01-02", 1.00), ("2024-01-03", 1.25)],
    )
    db2.commit()
    db2.close()
    db3 = _ledger(tmp_path / "portfolio.db", [_deposit("d1", "2024-01-02", 1_000, "USD")])
    build_portfolio_state(db3, str(tmp_path / "market.db"), end_date="2024-01-03")
    daily = {row["date"]: row for row in get_daily_values(db3)}
    assert daily["2024-01-02"]["total_value"] == pytest.approx(1_000.0)
    # No transaction on the 3rd: the dollar fell against the euro, and so did the cash.
    assert daily["2024-01-03"]["total_value"] == pytest.approx(800.0)
    assert daily["2024-01-03"]["daily_return"] == pytest.approx(-0.2)


def test_data_quality_flags_cost_valuations_stale_quotes_and_conversions(tmp_path: Path) -> None:
    db2 = _market(tmp_path / "market.db", [
        ("2024-01-02", "STALE", "EUR", 50.0),
        *[(day, "CSPX", "USD", 550.0) for day in _weekdays("2024-01-02", "2024-02-29")],
    ])
    db3 = _ledger(tmp_path / "portfolio.db", [
        _deposit("d1", "2024-01-02", 100_000),
        _trade("t1", "STALE", "EUR", "2024-01-02", 10, 50.0),
        _trade("t2", "CSPX", "EUR", "2024-01-02", 10, 500.0),
        _trade("t3", "NOPRICE", "EUR", "2024-01-02", 10, 20.0),
    ])
    build_portfolio_state(db3, db2, end_date="2024-02-29")
    report = portfolio_data_quality(db3, db2, "EUR", today="2024-03-01")
    status = {row["symbol"]: row for row in report["holdings"]}
    assert status["NOPRICE"]["status"] == "cost"
    assert status["STALE"]["status"] == "stale"
    assert status["CSPX"]["converted"] and status["CSPX"]["quote_currency"] == "USD"
    codes = {issue["code"] for issue in report["issues"]}
    assert {"valued_at_cost", "stale_prices", "converted_quotes", "risk_free_missing"} <= codes


def test_weekly_only_years_are_detected(tmp_path: Path) -> None:
    weekly = [(f"{year}-{month:02d}-{day:02d}", "VWCE", "EUR", 100.0) for year in (2021, 2022, 2023) for month in range(1, 13) for day in (1, 8, 15, 22)]
    db2 = _market(tmp_path / "market.db", weekly)
    conn = sqlite3.connect(db2)
    try:
        assert sparse_price_years(conn, "VWCE") == [2022]
    finally:
        conn.close()
