"""Atomicity test for Portfolio state reconstruction."""

from __future__ import annotations

from datetime import date

import pytest

import src.portfolio.portfolio_state as portfolio_state
from src.orchestrator.common.sqlite import connect_read, connect_write
from src.portfolio.schema import create_tables


def test_rebuild_rolls_back_cleared_state_when_pricing_fails(tmp_path, monkeypatch):
    portfolio_db = tmp_path / "portfolio.db"
    prices_db = tmp_path / "prices.db"
    create_tables(str(portfolio_db))

    prices = connect_write(prices_db)
    prices.execute(
        "CREATE TABLE Stock_Prices (Date TEXT, Ticker TEXT, Price REAL)"
    )
    prices.commit()
    prices.close()

    connection = connect_write(portfolio_db)
    today = date.today().isoformat()
    connection.execute(
        "INSERT INTO Portfolio_Daily (date, total_value) VALUES (?, ?)",
        ("2000-01-01", 123.0),
    )
    connection.execute(
        "INSERT INTO Transactions ("
        "transaction_id, activity_type, currency, trade_date, symbol, "
        "asset_category, quantity, buy_sell, trade_price"
        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("tx-1", "TRADE", "EUR", today, "TEST", "STK", 1, "BUY", 10),
    )
    connection.commit()
    connection.close()

    def fail_pricing(*_args, **_kwargs):
        raise RuntimeError("pricing failed")

    monkeypatch.setattr(portfolio_state, "_price_holding", fail_pricing)
    with pytest.raises(RuntimeError, match="pricing failed"):
        portfolio_state.build_portfolio_state(
            str(portfolio_db),
            str(prices_db),
        )

    connection = connect_read(portfolio_db)
    try:
        row = connection.execute(
            "SELECT date, total_value FROM Portfolio_Daily"
        ).fetchone()
        assert tuple(row) == ("2000-01-01", 123.0)
    finally:
        connection.close()
