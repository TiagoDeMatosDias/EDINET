"""Walk-forward portfolio-state tests with deterministic market data."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from src.portfolio.ibkr_parser import normalize_entries, parse_ibkr_xml
from src.portfolio.portfolio_state import (
    build_portfolio_state,
    get_current_holdings,
    get_daily_values,
    get_holdings_at_date,
)
from src.portfolio.schema import create_tables
from src.portfolio.transactions import insert_entries


@pytest.fixture
def portfolio_db(tmp_path: Path, sample_ibkr_content: str) -> str:
    path = str(tmp_path / "Portfolio.db")
    create_tables(path)
    insert_entries(
        path,
        normalize_entries(parse_ibkr_xml(sample_ibkr_content)),
        source_file="synthetic.xml",
    )
    return path


def _build(portfolio_db: str, market_db_path: str) -> dict:
    return build_portfolio_state(
        portfolio_db,
        db2_path=market_db_path,
        end_date="2024-01-20",
        base_currency="EUR",
    )


def test_build_creates_daily_values_and_current_holdings(
    portfolio_db: str,
    market_db_path: str,
) -> None:
    result = _build(portfolio_db, market_db_path)

    assert result == {"daily_rows": 19, "holdings_count": 4}
    daily = get_daily_values(portfolio_db)
    assert daily[0]["date"] == "2024-01-02"
    assert daily[-1]["date"] == "2024-01-20"
    assert all(row["total_value"] is not None for row in daily)

    holdings = get_current_holdings(portfolio_db)
    non_cash = {row["symbol"] for row in holdings if row["asset_category"] != "CASH"}
    assert non_cash == {"AAA", "BBB", "AAA  280121C00120000", "SPIN"}


def test_holdings_history_reflects_transaction_dates(
    portfolio_db: str,
    market_db_path: str,
) -> None:
    _build(portfolio_db, market_db_path)

    before_spinoff = {
        row["symbol"]
        for row in get_holdings_at_date(portfolio_db, "2024-01-09")
        if row["quantity"] > 0
    }
    after_spinoff = {
        row["symbol"]
        for row in get_holdings_at_date(portfolio_db, "2024-01-10")
        if row["quantity"] > 0
    }
    assert "SPIN" not in before_spinoff
    assert "SPIN" in after_spinoff


def test_dividend_income_nets_withholding_tax(
    portfolio_db: str,
    market_db_path: str,
) -> None:
    _build(portfolio_db, market_db_path)

    daily = get_daily_values(portfolio_db)
    dividend_by_date = {row["date"]: row["dividend_income"] for row in daily}
    assert dividend_by_date["2024-01-08"] == pytest.approx(15.3)
    assert sum(dividend_by_date.values()) == pytest.approx(15.3)


def test_market_prices_and_fx_are_applied(
    portfolio_db: str,
    market_db_path: str,
) -> None:
    _build(portfolio_db, market_db_path)

    holdings = {
        row["symbol"]: row
        for row in get_current_holdings(portfolio_db)
        if row["asset_category"] != "CASH"
    }
    assert holdings["AAA"]["currency"] == "USD"
    assert holdings["AAA"]["fx_rate"] == pytest.approx(0.9)
    assert holdings["AAA"]["market_price"] > 0
    assert holdings["BBB"]["currency"] == "EUR"
    assert holdings["BBB"]["fx_rate"] == pytest.approx(1.0)
    assert holdings["AAA  280121C00120000"]["market_value"] > 0


def test_rebuild_is_idempotent_and_removes_stale_rows(
    portfolio_db: str,
    market_db_path: str,
) -> None:
    first = _build(portfolio_db, market_db_path)
    with sqlite3.connect(portfolio_db) as conn:
        conn.execute(
            """INSERT INTO Portfolio_Daily
               (date, owner_user_id, total_value)
               VALUES ('2099-01-01', '', 999999)"""
        )
        conn.commit()

    second = _build(portfolio_db, market_db_path)

    assert second == first
    assert all(row["date"] != "2099-01-01" for row in get_daily_values(portfolio_db))


def test_build_is_scoped_to_owner(
    tmp_path: Path,
    sample_ibkr_content: str,
    market_db_path: str,
) -> None:
    path = str(tmp_path / "owners.db")
    create_tables(path)
    entries = normalize_entries(parse_ibkr_xml(sample_ibkr_content))
    insert_entries(path, entries, source_file="one.xml", owner_user_id="owner-one")
    insert_entries(path, entries, source_file="two.xml", owner_user_id="owner-two")

    first = build_portfolio_state(
        path,
        db2_path=market_db_path,
        end_date="2024-01-20",
        owner_user_id="owner-one",
    )

    assert first["daily_rows"] == 19
    assert len(get_daily_values(path, owner_user_id="owner-one")) == 19
    assert get_daily_values(path, owner_user_id="owner-two") == []

    build_portfolio_state(
        path,
        db2_path=market_db_path,
        end_date="2024-01-20",
        owner_user_id="owner-two",
    )
    assert len(get_daily_values(path, owner_user_id="owner-one")) == 19
    assert len(get_daily_values(path, owner_user_id="owner-two")) == 19


def test_empty_portfolio_build_is_a_noop(tmp_path: Path, market_db_path: str) -> None:
    path = str(tmp_path / "empty.db")
    create_tables(path)

    result = build_portfolio_state(path, db2_path=market_db_path)

    assert result == {"daily_rows": 0, "holdings_count": 0}
    assert get_daily_values(path) == []
