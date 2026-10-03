"""Dividend income by payment and company, and the latest continuous holding period."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from src.portfolio.income import per_share_amount, portfolio_income
from src.portfolio.portfolio_state import _compute_holding_periods
from src.portfolio.schema import create_tables
from src.portfolio.transactions import insert_entries
from src.utilities.stock_prices import _create_prices_table


def _market(path: Path) -> str:
    conn = sqlite3.connect(path)
    _create_prices_table(conn, "Stock_Prices")
    conn.executemany(
        "INSERT INTO Stock_Prices (Date, Ticker, Currency, Price) VALUES (?, 'EUR', 'USD', 1.0)",
        [(f"{year}-01-01",) for year in range(2020, 2027)],
    )
    conn.commit()
    conn.close()
    return str(path)


def _cash(transaction_id: str, activity: str, day: str, amount: float, description: str, symbol: str = "MO") -> dict:
    return {
        "transaction_id": transaction_id, "activity_type": activity, "symbol": symbol, "currency": "USD",
        "trade_date": day, "amount": amount, "fx_rate_to_base": 1.0, "description": description,
    }


def _ledger(path: Path, entries: list[dict]) -> str:
    create_tables(str(path))
    insert_entries(str(path), entries, source_file="income.xml")
    with sqlite3.connect(path) as conn:
        conn.execute("INSERT INTO Portfolio_Daily (date, total_value) VALUES ('2025-12-31', 1)")
        conn.execute(
            "INSERT INTO Portfolio_Holdings (symbol, asset_category, quantity, avg_cost, market_price, currency) "
            "VALUES ('MO', 'STK', 100, 40.0, 50.0, 'USD')"
        )
    return str(path)


def test_declared_amounts_per_share_are_read_from_descriptions() -> None:
    assert per_share_amount("MO(US02209S1033) CASH DIVIDEND USD 1.06 PER SHARE (Ordinary Dividend)") == ("USD", 1.06)
    assert per_share_amount("MSFT (US5949181045) CASH DIVIDEND USD 0.56 (Ordinary Dividend)") == ("USD", 0.56)
    assert per_share_amount("5984.T(JP3216800007) CASH DIVIDEND JPY 18.5 PER SHARE - JP TAX") == ("JPY", 18.5)
    assert per_share_amount("UL(US9047677045) PAYMENT IN LIEU OF DIVIDEND (Ordinary Dividend)") is None


def test_payments_join_tax_and_in_lieu_and_give_growth_and_yields(tmp_path: Path) -> None:
    quarterly = []
    for year, rate in ((2022, 0.90), (2023, 0.94), (2024, 0.98), (2025, 1.02)):
        for quarter, month in enumerate(("01", "04", "07", "10")):
            day = f"{year}-{month}-10"
            label = f"MO CASH DIVIDEND USD {rate:.2f} PER SHARE"
            quarterly.append(_cash(f"d{year}{quarter}", "DIVIDEND", day, rate * 100, f"{label} (Ordinary Dividend)"))
            quarterly.append(_cash(f"t{year}{quarter}", "WITHHOLDING_TAX", day, -rate * 100 * 0.15, f"{label} - US TAX"))
    entries = [
        *quarterly,
        # Shares lent out: part paid in lieu on the same day.
        _cash("pil", "PIL_DIVIDEND", "2025-10-10", 5.1, "MO PAYMENT IN LIEU OF DIVIDEND (Ordinary Dividend)"),
        # The broker reverses a tax charge and books it again two weeks later.
        _cash("rev", "WITHHOLDING_TAX", "2025-07-24", 15.3, "MO CASH DIVIDEND USD 1.02 PER SHARE - US TAX"),
        _cash("again", "WITHHOLDING_TAX", "2025-07-24", -15.3, "MO CASH DIVIDEND USD 1.02 PER SHARE - US TAX"),
    ]
    db3 = _ledger(tmp_path / "portfolio.db", entries)
    result = portfolio_income(db3, _market(tmp_path / "market.db"), "EUR")

    assert len(result["payments"]) == 16
    last = result["payments"][-1]
    assert last["date"] == "2025-10-10"
    assert last["gross_native"] == pytest.approx(102.0 + 5.1)
    assert last["in_lieu_native"] == pytest.approx(5.1)
    assert last["shares"] == pytest.approx(107.1 / 1.02)
    july = next(payment for payment in result["payments"] if payment["date"] == "2025-07-10")
    assert july["tax_native"] == pytest.approx(-15.3)

    company = result["companies"][0]
    assert company["symbol"] == "MO"
    assert company["frequency"] == 4
    assert company["withholding_rate"] == pytest.approx(0.15, rel=0.01)
    assert company["latest_per_share"] == pytest.approx(1.02)
    assert company["per_share_growth_1y"] == pytest.approx(1.02 / 0.98 - 1, rel=1e-4)
    years = {year["year"]: year for year in company["annual"]}
    assert years[2022]["partial"] and years[2025]["partial"]  # first year held; the valuation year
    assert years[2023]["per_share_growth"] is None
    assert years[2024]["per_share_growth"] == pytest.approx(3.92 / 3.76 - 1)
    assert company["ttm_per_share"] == pytest.approx(4 * 1.02, rel=1e-6)
    assert company["yield_on_cost"] == pytest.approx(company["ttm_per_share"] / 40.0, rel=1e-4)
    assert company["current_yield"] == pytest.approx(company["ttm_per_share"] / 50.0, rel=1e-4)
    assert sum(payment["net"] for payment in result["payments"]) == pytest.approx(result["total_net"], abs=0.01)


def test_a_full_sale_and_rebuy_starts_a_new_holding_period() -> None:
    days = [{"date": f"2024-01-{day:02d}"} for day in range(1, 11)] + [{"date": f"2024-03-{day:02d}"} for day in range(1, 6)]
    periods = _compute_holding_periods(days)
    assert periods["num_holding_periods"] == 2
    assert periods["held_since"] == "2024-03-01"
    assert periods["held_until"] == "2024-03-05"
    assert periods["latest_holding_days"] == 5
    assert periods["longest_holding_days"] == 10
