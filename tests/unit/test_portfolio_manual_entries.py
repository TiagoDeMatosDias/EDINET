"""Transactions typed in by hand are stored like imported ones and rebuild the ledger."""

from __future__ import annotations

import pytest

from src.portfolio.manual_entries import ManualEntryError, ManualTransaction, normalized_entry
from tests.unit.test_portfolio_api import client, empty_api_database, populated_api_database  # noqa: F401


def test_trades_follow_the_broker_sign_conventions():
    buy = normalized_entry(ManualTransaction(kind="buy", trade_date="2024-01-05", currency="usd", symbol="aaa", quantity=10, price=5, commission=1), 0.9)
    assert (buy["activity_type"], buy["buy_sell"], buy["quantity"], buy["trade_money"], buy["proceeds"], buy["commission"], buy["net_cash"]) == (
        "TRADE", "BUY", 10, 50, -50, -1, -51,
    )
    assert (buy["symbol"], buy["currency"], buy["fx_rate_to_base"]) == ("AAA", "USD", 0.9)
    assert buy["transaction_id"].startswith("manual-")
    sell = normalized_entry(ManualTransaction(kind="sell", trade_date="2024-01-05", currency="USD", symbol="AAA", quantity=4, price=6, commission=1), None)
    assert (sell["quantity"], sell["trade_money"], sell["proceeds"], sell["net_cash"]) == (-4, -24, 24, 23)


@pytest.mark.parametrize(("kind", "activity", "amount"), [
    ("dividend", "DIVIDEND", 12.5), ("withholding_tax", "WITHHOLDING_TAX", -12.5), ("deposit", "DEPOSIT_WITHDRAWAL", 12.5),
    ("withdrawal", "DEPOSIT_WITHDRAWAL", -12.5), ("fee", "OTHER_FEE", -12.5), ("interest", "BROKER_INTEREST", 12.5),
])
def test_cash_records_take_their_direction_from_the_kind(kind, activity, amount):
    record = normalized_entry(ManualTransaction(kind=kind, trade_date="2024-01-05", currency="EUR", symbol="AAA", amount=12.5), 1.0)
    assert (record["activity_type"], record["amount"]) == (activity, amount)


@pytest.mark.parametrize("entry", [
    ManualTransaction(kind="buy", trade_date="2024-01-05", currency="EUR", symbol="", quantity=1, price=1),
    ManualTransaction(kind="buy", trade_date="2024-01-05", currency="EUR", symbol="AAA", quantity=0, price=1),
    ManualTransaction(kind="deposit", trade_date="2024-01-05", currency="EUR", amount=0),
    ManualTransaction(kind="dividend", trade_date="2024-01-05", currency="EUR", amount=5),
])
def test_incomplete_records_are_refused(entry):
    with pytest.raises(ManualEntryError):
        normalized_entry(entry, None)


def test_a_manual_buy_and_deposit_appear_in_activity_and_holdings(populated_api_database, monkeypatch):  # noqa: F811
    fetched = []
    monkeypatch.setattr("src.portfolio.api.ensure_prices_for_tickers", lambda _db, tickers: fetched.append(tickers) or {"fetched": [], "failed": []})
    response = client.post("/api/portfolio/transactions/manual", json={
        "kind": "buy", "trade_date": "2024-01-10", "currency": "USD", "symbol": "AAA", "quantity": 3, "price": 10, "commission": 1,
    })
    assert response.status_code == 201, response.text
    body = response.json()
    assert body["transaction"]["source_file"] == "Manual entries"
    assert (body["transaction"]["quantity"], body["transaction"]["net_cash"]) == (3, -31)
    assert body["daily_rows"] > 0
    deposit = client.post("/api/portfolio/transactions/manual", json={"kind": "deposit", "trade_date": "2024-01-11", "currency": "EUR", "amount": 100})
    assert deposit.status_code == 201
    records = client.get("/api/portfolio/transactions?limit=10000").json()
    manual = [row for row in records if row["source_file"] == "Manual entries"]
    assert sorted(row["activity_type"] for row in manual) == ["DEPOSIT_WITHDRAWAL", "TRADE"]
    imports = {item["source_file"]: item["records"] for item in client.get("/api/portfolio/imports").json()}
    assert imports["Manual entries"] == 2
    # A trade asks for the holding's prices; a deposit does not.
    assert fetched == [{"AAA": "USD"}]


def test_manual_records_are_validated(empty_api_database):  # noqa: F811
    assert client.post("/api/portfolio/transactions/manual", json={"kind": "buy", "trade_date": "2024-01-10", "currency": "USD", "symbol": "AAA", "quantity": 0, "price": 1}).status_code == 422
    assert client.post("/api/portfolio/transactions/manual", json={"kind": "gift", "trade_date": "2024-01-10", "currency": "USD", "amount": 1}).status_code == 422
    assert client.post("/api/portfolio/transactions/manual", json={"kind": "deposit", "trade_date": "2999-01-01", "currency": "USD", "amount": 1}).status_code == 422
    assert client.post("/api/portfolio/transactions/manual", json={"kind": "deposit", "trade_date": "2024-02-30", "currency": "USD", "amount": 1}).status_code == 422
