"""Short sales, option events, corporate actions, and assets and prices entered by hand."""

from __future__ import annotations

import sqlite3

import pytest

from src.portfolio.data_quality import held_refresh_tickers
from src.portfolio.ibkr_parser import normalize_entries, parse_ibkr_xml
from src.portfolio.manual_entries import (
    ManualEntryError,
    ManualTransaction,
    normalized_entries,
    option_symbol,
    parse_option_symbol,
)
from src.portfolio.portfolio_state import (
    _compute_position_stats,
    _with_manual_splits,
    build_portfolio_state,
    get_all_holdings_performance,
)
from src.portfolio.transactions import insert_entries
from tests.unit.test_portfolio_api import (  # noqa: F401
    client,
    empty_api_database,
    populated_api_database,
)

CALL = "AAA   240621C00100000"
PUT = "AAA   240315P00100000"


def _post(kind: str, **fields):
    return client.post("/api/portfolio/transactions/manual", json={"kind": kind, **fields})


def _rows(path: str, sql: str, *params) -> list[dict]:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        return [dict(row) for row in conn.execute(sql, params).fetchall()]
    finally:
        conn.close()


def _holding(path: str, symbol: str, owner: str = "local") -> dict | None:
    rows = _rows(path, "SELECT * FROM Portfolio_Holdings WHERE owner_user_id = ? AND symbol = ?", owner, symbol)
    return rows[0] if rows else None


def _held(path: str, symbol: str, day: str, owner: str = "local") -> dict | None:
    rows = _rows(path, "SELECT * FROM Holdings_History WHERE owner_user_id = ? AND symbol = ? AND date = ?", owner, symbol, day)
    return rows[0] if rows else None


@pytest.fixture
def no_fetch(monkeypatch):
    fetched: list[dict] = []
    monkeypatch.setattr(
        "src.worker.handlers.market.ensure_prices_for_tickers",
        lambda _db, tickers: fetched.append(tickers) or {"fetched": [], "failed": []},
    )
    return fetched


# -- Records ------------------------------------------------------------------

def test_option_symbols_follow_the_occ_format():
    assert option_symbol("AFL", "2021-02-19", "p", 40) == "AFL   210219P00040000"
    assert parse_option_symbol("AFL   210219P00040000") == {"underlying": "AFL", "expiry": "2021-02-19", "put_call": "P", "strike": 40.0}
    assert parse_option_symbol("AFL") is None


def test_exercise_and_assignment_book_the_option_and_share_legs():
    terms = {"trade_date": "2024-03-01", "currency": "USD", "underlying": "AAA", "strike": 100, "expiry": "2024-06-21", "quantity": 2}
    long_call = normalized_entries(ManualTransaction(kind="option_exercise", put_call="C", position="long", commission=2, **terms), None)
    option, stock = long_call
    assert (option["symbol"], option["buy_sell"], option["quantity"], option["trade_price"]) == (CALL, "SELL", -2, 0.0)
    assert (stock["symbol"], stock["buy_sell"], stock["quantity"], stock["trade_price"], stock["net_cash"]) == ("AAA", "BUY", 200, 100, -20002)
    assert option["action_id"] == stock["action_id"] and option["action_id"].startswith("manual-")
    short_call = normalized_entries(ManualTransaction(kind="option_exercise", put_call="C", position="short", **terms), None)
    assert (short_call[0]["buy_sell"], short_call[1]["buy_sell"], short_call[1]["open_close"]) == ("BUY", "SELL", "O")
    long_put = normalized_entries(ManualTransaction(kind="option_exercise", put_call="P", position="long", **terms), None)
    assert (long_put[1]["buy_sell"], long_put[1]["open_close"]) == ("SELL", "O")
    short_put = normalized_entries(ManualTransaction(kind="option_exercise", put_call="P", position="short", **terms), None)
    assert (short_put[0]["buy_sell"], short_put[1]["buy_sell"]) == ("BUY", "BUY")
    # Cash settlement closes the contract at its intrinsic value and trades no shares.
    cash = normalized_entries(ManualTransaction(kind="option_exercise", put_call="C", settlement="cash", settlement_price=112.5, **terms), None)
    assert len(cash) == 1 and (cash[0]["trade_price"], cash[0]["net_cash"]) == (12.5, 2500)


def test_expiration_closes_the_side_held_at_no_price():
    terms = {"trade_date": "2024-03-15", "currency": "USD", "symbol": PUT, "put_call": "P", "strike": 100, "expiry": "2024-03-15", "quantity": 1}
    (short,) = normalized_entries(ManualTransaction(kind="option_expiration", position="short", **terms), None)
    (long,) = normalized_entries(ManualTransaction(kind="option_expiration", position="long", **terms), None)
    assert (short["buy_sell"], short["quantity"], short["net_cash"]) == ("BUY", 1, 0)
    assert (long["buy_sell"], long["quantity"]) == ("SELL", -1)


def test_corporate_actions_and_dividend_tax():
    (split,) = normalized_entries(ManualTransaction(kind="split", trade_date="2024-01-15", currency="USD", symbol="AAA", ratio_from=1, ratio_to=3), None)
    assert (split["activity_type"], split["multiplier"], split["description"]) == ("SPLIT", 3, "3-for-1 split of AAA")
    (merge,) = normalized_entries(ManualTransaction(kind="split", trade_date="2024-01-15", currency="USD", symbol="AAA", ratio_from=10, ratio_to=1), None)
    assert merge["multiplier"] == pytest.approx(0.1) and "consolidation" in merge["description"]
    (spinoff,) = normalized_entries(ManualTransaction(kind="spinoff", trade_date="2024-01-12", currency="USD", symbol="AAA", new_symbol="newco", quantity=3), None)
    assert (spinoff["activity_type"], spinoff["symbol"], spinoff["underlying_symbol"], spinoff["quantity"]) == ("SPINOFF", "NEWCO", "AAA", 3)
    dividend, tax = normalized_entries(ManualTransaction(kind="dividend", trade_date="2024-01-12", currency="USD", symbol="AAA", amount=30, withholding_tax=4.5), 0.9)
    assert (dividend["amount"], tax["activity_type"], tax["amount"]) == (30, "WITHHOLDING_TAX", -4.5)
    assert dividend["action_id"] == tax["action_id"]


@pytest.mark.parametrize("entry", [
    ManualTransaction(kind="sell_short", trade_date="2024-01-05", currency="USD", asset_category="OPT", underlying="AAA", put_call="P", quantity=1, price=1, expiry="2024-03-15"),
    ManualTransaction(kind="option_expiration", trade_date="2024-01-05", currency="USD", underlying="AAA", put_call="X", strike=1, expiry="2024-03-15", quantity=1),
    ManualTransaction(kind="option_exercise", trade_date="2024-01-05", currency="USD", underlying="AAA", put_call="C", strike=1, expiry="2024-03-15", quantity=1, settlement="cash"),
    ManualTransaction(kind="split", trade_date="2024-01-05", currency="USD", symbol="AAA", ratio_from=2, ratio_to=2),
    ManualTransaction(kind="spinoff", trade_date="2024-01-05", currency="USD", symbol="AAA", new_symbol="AAA", quantity=1),
    ManualTransaction(kind="dividend", trade_date="2024-01-05", currency="USD", symbol="AAA", amount=5, withholding_tax=6),
])
def test_incomplete_actions_are_refused(entry):
    with pytest.raises(ManualEntryError):
        normalized_entries(entry, None)


# -- Ledger -------------------------------------------------------------------

def test_a_short_sale_is_held_negative_until_it_is_covered(empty_api_database, no_fetch):  # noqa: F811
    path = empty_api_database
    assert _post("deposit", trade_date="2024-01-02", currency="USD", amount=10_000).status_code == 201
    # A plain sale of shares not held is refused and points to a short sale.
    refused = _post("sell", trade_date="2024-01-03", currency="USD", symbol="AAA", quantity=10, price=150)
    assert refused.status_code == 422 and "Sell short" in refused.json()["detail"]
    assert _post("sell_short", trade_date="2024-01-03", currency="USD", symbol="AAA", quantity=10, price=150, commission=1).status_code == 201
    short = _holding(path, "AAA")
    assert short["quantity"] == -10
    assert short["avg_cost"] == pytest.approx(149.9)
    assert short["market_value_native"] < 0
    cash = _rows(path, "SELECT cash_ccy_json FROM Portfolio_Daily WHERE owner_user_id = 'local' AND date = '2024-01-03'")[0]
    assert '"USD": 11499.0' in cash["cash_ccy_json"]

    assert _post("buy_to_cover", trade_date="2024-02-01", currency="USD", symbol="AAA", quantity=4, price=140).status_code == 201
    assert _holding(path, "AAA")["quantity"] == -6
    performance = next(row for row in get_all_holdings_performance(path, None, "USD", owner_user_id="local") if row["symbol"] == "AAA")["performance"]
    assert performance["current_shares"] == -6
    assert performance["cost_basis_native"] == pytest.approx(-899.4)
    assert performance["realized_pnl_native"] == pytest.approx(1499 * 0.4 - 560)
    assert _post("buy_to_cover", trade_date="2024-02-02", currency="USD", symbol="AAA", quantity=7, price=140).status_code == 422
    assert _post("buy_to_cover", trade_date="2024-02-02", currency="USD", symbol="AAA", quantity=6, price=130).status_code == 201
    assert _holding(path, "AAA") is None
    closed = client.get("/api/portfolio/holdings/closed?base_currency=USD").json()
    assert next(row for row in closed if row["symbol"] == "AAA")["realized_pnl"] == pytest.approx(1500 - 560 - 780)


def test_a_short_that_lost_more_than_it_was_opened_for_has_no_annual_rate(empty_api_database, no_fetch):  # noqa: F811
    assert _post("sell_short", trade_date="2024-01-03", currency="USD", symbol="AAA", quantity=10, price=20).status_code == 201
    performance = next(row for row in get_all_holdings_performance(empty_api_database, None, "USD", owner_user_id="local") if row["symbol"] == "AAA")["performance"]
    assert performance["total_return_native"] < -1
    assert performance["annualized_return"] is None and performance["annualized_return_native"] is None
    single = client.get("/api/portfolio/holdings/AAA/performance")
    assert single.status_code == 200 and single.json()["annualized_return"] is None


def test_a_plain_sale_stops_at_the_shares_held(populated_api_database, no_fetch):  # noqa: F811
    assert _post("sell", trade_date="2024-01-05", currency="USD", symbol="AAA", quantity=11, price=150).status_code == 422
    assert _post("sell", trade_date="2024-01-05", currency="USD", symbol="AAA", quantity=10, price=150).status_code == 201
    assert _holding(populated_api_database, "AAA") is None


def test_a_short_put_expires_without_leaving_a_position(empty_api_database, no_fetch):  # noqa: F811
    path = empty_api_database
    sold = _post("sell_short", trade_date="2024-01-03", currency="USD", asset_category="OPT", underlying="AAA",
                 put_call="P", strike=100, expiry="2024-03-15", quantity=1, price=2)
    assert sold.status_code == 201, sold.text
    assert sold.json()["transaction"]["symbol"] == PUT
    assert _held(path, PUT, "2024-02-01")["quantity"] == -1
    # The contract named by its symbol: its terms and side come from its records.
    expired = _post("option_expiration", trade_date="2024-03-15", currency="USD", symbol=PUT, quantity=1)
    assert expired.status_code == 201, expired.text
    assert (expired.json()["transaction"]["buy_sell"], expired.json()["transaction"]["trade_price"]) == ("BUY", 0)
    assert _held(path, PUT, "2024-03-15") is None
    assert _held(path, PUT, "2024-04-01") is None
    closed = client.get("/api/portfolio/holdings/closed?base_currency=USD").json()
    assert next(row for row in closed if row["symbol"] == PUT)["realized_pnl"] == pytest.approx(200)


def test_exercising_a_call_buys_the_shares_and_both_legs_delete_together(empty_api_database, no_fetch):  # noqa: F811
    path = empty_api_database
    assert _post("buy", trade_date="2024-01-03", currency="USD", asset_category="OPT", underlying="AAA",
                 put_call="C", strike=100, expiry="2024-06-21", quantity=2, price=5).status_code == 201
    exercised = _post("option_exercise", trade_date="2024-03-01", currency="USD", symbol=CALL, quantity=2, commission=2)
    assert exercised.status_code == 201, exercised.text
    legs = exercised.json()["transactions"]
    assert [(leg["symbol"], leg["buy_sell"], leg["quantity"]) for leg in legs] == [(CALL, "SELL", -2), ("AAA", "BUY", 200)]
    assert _holding(path, "AAA")["quantity"] == 200
    assert _holding(path, CALL) is None
    # Removing either leg removes the exercise.
    deleted = client.post("/api/portfolio/transactions/delete", json={"ids": [legs[0]["id"]], "confirm": True})
    assert deleted.status_code == 200 and deleted.json()["deleted"] == 2
    assert _holding(path, "AAA") is None
    assert _held(path, CALL, "2024-03-01")["quantity"] == 2


def test_an_assigned_short_call_opens_a_short_share_position(empty_api_database, no_fetch):  # noqa: F811
    path = empty_api_database
    assert _post("sell_short", trade_date="2024-01-03", currency="USD", asset_category="OPT", underlying="AAA",
                 put_call="C", strike=100, expiry="2024-06-21", quantity=1, price=4).status_code == 201
    assigned = _post("option_exercise", trade_date="2024-03-01", currency="USD", symbol=CALL, quantity=1)
    assert assigned.status_code == 201, assigned.text
    assert assigned.json()["transactions"][0]["buy_sell"] == "BUY"  # inferred: the call was held short
    assert _holding(path, "AAA")["quantity"] == -100
    assert _holding(path, CALL) is None


def test_a_dividend_with_tax_withheld_is_one_payment(populated_api_database, no_fetch):  # noqa: F811
    added = _post("dividend", trade_date="2024-01-12", currency="USD", symbol="AAA", amount=30, withholding_tax=4.5)
    assert added.status_code == 201
    dividend, tax = added.json()["transactions"]
    payment = next(item for item in client.get("/api/portfolio/income?display_currency=USD").json()["payments"] if item["date"] == "2024-01-12")
    assert (payment["gross_native"], payment["tax_native"], payment["shares"]) == (30, -4.5, 10)
    assert client.post("/api/portfolio/transactions/delete", json={"ids": [tax["id"]], "confirm": True}).json()["deleted"] == 2


def test_a_split_entered_by_hand_changes_shares_and_option_terms(populated_api_database, no_fetch):  # noqa: F811
    path = populated_api_database
    assert _post("split", trade_date="2024-01-15", currency="USD", symbol="AAA", ratio_from=1, ratio_to=2).status_code == 201
    assert _held(path, "AAA", "2024-01-14")["quantity"] == 10
    assert _held(path, "AAA", "2024-01-15")["quantity"] == 20
    assert _holding(path, "AAA")["avg_cost"] == pytest.approx(100.1 / 2)
    option = _held(path, "AAA  280121C00120000", "2024-01-15")
    assert option["strike"] == 60
    performance = next(row for row in get_all_holdings_performance(path, None, "USD", owner_user_id="local") if row["symbol"] == "AAA")["performance"]
    assert performance["current_shares"] == 20


def test_a_split_the_market_data_has_is_applied_once():
    market = {"AAA": [("2024-01-15", 2.0)]}
    record = {"activity_type": "SPLIT", "symbol": "AAA", "trade_date": "2024-01-17", "multiplier": 2.0}
    other = {"activity_type": "SPLIT", "symbol": "AAA", "trade_date": "2024-06-01", "multiplier": 3.0}
    merged, known = _with_manual_splits(market, [record, other])
    assert merged["AAA"] == [("2024-01-15", 2.0), ("2024-06-01", 3.0)]
    assert known == [record]
    assert market == {"AAA": [("2024-01-15", 2.0)]}


def test_position_stats_follow_spinoffs_and_splits():
    trades = [
        {"trade_date": "2024-01-02", "activity_type": "TRADE", "buy_sell": "BUY", "quantity": 10, "trade_money": 1000, "commission": -1, "fx_rate_to_base": 1, "currency": "EUR"},
        {"trade_date": "2024-01-10", "activity_type": "SPINOFF", "buy_sell": None, "quantity": 2, "trade_money": None, "commission": 0, "fx_rate_to_base": 1, "currency": "EUR"},
        {"trade_date": "2024-02-01", "activity_type": "TRADE", "buy_sell": "SELL", "quantity": -24, "trade_money": -1200, "commission": -2, "fx_rate_to_base": 1, "currency": "EUR"},
    ]
    stats = _compute_position_stats(trades, "EUR", None, [("2024-01-15", 2.0)])
    # 12 shares become 24 on the split; selling all of them realizes proceeds less cost.
    assert stats["current_shares"] == 0
    assert stats["realized_pnl_native"] == pytest.approx(1198 - 1001)


# -- Assets and prices ----------------------------------------------------------

def test_a_defined_asset_is_valued_at_the_prices_entered(populated_api_database, no_fetch):  # noqa: F811
    path = populated_api_database
    assert _post("spinoff", trade_date="2024-01-12", currency="USD", symbol="AAA", new_symbol="NEWCO", quantity=3).status_code == 201
    spun = _holding(path, "NEWCO")
    assert (spun["quantity"], spun["avg_cost"], spun["market_value"]) == (3, 0, None)
    # Without a price yet it is valued at its cost, nothing.
    for currency in ("USD", "EUR"):
        listed = client.get(f"/api/portfolio/holdings/performance?display_currency={currency}")
        assert listed.status_code == 200, listed.text
        assert next(row for row in listed.json() if row["symbol"] == "NEWCO")["performance"]["pnl_native"] == 0
    assert client.get("/api/portfolio/holdings/NEWCO/performance").status_code == 200
    # Defined to be priced by hand: no market price is fetched for it, now or on a refresh.
    no_fetch.clear()
    defined = client.post("/api/portfolio/assets", json={"symbol": "newco", "name": "New Company Ltd", "currency": "USD"})
    assert defined.status_code == 201, defined.text
    assert no_fetch == []
    assert "NEWCO" not in held_refresh_tickers(path, "local")
    priced = client.post("/api/portfolio/prices", json={"symbol": "NEWCO", "date": "2024-01-15", "price": 12.5, "currency": "USD"})
    assert priced.status_code == 201, priced.text
    holding = _holding(path, "NEWCO")
    assert (holding["market_price"], holding["market_value_native"], holding["price_source"]) == (12.5, 37.5, "manual")
    assert _held(path, "NEWCO", "2024-01-14")["market_value"] is None
    performance = next(row for row in client.get("/api/portfolio/holdings/performance?display_currency=USD").json() if row["symbol"] == "NEWCO")["performance"]
    assert performance["name"] == "New Company Ltd"
    assert performance["pnl_native"] == pytest.approx(37.5)
    quality = client.get("/api/portfolio/data-quality").json()
    assert next(row for row in quality["holdings"] if row["symbol"] == "NEWCO")["status"] == "manual"
    assert any(issue["code"] == "manual_prices" for issue in quality["issues"])
    listed = client.get("/api/portfolio/assets").json()["assets"]
    assert [(item["symbol"], item["name"], item["held"], item["latest_price"]["price"]) for item in listed] == [("NEWCO", "New Company Ltd", True, 12.5)]
    assert client.request("DELETE", "/api/portfolio/prices", params={"symbol": "NEWCO", "date": "2024-01-15"}).status_code == 200
    assert _holding(path, "NEWCO")["market_value"] is None
    assert client.delete("/api/portfolio/assets/NEWCO").status_code == 200
    assert client.get("/api/portfolio/assets").json()["assets"] == []


def test_a_price_entered_before_a_split_carries_forward_per_new_share(empty_api_database, no_fetch):  # noqa: F811
    path = empty_api_database
    assert client.post("/api/portfolio/assets", json={"symbol": "PRIV", "name": "Private Fund", "currency": "USD"}).status_code == 201
    assert _post("buy", trade_date="2024-01-02", currency="USD", symbol="PRIV", quantity=10, price=100).status_code == 201
    assert client.post("/api/portfolio/prices", json={"symbol": "PRIV", "date": "2024-01-05", "price": 110, "currency": "USD"}).status_code == 201
    assert _post("split", trade_date="2024-01-10", currency="USD", symbol="PRIV", ratio_from=1, ratio_to=2).status_code == 201
    assert _held(path, "PRIV", "2024-01-09")["market_value_native"] == 1100
    after = _held(path, "PRIV", "2024-01-10")
    assert (after["quantity"], after["market_price"], after["market_value_native"], after["price_source"]) == (20, 55, 1100, "manual")
    # A price entered after the split is already per new share.
    assert client.post("/api/portfolio/prices", json={"symbol": "PRIV", "date": "2024-01-12", "price": 60, "currency": "USD"}).status_code == 201
    assert _held(path, "PRIV", "2024-01-12")["market_price"] == 60
    assert _held(path, "PRIV", "2024-01-11")["market_price"] == 55


def test_prices_entered_by_hand_stay_with_their_owner(populated_api_database, market_db_path, sample_ibkr_content, no_fetch):  # noqa: F811
    path = populated_api_database
    insert_entries(path, normalize_entries(parse_ibkr_xml(sample_ibkr_content)), source_file="other.xml", owner_user_id="someone-else")
    build_portfolio_state(path, market_db_path, end_date="2024-01-20", owner_user_id="someone-else")
    market_rows = _rows(market_db_path, "SELECT COUNT(*) AS n FROM Stock_Prices WHERE Ticker = 'AAA'")[0]["n"]
    # A Saturday: the latest market close is Friday's, so the price entered stands.
    assert client.post("/api/portfolio/prices", json={"symbol": "AAA", "date": "2024-01-13", "price": 999, "currency": "USD"}).status_code == 201
    assert _held(path, "AAA", "2024-01-13")["market_price"] == 999
    assert _held(path, "AAA", "2024-01-13")["price_source"] == "manual"
    assert _held(path, "AAA", "2024-01-15")["price_source"] == "market"
    other = _held(path, "AAA", "2024-01-13", owner="someone-else")
    assert other["market_price"] != 999 and other["price_source"] == "market"
    build_portfolio_state(path, market_db_path, end_date="2024-01-20", owner_user_id="someone-else")
    assert _held(path, "AAA", "2024-01-13", owner="someone-else")["market_price"] != 999
    # Nothing is written to the shared market data.
    assert _rows(market_db_path, "SELECT COUNT(*) AS n FROM Stock_Prices WHERE Ticker = 'AAA'")[0]["n"] == market_rows
    assert _rows(market_db_path, "SELECT COUNT(*) AS n FROM Stock_Prices WHERE Price = 999")[0]["n"] == 0


def test_an_option_contract_can_be_priced_by_hand(populated_api_database, no_fetch):  # noqa: F811
    path = populated_api_database
    contract = "AAA  280121C00120000"
    modelled = _holding(path, contract)
    assert modelled["price_source"] == "model"
    # Saturday: newer than the underlying's Friday close.
    assert client.post("/api/portfolio/prices", json={"symbol": contract, "date": "2024-01-13", "price": 7.25, "currency": "USD"}).status_code == 201
    priced = _held(path, contract, "2024-01-13")
    assert (priced["market_price"], priced["price_source"], priced["market_value_native"]) == (7.25, "manual", 725)
    assert _held(path, contract, "2024-01-15")["price_source"] == "model"


def test_assets_and_prices_are_validated(empty_api_database):  # noqa: F811
    assert client.post("/api/portfolio/assets", json={"symbol": "X", "name": "", "currency": "USD"}).status_code == 422
    assert client.post("/api/portfolio/assets", json={"symbol": "X", "name": "X", "currency": "USD", "asset_category": "OPT"}).status_code == 422
    assert client.post("/api/portfolio/prices", json={"symbol": "X", "date": "2999-01-01", "price": 1, "currency": "USD"}).status_code == 422
    assert client.post("/api/portfolio/prices", json={"symbol": "X", "date": "2024-01-01", "price": 0, "currency": "USD"}).status_code == 422
    assert client.request("DELETE", "/api/portfolio/prices", params={"symbol": "X", "date": "2024-01-01"}).status_code == 404
    assert client.delete("/api/portfolio/assets/X").status_code == 404
