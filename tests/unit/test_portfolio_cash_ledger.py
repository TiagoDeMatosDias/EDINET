"""The cash ledger must reconcile with the broker's balances per currency."""

from __future__ import annotations

import sqlite3

import pytest

from src.portfolio.ibkr_parser import normalize_entries, parse_ibkr_xml
from src.portfolio.portfolio_state import _account_currency, _apply_transaction
from src.portfolio.schema import create_tables
from src.portfolio.transactions import get_transactions, insert_entries


def _apply(txn: dict, account_currency: str = "EUR") -> dict[str, float]:
    cash: dict[str, float] = {}
    flows = {"inflow": 0.0, "income": 0.0}
    _apply_transaction(txn, {}, cash, flows, {}, lambda amount, currency: amount, account_currency)
    return cash


FX_SELL = {
    "activity_type": "TRADE", "asset_category": "CASH", "symbol": "EUR.USD", "currency": "USD",
    "quantity": -97, "trade_money": -117.9229, "net_cash": 0, "commission": -1.65116,
    "buy_sell": "SELL", "fx_rate_to_base": 0.82,
}


def test_a_currency_conversion_moves_both_currencies_and_charges_its_commission() -> None:
    cash = _apply({**FX_SELL, "commission_currency": "EUR"})
    assert cash["EUR"] == pytest.approx(-97 - 1.65116)
    assert cash["USD"] == pytest.approx(117.9229)


def test_the_commission_is_charged_in_its_own_currency() -> None:
    cash = _apply({**FX_SELL, "commission_currency": "USD"})
    assert cash["EUR"] == pytest.approx(-97)
    assert cash["USD"] == pytest.approx(117.9229 - 1.65116)


def test_older_imports_charge_the_commission_in_the_account_currency() -> None:
    cash = _apply({**FX_SELL, "commission_currency": None}, account_currency="EUR")
    assert cash["EUR"] == pytest.approx(-98.65116)


def test_the_account_currency_is_the_one_booked_at_a_rate_of_one() -> None:
    records = [
        {"currency": "USD", "fx_rate_to_base": 0.9},
        {"currency": "GBP", "fx_rate_to_base": 1.0},
        {"currency": "GBP", "fx_rate_to_base": 1.0},
        {"currency": "EUR", "fx_rate_to_base": 0.85},
    ]
    assert _account_currency(records) == "GBP"
    assert _account_currency([]) == "EUR"


@pytest.mark.parametrize("activity", ["BROKER_INTEREST", "BOND_INTEREST", "OTHER_CASH"])
def test_interest_and_other_cash_records_move_cash(activity: str) -> None:
    assert _apply({"activity_type": activity, "currency": "USD", "amount": 0.37}) == {"USD": 0.37}


FLEX = """<FlexQueryResponse><FlexStatements><FlexStatement accountId="U1">
  <Trades>
    <Trade levelOfDetail="EXECUTION" assetCategory="CASH" transactionID="fx-1" accountId="U1"
      symbol="EUR.USD" currency="USD" tradeDate="2026-02-09" reportDate="2026-02-09"
      quantity="8899.93" tradeMoney="10561.99" proceeds="-10561.99" netCash="0"
      ibCommission="-1.6924" ibCommissionCurrency="EUR" buySell="BUY" fxRateToBase="0.85" />
  </Trades>
  <CashTransactions>
    <CashTransaction levelOfDetail="DETAIL" transactionID="int-1" accountId="U1"
      type="Broker Interest Received" currency="USD" dateTime="2026-03-04" reportDate="2026-03-04"
      amount="0.37" fxRateToBase="0.86" description="USD CREDIT INT FOR FEB-2026" />
    <CashTransaction levelOfDetail="DETAIL" transactionID="tax-fix" accountId="U1"
      type="Withholding Tax" symbol="O" currency="USD" dateTime="2021-03-15;202000"
      reportDate="2022-02-04" amount="3.52" fxRateToBase="0.89"
      description="O(US7561091049) CASH DIVIDEND USD 0.2345 PER SHARE - US TAX" />
  </CashTransactions>
</FlexStatement></FlexStatements></FlexQueryResponse>"""


def test_the_parser_keeps_interest_received_booking_dates_and_commission_currency() -> None:
    by_id = {entry["transaction_id"]: entry for entry in normalize_entries(parse_ibkr_xml(FLEX))}
    assert by_id["int-1"]["activity_type"] == "BROKER_INTEREST"
    assert by_id["int-1"]["amount"] == 0.37
    assert by_id["fx-1"]["commission_currency"] == "EUR"
    # A correction keeps the dividend's date but was booked a year later.
    assert by_id["tax-fix"]["trade_date"] == "2021-03-15"
    assert by_id["tax-fix"]["report_date"] == "2022-02-04"


def test_importing_a_file_again_fills_in_details_without_duplicating(tmp_path) -> None:
    db = str(tmp_path / "Portfolio.db")
    create_tables(db)
    entries = normalize_entries(parse_ibkr_xml(FLEX))
    older = [{**entry, "report_date": None, "commission_currency": None} for entry in entries]
    assert insert_entries(db, older, "2026.xml", owner_user_id="me")["inserted"] == 3

    again = insert_entries(db, entries, "2026.xml", owner_user_id="me")
    assert (again["inserted"], again["skipped"], again["updated"]) == (0, 3, 3)
    rows = {row["symbol"]: row for row in get_transactions(db, owner_user_id="me")}
    assert rows["EUR.USD"]["commission_currency"] == "EUR"
    assert rows["O"]["report_date"] == "2022-02-04"

    # A third import has nothing left to fill in, and another owner's rows are untouched.
    assert insert_entries(db, entries, "2026.xml", owner_user_id="me")["updated"] == 0
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM Transactions").fetchone()[0] == 3


def test_data_checks_ask_for_a_reimport_and_flag_unrecognised_cash(tmp_path) -> None:
    from src.portfolio.data_quality import _ledger_issues

    db = str(tmp_path / "Portfolio.db")
    create_tables(db)
    conversion = {
        "transaction_id": "fx-old", "activity_type": "TRADE", "asset_category": "CASH", "symbol": "EUR.USD",
        "currency": "USD", "trade_date": "2024-01-02", "quantity": -100, "trade_money": -110,
        "commission": -1.7, "buy_sell": "SELL", "fx_rate_to_base": 0.9,
    }
    other = {"transaction_id": "odd-1", "activity_type": "OTHER_CASH", "currency": "EUR", "trade_date": "2024-01-03", "amount": 5}
    insert_entries(db, [conversion, other], owner_user_id="me")
    with sqlite3.connect(db) as conn:
        assert {issue["code"] for issue in _ledger_issues(conn, "me")} == {"other_cash", "reimport_details"}
        assert _ledger_issues(conn, "someone else") == []

    insert_entries(db, [{**conversion, "commission_currency": "EUR"}], owner_user_id="me")
    with sqlite3.connect(db) as conn:
        assert {issue["code"] for issue in _ledger_issues(conn, "me")} == {"other_cash"}
