"""Transactions typed in by hand, stored like imported ones.

Each manual record is normalized with the same field conventions as an IBKR
Flex Query record (signs, proceeds, net cash), so holdings, cash, income, and
returns rebuild from it exactly as they would from a broker file.
"""

from __future__ import annotations

import sqlite3
import uuid
from dataclasses import dataclass

from src.orchestrator.common.sqlite import connect_read

MANUAL_SOURCE = "Manual entries"

# kind → (activity_type, sign applied to the amount the user enters)
CASH_KINDS: dict[str, tuple[str, int]] = {
    "dividend": ("DIVIDEND", 1),
    "withholding_tax": ("WITHHOLDING_TAX", -1),
    "deposit": ("DEPOSIT_WITHDRAWAL", 1),
    "withdrawal": ("DEPOSIT_WITHDRAWAL", -1),
    "fee": ("OTHER_FEE", -1),
    "interest": ("BROKER_INTEREST", 1),
}
TRADE_KINDS = ("buy", "sell")
KINDS = (*TRADE_KINDS, *CASH_KINDS)
# Records that belong to a holding name it; deposits, fees, and interest may not.
NEEDS_SYMBOL = {"buy", "sell", "dividend", "withholding_tax"}


@dataclass(frozen=True)
class ManualTransaction:
    kind: str
    trade_date: str
    currency: str
    symbol: str = ""
    description: str = ""
    quantity: float = 0.0
    price: float = 0.0
    commission: float = 0.0
    amount: float = 0.0
    asset_category: str = "STK"


class ManualEntryError(ValueError):
    pass


def validate(entry: ManualTransaction) -> None:
    if entry.kind not in KINDS:
        raise ManualEntryError(f"Unknown kind of transaction: {entry.kind}")
    if entry.kind in NEEDS_SYMBOL and not entry.symbol.strip():
        raise ManualEntryError("Name the security (its symbol)")
    if entry.kind in TRADE_KINDS:
        if not entry.quantity > 0 or not entry.price > 0:
            raise ManualEntryError("A trade needs a positive quantity and price")
    elif not entry.amount > 0:
        raise ManualEntryError("Enter a positive amount; the kind of transaction sets its direction")
    if entry.commission < 0:
        raise ManualEntryError("Enter the commission as a positive number")


def _describe(entry: ManualTransaction) -> str:
    if entry.description.strip():
        return entry.description.strip()
    if entry.kind in TRADE_KINDS:
        return f"Manual {entry.kind} of {entry.quantity:g} {entry.symbol} at {entry.price:g}"
    label = entry.kind.replace("_", " ")
    return f"Manual {label}{f' ({entry.symbol})' if entry.symbol else ''}"


def normalized_entry(entry: ManualTransaction, fx_rate_to_base: float | None) -> dict:
    """The record as ``insert_entries`` stores it, with IBKR's sign conventions."""
    validate(entry)
    symbol = entry.symbol.strip().upper()
    base = {
        "transaction_id": f"manual-{uuid.uuid4()}",
        "trade_id": None,
        "account_id": None,
        "asset_category": entry.asset_category if entry.kind in TRADE_KINDS else (entry.asset_category if symbol else None),
        "symbol": symbol or None,
        "description": _describe(entry),
        "isin": None,
        "conid": None,
        "currency": entry.currency.upper(),
        "trade_date": entry.trade_date,
        "settle_date": entry.trade_date,
        "report_date": None,
        "taxes": 0,
        "fx_rate_to_base": fx_rate_to_base,
        "strike": None,
        "expiry": None,
        "put_call": None,
        "underlying_symbol": None,
        "underlying_conid": None,
        "multiplier": 1,
        "action_id": None,
        "commission_currency": None,
    }
    if entry.kind in TRADE_KINDS:
        sign = 1 if entry.kind == "buy" else -1
        money = sign * entry.quantity * entry.price
        commission = -abs(entry.commission)
        return {
            **base,
            "activity_type": "TRADE",
            "quantity": sign * entry.quantity,
            "trade_price": entry.price,
            "trade_money": money,
            "amount": 0,
            "proceeds": -money,
            "commission": commission,
            "net_cash": -money + commission,
            "buy_sell": "BUY" if sign > 0 else "SELL",
            "action_description": None,
        }
    activity, sign = CASH_KINDS[entry.kind]
    return {
        **base,
        "activity_type": activity,
        "quantity": 0,
        "trade_price": None,
        "trade_money": None,
        "amount": sign * entry.amount,
        "proceeds": None,
        "commission": 0,
        "net_cash": None,
        "buy_sell": None,
        "action_description": _describe(entry),
    }


def account_currency(db_path: str, owner_user_id: str) -> str:
    """The account's base currency: the one its imports convert at exactly 1 (EUR if none)."""
    try:
        conn = connect_read(db_path)
    except (OSError, sqlite3.Error):
        return "EUR"
    try:
        row = conn.execute(
            "SELECT currency FROM Transactions WHERE owner_user_id = ? AND fx_rate_to_base = 1 "
            "GROUP BY currency ORDER BY COUNT(*) DESC LIMIT 1",
            (owner_user_id,),
        ).fetchone()
        return row["currency"] if row and row["currency"] else "EUR"
    except sqlite3.Error:
        return "EUR"
    finally:
        conn.close()


def stored_record(db_path: str, owner_user_id: str, transaction_id: str) -> dict | None:
    """The stored row for a record just inserted, in the activity table's shape."""
    conn = connect_read(db_path)
    try:
        row = conn.execute(
            "SELECT id, trade_date, settle_date, report_date, activity_type, asset_category, symbol, quantity, "
            "trade_price, amount, net_cash, currency, buy_sell, commission, commission_currency, taxes, description, "
            "trade_money, proceeds, account_id, source_file FROM Transactions WHERE transaction_id = ? AND owner_user_id = ?",
            (transaction_id, owner_user_id),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()
