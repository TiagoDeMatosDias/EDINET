"""Transactions typed in by hand, stored like imported ones.

Each manual record is normalized with the same field conventions as an IBKR
Flex Query record (signs, proceeds, net cash), so holdings, cash, income, and
returns rebuild from it exactly as they would from a broker file.

Some entries become more than one record, the way the broker books them: an
option exercised or assigned closes the contract at no price and trades the
shares at the strike; a dividend with tax withheld is a dividend and a
withholding-tax record.  Records entered together share an ``action_id`` and
are deleted together.
"""

from __future__ import annotations

import re
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import date as Date

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
# kind → (side, may the sale open a short position)
TRADE_KINDS: dict[str, tuple[str, bool]] = {
    "buy": ("BUY", False),
    "sell": ("SELL", False),
    "sell_short": ("SELL", True),
    "buy_to_cover": ("BUY", False),
}
CORPORATE_KINDS = ("split", "spinoff")
OPTION_KINDS = ("option_exercise", "option_expiration")
KINDS = (*TRADE_KINDS, *CASH_KINDS, *CORPORATE_KINDS, *OPTION_KINDS)
# Records that belong to a holding name it; deposits, fees, and interest may not.
NEEDS_SYMBOL = {*TRADE_KINDS, "dividend", "withholding_tax", "split", "spinoff"}
# Ledger categories a trade can be booked under ("OPT" needs the contract's terms).
TRADE_CATEGORIES = ("STK", "ETF", "FUND", "BOND", "OTHER", "OPT")
_OCC_SYMBOL = re.compile(r"^([A-Z0-9.]{1,6})\s*(\d{6})([CP])(\d{8})$")


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
    # An option contract, by its terms (its symbol is built from them unless given).
    underlying: str = ""
    put_call: str = ""
    strike: float = 0.0
    expiry: str = ""
    multiplier: float = 100.0
    # Exercise and expiration: which side of the contract the account held, and
    # whether it settles in shares or in cash at the underlying's price.
    position: str = "long"
    settlement: str = "shares"
    settlement_price: float = 0.0
    underlying_category: str = "STK"
    # Corporate actions: ``ratio_to`` new shares for every ``ratio_from`` held;
    # a spin-off delivers ``quantity`` shares of ``new_symbol``.
    ratio_from: float = 0.0
    ratio_to: float = 0.0
    new_symbol: str = ""
    new_category: str = "STK"
    # Dividend: tax withheld at source, booked as its own record.
    withholding_tax: float = 0.0


class ManualEntryError(ValueError):
    pass


def _is_option(entry: ManualTransaction) -> bool:
    return entry.kind in OPTION_KINDS or (entry.kind in TRADE_KINDS and entry.asset_category == "OPT")


def _validate_contract(entry: ManualTransaction) -> None:
    if not (entry.underlying.strip() or entry.symbol.strip()):
        raise ManualEntryError("Name the option's underlying")
    if entry.put_call.upper() not in ("C", "P"):
        raise ManualEntryError("Choose call or put")
    if not entry.strike > 0:
        raise ManualEntryError("Enter the option's strike")
    try:
        Date.fromisoformat(entry.expiry)
    except ValueError as exc:
        raise ManualEntryError("Enter the option's expiry date") from exc
    if not entry.multiplier > 0:
        raise ManualEntryError("Enter the contract multiplier (usually 100)")


def validate(entry: ManualTransaction) -> None:
    if entry.kind not in KINDS:
        raise ManualEntryError(f"Unknown kind of transaction: {entry.kind}")
    if entry.kind in NEEDS_SYMBOL and not entry.symbol.strip() and not (_is_option(entry) and entry.underlying.strip()):
        raise ManualEntryError("Name the security (its symbol)")
    if entry.commission < 0:
        raise ManualEntryError("Enter the commission as a positive number")
    if _is_option(entry):
        _validate_contract(entry)
    if entry.kind in TRADE_KINDS:
        if entry.asset_category not in TRADE_CATEGORIES:
            raise ManualEntryError(f"Unknown type of asset: {entry.asset_category}")
        if not entry.quantity > 0 or not entry.price > 0:
            raise ManualEntryError("A trade needs a positive quantity and price")
    elif entry.kind in OPTION_KINDS:
        if entry.position not in ("long", "short"):
            raise ManualEntryError("Choose whether the account held the option long or short")
        if not entry.quantity > 0:
            raise ManualEntryError("Enter how many contracts")
        if entry.kind == "option_exercise":
            if entry.settlement not in ("shares", "cash"):
                raise ManualEntryError("Choose settlement in shares or in cash")
            if entry.settlement == "cash" and not entry.settlement_price > 0:
                raise ManualEntryError("Enter the underlying's settlement price")
    elif entry.kind == "split":
        if not entry.ratio_from > 0 or not entry.ratio_to > 0:
            raise ManualEntryError("Enter the split ratio as new shares for old shares, both positive")
        if entry.ratio_from == entry.ratio_to:
            raise ManualEntryError("A split changes the number of shares; the ratio cannot be 1 for 1")
    elif entry.kind == "spinoff":
        if not entry.new_symbol.strip():
            raise ManualEntryError("Name the company spun off (its symbol)")
        if entry.new_symbol.strip().upper() == entry.symbol.strip().upper():
            raise ManualEntryError("The spun-off company needs its own symbol")
        if not entry.quantity > 0:
            raise ManualEntryError("Enter the number of shares received")
    elif not entry.amount > 0:
        raise ManualEntryError("Enter a positive amount; the kind of transaction sets its direction")
    if entry.kind == "dividend" and not 0 <= entry.withholding_tax <= entry.amount:
        raise ManualEntryError("The tax withheld must be between zero and the dividend")


def _number(value: float) -> str:
    return f"{value:,.10g}" if abs(value) >= 1000 else f"{value:.10g}"


def option_symbol(underlying: str, expiry: str, put_call: str, strike: float) -> str:
    """The contract's OCC symbol, as IBKR stores it: ``AFL   210219P00040000``."""
    day = Date.fromisoformat(expiry)
    return f"{underlying.strip().upper():<6}{day:%y%m%d}{put_call.upper()}{round(strike * 1000):08d}"


def option_label(underlying: str, expiry: str, put_call: str, strike: float) -> str:
    """The contract as IBKR describes it: ``AFL 19FEB21 40.0 P``."""
    try:
        day = f"{Date.fromisoformat(str(expiry)[:10]):%d%b%y}".upper()
    except ValueError:
        day = str(expiry)
    strike_text = f"{strike:.1f}" if float(strike) * 10 == int(float(strike) * 10) else f"{strike:g}"
    return f"{underlying} {day} {strike_text} {put_call.upper()}"


def _contract(entry: ManualTransaction) -> dict:
    underlying = (entry.underlying.strip() or entry.symbol.strip().split(" ")[0]).upper()
    symbol = entry.symbol.strip().upper() if entry.symbol.strip() else option_symbol(underlying, entry.expiry, entry.put_call, entry.strike)
    return {
        "symbol": symbol,
        "underlying_symbol": underlying,
        "strike": float(entry.strike),
        "expiry": entry.expiry,
        "put_call": entry.put_call.upper(),
        "multiplier": float(entry.multiplier),
        "label": option_label(underlying, entry.expiry, entry.put_call, entry.strike),
    }


def _describe(entry: ManualTransaction) -> str:
    if entry.description.strip():
        return entry.description.strip()
    if entry.kind in TRADE_KINDS:
        what = _contract(entry)["label"] if entry.asset_category == "OPT" else entry.symbol.strip().upper()
        verb = {"buy": "buy", "sell": "sell", "sell_short": "short sale", "buy_to_cover": "cover"}[entry.kind]
        return f"Manual {verb} of {_number(entry.quantity)} {what} at {_number(entry.price)}"
    label = entry.kind.replace("_", " ")
    return f"Manual {label}{f' ({entry.symbol.strip().upper()})' if entry.symbol.strip() else ''}"


def _base(entry: ManualTransaction, fx_rate_to_base: float | None, action_id: str | None) -> dict:
    symbol = entry.symbol.strip().upper()
    return {
        "transaction_id": f"manual-{uuid.uuid4()}",
        "trade_id": None,
        "account_id": None,
        "asset_category": entry.asset_category if symbol else None,
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
        "action_id": action_id,
        "commission_currency": None,
        "open_close": None,
    }


def _trade(base: dict, side: str, quantity: float, price: float, *, multiplier: float = 1, commission: float = 0, may_open_short: bool = False) -> dict:
    sign = 1 if side == "BUY" else -1
    money = sign * quantity * price * multiplier
    charge = -abs(commission)
    return {
        **base,
        "activity_type": "TRADE",
        "quantity": sign * quantity,
        "trade_price": price,
        "trade_money": money,
        "amount": 0,
        "proceeds": -money,
        "commission": charge,
        "net_cash": -money + charge,
        "buy_sell": side,
        "multiplier": multiplier,
        "action_description": None,
        "open_close": "O" if may_open_short else None,
    }


def _cash(base: dict, activity: str, amount: float, description: str | None = None) -> dict:
    return {
        **base,
        "activity_type": activity,
        "quantity": 0,
        "trade_price": None,
        "trade_money": None,
        "amount": amount,
        "proceeds": None,
        "commission": 0,
        "net_cash": None,
        "buy_sell": None,
        "action_description": description or base["description"],
        **({"description": description} if description else {}),
    }


def _option_records(entry: ManualTransaction, base: dict) -> list[dict]:
    contract = _contract(entry)
    terms = {key: contract[key] for key in ("symbol", "underlying_symbol", "strike", "expiry", "put_call")}
    option = {**base, **terms, "asset_category": "OPT"}
    contracts = entry.quantity
    long = entry.position == "long"
    call = contract["put_call"] == "C"
    # Closing the contract: a long position is sold, a short one bought back.
    side = "SELL" if long else "BUY"
    label = contract["label"]
    custom = entry.description.strip()
    if entry.kind == "option_expiration":
        option["description"] = custom or f"Expired {_number(contracts)} {label} ({'long' if long else 'short'})"
        return [_trade(option, side, contracts, 0.0, multiplier=contract["multiplier"])]
    verb = "Exercised" if long else "Assigned"
    if entry.settlement == "cash":
        spot, strike = entry.settlement_price, contract["strike"]
        intrinsic = max(spot - strike, 0.0) if call else max(strike - spot, 0.0)
        option["description"] = custom or f"{verb} {_number(contracts)} {label}, settled in cash at {_number(spot)}"
        return [_trade(option, side, contracts, intrinsic, multiplier=contract["multiplier"], commission=entry.commission)]
    option["description"] = custom or f"{verb} {_number(contracts)} {label}"
    shares = contracts * contract["multiplier"]
    # A long call or a short put buys the shares at the strike; a long put or a
    # short call sells them, which opens a short when they are not held.
    buys = call == long
    underlying = contract["underlying_symbol"]
    stock = {
        **base,
        "symbol": underlying,
        "asset_category": entry.underlying_category or "STK",
        "description": custom or f"{'Bought' if buys else 'Sold'} {_number(shares)} {underlying} at {_number(contract['strike'])} on {'exercise' if long else 'assignment'} of {label}",
        "transaction_id": f"manual-{uuid.uuid4()}",
    }
    return [
        _trade(option, side, contracts, 0.0, multiplier=contract["multiplier"]),
        _trade(stock, "BUY" if buys else "SELL", shares, contract["strike"], commission=entry.commission, may_open_short=not buys),
    ]


def normalized_entries(entry: ManualTransaction, fx_rate_to_base: float | None) -> list[dict]:
    """The records ``insert_entries`` stores for one entry, with IBKR's sign conventions."""
    validate(entry)
    linked = (entry.kind == "option_exercise" and entry.settlement == "shares") or (entry.kind == "dividend" and entry.withholding_tax > 0)
    base = _base(entry, fx_rate_to_base, f"manual-{uuid.uuid4()}" if linked else None)
    if entry.kind in TRADE_KINDS:
        side, may_open_short = TRADE_KINDS[entry.kind]
        if entry.asset_category == "OPT":
            contract = _contract(entry)
            base = {**base, **{key: contract[key] for key in ("symbol", "underlying_symbol", "strike", "expiry", "put_call")}, "asset_category": "OPT"}
            return [_trade(base, side, entry.quantity, entry.price, multiplier=contract["multiplier"], commission=entry.commission, may_open_short=may_open_short)]
        return [_trade(base, side, entry.quantity, entry.price, commission=entry.commission, may_open_short=may_open_short)]
    if entry.kind in OPTION_KINDS:
        return _option_records(entry, base)
    if entry.kind == "split":
        old, new = entry.ratio_from, entry.ratio_to
        what = "split" if new > old else "consolidation"
        description = entry.description.strip() or f"{_number(new)}-for-{_number(old)} {what} of {base['symbol']}"
        # The ledger multiplies the shares held that day (and adjusts options on them).
        return [{**_cash(base, "SPLIT", 0, description), "multiplier": new / old}]
    if entry.kind == "spinoff":
        parent = base["symbol"]
        child = entry.new_symbol.strip().upper()
        description = entry.description.strip() or f"Spin-off of {_number(entry.quantity)} {child} from {parent}"
        return [{
            **_cash(base, "SPINOFF", 0, description),
            "symbol": child,
            "asset_category": entry.new_category or "STK",
            "quantity": entry.quantity,
            "underlying_symbol": parent,
        }]
    activity, sign = CASH_KINDS[entry.kind]
    records = [_cash(base, activity, sign * entry.amount)]
    if entry.kind == "dividend" and entry.withholding_tax > 0:
        tax = {**base, "transaction_id": f"manual-{uuid.uuid4()}"}
        records.append(_cash(tax, "WITHHOLDING_TAX", -entry.withholding_tax, f"Tax withheld on the {base['symbol']} dividend"))
    return records


def normalized_entry(entry: ManualTransaction, fx_rate_to_base: float | None) -> dict:
    """The entry's first record (see ``normalized_entries``)."""
    return normalized_entries(entry, fx_rate_to_base)[0]


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


def known_category(db_path: str, owner_user_id: str, symbol: str) -> str | None:
    """The category a symbol is held under: its latest trade's, else its definition's.

    The ledger keys a holding by symbol and category, so a new record must
    reuse the category the symbol's earlier records have.
    """
    conn = connect_read(db_path)
    try:
        row = conn.execute(
            "SELECT asset_category FROM Transactions WHERE owner_user_id = ? AND symbol = ? "
            "AND activity_type IN ('TRADE', 'SPINOFF') AND COALESCE(asset_category, '') != '' "
            "ORDER BY trade_date DESC, id DESC LIMIT 1",
            (owner_user_id, symbol),
        ).fetchone()
        if row and row[0]:
            return str(row[0])
        row = conn.execute(
            "SELECT asset_category FROM Portfolio_Assets WHERE owner_user_id = ? AND symbol = ?",
            (owner_user_id, symbol),
        ).fetchone()
        return str(row[0]) if row and row[0] else None
    except sqlite3.OperationalError:
        return None
    finally:
        conn.close()


def parse_option_symbol(symbol: str) -> dict | None:
    """An OCC symbol's terms (``AFL   210219P00040000``), or None if it is not one."""
    match = _OCC_SYMBOL.match(str(symbol or "").upper())
    if not match:
        return None
    underlying, day, put_call, strike = match.groups()
    try:
        expiry = Date(2000 + int(day[:2]), int(day[2:4]), int(day[4:])).isoformat()
    except ValueError:
        return None
    return {"underlying": underlying.strip(), "expiry": expiry, "put_call": put_call, "strike": int(strike) / 1000}


def contract_terms(db_path: str, owner_user_id: str, symbol: str) -> dict:
    """A held contract's terms from its latest record, else from its symbol; and the account's net position in it."""
    terms: dict = {}
    conn = connect_read(db_path)
    try:
        row = conn.execute(
            "SELECT underlying_symbol, put_call, strike, expiry, multiplier FROM Transactions "
            "WHERE owner_user_id = ? AND symbol = ? AND asset_category = 'OPT' "
            "ORDER BY trade_date DESC, id DESC LIMIT 1",
            (owner_user_id, symbol),
        ).fetchone()
        net = conn.execute(
            "SELECT COALESCE(SUM(quantity), 0) FROM Transactions WHERE owner_user_id = ? AND symbol = ? "
            "AND activity_type = 'TRADE' AND asset_category = 'OPT'",
            (owner_user_id, symbol),
        ).fetchone()[0]
    except sqlite3.OperationalError:
        row, net = None, 0
    finally:
        conn.close()
    if row:
        terms = {
            "underlying": row["underlying_symbol"] or "",
            "put_call": row["put_call"] or "",
            "strike": float(row["strike"] or 0),
            "expiry": str(row["expiry"] or "")[:10],
            "multiplier": float(row["multiplier"] or 100),
        }
    parsed = parse_option_symbol(symbol) or {}
    for key, value in parsed.items():
        if not terms.get(key):
            terms[key] = value
    terms["net_quantity"] = float(net or 0)
    return terms


def held_on(db_path: str, owner_user_id: str, symbol: str, asset_category: str, day: str) -> float:
    """The quantity held at the end of *day* in the last rebuild (negative when short)."""
    conn = connect_read(db_path)
    try:
        last = conn.execute(
            "SELECT MAX(date) FROM Portfolio_Daily WHERE owner_user_id = ?", (owner_user_id,),
        ).fetchone()[0]
        # Past the last rebuild, the latest position stands.
        on = min(day, last) if last else day
        row = conn.execute(
            "SELECT quantity FROM Holdings_History WHERE owner_user_id = ? AND symbol = ? "
            "AND asset_category = ? AND date = ?",
            (owner_user_id, symbol, asset_category, on),
        ).fetchone()
        return float(row[0] or 0) if row else 0.0
    except sqlite3.OperationalError:
        return 0.0
    finally:
        conn.close()


_STORED_COLUMNS = (
    "id, trade_date, settle_date, report_date, activity_type, asset_category, symbol, quantity, "
    "trade_price, amount, net_cash, currency, buy_sell, commission, commission_currency, taxes, description, "
    "trade_money, proceeds, account_id, source_file, strike, expiry, put_call, underlying_symbol, multiplier, "
    "action_id, open_close"
)


def stored_record(db_path: str, owner_user_id: str, transaction_id: str) -> dict | None:
    """The stored row for a record just inserted, in the activity table's shape."""
    conn = connect_read(db_path)
    try:
        row = conn.execute(
            f"SELECT {_STORED_COLUMNS} FROM Transactions WHERE transaction_id = ? AND owner_user_id = ?",
            (transaction_id, owner_user_id),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()
