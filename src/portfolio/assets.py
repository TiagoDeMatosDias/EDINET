"""Assets and prices the user defines by hand.

An asset definition names a security the market data does not know (a
foreign stock, an ETF, a private fund) and sets its type and currency.  A
manual price values a holding on a day; between prices the latest one,
manual or market, carries forward.  Both live in app.db, per owner, because
they cannot be fetched again.
"""

from __future__ import annotations

import sqlite3
from collections import defaultdict
from dataclasses import dataclass

from src.orchestrator.common.sqlite import connect_read, transaction
from src.portfolio.manual_entries import MANUAL_SOURCE
from src.portfolio.schema import create_tables

# Ledger categories an asset can have, with the label the screen shows.
ASSET_CATEGORIES: dict[str, str] = {
    "STK": "Stock",
    "ETF": "ETF",
    "FUND": "Fund",
    "BOND": "Bond",
    "OTHER": "Other",
}

class AssetError(ValueError):
    pass


@dataclass(frozen=True)
class AssetDefinition:
    symbol: str
    name: str
    currency: str
    asset_category: str = "STK"
    isin: str = ""
    notes: str = ""
    # Whether the market data may price it too (a listed foreign stock), or
    # only the prices entered here (a private fund, a symbol another
    # security uses).
    fetch_prices: bool = False


def normalize_symbol(symbol: str) -> str:
    # Option symbols pad the underlying with spaces ("AFL   210219P00040000"), so only the ends are trimmed.
    return str(symbol or "").strip().upper()


def save_asset(db_path: str, owner_user_id: str, asset: AssetDefinition) -> dict:
    symbol = normalize_symbol(asset.symbol)
    if not symbol:
        raise AssetError("Give the asset a symbol")
    if not asset.name.strip():
        raise AssetError("Give the asset a name")
    if asset.asset_category not in ASSET_CATEGORIES:
        raise AssetError(f"Unknown type of asset: {asset.asset_category}")
    create_tables(db_path)
    with transaction(db_path) as conn:
        conn.execute(
            "INSERT INTO Portfolio_Assets (owner_user_id, symbol, name, asset_category, currency, isin, notes, fetch_prices) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(owner_user_id, symbol) DO UPDATE SET "
            "name = excluded.name, asset_category = excluded.asset_category, currency = excluded.currency, "
            "isin = excluded.isin, notes = excluded.notes, fetch_prices = excluded.fetch_prices, "
            "updated_at = datetime('now')",
            (owner_user_id, symbol, asset.name.strip(), asset.asset_category, asset.currency.upper(),
             asset.isin.strip().upper() or None, asset.notes.strip() or None, 1 if asset.fetch_prices else 0),
        )
    return get_asset(db_path, owner_user_id, symbol) or {}


def get_asset(db_path: str, owner_user_id: str, symbol: str) -> dict | None:
    create_tables(db_path)
    conn = connect_read(db_path)
    try:
        row = conn.execute(
            "SELECT * FROM Portfolio_Assets WHERE owner_user_id = ? AND symbol = ?",
            (owner_user_id, normalize_symbol(symbol)),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def list_assets(db_path: str, owner_user_id: str) -> list[dict]:
    """Defined assets, plus every symbol with a manual price, with their latest prices."""
    create_tables(db_path)
    conn = connect_read(db_path)
    try:
        assets = {
            row["symbol"]: {**dict(row), "defined": True}
            for row in conn.execute(
                "SELECT symbol, name, asset_category, currency, isin, notes, fetch_prices, created_at, updated_at "
                "FROM Portfolio_Assets WHERE owner_user_id = ? ORDER BY symbol",
                (owner_user_id,),
            ).fetchall()
        }
        prices: dict[str, list[dict]] = defaultdict(list)
        for row in conn.execute(
            "SELECT symbol, date, price, currency, note FROM Portfolio_Prices WHERE owner_user_id = ? "
            "ORDER BY symbol, date DESC",
            (owner_user_id,),
        ).fetchall():
            prices[row["symbol"]].append(dict(row))
        held = {
            row[0] for row in conn.execute(
                "SELECT symbol FROM Portfolio_Holdings WHERE owner_user_id = ? AND quantity != 0",
                (owner_user_id,),
            ).fetchall()
        }
    finally:
        conn.close()
    result = []
    for symbol in sorted(set(assets) | set(prices)):
        entry = assets.get(symbol) or {"symbol": symbol, "name": None, "asset_category": None, "currency": None, "fetch_prices": 1, "defined": False}
        rows = prices.get(symbol, [])
        result.append({**entry, "held": symbol in held, "prices": rows, "latest_price": rows[0] if rows else None})
    return result


def delete_asset(db_path: str, owner_user_id: str, symbol: str, *, with_prices: bool = True) -> dict:
    """Remove a definition (and by default its prices); its transactions stay."""
    create_tables(db_path)
    symbol = normalize_symbol(symbol)
    with transaction(db_path) as conn:
        assets = conn.execute(
            "DELETE FROM Portfolio_Assets WHERE owner_user_id = ? AND symbol = ?", (owner_user_id, symbol),
        ).rowcount
        prices = conn.execute(
            "DELETE FROM Portfolio_Prices WHERE owner_user_id = ? AND symbol = ?", (owner_user_id, symbol),
        ).rowcount if with_prices else 0
    return {"assets": assets, "prices": prices}


def save_price(db_path: str, owner_user_id: str, symbol: str, day: str, price: float, currency: str, note: str = "") -> dict:
    symbol = normalize_symbol(symbol)
    if not symbol:
        raise AssetError("Name the asset (its symbol)")
    if not price > 0:
        raise AssetError("Enter a positive price")
    create_tables(db_path)
    with transaction(db_path) as conn:
        conn.execute(
            "INSERT INTO Portfolio_Prices (owner_user_id, symbol, date, price, currency, note) VALUES (?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(owner_user_id, symbol, date) DO UPDATE SET price = excluded.price, "
            "currency = excluded.currency, note = excluded.note, created_at = datetime('now')",
            (owner_user_id, symbol, day, float(price), currency.upper(), note.strip() or None),
        )
    return {"symbol": symbol, "date": day, "price": float(price), "currency": currency.upper(), "note": note.strip() or None}


def delete_price(db_path: str, owner_user_id: str, symbol: str, day: str) -> int:
    create_tables(db_path)
    with transaction(db_path) as conn:
        return conn.execute(
            "DELETE FROM Portfolio_Prices WHERE owner_user_id = ? AND symbol = ? AND date = ?",
            (owner_user_id, normalize_symbol(symbol), day),
        ).rowcount


def manual_prices(conn: sqlite3.Connection, owner_user_id: str) -> dict[str, list[tuple[str, float, str]]]:
    """``{symbol: [(date, price, currency), ...]}`` sorted by date, for the ledger."""
    try:
        rows = conn.execute(
            "SELECT symbol, date, price, currency FROM Portfolio_Prices WHERE owner_user_id = ? "
            "AND price > 0 ORDER BY symbol, date",
            (owner_user_id,),
        ).fetchall()
    except sqlite3.OperationalError:
        return {}
    grouped: dict[str, list[tuple[str, float, str]]] = defaultdict(list)
    for symbol, day, price, currency in rows:
        grouped[str(symbol)].append((str(day)[:10], float(price), str(currency or "").upper()))
    return dict(grouped)


def defined_assets(conn: sqlite3.Connection, owner_user_id: str) -> dict[str, dict]:
    try:
        return {
            str(row["symbol"]): dict(row)
            for row in conn.execute(
                "SELECT symbol, name, asset_category, currency, fetch_prices FROM Portfolio_Assets WHERE owner_user_id = ?",
                (owner_user_id,),
            ).fetchall()
        }
    except sqlite3.OperationalError:
        return {}


def manual_only_symbols(conn: sqlite3.Connection, owner_user_id: str) -> set[str]:
    """Defined assets priced only by hand: market data is never fetched or used for them."""
    return {symbol for symbol, asset in defined_assets(conn, owner_user_id).items() if not asset.get("fetch_prices")}


def security_names(conn: sqlite3.Connection, owner_user_id: str) -> dict[str, str]:
    """A name per symbol: the asset's definition, else the latest imported trade's description.

    Descriptions of records typed in by hand are notes ("Manual buy of 10
    AAA"), so they never name a security.
    """
    names: dict[str, str] = {}
    try:
        for symbol, description in conn.execute(
            "SELECT symbol, description FROM Transactions WHERE owner_user_id = ? AND activity_type = 'TRADE' "
            "AND COALESCE(description, '') != '' AND COALESCE(source_file, '') != ? ORDER BY trade_date",
            (owner_user_id, MANUAL_SOURCE),
        ).fetchall():
            names[str(symbol)] = str(description)
    except sqlite3.OperationalError:
        pass
    for symbol, asset in defined_assets(conn, owner_user_id).items():
        names[symbol] = asset["name"]
    return names
