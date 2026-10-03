"""Transaction CRUD for the Transactions table in db3 (Portfolio.db).

Deduplication is on ``transactionID`` — re-uploading the same XML is safe,
and fills in details that older imports did not keep.
"""

from __future__ import annotations

import logging
import sqlite3
from collections import defaultdict

from src.orchestrator.common.db_config import get_db3
from src.orchestrator.common.sqlite import connect_read, transaction
from src.portfolio.schema import create_tables

logger = logging.getLogger(__name__)

# Columns expected in every normalized entry dict (from ibkr_parser).
_ENTRY_COLS = [
    "transaction_id", "trade_id", "account_id", "activity_type",
    "asset_category", "symbol", "description", "isin", "conid",
    "currency", "trade_date", "settle_date", "quantity", "trade_price",
    "trade_money", "amount", "proceeds", "commission", "taxes",
    "net_cash", "buy_sell", "fx_rate_to_base",
    "strike", "expiry", "put_call", "underlying_symbol",
    "underlying_conid", "multiplier", "action_description", "action_id",
    "report_date", "commission_currency",
]

# Details added after the first imports: re-importing a file fills them in on
# records already stored, without touching anything else.
_BACKFILL_COLS = ("report_date", "commission_currency")


def insert_entries(
    db_path: str | None = None,
    entries: list[dict] | None = None,
    source_file: str = "",
    owner_user_id: str = "",
) -> dict:
    """Insert parsed entries with deduplication on transactionID.

    Args:
        db_path: Path to Portfolio.db (defaults to ``get_db3()``).
        entries: List of normalized entry dicts from ``normalize_entries()``.
        source_file: Original XML filename for the ``source_file`` column.

    Returns:
        ``{'inserted': N, 'skipped': N, 'updated': N, 'by_activity': {...},
          'new_tickers': [...]}`` where *updated* counts stored records that
        gained details (see ``_BACKFILL_COLS``).
    """
    db_path = db_path or get_db3()
    entries = entries or []

    if not entries:
        return {"inserted": 0, "skipped": 0, "updated": 0, "by_activity": {}, "new_tickers": []}

    # Ensure tables exist
    create_tables(db_path)

    inserted = 0
    skipped = 0
    updated = 0
    by_activity: dict[str, int] = defaultdict(int)
    new_tickers: list[str] = []

    with transaction(db_path) as conn:
        # Build set of existing transaction IDs
        existing_ids = set()
        txn_ids = [
            e["transaction_id"] for e in entries
            if e.get("transaction_id")
        ]
        if txn_ids:
            placeholders = ",".join("?" for _ in txn_ids)
            rows = conn.execute(
                f"SELECT transaction_id FROM Transactions WHERE transaction_id IN ({placeholders}) AND owner_user_id = ?",
                [*txn_ids, owner_user_id],
            ).fetchall()
            existing_ids = {r[0] for r in rows}

        # Track known symbols for new_ticker detection
        known_symbols = _get_known_symbols(conn)

        # Build INSERT statement
        insert_columns = [*_ENTRY_COLS, "source_file", "owner_user_id"]
        cols_str = ", ".join(insert_columns)
        placeholders_str = ", ".join("?" for _ in insert_columns)
        sql = f"INSERT OR IGNORE INTO Transactions ({cols_str}) VALUES ({placeholders_str})"

        for e in entries:
            txn_id = e.get("transaction_id", "")
            if not txn_id:
                skipped += 1
                continue

            if txn_id in existing_ids:
                skipped += 1
                # Only fill what the file has and the stored record lacks.
                fill = {col: e[col] for col in _BACKFILL_COLS if e.get(col) is not None}
                if fill:
                    cursor = conn.execute(
                        "UPDATE Transactions SET "
                        + ", ".join(f"{col} = COALESCE({col}, ?)" for col in fill)
                        + " WHERE transaction_id = ? AND owner_user_id = ? AND ("
                        + " OR ".join(f"{col} IS NULL" for col in fill) + ")",
                        [*fill.values(), txn_id, owner_user_id],
                    )
                    updated += max(cursor.rowcount, 0)
                continue

            # Build tuple
            values = (*tuple(e.get(col) for col in _ENTRY_COLS), source_file or None, owner_user_id)
            try:
                cursor = conn.execute(sql, values)
                if cursor.rowcount != 1:
                    skipped += 1
                    continue
                inserted += 1
                by_activity[e.get("activity_type", "UNKNOWN")] += 1
                existing_ids.add(txn_id)

                # Track new tickers (STK only, not options or forex)
                if e.get("activity_type") == "TRADE" and e.get("asset_category") == "STK":
                    sym = e.get("symbol", "").strip()
                    if sym and sym not in known_symbols and "." not in sym[:2]:
                        new_tickers.append(sym)
                        known_symbols.add(sym)

            except sqlite3.IntegrityError:
                skipped += 1

    return {
        "inserted": inserted,
        "skipped": skipped,
        "updated": updated,
        "by_activity": dict(by_activity),
        "new_tickers": list(set(new_tickers)),
    }


def get_transactions(
    db_path: str | None = None,
    *,
    symbol: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    activity_type: str | None = None,
    limit: int = 1000,
    offset: int = 0,
    slim: bool = True,
    owner_user_id: str = "",
) -> list[dict]:
    """Query transactions with optional filters.

    When *slim* is True (default), returns only the columns needed for
    the transactions table (~70% smaller payload vs SELECT *).
    """
    if slim:
        cols = (
            "id, trade_date, settle_date, report_date, activity_type, asset_category, symbol, "
            "quantity, trade_price, amount, net_cash, currency, buy_sell, commission, "
            "commission_currency, taxes, description, trade_money, proceeds, account_id, source_file"
        )
    else:
        cols = "*"
    db_path = db_path or get_db3()
    create_tables(db_path)  # idempotent
    conn = connect_read(db_path)

    where = ["owner_user_id = ?"]
    params: list = [owner_user_id]

    if symbol:
        where.append("symbol = ?")
        params.append(symbol)
    if start_date:
        where.append("trade_date >= ?")
        params.append(start_date)
    if end_date:
        where.append("trade_date <= ?")
        params.append(end_date)
    if activity_type:
        where.append("activity_type = ?")
        params.append(activity_type)

    sql = f"SELECT {cols} FROM Transactions"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY trade_date DESC, id DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])

    try:
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


def get_unique_symbols(db_path: str | None = None, owner_user_id: str = "") -> list[dict]:
    """Return distinct symbols with asset categories from Transactions."""
    db_path = db_path or get_db3()
    create_tables(db_path)
    conn = connect_read(db_path)
    try:
        rows = conn.execute(
            "SELECT DISTINCT symbol, asset_category FROM Transactions "
            "WHERE symbol IS NOT NULL AND symbol != '' AND owner_user_id = ? "
            "ORDER BY symbol", (owner_user_id,)
        ).fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


def get_date_range(db_path: str | None = None, owner_user_id: str = "") -> dict:
    """Return min and max trade_date from Transactions."""
    db_path = db_path or get_db3()
    create_tables(db_path)
    conn = connect_read(db_path)
    try:
        row = conn.execute(
            "SELECT MIN(trade_date) AS min_date, "
            "MAX(trade_date) AS max_date FROM Transactions WHERE owner_user_id = ?",
            (owner_user_id,),
        ).fetchone()
    finally:
        conn.close()
    return {"min_date": row[0], "max_date": row[1]}


def get_activity_summary(db_path: str | None = None, owner_user_id: str = "") -> dict:
    """Return counts by activity_type."""
    db_path = db_path or get_db3()
    create_tables(db_path)
    conn = connect_read(db_path)
    try:
        rows = conn.execute(
            "SELECT activity_type, COUNT(*) AS cnt "
            "FROM Transactions WHERE owner_user_id = ? GROUP BY activity_type",
            (owner_user_id,),
        ).fetchall()
    finally:
        conn.close()
    return {r["activity_type"]: r["cnt"] for r in rows}


def delete_by_source(db_path: str | None = None, source_file: str = "", owner_user_id: str = "") -> int:
    """Delete all transactions from a given source file. Returns deleted count."""
    db_path = db_path or get_db3()
    create_tables(db_path)
    with transaction(db_path) as conn:
        cursor = conn.execute(
            "DELETE FROM Transactions WHERE source_file = ? AND owner_user_id = ?",
            (source_file, owner_user_id),
        )
        return cursor.rowcount


def get_import_files(db_path: str | None = None, owner_user_id: str = "") -> list[dict]:
    """Imported files with their record counts and date spans, newest import first.

    A record belongs to the file that first imported it: later files that
    overlap skip the records already stored.
    """
    db_path = db_path or get_db3()
    create_tables(db_path)
    conn = connect_read(db_path)
    try:
        rows = conn.execute(
            "SELECT COALESCE(source_file, '') AS source_file, COUNT(*) AS records, "
            "MIN(trade_date) AS first_date, MAX(trade_date) AS last_date, "
            "MAX(imported_at) AS imported_at, COUNT(DISTINCT symbol) AS symbols "
            "FROM Transactions WHERE owner_user_id = ? "
            "GROUP BY COALESCE(source_file, '') ORDER BY MAX(imported_at) DESC, source_file",
            (owner_user_id,),
        ).fetchall()
    finally:
        conn.close()
    return [dict(row) for row in rows]


class EmptySelection(ValueError):
    """A delete must name what to delete; nothing chosen never means everything."""


def _selection(
    owner_user_id: str,
    *,
    everything: bool = False,
    ids: list[int] | None = None,
    source_files: list[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> tuple[str, list]:
    """WHERE clause for a set of the owner's records; criteria combine with AND."""
    where = ["owner_user_id = ?"]
    params: list = [owner_user_id]
    if ids:
        where.append(f"id IN ({','.join('?' for _ in ids)})")
        params.extend(int(value) for value in ids)
    if source_files:
        where.append(f"COALESCE(source_file, '') IN ({','.join('?' for _ in source_files)})")
        params.extend(source_files)
    if start_date:
        where.append("trade_date >= ?")
        params.append(start_date)
    if end_date:
        where.append("trade_date <= ?")
        params.append(end_date)
    if len(where) == 1 and not everything:
        raise EmptySelection("Choose records, files, dates, or everything to delete")
    return " AND ".join(where), params


def summarize_selection(db_path: str | None = None, owner_user_id: str = "", **selection) -> dict:
    """What a delete would remove: counts by type, the date span, files, and symbols."""
    db_path = db_path or get_db3()
    create_tables(db_path)
    where, params = _selection(owner_user_id, **selection)
    conn = connect_read(db_path)
    try:
        by_type = {
            row[0]: row[1]
            for row in conn.execute(
                f"SELECT activity_type, COUNT(*) FROM Transactions WHERE {where} GROUP BY activity_type ORDER BY COUNT(*) DESC",
                params,
            ).fetchall()
        }
        span = conn.execute(
            f"SELECT MIN(trade_date), MAX(trade_date), COUNT(DISTINCT symbol) FROM Transactions WHERE {where}",
            params,
        ).fetchone()
        files = [
            row[0]
            for row in conn.execute(
                f"SELECT DISTINCT COALESCE(source_file, '') FROM Transactions WHERE {where} ORDER BY 1",
                params,
            ).fetchall()
        ]
        total = conn.execute(
            "SELECT COUNT(*) FROM Transactions WHERE owner_user_id = ?", (owner_user_id,),
        ).fetchone()[0]
    finally:
        conn.close()
    count = sum(by_type.values())
    return {
        "records": count,
        "by_type": by_type,
        "first_date": span[0],
        "last_date": span[1],
        "symbols": span[2] or 0,
        "source_files": files,
        "remaining": total - count,
    }


def delete_selection(db_path: str | None = None, owner_user_id: str = "", **selection) -> int:
    """Delete the owner's chosen records; returns how many were removed."""
    db_path = db_path or get_db3()
    create_tables(db_path)
    where, params = _selection(owner_user_id, **selection)
    with transaction(db_path) as conn:
        cursor = conn.execute(f"DELETE FROM Transactions WHERE {where}", params)
        deleted = cursor.rowcount
        if selection.get("everything") and len(params) == 1:
            conn.execute("DELETE FROM Portfolio_Metrics WHERE owner_user_id = ?", (owner_user_id,))
        return deleted


def _get_known_symbols(conn: sqlite3.Connection) -> set[str]:
    """Return the set of symbols already in the Transactions table."""
    rows = conn.execute(
        "SELECT DISTINCT symbol FROM Transactions WHERE symbol IS NOT NULL"
    ).fetchall()
    return {r[0] for r in rows if r[0]}
