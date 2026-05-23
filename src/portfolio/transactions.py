"""Transaction CRUD for the Transactions table in db3 (Portfolio.db).

Deduplication is on ``transactionID`` — re-uploading the same XML is safe.
"""

from __future__ import annotations

import sqlite3
import logging
from collections import defaultdict

from src.orchestrator.common.db_config import get_db3
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
]


def insert_entries(
    db_path: str | None = None,
    entries: list[dict] | None = None,
    source_file: str = "",
) -> dict:
    """Insert parsed entries with deduplication on transactionID.

    Args:
        db_path: Path to Portfolio.db (defaults to ``get_db3()``).
        entries: List of normalized entry dicts from ``normalize_entries()``.
        source_file: Original XML filename for the ``source_file`` column.

    Returns:
        ``{'inserted': N, 'skipped': N, 'by_activity': {...},
          'new_tickers': [...]}``
    """
    db_path = db_path or get_db3()
    entries = entries or []

    if not entries:
        return {"inserted": 0, "skipped": 0, "by_activity": {}, "new_tickers": []}

    # Ensure tables exist
    create_tables(db_path)

    inserted = 0
    skipped = 0
    by_activity: dict[str, int] = defaultdict(int)
    new_tickers: list[str] = []

    conn = sqlite3.connect(db_path)
    try:
        # Build set of existing transaction IDs
        existing_ids = set()
        txn_ids = [
            e["transaction_id"] for e in entries
            if e.get("transaction_id")
        ]
        if txn_ids:
            placeholders = ",".join("?" for _ in txn_ids)
            rows = conn.execute(
                f"SELECT transaction_id FROM Transactions WHERE transaction_id IN ({placeholders})",
                txn_ids,
            ).fetchall()
            existing_ids = {r[0] for r in rows}

        # Track known symbols for new_ticker detection
        known_symbols = _get_known_symbols(conn)

        # Build INSERT statement
        cols_str = ", ".join(_ENTRY_COLS)
        placeholders_str = ", ".join("?" for _ in _ENTRY_COLS)
        sql = f"INSERT OR IGNORE INTO Transactions ({cols_str}) VALUES ({placeholders_str})"

        for e in entries:
            txn_id = e.get("transaction_id", "")
            if not txn_id:
                skipped += 1
                continue

            if txn_id in existing_ids:
                skipped += 1
                continue

            # Build tuple
            values = tuple(e.get(col) for col in _ENTRY_COLS)
            try:
                conn.execute(sql, values)
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

        # Write source_file to all inserted rows
        if source_file and inserted > 0:
            conn.execute(
                "UPDATE Transactions SET source_file = ? WHERE source_file IS NULL",
                (source_file,),
            )

        conn.commit()
    finally:
        conn.close()

    return {
        "inserted": inserted,
        "skipped": skipped,
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
) -> list[dict]:
    """Query transactions with optional filters.

    When *slim* is True (default), returns only the columns needed for
    the transactions table (~70% smaller payload vs SELECT *).
    """
    if slim:
        cols = "trade_date, activity_type, symbol, quantity, trade_price, amount, net_cash, currency, buy_sell, commission, description, trade_money, proceeds, source_file"
    else:
        cols = "*"
    db_path = db_path or get_db3()
    create_tables(db_path)  # idempotent
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    where = []
    params: list = []

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

    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_unique_symbols(db_path: str | None = None) -> list[dict]:
    """Return distinct symbols with asset categories from Transactions."""
    db_path = db_path or get_db3()
    create_tables(db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT DISTINCT symbol, asset_category FROM Transactions "
        "WHERE symbol IS NOT NULL AND symbol != '' "
        "ORDER BY symbol"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_date_range(db_path: str | None = None) -> dict:
    """Return min and max trade_date from Transactions."""
    db_path = db_path or get_db3()
    create_tables(db_path)
    conn = sqlite3.connect(db_path)
    row = conn.execute(
        "SELECT MIN(trade_date) AS min_date, MAX(trade_date) AS max_date FROM Transactions"
    ).fetchone()
    conn.close()
    return {"min_date": row[0], "max_date": row[1]}


def get_activity_summary(db_path: str | None = None) -> dict:
    """Return counts by activity_type."""
    db_path = db_path or get_db3()
    create_tables(db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT activity_type, COUNT(*) AS cnt FROM Transactions GROUP BY activity_type"
    ).fetchall()
    conn.close()
    return {r["activity_type"]: r["cnt"] for r in rows}


def delete_by_source(db_path: str | None = None, source_file: str = "") -> int:
    """Delete all transactions from a given source file. Returns deleted count."""
    db_path = db_path or get_db3()
    create_tables(db_path)
    conn = sqlite3.connect(db_path)
    cur = conn.execute(
        "DELETE FROM Transactions WHERE source_file = ?", (source_file,)
    )
    conn.commit()
    deleted = cur.rowcount
    conn.close()
    return deleted


def _get_known_symbols(conn: sqlite3.Connection) -> set[str]:
    """Return the set of symbols already in the Transactions table."""
    try:
        rows = conn.execute(
            "SELECT DISTINCT symbol FROM Transactions WHERE symbol IS NOT NULL"
        ).fetchall()
        return {r[0] for r in rows if r[0]}
    except sqlite3.OperationalError:
        return set()
