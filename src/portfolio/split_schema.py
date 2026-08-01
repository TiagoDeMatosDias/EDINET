"""Idempotent DDL for the Stock_Splits table in db2 (Standardized.db).

Stores corporate split events alongside the Stock_Prices table so that
split-adjusted prices can be computed at read time without rewriting
historical price data.
"""

from __future__ import annotations

import logging
import sqlite3

logger = logging.getLogger(__name__)

_DDL_STOCK_SPLITS = """
CREATE TABLE IF NOT EXISTS Stock_Splits (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker            TEXT NOT NULL,
    split_date        TEXT NOT NULL,
    ratio_from        REAL NOT NULL,
    ratio_to          REAL NOT NULL,
    detection_method  TEXT NOT NULL DEFAULT 'price_heuristic',
    confirmation      TEXT NOT NULL DEFAULT 'pending',
    confirmed_by      TEXT,
    source_detail     TEXT,
    share_count_before REAL,
    share_count_after  REAL,
    share_count_ratio  REAL,
    price_basis       TEXT NOT NULL DEFAULT 'raw',
    provider          TEXT,
    source_id         TEXT,
    source_revision   TEXT,
    retrieved_at      TEXT,
    announced_at      TEXT,
    ex_date           TEXT,
    effective_date    TEXT,
    record_date       TEXT,
    superseded_by     INTEGER,
    created_at        TEXT DEFAULT (datetime('now')),
    updated_at        TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_stock_splits_confirmation
    ON Stock_Splits(confirmation);

CREATE TABLE IF NOT EXISTS Split_Detection_Watermarks (
    ticker          TEXT PRIMARY KEY,
    last_price_date TEXT NOT NULL,
    updated_at      TEXT DEFAULT (datetime('now'))
);

"""


def _column_names(conn: sqlite3.Connection, table_name: str) -> set[str]:
    return {
        str(row[1])
        for row in conn.execute(f'PRAGMA table_info("{table_name}")')
    }


def _create_split_tables(conn: sqlite3.Connection) -> None:
    """Execute the Stock_Splits DDL against an existing connection."""
    for statement in _DDL_STOCK_SPLITS.split(";"):
        stmt = statement.strip()
        if stmt:
            conn.execute(stmt)
    # Migrate pre-existing tables one column at a time.  ALTER TABLE is used
    # rather than replacing the table so existing detection history remains
    # intact.
    migrations = {
        "price_basis": "TEXT NOT NULL DEFAULT 'raw'",
        "provider": "TEXT",
        "source_id": "TEXT",
        "source_revision": "TEXT",
        "retrieved_at": "TEXT",
        "announced_at": "TEXT",
        "ex_date": "TEXT",
        "effective_date": "TEXT",
        "record_date": "TEXT",
        "superseded_by": "INTEGER",
    }
    columns = _column_names(conn, "Stock_Splits")
    for name, definition in migrations.items():
        if name not in columns:
            conn.execute(
                f'ALTER TABLE "Stock_Splits" ADD COLUMN "{name}" {definition}'
            )
    # A legacy database may contain duplicate heuristic rows from scans run
    # before the unique index was introduced.  Preserve those rows and fall
    # back to a lookup index instead of making schema startup fail.
    try:
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_stock_splits_ticker_date "
            "ON Stock_Splits(ticker, split_date)"
        )
    except sqlite3.IntegrityError:
        logger.warning(
            "Stock_Splits contains duplicate ticker/date rows; keeping them "
            "and creating a non-unique lookup index"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_stock_splits_ticker_date_lookup "
            "ON Stock_Splits(ticker, split_date)"
        )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_stock_splits_source "
        "ON Stock_Splits(provider, source_id)"
    )


def ensure_split_tables(db2_path: str | None = None, conn: sqlite3.Connection | None = None) -> None:
    """Create the Stock_Splits table and indexes if they don't exist.

    Args:
        db2_path: Path to Standardized.db (absolute or relative). Ignored
            when *conn* is provided.
        conn: Optional existing connection to use instead of opening one.

    This is idempotent — safe to call on every application start and
    every pipeline job run.
    """
    if conn is not None:
        _create_split_tables(conn)
        # The caller may already be inside a larger ingestion/reconciliation
        # transaction.  Do not commit its price or split writes implicitly;
        # the caller owns commit/rollback semantics for an injected connection.
        return
    own_conn = sqlite3.connect(db2_path or ":memory:")
    try:
        own_conn.execute("PRAGMA busy_timeout = 30000")
        _create_split_tables(own_conn)
        own_conn.commit()
        logger.debug("Stock_Splits schema is current")
    finally:
        own_conn.close()
