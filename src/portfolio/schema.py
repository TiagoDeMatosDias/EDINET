"""Versioned database DDL and migrations for the Portfolio module.

Creates all tables in db3 (Portfolio.db) on first use.  All DDL uses
``IF NOT EXISTS`` so calls are idempotent.
"""

from __future__ import annotations

import sqlite3
import logging
from datetime import datetime, timezone
from pathlib import Path
import shutil

from src.orchestrator.common.sqlite import (
    connect_read,
    connect_write,
    initialize_managed_database,
    table_exists,
)
# Compatibility re-exports; new code imports contracts from portfolio.models.
from src.portfolio.models import (
    ActivitySummaryResponse,
    BenchmarkInfo,
    DateRangeResponse,
    DividendBreakdown,
    HoldingItem,
    PerformanceResponse,
    RebuildResponse,
    ReturnAttribution,
    ReturnDistribution,
    TransactionEntry,
    UploadResponse,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DDL statements
# ---------------------------------------------------------------------------

_DDL_TRANSACTIONS = """
CREATE TABLE IF NOT EXISTS Transactions (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    transaction_id   TEXT NOT NULL UNIQUE,
    trade_id         TEXT,
    account_id       TEXT,
    activity_type    TEXT NOT NULL,
    asset_category   TEXT,
    symbol           TEXT,
    description      TEXT,
    isin             TEXT,
    conid            TEXT,
    currency         TEXT NOT NULL,
    trade_date       TEXT NOT NULL,
    settle_date      TEXT,
    quantity         REAL DEFAULT 0,
    trade_price      REAL,
    trade_money      REAL,
    amount           REAL DEFAULT 0,
    proceeds         REAL,
    commission       REAL DEFAULT 0,
    taxes            REAL DEFAULT 0,
    net_cash         REAL,
    buy_sell         TEXT,
    fx_rate_to_base  REAL,

    strike           REAL,
    expiry           TEXT,
    put_call         TEXT,
    underlying_symbol TEXT,
    underlying_conid  TEXT,
    multiplier       REAL DEFAULT 1,

    action_description TEXT,
    action_id         TEXT,

    source_file      TEXT,
    imported_at      TEXT DEFAULT (datetime('now')),
    notes            TEXT
);

CREATE INDEX IF NOT EXISTS idx_trans_txn_id     ON Transactions(transaction_id);
CREATE INDEX IF NOT EXISTS idx_trans_symbol     ON Transactions(symbol);
CREATE INDEX IF NOT EXISTS idx_trans_date       ON Transactions(trade_date);
CREATE INDEX IF NOT EXISTS idx_trans_activity   ON Transactions(activity_type);
CREATE INDEX IF NOT EXISTS idx_trans_category   ON Transactions(asset_category);
CREATE INDEX IF NOT EXISTS idx_trans_underlying ON Transactions(underlying_symbol);
"""

_DDL_PORTFOLIO_DAILY = """
CREATE TABLE IF NOT EXISTS Portfolio_Daily (
    date               TEXT PRIMARY KEY,
    total_value        REAL,
    cash_balance       REAL,
    stock_value        REAL,
    option_value       REAL,
    daily_return       REAL,
    cumulative_return  REAL,
    dividend_income    REAL,
    net_inflow         REAL,
    cash_ccy_json      TEXT
);

CREATE INDEX IF NOT EXISTS idx_portdaily_date ON Portfolio_Daily(date);
"""

_DDL_HOLDINGS = """
CREATE TABLE IF NOT EXISTS Portfolio_Holdings (
    symbol          TEXT NOT NULL,
    asset_category  TEXT NOT NULL,
    quantity        REAL NOT NULL,
    avg_cost        REAL,
    market_price    REAL,
    market_value    REAL,
    market_value_native REAL,
    currency        TEXT NOT NULL,
    fx_rate         REAL,
    is_option       INTEGER DEFAULT 0,
    strike          REAL,
    expiry          TEXT,
    put_call        TEXT,
    underlying      TEXT,
    PRIMARY KEY (symbol, asset_category)
);
"""

_DDL_HOLDINGS_HISTORY = """
CREATE TABLE IF NOT EXISTS Holdings_History (
    date            TEXT NOT NULL,
    symbol          TEXT NOT NULL,
    asset_category  TEXT NOT NULL,
    quantity        REAL NOT NULL,
    market_price    REAL,
    market_value    REAL,
    market_value_native REAL,
    currency        TEXT,
    fx_rate         REAL,
    is_option       INTEGER DEFAULT 0,
    strike          REAL,
    expiry          TEXT,
    put_call        TEXT,
    underlying      TEXT,
    PRIMARY KEY (date, symbol, asset_category)
);

CREATE INDEX IF NOT EXISTS idx_hh_date ON Holdings_History(date);
"""

_DDL_METRICS = """
CREATE TABLE IF NOT EXISTS Portfolio_Metrics (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    computed_at         TEXT DEFAULT (datetime('now')),
    start_date          TEXT NOT NULL,
    end_date            TEXT NOT NULL,
    base_currency       TEXT NOT NULL,
    total_return        REAL,
    annualized_return   REAL,
    volatility          REAL,
    sharpe_ratio        REAL,
    sortino_ratio       REAL,
    max_drawdown        REAL,
    max_dd_peak_date    TEXT,
    max_dd_trough_date  TEXT,
    calmar_ratio        REAL,
    win_rate            REAL,
    avg_win             REAL,
    avg_loss            REAL,
    profit_factor       REAL,
    var_95              REAL,
    cvar_95             REAL,
    total_dividend_income REAL,
    benchmark_ticker    TEXT,
    benchmark_return    REAL,
    excess_return       REAL,
    alpha               REAL,
    beta                REAL,
    information_ratio   REAL,
    tracking_error      REAL,
    risk_free_rate      REAL,
    params_json         TEXT
);
"""

_ALL_DDL = [_DDL_TRANSACTIONS, _DDL_PORTFOLIO_DAILY, _DDL_HOLDINGS,
            _DDL_HOLDINGS_HISTORY, _DDL_METRICS]


# ---------------------------------------------------------------------------
# Versioned migrations
# ---------------------------------------------------------------------------


def _execute_ddl(conn: sqlite3.Connection, ddl: str) -> None:
    for statement in ddl.split(";"):
        if statement.strip():
            conn.execute(statement)


def _column_names(conn: sqlite3.Connection, table_name: str) -> set[str]:
    return {
        str(row[1])
        for row in conn.execute(f'PRAGMA table_info("{table_name}")')
    }


def _migration_1(conn: sqlite3.Connection) -> None:
    for ddl in _ALL_DDL:
        _execute_ddl(conn, ddl)


def _add_column(
    conn: sqlite3.Connection,
    table_name: str,
    column_name: str,
    declaration: str,
) -> None:
    if column_name not in _column_names(conn, table_name):
        conn.execute(
            f'ALTER TABLE "{table_name}" ADD COLUMN '
            f'"{column_name}" {declaration}'
        )


def _migration_2(conn: sqlite3.Connection) -> None:
    _add_column(conn, "Portfolio_Holdings", "market_value_native", "REAL")
    _add_column(conn, "Holdings_History", "market_value_native", "REAL")
    _add_column(conn, "Portfolio_Daily", "cash_ccy_json", "TEXT")


_MIGRATIONS = ((1, _migration_1), (2, _migration_2))


def _needs_material_migration(path: Path) -> bool:
    if not path.exists() or path.stat().st_size == 0:
        return False
    try:
        conn = connect_read(path)
    except (OSError, sqlite3.DatabaseError):
        return False
    try:
        required_tables = {
            "Transactions",
            "Portfolio_Daily",
            "Portfolio_Holdings",
            "Holdings_History",
            "Portfolio_Metrics",
        }
        existing = {
            str(row[0])
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        if existing.intersection(required_tables) and not required_tables <= existing:
            return True
        required_columns = {
            "Portfolio_Daily": "cash_ccy_json",
            "Portfolio_Holdings": "market_value_native",
            "Holdings_History": "market_value_native",
        }
        return any(
            table_exists(conn, table)
            and column not in _column_names(conn, table)
            for table, column in required_columns.items()
        )
    finally:
        conn.close()


def _backup_database(path: Path) -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    backup = path.with_name(f"{path.name}.backup-{timestamp}")
    shutil.copy2(path, backup)
    return backup


def _apply_migrations(path: Path) -> None:
    conn = connect_write(path)
    try:
        initialize_managed_database(conn)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations ("
            "version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)"
        )
        conn.commit()
        row = conn.execute(
            "SELECT COALESCE(MAX(version), 0) FROM schema_migrations"
        ).fetchone()
        current_version = int(row[0])
        for version, migration in _MIGRATIONS:
            if version <= current_version:
                continue
            try:
                conn.execute("BEGIN IMMEDIATE")
                migration(conn)
                conn.execute(
                    "INSERT INTO schema_migrations(version, applied_at) "
                    "VALUES (?, ?)",
                    (version, datetime.now(timezone.utc).isoformat()),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def create_tables(db_path: str) -> None:
    """Create or upgrade the Portfolio database through explicit migrations."""
    path = Path(db_path).expanduser().resolve(strict=False)
    path.parent.mkdir(parents=True, exist_ok=True)
    if _needs_material_migration(path):
        backup = _backup_database(path)
        logger.info("Backed up Portfolio database before migration: %s", backup)
    _apply_migrations(path)
    logger.info("Portfolio schema is current at %s", path)
