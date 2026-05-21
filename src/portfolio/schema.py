"""Database DDL and Pydantic models for the Portfolio module.

Creates all tables in db3 (Portfolio.db) on first use.  All DDL uses
``IF NOT EXISTS`` so calls are idempotent.
"""

from __future__ import annotations

import sqlite3
import logging
from typing import Optional, Any
from datetime import date as Date

from pydantic import BaseModel, Field

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

_DDL_PORTDAILY_MIGRATE = """
ALTER TABLE Portfolio_Daily ADD COLUMN cash_ccy_json TEXT;
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

_DDL_HOLDINGS_MIGRATE = """
ALTER TABLE Portfolio_Holdings ADD COLUMN market_value_native REAL;
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

_DDL_HH_MIGRATE = """
ALTER TABLE Holdings_History ADD COLUMN market_value_native REAL;
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
# Helper
# ---------------------------------------------------------------------------

def _exec_ddl(db_path: str, ddl: str) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.executescript(ddl)
        conn.commit()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def create_tables(db_path: str) -> None:
    """Create all Portfolio module tables (idempotent).

    Also runs safe migrations for schema upgrades (e.g. adding
    ``market_value_native`` column to older databases).
    """
    for ddl in _ALL_DDL:
        _exec_ddl(db_path, ddl)
    # Safe migration: add market_value_native if missing
    try:
        _exec_ddl(db_path, _DDL_HOLDINGS_MIGRATE)
    except sqlite3.OperationalError:
        pass  # column already exists
    try:
        _exec_ddl(db_path, _DDL_HH_MIGRATE)
    except sqlite3.OperationalError:
        pass  # column already exists
    try:
        _exec_ddl(db_path, _DDL_PORTDAILY_MIGRATE)
    except sqlite3.OperationalError:
        pass  # column already exists
    logger.info("Portfolio tables created/verified at %s", db_path)


# ---------------------------------------------------------------------------
# Pydantic models (API request / response shapes)
# ---------------------------------------------------------------------------

class TransactionEntry(BaseModel):
    """A single entry from IBKR XML, as stored in the Transactions table."""
    id: Optional[int] = None
    transaction_id: str
    trade_id: Optional[str] = None
    account_id: Optional[str] = None
    activity_type: str
    asset_category: Optional[str] = None
    symbol: Optional[str] = None
    description: Optional[str] = None
    isin: Optional[str] = None
    conid: Optional[str] = None
    currency: str
    trade_date: str
    settle_date: Optional[str] = None
    quantity: float = 0
    trade_price: Optional[float] = None
    trade_money: Optional[float] = None
    amount: float = 0
    proceeds: Optional[float] = None
    commission: float = 0
    taxes: float = 0
    net_cash: Optional[float] = None
    buy_sell: Optional[str] = None
    fx_rate_to_base: Optional[float] = None
    strike: Optional[float] = None
    expiry: Optional[str] = None
    put_call: Optional[str] = None
    underlying_symbol: Optional[str] = None
    underlying_conid: Optional[str] = None
    multiplier: float = 1
    action_description: Optional[str] = None
    action_id: Optional[str] = None
    source_file: Optional[str] = None
    imported_at: Optional[str] = None
    notes: Optional[str] = None


class UploadResponse(BaseModel):
    """Summary returned after an XML upload."""
    source_file: str
    total_entries: int
    inserted: int
    skipped: int
    by_activity: dict[str, int] = Field(default_factory=dict)
    new_tickers_fetched: list[str] = Field(default_factory=list)
    ticker_fetch_failures: list[str] = Field(default_factory=list)


class HoldingItem(BaseModel):
    """One row of the current holdings snapshot."""
    symbol: str
    asset_category: str
    quantity: float
    avg_cost: Optional[float] = None
    market_price: Optional[float] = None
    market_value: Optional[float] = None          # base currency
    market_value_native: Optional[float] = None   # asset native currency
    currency: str
    fx_rate: Optional[float] = None
    weight: Optional[float] = None
    is_option: bool = False
    strike: Optional[float] = None
    expiry: Optional[str] = None
    put_call: Optional[str] = None
    underlying: Optional[str] = None


class BenchmarkInfo(BaseModel):
    """Benchmark comparison results."""
    ticker: Optional[str] = None
    total_return: Optional[float] = None
    excess_return: Optional[float] = None
    alpha: Optional[float] = None
    beta: Optional[float] = None
    information_ratio: Optional[float] = None
    tracking_error: Optional[float] = None
    series: list[dict] = Field(default_factory=list)  # [{date, cumulative_return}]


class DividendBreakdown(BaseModel):
    """Dividend income breakdown from XML."""
    total_gross: float = 0
    total_tax: float = 0
    total_net: float = 0


class ReturnDistribution(BaseModel):
    """Daily return distribution statistics."""
    min: float = 0
    p25: float = 0
    median: float = 0
    p75: float = 0
    max: float = 0
    skewness: float = 0
    kurtosis: float = 0
    positive_days: int = 0
    negative_days: int = 0
    zero_days: int = 0


class ReturnAttribution(BaseModel):
    """Breakdown of total return into components."""
    total_return: float = 0
    dividend_yield: float = 0
    capital_appreciation: float = 0
    real_return: float = 0
    inflation_total: float = 0


class PerformanceResponse(BaseModel):
    """Portfolio performance metrics."""
    start_date: str
    end_date: str
    base_currency: str
    total_return: Optional[float] = None
    annualized_return: Optional[float] = None
    volatility: Optional[float] = None
    sharpe_ratio: Optional[float] = None
    sortino_ratio: Optional[float] = None
    max_drawdown: Optional[float] = None
    max_dd_peak_date: Optional[str] = None
    max_dd_trough_date: Optional[str] = None
    calmar_ratio: Optional[float] = None
    win_rate: Optional[float] = None
    avg_win: Optional[float] = None
    avg_loss: Optional[float] = None
    profit_factor: Optional[float] = None
    var_95: Optional[float] = None
    cvar_95: Optional[float] = None
    total_dividend_income: float = 0
    risk_free_rate: Optional[float] = None
    benchmark: Optional[BenchmarkInfo] = None
    dividend_breakdown: Optional[DividendBreakdown] = None
    return_distribution: Optional[ReturnDistribution] = None
    return_attribution: Optional[ReturnAttribution] = None
    inflation_series: list[dict] = Field(default_factory=list)  # [{date, cumulative}]


class DateRangeResponse(BaseModel):
    min_date: Optional[str] = None
    max_date: Optional[str] = None


class ActivitySummaryResponse(BaseModel):
    by_activity: dict[str, int] = Field(default_factory=dict)


class RebuildResponse(BaseModel):
    message: str
    daily_rows: int
    holdings_count: int
