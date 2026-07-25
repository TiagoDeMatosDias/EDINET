"""Versioned market-data tables for point-in-time backtesting.

Adds open/high/low/close/adjusted_close/volume without breaking existing
Stock_Prices consumers.
"""

from __future__ import annotations

import sqlite3
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from src.orchestrator.common.sqlite import connect_write, transaction


class MarketDataStore:
    """Versioned market prices, corporate actions, aliases, and listings."""

    def __init__(self, path: str | Path, *, busy_timeout_ms: int = 30_000) -> None:
        self.path = Path(path).expanduser()
        self.busy_timeout_ms = busy_timeout_ms
        self.initialize()

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = connect_write(self.path, busy_timeout_ms=self.busy_timeout_ms)
        try:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS market_prices (
                    date TEXT NOT NULL,
                    ticker TEXT NOT NULL,
                    currency TEXT NOT NULL,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    adjusted_close REAL,
                    volume INTEGER,
                    provider TEXT,
                    source_id TEXT,
                    retrieved_at TEXT NOT NULL,
                    PRIMARY KEY(date, ticker)
                );
                CREATE INDEX IF NOT EXISTS idx_market_prices_ticker_date ON market_prices(ticker, date);
                CREATE TABLE IF NOT EXISTS corporate_actions (
                    action_id TEXT PRIMARY KEY,
                    ticker TEXT NOT NULL,
                    action_date TEXT NOT NULL,
                    action_type TEXT NOT NULL CHECK (action_type IN ('split', 'dividend', 'delisting')),
                    value REAL,
                    currency TEXT,
                    provider TEXT,
                    source_id TEXT,
                    retrieved_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_actions_ticker_date ON corporate_actions(ticker, action_date);
                CREATE TABLE IF NOT EXISTS security_aliases (
                    company_code TEXT NOT NULL,
                    ticker TEXT NOT NULL,
                    valid_from TEXT NOT NULL,
                    valid_to TEXT,
                    exchange TEXT,
                    source TEXT,
                    PRIMARY KEY(company_code, ticker, valid_from)
                );
                CREATE TABLE IF NOT EXISTS security_listings (
                    company_code TEXT NOT NULL,
                    ticker TEXT NOT NULL,
                    valid_from TEXT NOT NULL,
                    valid_to TEXT,
                    status TEXT NOT NULL DEFAULT 'active',
                    delisting_cash_value REAL,
                    source TEXT,
                    PRIMARY KEY(company_code, ticker, valid_from)
                );
                """
            )
            conn.commit()
        finally:
            conn.close()

    def insert_price(self, **values: Any) -> None:
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            columns = ", ".join(values.keys())
            placeholders = ", ".join(f":{k}" for k in values)
            conn.execute(
                f"INSERT OR REPLACE INTO market_prices ({columns}) VALUES ({placeholders})",
                values,
            )

    def get_prices(self, ticker: str, from_date: str, to_date: str) -> list[sqlite3.Row]:
        conn = connect_write(self.path, busy_timeout_ms=self.busy_timeout_ms)
        try:
            return list(
                conn.execute(
                    "SELECT * FROM market_prices WHERE ticker = ? AND date BETWEEN ? AND ? ORDER BY date",
                    (ticker, from_date, to_date),
                ).fetchall()
            )
        finally:
            conn.close()

    def insert_action(self, **values: Any) -> None:
        from uuid import uuid4

        values.setdefault("action_id", str(uuid4()))
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        values.setdefault("retrieved_at", now)
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            columns = ", ".join(values.keys())
            placeholders = ", ".join(f":{k}" for k in values)
            conn.execute(
                f"INSERT OR REPLACE INTO corporate_actions ({columns}) VALUES ({placeholders})",
                values,
            )

    def get_actions(self, ticker: str, from_date: str | None = None) -> list[sqlite3.Row]:
        conn = connect_write(self.path, busy_timeout_ms=self.busy_timeout_ms)
        try:
            if from_date:
                return list(
                    conn.execute(
                        "SELECT * FROM corporate_actions WHERE ticker = ? AND action_date >= ? ORDER BY action_date",
                        (ticker, from_date),
                    ).fetchall()
                )
            return list(
                conn.execute(
                    "SELECT * FROM corporate_actions WHERE ticker = ? ORDER BY action_date",
                    (ticker,),
                ).fetchall()
            )
        finally:
            conn.close()

    def upsert_alias(self, **values: Any) -> None:
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            columns = ", ".join(values.keys())
            placeholders = ", ".join(f":{k}" for k in values)
            conn.execute(
                f"INSERT OR REPLACE INTO security_aliases ({columns}) VALUES ({placeholders})",
                values,
            )

    def get_ticker_for(self, company_code: str, as_of: str) -> str | None:
        conn = connect_write(self.path, busy_timeout_ms=self.busy_timeout_ms)
        try:
            row = conn.execute(
                """SELECT ticker FROM security_aliases
                   WHERE company_code = ? AND valid_from <= ?
                   AND (valid_to IS NULL OR valid_to > ?)
                   ORDER BY valid_from DESC LIMIT 1""",
                (company_code, as_of, as_of),
            ).fetchone()
            return row["ticker"] if row else None
        finally:
            conn.close()

    def upsert_listing(self, **values: Any) -> None:
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            columns = ", ".join(values.keys())
            placeholders = ", ".join(f":{k}" for k in values)
            conn.execute(
                f"INSERT OR REPLACE INTO security_listings ({columns}) VALUES ({placeholders})",
                values,
            )

    def is_listed(self, company_code: str, as_of: str) -> bool:
        conn = connect_write(self.path, busy_timeout_ms=self.busy_timeout_ms)
        try:
            row = conn.execute(
                """SELECT 1 FROM security_listings
                   WHERE company_code = ? AND valid_from <= ? AND status = 'active'
                   AND (valid_to IS NULL OR valid_to > ?) LIMIT 1""",
                (company_code, as_of, as_of),
            ).fetchone()
            return row is not None
        finally:
            conn.close()
