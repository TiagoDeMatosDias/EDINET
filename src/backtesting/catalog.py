"""Who ran each saved backtest, and how the saved-results list describes it.

A saved backtest is a folder of generated files in ``data/artifacts/backtests``
(``result.json``, ``backtest.zip``, charts). Its row in ``app.db`` records the
account that ran it and the title, subtitle, and headline figures the
saved-results list shows. A folder without a row is an unowned result from
before ownership was recorded, visible to administrators only.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.orchestrator.common.sqlite import (
    DEFAULT_BUSY_TIMEOUT_MS,
    connect_write,
    initialize_managed_database,
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS saved_backtests (
    backtest_id TEXT PRIMARY KEY,
    owner_user_id TEXT,
    kind TEXT,
    title TEXT,
    subtitle TEXT,
    headline_json TEXT NOT NULL DEFAULT '{}',
    recorded_at TEXT NOT NULL
);
"""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _row(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "owner_user_id": row["owner_user_id"],
        "kind": row["kind"],
        "title": row["title"],
        "subtitle": row["subtitle"],
        "headline": json.loads(row["headline_json"] or "{}"),
    }


class BacktestCatalog:
    """Rows of the ``saved_backtests`` table, keyed by result folder name."""

    def __init__(self, path: str | Path, *, busy_timeout_ms: int = DEFAULT_BUSY_TIMEOUT_MS) -> None:
        self.path = Path(path)
        self.busy_timeout_ms = busy_timeout_ms
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = self._connect()
        try:
            initialize_managed_database(conn)
            conn.executescript(SCHEMA)
            conn.commit()
        finally:
            conn.close()

    def _connect(self) -> sqlite3.Connection:
        return connect_write(self.path, busy_timeout_ms=self.busy_timeout_ms)

    def record_owner(self, backtest_id: str, owner_user_id: str | None) -> None:
        conn = self._connect()
        try:
            conn.execute(
                "INSERT INTO saved_backtests(backtest_id, owner_user_id, recorded_at) VALUES (?, ?, ?) "
                "ON CONFLICT(backtest_id) DO UPDATE SET owner_user_id = excluded.owner_user_id",
                (backtest_id, owner_user_id, _utc_now()),
            )
            conn.commit()
        finally:
            conn.close()

    def describe(
        self,
        backtest_id: str,
        *,
        kind: str,
        title: str,
        subtitle: str = "",
        headline: dict[str, Any] | None = None,
    ) -> None:
        conn = self._connect()
        try:
            conn.execute(
                "INSERT INTO saved_backtests(backtest_id, kind, title, subtitle, headline_json, recorded_at) "
                "VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(backtest_id) DO UPDATE SET kind = excluded.kind, "
                "title = excluded.title, subtitle = excluded.subtitle, headline_json = excluded.headline_json",
                (backtest_id, kind, title, subtitle, json.dumps(headline or {}, default=str), _utc_now()),
            )
            conn.commit()
        finally:
            conn.close()

    def get(self, backtest_id: str) -> dict[str, Any] | None:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT * FROM saved_backtests WHERE backtest_id = ?", (backtest_id,)
            ).fetchone()
        finally:
            conn.close()
        return _row(row) if row else None

    def all(self) -> dict[str, dict[str, Any]]:
        conn = self._connect()
        try:
            rows = conn.execute("SELECT * FROM saved_backtests").fetchall()
        finally:
            conn.close()
        return {str(row["backtest_id"]): _row(row) for row in rows}
