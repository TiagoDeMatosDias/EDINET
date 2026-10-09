"""The ``settings`` table in ``app.db``.

Every operator setting and every server-generated secret is one row holding a
JSON value. Which keys exist, their types, defaults, and which are secret is
declared in ``src.settings.registry``; this module only stores values.

Pipeline steps import ``src.settings``, and importing anything under
``src.orchestrator`` discovers the steps, so this package opens SQLite itself
rather than through ``src.orchestrator.common.sqlite``.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DEFAULT_BUSY_TIMEOUT_MS = 30_000
SCHEMA = """
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value_json TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    updated_by TEXT
);
"""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_stored_value(app_db: Path, key: str) -> Any | None:
    """Read one stored value without creating or locking the database.

    Returns ``None`` when the database, the table, or the row is missing, so
    callers can read settings before startup has created ``app.db``.
    """
    if not app_db.is_file():
        return None
    try:
        conn = sqlite3.connect(f"{app_db.resolve().as_uri()}?mode=ro", uri=True)
    except sqlite3.OperationalError:
        return None
    try:
        row = conn.execute(
            "SELECT value_json FROM settings WHERE key = ?", (key,)
        ).fetchone()
    except sqlite3.OperationalError:
        return None
    finally:
        conn.close()
    return json.loads(row[0]) if row else None


def read_stored_values(app_db: Path) -> dict[str, Any]:
    """Read every stored value without creating or locking the database."""
    if not app_db.is_file():
        return {}
    try:
        conn = sqlite3.connect(f"{app_db.resolve().as_uri()}?mode=ro", uri=True)
    except sqlite3.OperationalError:
        return {}
    try:
        rows = conn.execute("SELECT key, value_json FROM settings").fetchall()
    except sqlite3.OperationalError:
        return {}
    finally:
        conn.close()
    return {str(key): json.loads(value) for key, value in rows}


class SettingsStore:
    """Read and write rows of the ``settings`` table."""

    def __init__(
        self,
        path: str | Path,
        *,
        busy_timeout_ms: int = DEFAULT_BUSY_TIMEOUT_MS,
    ) -> None:
        self.path = Path(path).expanduser()
        self.busy_timeout_ms = busy_timeout_ms
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = self._connect()
        try:
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = NORMAL")
            conn.executescript(SCHEMA)
            conn.commit()
        finally:
            conn.close()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.path), timeout=self.busy_timeout_ms / 1000)
        conn.row_factory = sqlite3.Row
        conn.execute(f"PRAGMA busy_timeout = {int(self.busy_timeout_ms)}")
        return conn

    def get(self, key: str) -> Any | None:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT value_json FROM settings WHERE key = ?", (key,)
            ).fetchone()
        finally:
            conn.close()
        return json.loads(row["value_json"]) if row else None

    def rows(self) -> dict[str, dict[str, Any]]:
        """Every stored row: value, when it changed, and who changed it."""
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT key, value_json, updated_at, updated_by FROM settings"
            ).fetchall()
        finally:
            conn.close()
        return {
            str(row["key"]): {
                "value": json.loads(row["value_json"]),
                "updated_at": row["updated_at"],
                "updated_by": row["updated_by"],
            }
            for row in rows
        }

    def set(self, key: str, value: Any, *, updated_by: str | None = None) -> None:
        conn = self._connect()
        try:
            conn.execute(
                "INSERT INTO settings(key, value_json, updated_at, updated_by) "
                "VALUES (?, ?, ?, ?) ON CONFLICT(key) DO UPDATE SET "
                "value_json = excluded.value_json, updated_at = excluded.updated_at, "
                "updated_by = excluded.updated_by",
                (key, json.dumps(value), _utc_now(), updated_by),
            )
            conn.commit()
        finally:
            conn.close()

    def set_if_missing(self, key: str, value: Any) -> Any:
        """Store ``value`` unless the key exists; return the stored value.

        Two processes creating the same secret at once both end up using
        whichever value was written first.
        """
        conn = self._connect()
        try:
            conn.execute(
                "INSERT OR IGNORE INTO settings(key, value_json, updated_at, updated_by) "
                "VALUES (?, ?, ?, NULL)",
                (key, json.dumps(value), _utc_now()),
            )
            conn.commit()
            row = conn.execute(
                "SELECT value_json FROM settings WHERE key = ?", (key,)
            ).fetchone()
        finally:
            conn.close()
        return json.loads(row["value_json"])

    def delete(self, key: str) -> None:
        conn = self._connect()
        try:
            conn.execute("DELETE FROM settings WHERE key = ?", (key,))
            conn.commit()
        finally:
            conn.close()
