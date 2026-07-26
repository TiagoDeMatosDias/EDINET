"""SQLite storage for watchlists, notes, alerts, comparisons, and reports."""

from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.orchestrator.common.sqlite import connect_write, transaction


def _timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class ResearchStore:
    """Persist user-authored state outside rebuildable market databases."""

    def __init__(self, path: str | Path, *, busy_timeout_ms: int = 30_000) -> None:
        self.path = Path(path).expanduser()
        self.busy_timeout_ms = busy_timeout_ms
        self.initialize()

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = connect_write(self.path, busy_timeout_ms=self.busy_timeout_ms)
        try:
            conn.execute("PRAGMA journal_mode = WAL")
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS watchlists (
                    watchlist_id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(user_id, name)
                );
                CREATE TABLE IF NOT EXISTS watchlist_items (
                    watchlist_id TEXT NOT NULL REFERENCES watchlists(watchlist_id) ON DELETE CASCADE,
                    edinet_code TEXT NOT NULL,
                    company_name TEXT,
                    added_at TEXT NOT NULL,
                    PRIMARY KEY(watchlist_id, edinet_code)
                );
                CREATE TABLE IF NOT EXISTS research_notes (
                    note_id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    edinet_code TEXT,
                    title TEXT NOT NULL,
                    body TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_notes_owner_company ON research_notes(user_id, edinet_code, updated_at DESC);
                CREATE TABLE IF NOT EXISTS alert_rules (
                    alert_id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    edinet_code TEXT,
                    expression_json TEXT NOT NULL,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS alert_events (
                    event_id TEXT PRIMARY KEY,
                    alert_id TEXT NOT NULL REFERENCES alert_rules(alert_id) ON DELETE CASCADE,
                    occurred_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    acknowledged_at TEXT,
                    dedupe_key TEXT
                );
                CREATE TABLE IF NOT EXISTS comparison_templates (
                    template_id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    companies_json TEXT NOT NULL,
                    metrics_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(user_id, name)
                );
                CREATE TABLE IF NOT EXISTS report_recipes (
                    recipe_id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    definition_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(user_id, name)
                );
                CREATE TABLE IF NOT EXISTS report_runs (
                    run_id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    recipe_id TEXT,
                    status TEXT NOT NULL,
                    as_of TEXT NOT NULL,
                    manifest_json TEXT NOT NULL,
                    artifact_path TEXT,
                    size_bytes INTEGER,
                    sha256 TEXT,
                    created_at TEXT NOT NULL,
                    completed_at TEXT,
                    error_message TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_report_runs_owner ON report_runs(user_id, created_at DESC);
                CREATE TABLE IF NOT EXISTS company_research (
                    user_id TEXT NOT NULL,
                    edinet_code TEXT NOT NULL,
                    thesis_status TEXT,
                    target_value REAL,
                    target_currency TEXT,
                    review_on TEXT,
                    version INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(user_id, edinet_code)
                );
                CREATE TABLE IF NOT EXISTS research_note_revisions (
                    revision_id TEXT PRIMARY KEY,
                    note_id TEXT NOT NULL REFERENCES research_notes(note_id) ON DELETE CASCADE,
                    version INTEGER NOT NULL,
                    title TEXT NOT NULL,
                    body TEXT NOT NULL,
                    saved_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_note_revisions ON research_note_revisions(note_id, version);
                CREATE TABLE IF NOT EXISTS company_tags (
                    user_id TEXT NOT NULL,
                    edinet_code TEXT NOT NULL,
                    tag TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(user_id, edinet_code, tag)
                );
                CREATE TABLE IF NOT EXISTS tag_definitions (
                    user_id TEXT NOT NULL,
                    tag TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(user_id, tag)
                );
                CREATE TABLE IF NOT EXISTS saved_screens (
                    screen_id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    definition_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    version INTEGER NOT NULL DEFAULT 1,
                    UNIQUE(user_id, name)
                );
                CREATE TABLE IF NOT EXISTS screening_runs (
                    run_id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    screen_id TEXT,
                    requested_at TEXT NOT NULL,
                    as_of TEXT,
                    summary_json TEXT,
                    artifact_relpath TEXT
                );
                """
            )
            event_columns = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(alert_events)").fetchall()
            }
            if "dedupe_key" not in event_columns:
                conn.execute("ALTER TABLE alert_events ADD COLUMN dedupe_key TEXT")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_alert_events_dedupe "
                "ON alert_events(alert_id, dedupe_key, occurred_at)"
            )
            note_columns = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(research_notes)").fetchall()
            }
            if "version" not in note_columns:
                conn.execute("ALTER TABLE research_notes ADD COLUMN version INTEGER NOT NULL DEFAULT 1")
            legacy_lists = conn.execute(
                "SELECT watchlist_id, user_id, name, created_at FROM watchlists"
            ).fetchall()
            for watchlist in legacy_lists:
                tag = str(watchlist["name"]).strip()
                if not tag:
                    continue
                conn.execute(
                    "INSERT OR IGNORE INTO tag_definitions(user_id, tag, created_at) VALUES (?, ?, ?)",
                    (watchlist["user_id"], tag, watchlist["created_at"]),
                )
                items = conn.execute(
                    "SELECT edinet_code, added_at FROM watchlist_items WHERE watchlist_id = ?",
                    (watchlist["watchlist_id"],),
                ).fetchall()
                for item in items:
                    conn.execute(
                        "INSERT OR IGNORE INTO company_tags(user_id, edinet_code, tag, created_at) VALUES (?, ?, ?, ?)",
                        (watchlist["user_id"], item["edinet_code"], tag, item["added_at"]),
                    )
            # Older versions could write company_tags without creating a
            # matching tag definition. Promote those memberships so the tag
            # list and member endpoints agree after an application restart.
            conn.execute(
                """
                INSERT OR IGNORE INTO tag_definitions(user_id, tag, created_at)
                SELECT user_id, tag, MIN(created_at)
                FROM company_tags
                WHERE TRIM(tag) <> ''
                GROUP BY user_id, tag
                """
            )
            conn.commit()
        finally:
            conn.close()

    def create_watchlist(self, user_id: str, name: str) -> dict[str, Any]:
        watchlist_id = str(uuid.uuid4())
        now = _timestamp()
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            conn.execute(
                "INSERT INTO watchlists(watchlist_id, user_id, name, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
                (watchlist_id, user_id, name.strip(), now, now),
            )
        return {"watchlist_id": watchlist_id, "user_id": user_id, "name": name.strip(), "created_at": now, "updated_at": now}

    def list_watchlists(self, user_id: str) -> list[dict[str, Any]]:
        conn = self._connection()
        try:
            rows = conn.execute(
                """SELECT w.*, COUNT(i.edinet_code) AS item_count
                   FROM watchlists w LEFT JOIN watchlist_items i ON i.watchlist_id = w.watchlist_id
                   WHERE w.user_id = ? GROUP BY w.watchlist_id ORDER BY w.name""",
                (user_id,),
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def get_watchlist(self, user_id: str, watchlist_id: str) -> dict[str, Any] | None:
        conn = self._connection()
        try:
            row = conn.execute(
                "SELECT * FROM watchlists WHERE user_id = ? AND watchlist_id = ?",
                (user_id, watchlist_id),
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def list_watchlist_items(self, user_id: str, watchlist_id: str) -> list[dict[str, Any]]:
        conn = self._connection()
        try:
            rows = conn.execute(
                """SELECT i.* FROM watchlist_items i JOIN watchlists w
                   ON w.watchlist_id = i.watchlist_id
                   WHERE w.user_id = ? AND w.watchlist_id = ?
                   ORDER BY i.added_at""",
                (user_id, watchlist_id),
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def rename_watchlist(self, user_id: str, watchlist_id: str, name: str) -> bool:
        now = _timestamp()
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            result = conn.execute(
                "UPDATE watchlists SET name = ?, updated_at = ? "
                "WHERE user_id = ? AND watchlist_id = ?",
                (name.strip(), now, user_id, watchlist_id),
            )
            return result.rowcount == 1

    def delete_watchlist(self, user_id: str, watchlist_id: str) -> bool:
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            result = conn.execute(
                "DELETE FROM watchlists WHERE user_id = ? AND watchlist_id = ?",
                (user_id, watchlist_id),
            )
            return result.rowcount == 1

    def add_watchlist_item(self, user_id: str, watchlist_id: str, edinet_code: str, company_name: str | None) -> None:
        now = _timestamp()
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            owner = conn.execute("SELECT 1 FROM watchlists WHERE watchlist_id = ? AND user_id = ?", (watchlist_id, user_id)).fetchone()
            if owner is None:
                raise KeyError("Watchlist not found")
            conn.execute(
                "INSERT OR REPLACE INTO watchlist_items(watchlist_id, edinet_code, company_name, added_at) VALUES (?, ?, ?, ?)",
                (watchlist_id, edinet_code.strip(), company_name, now),
            )
            conn.execute("UPDATE watchlists SET updated_at = ? WHERE watchlist_id = ?", (now, watchlist_id))

    def remove_watchlist_item(self, user_id: str, watchlist_id: str, edinet_code: str) -> bool:
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            owner = conn.execute("SELECT 1 FROM watchlists WHERE watchlist_id = ? AND user_id = ?", (watchlist_id, user_id)).fetchone()
            if owner is None:
                raise KeyError("Watchlist not found")
            result = conn.execute("DELETE FROM watchlist_items WHERE watchlist_id = ? AND edinet_code = ?", (watchlist_id, edinet_code.strip()))
            return result.rowcount == 1

    def create_note(self, user_id: str, title: str, body: str, edinet_code: str | None = None) -> dict[str, Any]:
        note_id = str(uuid.uuid4())
        now = _timestamp()
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            conn.execute(
                "INSERT INTO research_notes(note_id, user_id, edinet_code, title, body, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (note_id, user_id, edinet_code, title.strip(), body, now, now),
            )
        return {"note_id": note_id, "user_id": user_id, "edinet_code": edinet_code, "title": title.strip(), "body": body, "created_at": now, "updated_at": now}

    def list_notes(self, user_id: str, edinet_code: str | None = None) -> list[dict[str, Any]]:
        conn = self._connection()
        try:
            if edinet_code:
                rows = conn.execute("SELECT * FROM research_notes WHERE user_id = ? AND edinet_code = ? ORDER BY updated_at DESC", (user_id, edinet_code)).fetchall()
            else:
                rows = conn.execute("SELECT * FROM research_notes WHERE user_id = ? ORDER BY updated_at DESC", (user_id,)).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def delete_note(self, user_id: str, note_id: str) -> bool:
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            result = conn.execute("DELETE FROM research_notes WHERE note_id = ? AND user_id = ?", (note_id, user_id))
            return result.rowcount == 1

    def update_note(
        self,
        user_id: str,
        note_id: str,
        title: str,
        body: str,
        edinet_code: str | None,
        expected_version: int | None = None,
    ) -> tuple[bool, int | None]:
        now = _timestamp()
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            row = conn.execute(
                "SELECT version, title, body FROM research_notes WHERE user_id = ? AND note_id = ?",
                (user_id, note_id),
            ).fetchone()
            if row is None:
                return False, None
            current_version = int(row["version"])
            if expected_version is not None and expected_version != current_version:
                return False, current_version
            new_version = current_version + 1
            conn.execute(
                """UPDATE research_notes SET title = ?, body = ?, edinet_code = ?,
                   updated_at = ?, version = ? WHERE user_id = ? AND note_id = ?""",
                (title.strip(), body, edinet_code, now, new_version, user_id, note_id),
            )
            conn.execute(
                "INSERT INTO research_note_revisions(revision_id, note_id, version, title, body, saved_at) VALUES (?, ?, ?, ?, ?, ?)",
                (str(uuid.uuid4()), note_id, current_version, row["title"], row["body"], now),
            )
            return True, new_version

    def create_alert(
        self,
        user_id: str,
        name: str,
        edinet_code: str | None,
        expression_json: str,
    ) -> dict[str, Any]:
        alert_id = str(uuid.uuid4())
        now = _timestamp()
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            conn.execute(
                """INSERT INTO alert_rules
                (alert_id, user_id, name, edinet_code, expression_json, enabled, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, 1, ?, ?)""",
                (alert_id, user_id, name.strip(), edinet_code, expression_json, now, now),
            )
        return {
            "alert_id": alert_id,
            "user_id": user_id,
            "name": name.strip(),
            "edinet_code": edinet_code,
            "expression_json": expression_json,
            "enabled": True,
            "created_at": now,
            "updated_at": now,
        }

    def list_alerts(self, user_id: str) -> list[dict[str, Any]]:
        conn = self._connection()
        try:
            return [dict(row) for row in conn.execute("SELECT * FROM alert_rules WHERE user_id = ? ORDER BY updated_at DESC", (user_id,)).fetchall()]
        finally:
            conn.close()

    def alert_for_user(self, user_id: str, alert_id: str) -> dict[str, Any] | None:
        conn = self._connection()
        try:
            row = conn.execute("SELECT * FROM alert_rules WHERE user_id = ? AND alert_id = ?", (user_id, alert_id)).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def record_alert_event(self, alert_id: str, payload_json: str, dedupe_key: str | None = None) -> dict[str, Any] | None:
        event_id = str(uuid.uuid4())
        occurred = _timestamp()
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            if dedupe_key:
                existing = conn.execute(
                    "SELECT event_id FROM alert_events WHERE alert_id = ? AND dedupe_key = ? "
                    "ORDER BY occurred_at DESC LIMIT 1",
                    (alert_id, dedupe_key),
                ).fetchone()
                if existing:
                    return None
            conn.execute(
                "INSERT INTO alert_events(event_id, alert_id, occurred_at, payload_json, acknowledged_at, dedupe_key) VALUES (?, ?, ?, ?, NULL, ?)",
                (event_id, alert_id, occurred, payload_json, dedupe_key),
            )
        return {"event_id": event_id, "alert_id": alert_id, "occurred_at": occurred, "payload_json": payload_json, "dedupe_key": dedupe_key}

    def list_alert_events(self, user_id: str, alert_id: str) -> list[dict[str, Any]]:
        conn = self._connection()
        try:
            rows = conn.execute(
                """SELECT e.* FROM alert_events e JOIN alert_rules a ON a.alert_id = e.alert_id
                   WHERE a.user_id = ? AND a.alert_id = ? ORDER BY e.occurred_at DESC LIMIT 100""",
                (user_id, alert_id),
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def acknowledge_alert_event(self, user_id: str, event_id: str) -> bool:
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            result = conn.execute(
                """UPDATE alert_events SET acknowledged_at = ?
                   WHERE event_id = ? AND EXISTS (
                     SELECT 1 FROM alert_rules a
                     WHERE a.alert_id = alert_events.alert_id AND a.user_id = ?
                   )""",
                (_timestamp(), event_id, user_id),
            )
            return result.rowcount == 1

    def create_comparison_template(self, user_id: str, name: str, companies_json: str, metrics_json: str) -> dict[str, Any]:
        template_id = str(uuid.uuid4())
        now = _timestamp()
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            conn.execute(
                """INSERT INTO comparison_templates
                   (template_id, user_id, name, companies_json, metrics_json, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (template_id, user_id, name.strip(), companies_json, metrics_json, now, now),
            )
        return {"template_id": template_id, "user_id": user_id, "name": name.strip(), "companies_json": companies_json, "metrics_json": metrics_json, "created_at": now, "updated_at": now}

    def list_comparison_templates(self, user_id: str) -> list[dict[str, Any]]:
        conn = self._connection()
        try:
            return [dict(row) for row in conn.execute("SELECT * FROM comparison_templates WHERE user_id = ? ORDER BY name", (user_id,)).fetchall()]
        finally:
            conn.close()

    def delete_comparison_template(self, user_id: str, template_id: str) -> bool:
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            result = conn.execute("DELETE FROM comparison_templates WHERE user_id = ? AND template_id = ?", (user_id, template_id))
            return result.rowcount == 1

    def create_report_recipe(self, user_id: str, name: str, definition_json: str) -> dict[str, Any]:
        recipe_id = str(uuid.uuid4())
        now = _timestamp()
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            conn.execute("INSERT INTO report_recipes(recipe_id, user_id, name, definition_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)", (recipe_id, user_id, name.strip(), definition_json, now, now))
        return {"recipe_id": recipe_id, "user_id": user_id, "name": name.strip(), "definition_json": definition_json, "created_at": now, "updated_at": now}

    def list_report_recipes(self, user_id: str) -> list[dict[str, Any]]:
        conn = self._connection()
        try:
            return [dict(row) for row in conn.execute("SELECT * FROM report_recipes WHERE user_id = ? ORDER BY name", (user_id,)).fetchall()]
        finally:
            conn.close()

    def delete_report_recipe(self, user_id: str, recipe_id: str) -> bool:
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            result = conn.execute("DELETE FROM report_recipes WHERE user_id = ? AND recipe_id = ?", (user_id, recipe_id))
            return result.rowcount == 1

    def create_report_run(self, user_id: str, recipe_id: str | None, as_of: str, manifest_json: str) -> str:
        run_id = str(uuid.uuid4())
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            conn.execute(
                """INSERT INTO report_runs
                   (run_id, user_id, recipe_id, status, as_of, manifest_json,
                    artifact_path, size_bytes, sha256, created_at, completed_at, error_message)
                   VALUES (?, ?, ?, 'running', ?, ?, NULL, NULL, NULL, ?, NULL, NULL)""",
                (run_id, user_id, recipe_id, as_of, manifest_json, _timestamp()),
            )
        return run_id

    def finish_report_run(self, user_id: str, run_id: str, status: str, artifact_path: str | None, size_bytes: int | None, sha256: str | None, error_message: str | None = None, manifest_json: str | None = None) -> bool:
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            result = conn.execute(
                """UPDATE report_runs SET status = ?, artifact_path = ?, size_bytes = ?,
                   sha256 = ?, completed_at = ?, error_message = ?,
                   manifest_json = COALESCE(?, manifest_json)
                   WHERE user_id = ? AND run_id = ?""",
                (status, artifact_path, size_bytes, sha256, _timestamp(), error_message, manifest_json, user_id, run_id),
            )
            return result.rowcount == 1

    def list_report_runs(self, user_id: str) -> list[dict[str, Any]]:
        conn = self._connection()
        try:
            return [dict(row) for row in conn.execute("SELECT * FROM report_runs WHERE user_id = ? ORDER BY created_at DESC LIMIT 100", (user_id,)).fetchall()]
        finally:
            conn.close()

    def get_report_run(self, user_id: str, run_id: str) -> dict[str, Any] | None:
        conn = self._connection()
        try:
            row = conn.execute("SELECT * FROM report_runs WHERE user_id = ? AND run_id = ?", (user_id, run_id)).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def delete_report_run(self, user_id: str, run_id: str) -> bool:
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            result = conn.execute("DELETE FROM report_runs WHERE user_id = ? AND run_id = ?", (user_id, run_id))
            return result.rowcount == 1

    # -- company research --

    def get_company_research(self, user_id: str, edinet_code: str) -> dict[str, Any] | None:
        conn = self._connection()
        try:
            row = conn.execute(
                "SELECT * FROM company_research WHERE user_id = ? AND edinet_code = ?",
                (user_id, edinet_code),
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def upsert_company_research(
        self,
        user_id: str,
        edinet_code: str,
        *,
        thesis_status: str | None = None,
        target_value: float | None = None,
        target_currency: str | None = None,
        review_on: str | None = None,
    ) -> dict[str, Any]:
        now = _timestamp()
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            existing = conn.execute(
                "SELECT version FROM company_research WHERE user_id = ? AND edinet_code = ?",
                (user_id, edinet_code),
            ).fetchone()
            version = (int(existing["version"]) + 1) if existing else 1
            conn.execute(
                """INSERT INTO company_research
                   (user_id, edinet_code, thesis_status, target_value, target_currency, review_on, version, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(user_id, edinet_code) DO UPDATE SET
                   thesis_status=excluded.thesis_status, target_value=excluded.target_value,
                   target_currency=excluded.target_currency, review_on=excluded.review_on,
                   version=excluded.version, updated_at=excluded.updated_at""",
                (user_id, edinet_code, thesis_status, target_value, target_currency, review_on, version, now, now),
            )
        return self.get_company_research(user_id, edinet_code) or {}

    # -- company tags --

    def create_tag(self, user_id: str, tag: str) -> dict[str, Any]:
        cleaned = tag.strip()
        now = _timestamp()
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            conn.execute(
                "INSERT INTO tag_definitions(user_id, tag, created_at) VALUES (?, ?, ?)",
                (user_id, cleaned, now),
            )
        return {"user_id": user_id, "tag": cleaned, "created_at": now}

    def rename_tag(self, user_id: str, old_tag: str, new_tag: str) -> bool:
        old = old_tag.strip()
        new = new_tag.strip()
        if old == new:
            return bool(self.get_tag(user_id, old))
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            existing = conn.execute(
                "SELECT 1 FROM tag_definitions WHERE user_id = ? AND tag = ?",
                (user_id, old),
            ).fetchone()
            if existing is None:
                return False
            conflict = conn.execute(
                "SELECT 1 FROM tag_definitions WHERE user_id = ? AND tag = ?",
                (user_id, new),
            ).fetchone()
            if conflict is not None:
                raise ValueError("A tag with that name already exists")
            conn.execute(
                "UPDATE tag_definitions SET tag = ? WHERE user_id = ? AND tag = ?",
                (new, user_id, old),
            )
            conn.execute(
                "UPDATE company_tags SET tag = ? WHERE user_id = ? AND tag = ?",
                (new, user_id, old),
            )
            return True

    def get_tag(self, user_id: str, tag: str) -> dict[str, Any] | None:
        conn = self._connection()
        try:
            row = conn.execute(
                "SELECT * FROM tag_definitions WHERE user_id = ? AND tag = ?",
                (user_id, tag.strip()),
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def delete_tag(self, user_id: str, tag: str) -> bool:
        cleaned = tag.strip()
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            definition = conn.execute(
                "SELECT 1 FROM tag_definitions WHERE user_id = ? AND tag = ?",
                (user_id, cleaned),
            ).fetchone()
            memberships = conn.execute(
                "SELECT 1 FROM company_tags WHERE user_id = ? AND tag = ? LIMIT 1",
                (user_id, cleaned),
            ).fetchone()
            conn.execute("DELETE FROM company_tags WHERE user_id = ? AND tag = ?", (user_id, cleaned))
            conn.execute("DELETE FROM tag_definitions WHERE user_id = ? AND tag = ?", (user_id, cleaned))
            return definition is not None or memberships is not None

    def set_company_tags(self, user_id: str, edinet_code: str, tags: list[str]) -> list[dict[str, Any]]:
        now = _timestamp()
        cleaned_tags = list(dict.fromkeys(tag.strip() for tag in tags if tag.strip()))
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            conn.execute(
                "DELETE FROM company_tags WHERE user_id = ? AND edinet_code = ?",
                (user_id, edinet_code),
            )
            for tag in cleaned_tags:
                conn.execute(
                    "INSERT OR IGNORE INTO tag_definitions(user_id, tag, created_at) VALUES (?, ?, ?)",
                    (user_id, tag, now),
                )
                conn.execute(
                    "INSERT INTO company_tags(user_id, edinet_code, tag, created_at) VALUES (?, ?, ?, ?)",
                    (user_id, edinet_code, tag.strip(), now),
                )
        return self.list_company_tags(user_id, edinet_code)

    def list_company_tags(self, user_id: str, edinet_code: str) -> list[dict[str, Any]]:
        conn = self._connection()
        try:
            return [
                dict(row)
                for row in conn.execute(
                    "SELECT * FROM company_tags WHERE user_id = ? AND edinet_code = ? ORDER BY tag",
                    (user_id, edinet_code),
                ).fetchall()
            ]
        finally:
            conn.close()

    def list_tag_companies(self, user_id: str, tag: str) -> list[dict[str, Any]]:
        conn = self._connection()
        try:
            rows = conn.execute(
                "SELECT edinet_code, tag, created_at FROM company_tags "
                "WHERE user_id = ? AND tag = ? ORDER BY created_at, edinet_code",
                (user_id, tag.strip()),
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def list_all_tags(self, user_id: str) -> list[dict[str, Any]]:
        conn = self._connection()
        try:
            return [
                dict(row)
                for row in conn.execute(
                    """SELECT user_id, tag, created_at FROM tag_definitions WHERE user_id = ?
                       UNION ALL
                       SELECT c.user_id, c.tag, c.created_at FROM company_tags c
                       WHERE c.user_id = ? AND NOT EXISTS (
                         SELECT 1 FROM tag_definitions d
                         WHERE d.user_id = c.user_id AND d.tag = c.tag
                       )
                       ORDER BY tag, created_at""",
                    (user_id, user_id),
                ).fetchall()
            ]
        finally:
            conn.close()

    # -- watchlist member reorder --

    def reorder_watchlist_items(self, user_id: str, watchlist_id: str, ordered_codes: list[str]) -> bool:
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            owner = conn.execute(
                "SELECT 1 FROM watchlists WHERE watchlist_id = ? AND user_id = ?",
                (watchlist_id, user_id),
            ).fetchone()
            if owner is None:
                raise KeyError("Watchlist not found")
            existing = {
                row["edinet_code"]
                for row in conn.execute(
                    "SELECT edinet_code FROM watchlist_items WHERE watchlist_id = ?",
                    (watchlist_id,),
                ).fetchall()
            }
            if set(ordered_codes) != existing:
                raise ValueError("Ordered codes must match existing members exactly")
            now = _timestamp()
            for idx, code in enumerate(ordered_codes):
                conn.execute(
                    "UPDATE watchlist_items SET added_at = ? WHERE watchlist_id = ? AND edinet_code = ?",
                    (f"{now}#pos={idx:04d}", watchlist_id, code),
                )
            conn.execute("UPDATE watchlists SET updated_at = ? WHERE watchlist_id = ?", (now, watchlist_id))
            return True

    # -- saved screens --

    def create_saved_screen(self, user_id: str, name: str, definition_json: str) -> dict[str, Any]:
        screen_id = str(uuid.uuid4())
        now = _timestamp()
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            conn.execute(
                """INSERT INTO saved_screens
                   (screen_id, user_id, name, definition_json, created_at, updated_at, version)
                   VALUES (?, ?, ?, ?, ?, ?, 1)""",
                (screen_id, user_id, name.strip(), definition_json, now, now),
            )
        return {"screen_id": screen_id, "user_id": user_id, "name": name.strip(), "definition_json": definition_json, "created_at": now, "updated_at": now, "version": 1}

    def list_saved_screens(self, user_id: str) -> list[dict[str, Any]]:
        conn = self._connection()
        try:
            return [
                dict(row)
                for row in conn.execute(
                    "SELECT * FROM saved_screens WHERE user_id = ? ORDER BY updated_at DESC",
                    (user_id,),
                ).fetchall()
            ]
        finally:
            conn.close()

    def delete_saved_screen(self, user_id: str, screen_id: str) -> bool:
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            result = conn.execute(
                "DELETE FROM saved_screens WHERE user_id = ? AND screen_id = ?",
                (user_id, screen_id),
            )
            return result.rowcount == 1

    def _connection(self) -> sqlite3.Connection:
        return connect_write(self.path, busy_timeout_ms=self.busy_timeout_ms)
