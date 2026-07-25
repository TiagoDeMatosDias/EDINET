"""SQLite persistence for non-rebuildable account and authentication state."""

from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from src.orchestrator.common.sqlite import (
    connect_write,
    initialize_managed_database,
    transaction,
)


def utc_now() -> datetime:
    """Return a timezone-aware UTC timestamp."""
    return datetime.now(timezone.utc)


def timestamp(value: datetime) -> str:
    """Serialize an instant in a sortable ISO-8601 form."""
    return value.astimezone(timezone.utc).isoformat(timespec="seconds")


def parse_timestamp(value: str) -> datetime:
    """Parse a stored UTC timestamp."""
    return datetime.fromisoformat(value).astimezone(timezone.utc)


class AuthStore:
    """Dedicated account database with explicit schema initialization."""

    def __init__(self, path: str | Path, *, busy_timeout_ms: int = 30_000) -> None:
        self.path = Path(path).expanduser()
        self.busy_timeout_ms = busy_timeout_ms
        self.initialize()

    def initialize(self) -> None:
        """Create the auth database schema without touching any data database."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = connect_write(self.path, busy_timeout_ms=self.busy_timeout_ms)
        try:
            initialize_managed_database(conn)
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version INTEGER PRIMARY KEY,
                    applied_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS users (
                    user_id TEXT PRIMARY KEY,
                    username TEXT NOT NULL UNIQUE,
                    email TEXT UNIQUE,
                    password_hash TEXT NOT NULL,
                    role TEXT NOT NULL CHECK (role IN ('admin', 'operator', 'member')),
                    status TEXT NOT NULL CHECK (status IN ('active', 'disabled')),
                    token_version INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    last_login_at TEXT
                );
                CREATE TABLE IF NOT EXISTS sessions (
                    session_id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
                    family_id TEXT,
                    token_hash TEXT NOT NULL UNIQUE,
                    token_type TEXT NOT NULL CHECK (token_type IN ('access', 'refresh')),
                    created_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    revoked_at TEXT,
                    replaced_by TEXT,
                    user_agent TEXT,
                    remote_addr TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_sessions_hash ON sessions(token_hash);
                CREATE TABLE IF NOT EXISTS api_tokens (
                    token_id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
                    name TEXT NOT NULL,
                    token_hash TEXT NOT NULL UNIQUE,
                    token_prefix TEXT NOT NULL,
                    scopes_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    expires_at TEXT,
                    last_used_at TEXT,
                    revoked_at TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_api_tokens_hash ON api_tokens(token_hash);
                CREATE TABLE IF NOT EXISTS auth_audit (
                    event_id TEXT PRIMARY KEY,
                    user_id TEXT,
                    event_type TEXT NOT NULL,
                    occurred_at TEXT NOT NULL,
                    remote_addr TEXT,
                    detail TEXT
                );
                CREATE TABLE IF NOT EXISTS login_throttle (
                    login_key TEXT PRIMARY KEY,
                    failures INTEGER NOT NULL DEFAULT 0,
                    first_failure_at TEXT,
                    locked_until TEXT
                );
                CREATE TABLE IF NOT EXISTS invitations (
                    invitation_id TEXT PRIMARY KEY,
                    token_hash TEXT NOT NULL UNIQUE,
                    email_normalized TEXT,
                    role TEXT NOT NULL DEFAULT 'member',
                    created_by TEXT,
                    created_at TEXT NOT NULL,
                    expires_at TEXT,
                    accepted_by TEXT,
                    accepted_at TEXT,
                    revoked_at TEXT
                );
                CREATE TABLE IF NOT EXISTS credential_resets (
                    reset_id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
                    token_hash TEXT NOT NULL UNIQUE,
                    created_by TEXT,
                    created_at TEXT NOT NULL,
                    expires_at TEXT,
                    consumed_at TEXT,
                    revoked_at TEXT
                );
                CREATE TABLE IF NOT EXISTS auth_settings (
                    singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
                    registration_mode TEXT NOT NULL DEFAULT 'closed',
                    default_role TEXT NOT NULL DEFAULT 'member',
                    access_token_seconds INTEGER NOT NULL DEFAULT 900,
                    refresh_idle_seconds INTEGER,
                    refresh_absolute_seconds INTEGER,
                    updated_at TEXT NOT NULL,
                    updated_by TEXT
                );
                """
            )
            columns = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(sessions)").fetchall()
            }
            if "family_id" not in columns:
                conn.execute("ALTER TABLE sessions ADD COLUMN family_id TEXT")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_sessions_family ON sessions(family_id)"
            )
            user_columns = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(users)").fetchall()
            }
            if "token_version" not in user_columns:
                conn.execute("ALTER TABLE users ADD COLUMN token_version INTEGER NOT NULL DEFAULT 1")
            conn.commit()
        finally:
            conn.close()

    def connection(self) -> sqlite3.Connection:
        """Open a configured writable connection for short read operations."""
        return connect_write(self.path, busy_timeout_ms=self.busy_timeout_ms)

    def count_users(self) -> int:
        conn = self.connection()
        try:
            return int(conn.execute("SELECT COUNT(*) FROM users").fetchone()[0])
        finally:
            conn.close()

    def get_user_by_login(self, login: str) -> sqlite3.Row | None:
        normalized = login.strip().casefold()
        conn = self.connection()
        try:
            return conn.execute(
                "SELECT * FROM users WHERE username = ? OR email = ? LIMIT 1",
                (normalized, normalized),
            ).fetchone()
        finally:
            conn.close()

    def get_user(self, user_id: str) -> sqlite3.Row | None:
        conn = self.connection()
        try:
            return conn.execute(
                "SELECT * FROM users WHERE user_id = ?",
                (user_id,),
            ).fetchone()
        finally:
            conn.close()

    def create_user(self, values: dict[str, Any]) -> bool:
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            first_user = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0] == 0
            role = "admin" if first_user else values["role"]
            conn.execute(
                """INSERT INTO users
                (user_id, username, email, password_hash, role, status,
                 created_at, updated_at, last_login_at)
                VALUES (:user_id, :username, :email, :password_hash, :role,
                        :status, :created_at, :updated_at, NULL)""",
                {**values, "role": role},
            )
            return first_user

    def update_last_login(
        self,
        user_id: str,
        when: datetime,
        password_hash: str | None = None,
    ) -> None:
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            if password_hash is None:
                conn.execute(
                    "UPDATE users SET last_login_at = ?, updated_at = ? WHERE user_id = ?",
                    (timestamp(when), timestamp(when), user_id),
                )
            else:
                conn.execute(
                    """UPDATE users SET password_hash = ?, last_login_at = ?,
                       updated_at = ? WHERE user_id = ?""",
                    (password_hash, timestamp(when), timestamp(when), user_id),
                )

    @staticmethod
    def _insert_session(conn: sqlite3.Connection, values: dict[str, Any]) -> None:
        conn.execute(
            """INSERT INTO sessions
            (session_id, user_id, family_id, token_hash, token_type, created_at, expires_at,
             revoked_at, replaced_by, user_agent, remote_addr)
            VALUES (:session_id, :user_id, :family_id, :token_hash, :token_type, :created_at,
                    :expires_at, NULL, NULL, :user_agent, :remote_addr)""",
            values,
        )

    def create_session(self, values: dict[str, Any]) -> None:
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            self._insert_session(conn, values)

    def find_active_session(
        self,
        token_hash: str,
        token_type: str,
        now: datetime,
    ) -> sqlite3.Row | None:
        conn = self.connection()
        try:
            return conn.execute(
                """SELECT s.*, u.username, u.email, u.role, u.status, u.token_version
                   FROM sessions s JOIN users u ON u.user_id = s.user_id
                   WHERE s.token_hash = ? AND s.token_type = ? AND s.revoked_at IS NULL
                     AND s.expires_at > ? AND u.status = 'active'""",
                (token_hash, token_type, timestamp(now)),
            ).fetchone()
        finally:
            conn.close()

    def rotate_refresh(
        self,
        old_session_id: str,
        access_values: dict[str, Any],
        refresh_values: dict[str, Any],
        when: datetime,
    ) -> None:
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            updated = conn.execute(
                """UPDATE sessions SET revoked_at = ?, replaced_by = ?
                   WHERE session_id = ? AND revoked_at IS NULL""",
                (timestamp(when), refresh_values["session_id"], old_session_id),
            )
            if updated.rowcount != 1:
                raise ValueError("Refresh token has already been rotated")
            self._insert_session(conn, access_values)
            self._insert_session(conn, refresh_values)

    def find_session(self, token_hash: str, token_type: str) -> sqlite3.Row | None:
        conn = self.connection()
        try:
            return conn.execute(
                "SELECT * FROM sessions WHERE token_hash = ? AND token_type = ?",
                (token_hash, token_type),
            ).fetchone()
        finally:
            conn.close()

    def revoke_family(self, family_id: str | None, when: datetime) -> int:
        if not family_id:
            return 0
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            result = conn.execute(
                "UPDATE sessions SET revoked_at = ? "
                "WHERE family_id = ? AND revoked_at IS NULL",
                (timestamp(when), family_id),
            )
            return result.rowcount

    def revoke_session(self, token_hash: str, when: datetime) -> bool:
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            result = conn.execute(
                "UPDATE sessions SET revoked_at = ? WHERE token_hash = ? AND revoked_at IS NULL",
                (timestamp(when), token_hash),
            )
            return result.rowcount == 1

    def throttle_row(self, login_key: str) -> sqlite3.Row | None:
        conn = self.connection()
        try:
            return conn.execute(
                "SELECT * FROM login_throttle WHERE login_key = ?",
                (login_key,),
            ).fetchone()
        finally:
            conn.close()

    def record_login_failure(self, login_key: str, now: datetime, *, lock_after: int = 5, lock_seconds: int = 300) -> None:
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            row = conn.execute("SELECT failures, first_failure_at FROM login_throttle WHERE login_key = ?", (login_key,)).fetchone()
            failures = int(row["failures"]) + 1 if row else 1
            first = row["first_failure_at"] if row else timestamp(now)
            locked = timestamp(now + timedelta(seconds=lock_seconds)) if failures >= lock_after else None
            conn.execute(
                "INSERT INTO login_throttle(login_key, failures, first_failure_at, locked_until) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(login_key) DO UPDATE SET failures=excluded.failures, first_failure_at=excluded.first_failure_at, locked_until=excluded.locked_until",
                (login_key, failures, first, locked),
            )

    def clear_login_throttle(self, login_key: str) -> None:
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            conn.execute("DELETE FROM login_throttle WHERE login_key = ?", (login_key,))

    def add_api_token(self, values: dict[str, Any]) -> None:
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            conn.execute(
                """INSERT INTO api_tokens
                (token_id, user_id, name, token_hash, token_prefix, scopes_json,
                 created_at, expires_at, last_used_at, revoked_at)
                VALUES (:token_id, :user_id, :name, :token_hash, :token_prefix,
                        :scopes_json, :created_at, :expires_at, NULL, NULL)""",
                values,
            )

    def find_active_api_token(
        self,
        token_hash: str,
        now: datetime,
    ) -> sqlite3.Row | None:
        conn = self.connection()
        try:
            return conn.execute(
                """SELECT t.*, u.username, u.email, u.role, u.status
                   FROM api_tokens t JOIN users u ON u.user_id = t.user_id
                   WHERE t.token_hash = ? AND t.revoked_at IS NULL
                     AND (t.expires_at IS NULL OR t.expires_at > ?) AND u.status = 'active'""",
                (token_hash, timestamp(now)),
            ).fetchone()
        finally:
            conn.close()

    def touch_api_token(self, token_id: str, when: datetime) -> None:
        with transaction(
            self.path,
            busy_timeout_ms=self.busy_timeout_ms,
            immediate=False,
        ) as conn:
            conn.execute(
                "UPDATE api_tokens SET last_used_at = ? WHERE token_id = ?",
                (timestamp(when), token_id),
            )

    def list_api_tokens(self, user_id: str) -> list[sqlite3.Row]:
        conn = self.connection()
        try:
            return list(
                conn.execute(
                    """SELECT token_id, name, token_prefix, scopes_json, created_at,
                       expires_at, last_used_at, revoked_at FROM api_tokens
                       WHERE user_id = ? ORDER BY created_at DESC""",
                    (user_id,),
                ).fetchall()
            )
        finally:
            conn.close()

    def revoke_api_token(self, user_id: str, token_id: str, when: datetime) -> bool:
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            result = conn.execute(
                """UPDATE api_tokens SET revoked_at = ?
                   WHERE user_id = ? AND token_id = ? AND revoked_at IS NULL""",
                (timestamp(when), user_id, token_id),
            )
            return result.rowcount == 1

    def change_user_password(self, user_id: str, password_hash: str, when: datetime) -> bool:
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            result = conn.execute(
                "UPDATE users SET password_hash = ?, token_version = token_version + 1, updated_at = ? "
                "WHERE user_id = ? AND status = 'active'",
                (password_hash, timestamp(when), user_id),
            )
            if result.rowcount == 1:
                conn.execute(
                    "UPDATE sessions SET revoked_at = ? WHERE user_id = ? AND revoked_at IS NULL",
                    (timestamp(when), user_id),
                )
            return result.rowcount == 1

    def update_user_profile(
        self,
        user_id: str,
        username: str | None = None,
        email: str | None = None,
        when: datetime | None = None,
    ) -> bool:
        when = when or utc_now()
        set_clauses = ["updated_at = ?"]
        params: list[Any] = [timestamp(when)]
        if username is not None:
            set_clauses.append("username = ?")
            params.append(username)
        if email is not None:
            set_clauses.append("email = ?")
            params.append(email)
        params.append(user_id)
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            result = conn.execute(
                f"UPDATE users SET {', '.join(set_clauses)} WHERE user_id = ?",
                params,
            )
            return result.rowcount == 1

    def disable_user(self, user_id: str, when: datetime) -> bool:
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            # Prevent disabling the last active admin
            active_admin_count = conn.execute(
                "SELECT COUNT(*) FROM users WHERE role = 'admin' AND status = 'active'"
            ).fetchone()[0]
            current = conn.execute(
                "SELECT role, status FROM users WHERE user_id = ?", (user_id,)
            ).fetchone()
            if current is None:
                return False
            if current["role"] == "admin" and current["status"] == "active" and active_admin_count <= 1:
                raise ValueError("Cannot disable the last active administrator")
            result = conn.execute(
                "UPDATE users SET status = 'disabled', token_version = token_version + 1, updated_at = ? "
                "WHERE user_id = ?",
                (timestamp(when), user_id),
            )
            return result.rowcount == 1

    def list_users(self) -> list[sqlite3.Row]:
        conn = self.connection()
        try:
            return list(
                conn.execute(
                    "SELECT user_id, username, email, role, status, token_version, "
                    "created_at, updated_at, last_login_at FROM users ORDER BY username"
                ).fetchall()
            )
        finally:
            conn.close()

    def update_user_role(self, user_id: str, role: str, when: datetime) -> bool:
        if role not in {"admin", "operator", "member"}:
            raise ValueError("Invalid role")
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            current = conn.execute(
                "SELECT role, status FROM users WHERE user_id = ?", (user_id,)
            ).fetchone()
            if current is None:
                return False
            # Prevent demoting the last active admin
            if current["role"] == "admin" and current["status"] == "active" and role != "admin":
                active_admin_count = conn.execute(
                    "SELECT COUNT(*) FROM users WHERE role = 'admin' AND status = 'active'"
                ).fetchone()[0]
                if active_admin_count <= 1:
                    raise ValueError("Cannot demote the last active administrator")
            result = conn.execute(
                "UPDATE users SET role = ?, token_version = token_version + 1, updated_at = ? "
                "WHERE user_id = ?",
                (role, timestamp(when), user_id),
            )
            return result.rowcount == 1

    def list_user_sessions(self, user_id: str) -> list[sqlite3.Row]:
        conn = self.connection()
        try:
            return list(
                conn.execute(
                    "SELECT session_id, token_type, created_at, expires_at, revoked_at, "
                    "user_agent FROM sessions WHERE user_id = ? ORDER BY created_at DESC LIMIT 50",
                    (user_id,),
                ).fetchall()
            )
        finally:
            conn.close()

    def revoke_user_session(self, user_id: str, session_id: str, when: datetime) -> bool:
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            result = conn.execute(
                "UPDATE sessions SET revoked_at = ? WHERE user_id = ? AND session_id = ? AND revoked_at IS NULL",
                (timestamp(when), user_id, session_id),
            )
            return result.rowcount == 1

    def revoke_all_user_sessions(self, user_id: str, when: datetime) -> int:
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            result = conn.execute(
                "UPDATE sessions SET revoked_at = ? WHERE user_id = ? AND revoked_at IS NULL",
                (timestamp(when), user_id),
            )
            return result.rowcount

    def bump_token_version(self, user_id: str, when: datetime) -> None:
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            conn.execute(
                "UPDATE users SET token_version = token_version + 1, updated_at = ? WHERE user_id = ?",
                (timestamp(when), user_id),
            )
            conn.execute(
                "UPDATE sessions SET revoked_at = ? WHERE user_id = ? AND revoked_at IS NULL",
                (timestamp(when), user_id),
            )

    def list_audit_events(self, limit: int = 100) -> list[sqlite3.Row]:
        conn = self.connection()
        try:
            return list(
                conn.execute(
                    "SELECT * FROM auth_audit ORDER BY occurred_at DESC LIMIT ?",
                    (limit,),
                ).fetchall()
            )
        finally:
            conn.close()

    # -- invitations --

    def create_invitation(
        self,
        token_hash: str,
        role: str = "member",
        email_normalized: str | None = None,
        created_by: str | None = None,
        expires_at: datetime | None = None,
    ) -> str:
        from uuid import uuid4

        invitation_id = str(uuid4())
        now = utc_now()
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            conn.execute(
                """INSERT INTO invitations
                   (invitation_id, token_hash, email_normalized, role, created_by, created_at, expires_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (invitation_id, token_hash, email_normalized, role, created_by, timestamp(now), timestamp(expires_at) if expires_at else None),
            )
        return invitation_id

    def find_valid_invitation(self, token_hash: str, now: datetime) -> sqlite3.Row | None:
        conn = self.connection()
        try:
            return conn.execute(
                """SELECT * FROM invitations WHERE token_hash = ?
                   AND revoked_at IS NULL AND accepted_at IS NULL
                   AND (expires_at IS NULL OR expires_at > ?)""",
                (token_hash, timestamp(now)),
            ).fetchone()
        finally:
            conn.close()

    def accept_invitation(self, token_hash: str, accepted_by: str, when: datetime) -> bool:
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            result = conn.execute(
                """UPDATE invitations SET accepted_by = ?, accepted_at = ?
                   WHERE token_hash = ? AND revoked_at IS NULL AND accepted_at IS NULL
                   AND (expires_at IS NULL OR expires_at > ?)""",
                (accepted_by, timestamp(when), token_hash, timestamp(when)),
            )
            return result.rowcount == 1

    def revoke_invitation(self, invitation_id: str, when: datetime) -> bool:
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            result = conn.execute(
                "UPDATE invitations SET revoked_at = ? WHERE invitation_id = ? AND revoked_at IS NULL",
                (timestamp(when), invitation_id),
            )
            return result.rowcount == 1

    # -- credential resets --

    def create_credential_reset(
        self,
        user_id: str,
        token_hash: str,
        created_by: str | None = None,
        expires_at: datetime | None = None,
    ) -> str:
        from uuid import uuid4

        reset_id = str(uuid4())
        now = utc_now()
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            conn.execute(
                """INSERT INTO credential_resets
                   (reset_id, user_id, token_hash, created_by, created_at, expires_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (reset_id, user_id, token_hash, created_by, timestamp(now), timestamp(expires_at) if expires_at else None),
            )
        return reset_id

    def consume_credential_reset(self, token_hash: str, when: datetime) -> sqlite3.Row | None:
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            row = conn.execute(
                """SELECT * FROM credential_resets WHERE token_hash = ?
                   AND consumed_at IS NULL AND revoked_at IS NULL
                   AND (expires_at IS NULL OR expires_at > ?)""",
                (token_hash, timestamp(when)),
            ).fetchone()
            if row is None:
                return None
            conn.execute(
                "UPDATE credential_resets SET consumed_at = ? WHERE reset_id = ?",
                (timestamp(when), row["reset_id"]),
            )
            return row

    # -- auth settings --

    def get_auth_settings(self) -> dict[str, Any]:
        conn = self.connection()
        try:
            row = conn.execute(
                "SELECT registration_mode, default_role, access_token_seconds, refresh_idle_seconds, refresh_absolute_seconds, updated_at FROM auth_settings WHERE singleton_id = 1"
            ).fetchone()
            return dict(row) if row else {"registration_mode": "closed", "default_role": "member"}
        finally:
            conn.close()

    def update_auth_settings(self, **kwargs: Any) -> None:
        now = utc_now()
        set_pairs = ", ".join(f"{k} = :{k}" for k in kwargs)
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            conn.execute(
                "INSERT OR IGNORE INTO auth_settings(singleton_id, registration_mode, default_role, updated_at) VALUES (1, 'closed', 'member', ?)",
                (timestamp(now),),
            )
            conn.execute(
                f"UPDATE auth_settings SET {set_pairs}, updated_at = :updated_at WHERE singleton_id = 1",
                {**kwargs, "updated_at": timestamp(now)},
            )

    def audit(
        self,
        event_type: str,
        user_id: str | None,
        detail: str = "",
        remote_addr: str | None = None,
    ) -> None:
        with transaction(self.path, busy_timeout_ms=self.busy_timeout_ms) as conn:
            conn.execute(
                """INSERT INTO auth_audit
                   (event_id, user_id, event_type, occurred_at, remote_addr, detail)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    str(uuid.uuid4()),
                    user_id,
                    event_type,
                    timestamp(utc_now()),
                    remote_addr,
                    detail[:500],
                ),
            )
