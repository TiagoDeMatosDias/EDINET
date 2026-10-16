"""Registered workers and enrollment codes, stored in ``app.db``.

Worker tokens are random, shown once, and stored only as SHA-256 hashes.
Enrollment strings let an operator add a worker without typing credentials:
``SRW1.`` followed by base64url of ``{"u": server url, "f": certificate
fingerprint, "c": one-time code}``.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import secrets
import sqlite3
from contextlib import closing, contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator
from uuid import uuid4

from src.orchestrator.common.sqlite import (
    DEFAULT_BUSY_TIMEOUT_MS,
    connect_write,
    initialize_managed_database,
    record_schema_version,
    schema_version,
    transaction,
)

from .protocol import ENROLLMENT_PREFIX, LOCAL_WORKER_ID, WORKER_TOKEN_PREFIX

SCHEMA_COMPONENT = "workers"
ENROLLMENT_VALID_MINUTES = 30
DEFAULT_INTERACTIVE_SLOTS = 1
DEFAULT_BATCH_SLOTS = 2

_MIGRATION_1 = """
CREATE TABLE IF NOT EXISTS workers (
    worker_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('local','enrolled')),
    token_hash TEXT,
    interactive_slots INTEGER NOT NULL DEFAULT 1 CHECK (interactive_slots BETWEEN 0 AND 8),
    batch_slots INTEGER NOT NULL DEFAULT 2 CHECK (batch_slots BETWEEN 0 AND 8),
    enabled INTEGER NOT NULL DEFAULT 1,
    config_version INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    revoked_at TEXT
);
CREATE TABLE IF NOT EXISTS worker_enrollments (
    code_hash TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    created_by TEXT,
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    used_at TEXT,
    worker_id TEXT
);
"""

_UPDATABLE = ("name", "interactive_slots", "batch_slots", "enabled")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def hash_secret(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class WorkerRecord:
    worker_id: str
    name: str
    kind: str
    token_hash: str | None
    interactive_slots: int
    batch_slots: int
    enabled: bool
    config_version: int
    created_at: str
    updated_at: str
    revoked_at: str | None

    @property
    def revoked(self) -> bool:
        return self.revoked_at is not None


def _record(row: sqlite3.Row) -> WorkerRecord:
    values = dict(row)
    values["enabled"] = bool(values["enabled"])
    return WorkerRecord(**values)


def parse_enrollment(enrollment: str) -> tuple[str, str, str]:
    """Split an enrollment string into ``(server_url, fingerprint, code)``."""
    text = enrollment.strip()
    if not text.startswith(ENROLLMENT_PREFIX):
        raise ValueError("Not an enrollment string")
    body = text[len(ENROLLMENT_PREFIX):]
    try:
        data = json.loads(base64.urlsafe_b64decode(body + "=" * (-len(body) % 4)))
        return str(data["u"]), str(data["f"]), str(data["c"])
    except (ValueError, KeyError, TypeError) as exc:
        raise ValueError("Not an enrollment string") from exc


class WorkerRegistry:
    """The ``workers`` and ``worker_enrollments`` tables of ``app.db``."""

    def __init__(self, path: str | Path, *, busy_timeout_ms: int = DEFAULT_BUSY_TIMEOUT_MS):
        self.path = Path(path)
        self.busy_timeout_ms = busy_timeout_ms
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(connect_write(self.path, busy_timeout_ms=busy_timeout_ms)) as conn:
            initialize_managed_database(conn)
            if schema_version(conn, SCHEMA_COMPONENT) < 1:
                conn.executescript(_MIGRATION_1)
                record_schema_version(conn, SCHEMA_COMPONENT, 1, _now())
            conn.commit()

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = connect_write(self.path, busy_timeout_ms=self.busy_timeout_ms)
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _transaction(self):
        return transaction(self.path, busy_timeout_ms=self.busy_timeout_ms)

    @staticmethod
    def _new_token() -> str:
        return WORKER_TOKEN_PREFIX + secrets.token_urlsafe(32)

    def ensure_local_worker(self, worker_id: str = LOCAL_WORKER_ID, name: str = "Local worker") -> str:
        """Create or refresh the server-launched worker; returns a fresh token.

        The previous token stops working. Slots and the enabled flag are kept.
        """
        token = self._new_token()
        now = _now()
        with self._transaction() as conn:
            conn.execute(
                "INSERT INTO workers(worker_id, name, kind, token_hash, created_at, updated_at) "
                "VALUES (?, ?, 'local', ?, ?, ?) "
                "ON CONFLICT(worker_id) DO UPDATE SET kind = 'local', token_hash = excluded.token_hash, "
                "revoked_at = NULL, updated_at = excluded.updated_at",
                (worker_id, name, hash_secret(token), now, now),
            )
        return token

    def authenticate(self, token: str) -> WorkerRecord | None:
        """The worker owning ``token``, or ``None`` when unknown or revoked."""
        if not token or not token.startswith(WORKER_TOKEN_PREFIX):
            return None
        digest = hash_secret(token)
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM workers WHERE token_hash = ? AND revoked_at IS NULL", (digest,)
            ).fetchone()
        if row is None or not hmac.compare_digest(str(row["token_hash"]), digest):
            return None
        return _record(row)

    def get(self, worker_id: str) -> WorkerRecord:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM workers WHERE worker_id = ?", (worker_id,)).fetchone()
        if row is None:
            raise KeyError(worker_id)
        return _record(row)

    def list(self, *, include_revoked: bool = False) -> list[WorkerRecord]:
        sql = "SELECT * FROM workers"
        if not include_revoked:
            sql += " WHERE revoked_at IS NULL"
        sql += " ORDER BY kind = 'local' DESC, created_at, worker_id"
        with self._connect() as conn:
            return [_record(row) for row in conn.execute(sql).fetchall()]

    def update(self, worker_id: str, **changes: Any) -> WorkerRecord:
        """Change name, slots, or the enabled flag; bumps ``config_version``."""
        unknown = set(changes) - set(_UPDATABLE)
        if unknown:
            raise ValueError(f"Unknown worker fields: {sorted(unknown)}")
        assignments: list[str] = []
        values: list[Any] = []
        for field, value in changes.items():
            if field == "name":
                value = str(value).strip()
                if not value:
                    raise ValueError("The worker name cannot be empty")
            elif field == "enabled":
                value = int(bool(value))
            else:
                value = int(value)
                if not 0 <= value <= 8:
                    raise ValueError(f"{field} must be between 0 and 8")
            assignments.append(f"{field} = ?")
            values.append(value)
        with self._transaction() as conn:
            row = conn.execute(
                "SELECT revoked_at FROM workers WHERE worker_id = ?", (worker_id,)
            ).fetchone()
            if row is None or row["revoked_at"] is not None:
                raise KeyError(worker_id)
            if assignments:
                conn.execute(
                    "UPDATE workers SET " + ", ".join(assignments)
                    + ", config_version = config_version + 1, updated_at = ? WHERE worker_id = ?",
                    [*values, _now(), worker_id],
                )
        return self.get(worker_id)

    def revoke(self, worker_id: str) -> None:
        """Invalidate the token and disable the worker; the row stays for history."""
        now = _now()
        with self._connect() as conn:
            cursor = conn.execute(
                "UPDATE workers SET revoked_at = ?, enabled = 0, token_hash = NULL, "
                "config_version = config_version + 1, updated_at = ? "
                "WHERE worker_id = ? AND revoked_at IS NULL",
                (now, now, worker_id),
            )
            if cursor.rowcount != 1:
                raise KeyError(worker_id)

    def create_enrollment(
        self, name: str, created_by: str | None, server_url: str, fingerprint: str
    ) -> tuple[str, str]:
        """A one-time enrollment string valid for 30 minutes: ``(string, expires_at)``."""
        code = secrets.token_urlsafe(24)
        now = datetime.now(timezone.utc)
        expires_at = (now + timedelta(minutes=ENROLLMENT_VALID_MINUTES)).isoformat(timespec="microseconds")
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO worker_enrollments(code_hash, name, created_by, created_at, expires_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (hash_secret(code), name.strip(), created_by, now.isoformat(timespec="microseconds"), expires_at),
            )
        payload = json.dumps({"u": server_url, "f": fingerprint, "c": code}, separators=(",", ":"))
        encoded = base64.urlsafe_b64encode(payload.encode("utf-8")).decode("ascii").rstrip("=")
        return ENROLLMENT_PREFIX + encoded, expires_at

    def redeem_enrollment(self, code: str, name: str) -> tuple[WorkerRecord, str]:
        """Use a code once: creates an enrolled worker and returns it with its token."""
        digest = hash_secret(code)
        now = _now()
        token = self._new_token()
        worker_id = str(uuid4())
        with self._transaction() as conn:
            row = conn.execute(
                "SELECT * FROM worker_enrollments WHERE code_hash = ?", (digest,)
            ).fetchone()
            if row is None or row["used_at"] is not None or row["expires_at"] <= now:
                raise ValueError("The enrollment code is invalid, expired, or already used")
            worker_name = row["name"] or name.strip() or "Worker"
            conn.execute(
                "INSERT INTO workers(worker_id, name, kind, token_hash, interactive_slots, batch_slots, "
                "created_at, updated_at) VALUES (?, ?, 'enrolled', ?, ?, ?, ?, ?)",
                (
                    worker_id, worker_name, hash_secret(token),
                    DEFAULT_INTERACTIVE_SLOTS, DEFAULT_BATCH_SLOTS, now, now,
                ),
            )
            conn.execute(
                "UPDATE worker_enrollments SET used_at = ?, worker_id = ? WHERE code_hash = ?",
                (now, worker_id, digest),
            )
        return self.get(worker_id), token


__all__ = ["WorkerRecord", "WorkerRegistry", "hash_secret", "parse_enrollment"]
