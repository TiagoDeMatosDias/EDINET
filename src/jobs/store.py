"""SQLite persistence for jobs, their steps, artifacts, and worker sessions.

The server is the only writer of ``jobs.db``; workers act through the HTTP API
and the claim below is made on their behalf. Every method opens its own
connection, so the store is safe to use from any thread. Rows are returned as
plain dicts; ``JobService`` turns them into ``JobRecord`` objects.

State machine and claim rules: ``docs/Feature Development/Job Workers Plan.md``
sections 4.1 and 4.2.
"""

from __future__ import annotations

import hmac
import json
import logging
import secrets
import sqlite3
from contextlib import closing, contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator
from uuid import uuid4

from src.orchestrator.common.sqlite import (
    DEFAULT_BUSY_TIMEOUT_MS,
    connect_write,
    initialize_managed_database,
    record_schema_version,
    schema_version,
    transaction,
)
from src.pipeline_jobs.redaction import safe_public_text, serialize_bounded

from .errors import InvalidJobState, LeaseLost, NoDataAccess, UnknownSession
from .kinds import KINDS
from .protocol import (
    MAX_PROGRESS_BYTES,
    RUNNING_STATUSES,
    TERMINAL_STATUSES,
    StepEvent,
)

logger = logging.getLogger(__name__)

SCHEMA_COMPONENT = "jobs"
SESSION_RETENTION = timedelta(days=1)

_MIGRATION_1 = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    lane TEXT NOT NULL CHECK (lane IN ('interactive','batch')),
    status TEXT NOT NULL CHECK (status IN ('open','in_progress','cancelling','completed','failed','cancelled','interrupted')),
    title TEXT NOT NULL DEFAULT '',
    owner_user_id TEXT,
    priority INTEGER NOT NULL DEFAULT 0,
    payload_json TEXT NOT NULL,
    result_json TEXT,
    error_message TEXT,
    status_message TEXT,
    progress_percent REAL NOT NULL DEFAULT 0,
    progress_json TEXT,
    exclusive TEXT NOT NULL DEFAULT '',
    attempt INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 1,
    worker_id TEXT,
    session_id TEXT,
    lease_token TEXT,
    lease_expires_at TEXT,
    cancel_requested TEXT NOT NULL DEFAULT 'none' CHECK (cancel_requested IN ('none','soft','hard')),
    notify INTEGER NOT NULL DEFAULT 0,
    dedupe_key TEXT,
    created_at TEXT NOT NULL,
    claimed_at TEXT,
    started_at TEXT,
    completed_at TEXT,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_jobs_claim ON jobs(status, lane, priority DESC, created_at);
CREATE INDEX IF NOT EXISTS idx_jobs_owner ON jobs(owner_user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_jobs_kind ON jobs(kind, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_jobs_updated ON jobs(updated_at);
CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_dedupe_active ON jobs(dedupe_key)
    WHERE dedupe_key IS NOT NULL AND status IN ('open','in_progress','cancelling');

CREATE TABLE IF NOT EXISTS job_steps (
    job_id TEXT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL,
    step_name TEXT NOT NULL,
    overwrite INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'pending',
    started_at TEXT, completed_at TEXT, duration_ms INTEGER,
    result_json TEXT, error_message TEXT,
    progress_percent REAL NOT NULL DEFAULT 0, status_message TEXT,
    PRIMARY KEY (job_id, ordinal)
);

CREATE TABLE IF NOT EXISTS job_artifacts (
    job_id TEXT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    sha256 TEXT NOT NULL,
    content_type TEXT,
    stored_path TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (job_id, name)
);

CREATE TABLE IF NOT EXISTS worker_sessions (
    session_id TEXT PRIMARY KEY,
    worker_id TEXT NOT NULL,
    hostname TEXT, pid INTEGER, platform TEXT, version TEXT,
    interactive_slots INTEGER NOT NULL, batch_slots INTEGER NOT NULL,
    interactive_busy INTEGER NOT NULL DEFAULT 0, batch_busy INTEGER NOT NULL DEFAULT 0,
    data_access INTEGER NOT NULL DEFAULT 0,
    data_access_detail TEXT,
    started_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    ended_at TEXT
);
"""


def utc_now() -> str:
    """UTC ISO-8601 with fixed microsecond precision, so text order is time order."""
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def plus_seconds(moment: str, seconds: float) -> str:
    return (datetime.fromisoformat(moment) + timedelta(seconds=seconds)).isoformat(
        timespec="microseconds"
    )


def split_resources(value: str | None) -> set[str]:
    return {part for part in (value or "").split(",") if part}


def _retries_when_lost(row: sqlite3.Row | dict[str, Any]) -> bool:
    """Whether this job may run again after losing its worker."""
    spec = KINDS.get(str(row["kind"]))
    return bool(spec and spec.retry_on_lost and row["attempt"] < row["max_attempts"])


class JobsStore:
    """All SQL for ``jobs.db``."""

    def __init__(self, path: str | Path, *, busy_timeout_ms: int = DEFAULT_BUSY_TIMEOUT_MS):
        self.path = Path(path)
        self.busy_timeout_ms = busy_timeout_ms
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._migrate()

    # -- connections ---------------------------------------------------------

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = connect_write(self.path, busy_timeout_ms=self.busy_timeout_ms)
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def transaction(self):
        """One ``BEGIN IMMEDIATE`` transaction (commit on success, rollback on error)."""
        return transaction(self.path, busy_timeout_ms=self.busy_timeout_ms)

    def _migrate(self) -> None:
        with closing(connect_write(self.path, busy_timeout_ms=self.busy_timeout_ms)) as conn:
            initialize_managed_database(conn)
            if schema_version(conn, SCHEMA_COMPONENT) < 1:
                conn.executescript(_MIGRATION_1)
                record_schema_version(conn, SCHEMA_COMPONENT, 1, utc_now())
            conn.commit()

    # -- reads ---------------------------------------------------------------

    def get(self, job_id: str) -> dict[str, Any]:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
        if row is None:
            raise KeyError(job_id)
        return dict(row)

    def list_jobs(
        self,
        *,
        kinds: Iterable[str] | None = None,
        owner_user_id: Any = ...,
        include_system: bool = False,
        also_kinds: Iterable[str] | None = None,
        active_only: bool = False,
        updated_since: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """Newest first. ``owner_user_id=...`` means any owner.

        With an owner, ``include_system`` adds unowned (system) jobs and
        ``also_kinds`` adds every job of those kinds whoever owns it.
        """
        where: list[str] = []
        params: list[Any] = []
        if kinds is not None:
            kind_list = list(kinds)
            where.append(f"kind IN ({','.join('?' * len(kind_list))})" if kind_list else "0")
            params.extend(kind_list)
        if owner_user_id is not ...:
            owner_terms: list[str] = []
            if owner_user_id is None:
                owner_terms.append("owner_user_id IS NULL")
            else:
                owner_terms.append("owner_user_id = ?")
                params.append(owner_user_id)
                if include_system:
                    owner_terms.append("owner_user_id IS NULL")
            extra = list(also_kinds or ())
            if extra:
                owner_terms.append(f"kind IN ({','.join('?' * len(extra))})")
                params.extend(extra)
            where.append("(" + " OR ".join(owner_terms) + ")")
        if active_only:
            where.append("status IN ('open','in_progress','cancelling')")
        if updated_since:
            where.append("updated_at > ?")
            params.append(updated_since)
        sql = "SELECT * FROM jobs"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY created_at DESC, rowid DESC LIMIT ? OFFSET ?"
        params.extend([int(limit), int(offset)])
        with self._connect() as conn:
            return [dict(row) for row in conn.execute(sql, params).fetchall()]

    def steps(self, job_id: str) -> list[dict[str, Any]]:
        with self._connect() as conn:
            return [
                dict(row)
                for row in conn.execute(
                    "SELECT * FROM job_steps WHERE job_id = ? ORDER BY ordinal", (job_id,)
                ).fetchall()
            ]

    def artifacts(self, job_id: str) -> list[dict[str, Any]]:
        with self._connect() as conn:
            return [
                dict(row)
                for row in conn.execute(
                    "SELECT * FROM job_artifacts WHERE job_id = ? ORDER BY name", (job_id,)
                ).fetchall()
            ]

    def counts_by_status(self) -> dict[str, int]:
        with self._connect() as conn:
            rows = conn.execute("SELECT status, COUNT(*) AS n FROM jobs GROUP BY status").fetchall()
        return {str(row["status"]): int(row["n"]) for row in rows}

    def running_jobs_by_worker(self) -> dict[str, list[dict[str, Any]]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT job_id, kind, title, started_at, progress_percent, worker_id FROM jobs "
                "WHERE status IN ('in_progress','cancelling') AND worker_id IS NOT NULL "
                "ORDER BY claimed_at"
            ).fetchall()
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            grouped.setdefault(str(row["worker_id"]), []).append(dict(row))
        return grouped

    # -- producers -----------------------------------------------------------

    def insert_job(
        self,
        *,
        job_id: str,
        kind: str,
        lane: str,
        title: str,
        owner_user_id: str | None,
        priority: int,
        payload: dict[str, Any],
        exclusive: str,
        max_attempts: int,
        notify: bool,
        dedupe_key: str | None,
        steps: Iterable[dict[str, Any]] | None = None,
        now: str | None = None,
    ) -> tuple[dict[str, Any], bool]:
        """Insert a job; with an active job under the same ``dedupe_key``, return that one.

        Returns ``(row, created)``.
        """
        now = now or utc_now()
        with self.transaction() as conn:
            if dedupe_key is not None:
                existing = conn.execute(
                    "SELECT * FROM jobs WHERE dedupe_key = ? "
                    "AND status IN ('open','in_progress','cancelling')",
                    (dedupe_key,),
                ).fetchone()
                if existing is not None:
                    return dict(existing), False
            conn.execute(
                "INSERT INTO jobs(job_id, kind, lane, status, title, owner_user_id, priority, "
                "payload_json, exclusive, max_attempts, notify, dedupe_key, created_at, updated_at) "
                "VALUES (?, ?, ?, 'open', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    job_id, kind, lane, title, owner_user_id, priority,
                    json.dumps(payload, default=str, ensure_ascii=False, separators=(",", ":")),
                    exclusive, max_attempts, int(bool(notify)), dedupe_key, now, now,
                ),
            )
            conn.executemany(
                "INSERT INTO job_steps(job_id, ordinal, step_name, overwrite) VALUES (?, ?, ?, ?)",
                [
                    (job_id, ordinal, str(step["name"]), int(bool(step.get("overwrite", False))))
                    for ordinal, step in enumerate(steps or ())
                ],
            )
            row = conn.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
        return dict(row), True

    def set_notify(self, job_id: str, notify: bool = True) -> None:
        with self._connect() as conn:
            conn.execute("UPDATE jobs SET notify = ? WHERE job_id = ?", (int(notify), job_id))

    # -- worker sessions -----------------------------------------------------

    def upsert_session(
        self,
        *,
        worker_id: str,
        session_id: str,
        hostname: str,
        pid: int,
        platform: str,
        version: str,
        interactive_slots: int,
        batch_slots: int,
        now: str | None = None,
    ) -> None:
        """Register a session and end older un-ended sessions of the same worker."""
        now = now or utc_now()
        with self.transaction() as conn:
            existing = conn.execute(
                "SELECT worker_id FROM worker_sessions WHERE session_id = ?", (session_id,)
            ).fetchone()
            if existing is not None and existing["worker_id"] != worker_id:
                raise UnknownSession("The session id belongs to another worker")
            conn.execute(
                "UPDATE worker_sessions SET ended_at = ? "
                "WHERE worker_id = ? AND session_id != ? AND ended_at IS NULL",
                (now, worker_id, session_id),
            )
            conn.execute(
                "INSERT INTO worker_sessions(session_id, worker_id, hostname, pid, platform, version, "
                "interactive_slots, batch_slots, started_at, last_seen_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(session_id) DO UPDATE SET hostname = excluded.hostname, "
                "pid = excluded.pid, platform = excluded.platform, version = excluded.version, "
                "interactive_slots = excluded.interactive_slots, batch_slots = excluded.batch_slots, "
                "last_seen_at = excluded.last_seen_at, ended_at = NULL",
                (
                    session_id, worker_id, hostname, pid, platform, version,
                    interactive_slots, batch_slots, now, now,
                ),
            )

    def get_session(self, session_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM worker_sessions WHERE session_id = ?", (session_id,)
            ).fetchone()
        return dict(row) if row else None

    def touch_session(self, worker_id: str, session_id: str, now: str | None = None) -> None:
        """Mark a live session seen; ``UnknownSession`` when it is missing or ended."""
        now = now or utc_now()
        with self._connect() as conn:
            cursor = conn.execute(
                "UPDATE worker_sessions SET last_seen_at = ? "
                "WHERE session_id = ? AND worker_id = ? AND ended_at IS NULL",
                (now, session_id, worker_id),
            )
            if cursor.rowcount != 1:
                raise UnknownSession()

    def end_session(self, worker_id: str, session_id: str, now: str | None = None) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE worker_sessions SET ended_at = ? "
                "WHERE session_id = ? AND worker_id = ? AND ended_at IS NULL",
                (now or utc_now(), session_id, worker_id),
            )

    def online_sessions(self, cutoff: str) -> list[dict[str, Any]]:
        """Un-ended sessions seen at or after ``cutoff``."""
        with self._connect() as conn:
            return [
                dict(row)
                for row in conn.execute(
                    "SELECT * FROM worker_sessions WHERE ended_at IS NULL AND last_seen_at >= ?",
                    (cutoff,),
                ).fetchall()
            ]

    def latest_sessions(self) -> dict[str, dict[str, Any]]:
        """For each worker, its newest un-ended session."""
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM worker_sessions WHERE ended_at IS NULL "
                "ORDER BY started_at ASC, rowid ASC"
            ).fetchall()
        return {str(row["worker_id"]): dict(row) for row in rows}

    # -- claim ---------------------------------------------------------------

    def claim(
        self,
        *,
        worker_id: str,
        session_id: str,
        lanes: list[str],
        kinds: Iterable[str] = (),
        lease_seconds: float,
        now: str | None = None,
    ) -> dict[str, Any] | None:
        """Hand the best claimable job to a session (plan section 4.2)."""
        now = now or utc_now()
        kind_list = list(kinds)
        with self.transaction() as conn:
            session = conn.execute(
                "SELECT * FROM worker_sessions WHERE session_id = ?", (session_id,)
            ).fetchone()
            if session is None or session["worker_id"] != worker_id or session["ended_at"]:
                raise UnknownSession()
            if not session["data_access"]:
                raise NoDataAccess()
            conn.execute(
                "UPDATE worker_sessions SET last_seen_at = ? WHERE session_id = ?",
                (now, session_id),
            )
            held: set[str] = set()
            for row in conn.execute(
                "SELECT exclusive FROM jobs WHERE status IN ('in_progress','cancelling') "
                "AND exclusive != ''"
            ):
                held |= split_resources(row["exclusive"])
            for lane in dict.fromkeys(lanes):
                sql = "SELECT * FROM jobs WHERE status = 'open' AND lane = ?"
                params: list[Any] = [lane]
                if kind_list:
                    sql += f" AND kind IN ({','.join('?' * len(kind_list))})"
                    params.extend(kind_list)
                sql += " ORDER BY priority DESC, created_at ASC, rowid ASC"
                for candidate in conn.execute(sql, params):
                    if split_resources(candidate["exclusive"]) & held:
                        continue
                    conn.execute(
                        "UPDATE jobs SET status = 'in_progress', worker_id = ?, session_id = ?, "
                        "lease_token = ?, lease_expires_at = ?, attempt = attempt + 1, "
                        "claimed_at = ?, started_at = COALESCE(started_at, ?), updated_at = ? "
                        "WHERE job_id = ?",
                        (
                            worker_id, session_id, _new_lease_token(),
                            plus_seconds(now, lease_seconds), now, now, now,
                            candidate["job_id"],
                        ),
                    )
                    claimed = conn.execute(
                        "SELECT * FROM jobs WHERE job_id = ?", (candidate["job_id"],)
                    ).fetchone()
                    return dict(claimed)
        return None

    # -- leases --------------------------------------------------------------

    @staticmethod
    def _holds_lease(row: sqlite3.Row, worker_id: str, lease_token: str) -> bool:
        return bool(
            row["lease_token"]
            and row["worker_id"] == worker_id
            and hmac.compare_digest(str(row["lease_token"]), str(lease_token))
        )

    def _leased_row(
        self, conn: sqlite3.Connection, job_id: str, worker_id: str, lease_token: str
    ) -> sqlite3.Row:
        """The job when this worker holds its live lease, else ``LeaseLost``/``KeyError``."""
        row = conn.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
        if row is None:
            raise KeyError(job_id)
        if row["status"] not in RUNNING_STATUSES or not self._holds_lease(row, worker_id, lease_token):
            raise LeaseLost()
        return row

    def check_lease(
        self,
        job_id: str,
        worker_id: str,
        lease_token: str,
        *,
        extend_seconds: float | None = None,
        now: str | None = None,
    ) -> dict[str, Any]:
        """Validate (and optionally extend) a lease; returns the job row."""
        now = now or utc_now()
        with self.transaction() as conn:
            row = self._leased_row(conn, job_id, worker_id, lease_token)
            if extend_seconds is not None:
                conn.execute(
                    "UPDATE jobs SET lease_expires_at = ? WHERE job_id = ?",
                    (plus_seconds(now, extend_seconds), job_id),
                )
                row = conn.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
        return dict(row)

    def heartbeat(
        self,
        *,
        worker_id: str,
        session_id: str,
        data_access: bool,
        data_access_detail: str | None,
        interactive_busy: int,
        batch_busy: int,
        leases: Iterable[tuple[str, str]],
        lease_seconds: float,
        now: str | None = None,
    ) -> list[dict[str, Any]]:
        """Record session state and extend the named leases.

        Returns one dict per lease: ``job_id``, ``lease_ok``, ``cancel``, ``lease_expires_at``.
        """
        now = now or utc_now()
        states: list[dict[str, Any]] = []
        with self.transaction() as conn:
            cursor = conn.execute(
                "UPDATE worker_sessions SET data_access = ?, data_access_detail = ?, "
                "interactive_busy = ?, batch_busy = ?, last_seen_at = ? "
                "WHERE session_id = ? AND worker_id = ? AND ended_at IS NULL",
                (
                    int(bool(data_access)),
                    safe_public_text(data_access_detail, 500),
                    max(0, int(interactive_busy)),
                    max(0, int(batch_busy)),
                    now, session_id, worker_id,
                ),
            )
            if cursor.rowcount != 1:
                raise UnknownSession()
            for job_id, token in leases:
                row = conn.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
                if (
                    row is None
                    or row["status"] not in RUNNING_STATUSES
                    or not self._holds_lease(row, worker_id, token)
                ):
                    states.append(
                        {"job_id": job_id, "lease_ok": False, "cancel": "none", "lease_expires_at": None}
                    )
                    continue
                expires = plus_seconds(now, lease_seconds)
                conn.execute(
                    "UPDATE jobs SET lease_expires_at = ? WHERE job_id = ?", (expires, job_id)
                )
                states.append(
                    {
                        "job_id": job_id,
                        "lease_ok": True,
                        "cancel": row["cancel_requested"],
                        "lease_expires_at": expires,
                    }
                )
        return states

    # -- progress ------------------------------------------------------------

    def progress(
        self,
        job_id: str,
        worker_id: str,
        lease_token: str,
        *,
        percent: float | None,
        message: str | None,
        progress: dict[str, Any] | None,
        step: StepEvent | None,
        lease_seconds: float,
        now: str | None = None,
    ) -> dict[str, Any]:
        now = now or utc_now()
        progress_json: str | None = None
        if progress is not None:
            encoded = json.dumps(progress, default=str, ensure_ascii=False, separators=(",", ":"))
            if len(encoded.encode("utf-8")) > MAX_PROGRESS_BYTES:
                logger.warning("Dropped oversized progress for job %s", job_id)
            else:
                progress_json = encoded
        with self.transaction() as conn:
            self._leased_row(conn, job_id, worker_id, lease_token)
            conn.execute(
                "UPDATE jobs SET lease_expires_at = ?, updated_at = ?, "
                "progress_percent = COALESCE(?, progress_percent), "
                "status_message = COALESCE(?, status_message), "
                "progress_json = COALESCE(?, progress_json) WHERE job_id = ?",
                (
                    plus_seconds(now, lease_seconds), now, percent,
                    safe_public_text(message, 500), progress_json, job_id,
                ),
            )
            if step is not None:
                self._apply_step(conn, job_id, step, now)
            row = conn.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
        return dict(row)

    @staticmethod
    def _apply_step(conn: sqlite3.Connection, job_id: str, step: StepEvent, now: str) -> None:
        current = conn.execute(
            "SELECT status FROM job_steps WHERE job_id = ? AND ordinal = ?", (job_id, step.ordinal)
        ).fetchone()
        if current is None:
            return
        message = safe_public_text(step.status_message, 500)
        if step.status == "running":
            if current["status"] != "running":
                conn.execute(
                    "UPDATE job_steps SET status = 'running', started_at = ?, completed_at = NULL, "
                    "progress_percent = ?, status_message = ? WHERE job_id = ? AND ordinal = ?",
                    (now, step.progress_percent or 0.0, message, job_id, step.ordinal),
                )
            else:
                conn.execute(
                    "UPDATE job_steps SET progress_percent = COALESCE(?, progress_percent), "
                    "status_message = ? WHERE job_id = ? AND ordinal = ?",
                    (step.progress_percent, message, job_id, step.ordinal),
                )
            return
        result_json = serialize_bounded(step.result) if step.result is not None else None
        conn.execute(
            "UPDATE job_steps SET status = ?, completed_at = ?, duration_ms = ?, result_json = ?, "
            "error_message = ?, status_message = NULL, "
            "progress_percent = CASE WHEN ? = 'completed' THEN 100.0 "
            "ELSE COALESCE(?, progress_percent) END WHERE job_id = ? AND ordinal = ?",
            (
                step.status, now, step.duration_ms, result_json,
                safe_public_text(step.error_message, 1000), step.status,
                step.progress_percent, job_id, step.ordinal,
            ),
        )

    # -- artifacts -----------------------------------------------------------

    def record_artifact(
        self,
        job_id: str,
        *,
        name: str,
        size_bytes: int,
        sha256: str,
        content_type: str | None,
        stored_path: str,
        now: str | None = None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO job_artifacts(job_id, name, size_bytes, sha256, content_type, "
                "stored_path, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (job_id, name, size_bytes, sha256, content_type, stored_path, now or utc_now()),
            )

    # -- terminal transitions ------------------------------------------------

    @staticmethod
    def _finish(
        conn: sqlite3.Connection,
        job_id: str,
        status: str,
        now: str,
        *,
        error: str | None = None,
        result_json: str | None = None,
    ) -> None:
        conn.execute(
            "UPDATE jobs SET status = ?, completed_at = ?, updated_at = ?, error_message = ?, "
            "result_json = COALESCE(?, result_json), lease_expires_at = NULL, status_message = NULL, "
            "progress_percent = CASE WHEN ? = 'completed' THEN 100.0 ELSE progress_percent END "
            "WHERE job_id = ?",
            (status, now, now, error, result_json, status, job_id),
        )

    @staticmethod
    def _requeue(conn: sqlite3.Connection, row: sqlite3.Row, now: str, *, refund_attempt: bool) -> None:
        attempt = max(0, row["attempt"] - 1) if refund_attempt else row["attempt"]
        conn.execute(
            "UPDATE jobs SET status = 'open', worker_id = NULL, session_id = NULL, lease_token = NULL, "
            "lease_expires_at = NULL, cancel_requested = 'none', progress_percent = 0, "
            "progress_json = NULL, status_message = NULL, attempt = ?, updated_at = ? "
            "WHERE job_id = ?",
            (attempt, now, row["job_id"]),
        )

    @staticmethod
    def _interrupt_steps(conn: sqlite3.Connection, job_id: str, now: str, reason: str) -> None:
        conn.execute(
            "UPDATE job_steps SET status = 'interrupted', completed_at = ?, status_message = NULL, "
            "error_message = COALESCE(error_message, ?) WHERE job_id = ? AND status = 'running'",
            (now, reason, job_id),
        )

    def _settled(self, row: sqlite3.Row, worker_id: str, lease_token: str) -> bool:
        """An earlier call by the same lease already ended this job (idempotent retry)."""
        return row["status"] in TERMINAL_STATUSES and self._holds_lease(row, worker_id, lease_token)

    def complete(
        self,
        job_id: str,
        worker_id: str,
        lease_token: str,
        result_json: str,
        now: str | None = None,
    ) -> dict[str, Any]:
        now = now or utc_now()
        with self.transaction() as conn:
            row = conn.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
            if row is None:
                raise KeyError(job_id)
            if self._settled(row, worker_id, lease_token):
                return dict(row)
            self._leased_row(conn, job_id, worker_id, lease_token)
            self._finish(conn, job_id, "completed", now, result_json=result_json)
            row = conn.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
        return dict(row)

    def fail(
        self,
        job_id: str,
        worker_id: str,
        lease_token: str,
        *,
        error: str,
        retryable: bool = False,
        cancelled: bool = False,
        now: str | None = None,
    ) -> dict[str, Any]:
        now = now or utc_now()
        with self.transaction() as conn:
            row = conn.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
            if row is None:
                raise KeyError(job_id)
            if self._settled(row, worker_id, lease_token):
                return dict(row)
            row = self._leased_row(conn, job_id, worker_id, lease_token)
            if cancelled or (row["status"] == "cancelling" and retryable):
                self._finish(conn, job_id, "cancelled", now)
            elif retryable and _retries_when_lost(row):
                self._requeue(conn, row, now, refund_attempt=False)
            else:
                self._finish(conn, job_id, "failed", now, error=safe_public_text(error, 1000))
            row = conn.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
        return dict(row)

    def release(
        self,
        job_id: str,
        worker_id: str,
        lease_token: str,
        *,
        reason: str = "The worker was stopped",
        now: str | None = None,
    ) -> dict[str, Any]:
        """Graceful worker shutdown: retryable kinds go back to ``open`` without using an attempt."""
        now = now or utc_now()
        with self.transaction() as conn:
            row = conn.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
            if row is None:
                raise KeyError(job_id)
            if self._settled(row, worker_id, lease_token):
                return dict(row)
            row = self._leased_row(conn, job_id, worker_id, lease_token)
            spec = KINDS.get(str(row["kind"]))
            message = safe_public_text(reason, 500) or "The worker was stopped"
            if row["status"] == "cancelling":
                self._finish(conn, job_id, "cancelled", now)
                self._interrupt_steps(conn, job_id, now, message)
            elif spec is not None and spec.retry_on_lost:
                self._requeue(conn, row, now, refund_attempt=True)
                self._interrupt_steps(conn, job_id, now, message)
            else:
                self._finish(conn, job_id, "interrupted", now, error=message)
                self._interrupt_steps(conn, job_id, now, message)
            row = conn.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
        return dict(row)

    def release_worker(self, worker_id: str, *, reason: str, now: str | None = None) -> int:
        """Release every job a stopped worker still holds and end its sessions.

        For a worker whose process is known to be gone (the local worker after the
        server stopped it), so its jobs need not wait for their leases to expire.
        Same outcomes as ``release``; returns how many jobs changed.
        """
        now = now or utc_now()
        message = safe_public_text(reason, 500) or "The worker was stopped"
        with self.transaction() as conn:
            rows = conn.execute(
                "SELECT * FROM jobs WHERE worker_id = ? AND status IN ('in_progress','cancelling')",
                (worker_id,),
            ).fetchall()
            for row in rows:
                spec = KINDS.get(str(row["kind"]))
                if row["status"] == "cancelling":
                    self._finish(conn, row["job_id"], "cancelled", now)
                elif spec is not None and spec.retry_on_lost:
                    self._requeue(conn, row, now, refund_attempt=True)
                else:
                    self._finish(conn, row["job_id"], "interrupted", now, error=message)
                self._interrupt_steps(conn, row["job_id"], now, message)
            conn.execute(
                "UPDATE worker_sessions SET ended_at = ? WHERE worker_id = ? AND ended_at IS NULL",
                (now, worker_id),
            )
        return len(rows)

    def request_cancel(self, job_id: str, *, force: bool = False, now: str | None = None) -> dict[str, Any]:
        """Cancel an open job at once; ask the worker to stop a running one."""
        now = now or utc_now()
        with self.transaction() as conn:
            row = conn.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
            if row is None:
                raise KeyError(job_id)
            status = row["status"]
            if status in TERMINAL_STATUSES:
                raise InvalidJobState("The job has already finished")
            if status == "open":
                self._finish(conn, job_id, "cancelled", now)
            elif status == "in_progress":
                conn.execute(
                    "UPDATE jobs SET status = 'cancelling', cancel_requested = ?, "
                    "status_message = 'Cancellation requested', updated_at = ? WHERE job_id = ?",
                    ("hard" if force else "soft", now, job_id),
                )
            elif force and row["cancel_requested"] != "hard":
                conn.execute(
                    "UPDATE jobs SET cancel_requested = 'hard', updated_at = ? WHERE job_id = ?",
                    (now, job_id),
                )
            row = conn.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
        return dict(row)

    # -- maintenance ---------------------------------------------------------

    def sweep(self, now: str | None = None) -> int:
        """Resolve jobs whose lease expired (plan section 5.8); returns how many changed."""
        now = now or utc_now()
        changed = 0
        with self.transaction() as conn:
            expired = conn.execute(
                "SELECT * FROM jobs WHERE status IN ('in_progress','cancelling') "
                "AND lease_expires_at IS NOT NULL AND lease_expires_at < ?",
                (now,),
            ).fetchall()
            for row in expired:
                changed += 1
                if row["status"] == "cancelling":
                    self._finish(conn, row["job_id"], "cancelled", now)
                    self._interrupt_steps(conn, row["job_id"], now, "The worker stopped responding")
                elif _retries_when_lost(row):
                    self._requeue(conn, row, now, refund_attempt=False)
                    self._interrupt_steps(conn, row["job_id"], now, "The worker stopped responding")
                elif row["kind"] in KINDS and KINDS[row["kind"]].retry_on_lost:
                    self._finish(
                        conn, row["job_id"], "failed", now, error="The worker stopped responding"
                    )
                    self._interrupt_steps(conn, row["job_id"], now, "The worker stopped responding")
                else:
                    self._finish(
                        conn, row["job_id"], "interrupted", now,
                        error="The worker stopped responding during the job",
                    )
                    self._interrupt_steps(
                        conn, row["job_id"], now, "The worker stopped responding during the job"
                    )
            cutoff = (datetime.fromisoformat(now) - SESSION_RETENTION).isoformat(timespec="microseconds")
            conn.execute("DELETE FROM worker_sessions WHERE last_seen_at < ?", (cutoff,))
        return changed

    def delete_terminal_before(self, cutoff: str) -> list[str]:
        """Delete finished jobs completed before ``cutoff``; returns their ids."""
        with self.transaction() as conn:
            ids = [
                str(row["job_id"])
                for row in conn.execute(
                    "SELECT job_id FROM jobs WHERE status IN ('completed','failed','cancelled','interrupted') "
                    "AND COALESCE(completed_at, updated_at) < ?",
                    (cutoff,),
                ).fetchall()
            ]
            conn.executemany("DELETE FROM jobs WHERE job_id = ?", [(job_id,) for job_id in ids])
        return ids


def _new_lease_token() -> str:
    return secrets.token_urlsafe(24)


def new_job_id() -> str:
    return str(uuid4())


__all__ = ["JobsStore", "new_job_id", "plus_seconds", "split_resources", "utc_now"]
