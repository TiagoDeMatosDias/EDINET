"""SQLite persistence for pipeline jobs and their ordered steps."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from src.orchestrator.common.sqlite import (
    DEFAULT_BUSY_TIMEOUT_MS,
    connect_write,
    initialize_managed_database,
)

ACTIVE_STATUSES = frozenset({"pending", "running", "cancelling"})
TERMINAL_STATUSES = frozenset({"cancelled", "completed", "failed", "interrupted"})

_MIGRATION_1 = """
CREATE TABLE IF NOT EXISTS pipeline_jobs (
    job_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    started_at TEXT,
    completed_at TEXT,
    current_step TEXT,
    progress_percent REAL NOT NULL DEFAULT 0,
    step_count INTEGER NOT NULL DEFAULT 0,
    completed_step_count INTEGER NOT NULL DEFAULT 0,
    error_message TEXT,
    status_message TEXT
);

CREATE TABLE IF NOT EXISTS pipeline_job_steps (
    job_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    step_name TEXT NOT NULL,
    overwrite INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'pending',
    started_at TEXT,
    completed_at TEXT,
    duration_ms INTEGER,
    result_json TEXT,
    error_message TEXT,
    progress_percent REAL NOT NULL DEFAULT 0,
    status_message TEXT,
    PRIMARY KEY (job_id, ordinal),
    FOREIGN KEY (job_id) REFERENCES pipeline_jobs(job_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_pipeline_jobs_created
ON pipeline_jobs(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_pipeline_jobs_status
ON pipeline_jobs(status);
"""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class JobStore:
    """Persist truthful pipeline job and step state."""

    def __init__(
        self,
        path: str | Path,
        *,
        busy_timeout_ms: int = DEFAULT_BUSY_TIMEOUT_MS,
    ):
        self.path = Path(path)
        self.busy_timeout_ms = busy_timeout_ms
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._migrate()

    def _connect(self) -> sqlite3.Connection:
        return connect_write(
            self.path,
            busy_timeout_ms=self.busy_timeout_ms,
        )

    def _migrate(self) -> None:
        with self._connect() as conn:
            initialize_managed_database(conn)
            conn.execute(
                "CREATE TABLE IF NOT EXISTS schema_migrations ("
                "version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)"
            )
            row = conn.execute(
                "SELECT COALESCE(MAX(version), 0) AS version FROM schema_migrations"
            ).fetchone()
            version = int(row["version"])
            if version < 1:
                conn.executescript(_MIGRATION_1)
                conn.execute(
                    "INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                    (1, utc_now()),
                )
                version = 1
            if version < 2:
                self._migrate_progress_columns(conn)
                conn.execute(
                    "INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                    (2, utc_now()),
                )

    @staticmethod
    def _migrate_progress_columns(conn: sqlite3.Connection) -> None:
        job_columns = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(pipeline_jobs)")
        }
        step_columns = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(pipeline_job_steps)")
        }
        if "status_message" not in job_columns:
            conn.execute("ALTER TABLE pipeline_jobs ADD COLUMN status_message TEXT")
        if "progress_percent" not in step_columns:
            conn.execute(
                "ALTER TABLE pipeline_job_steps ADD COLUMN "
                "progress_percent REAL NOT NULL DEFAULT 0"
            )
        if "status_message" not in step_columns:
            conn.execute(
                "ALTER TABLE pipeline_job_steps ADD COLUMN status_message TEXT"
            )

    def create_job(self, job_id: str, steps: Iterable[dict[str, Any]]) -> dict:
        step_list = list(steps)
        created_at = utc_now()
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO pipeline_jobs("
                "job_id, status, created_at, step_count"
                ") VALUES (?, 'pending', ?, ?)",
                (job_id, created_at, len(step_list)),
            )
            conn.executemany(
                "INSERT INTO pipeline_job_steps("
                "job_id, ordinal, step_name, overwrite"
                ") VALUES (?, ?, ?, ?)",
                [
                    (
                        job_id,
                        ordinal,
                        step["name"],
                        int(bool(step.get("overwrite", False))),
                    )
                    for ordinal, step in enumerate(step_list)
                ],
            )
        return self.get_job(job_id)

    def get_job(self, job_id: str, *, include_steps: bool = False) -> dict:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM pipeline_jobs WHERE job_id = ?",
                (job_id,),
            ).fetchone()
            if row is None:
                raise KeyError(job_id)
            job = dict(row)
            if include_steps:
                job["steps"] = [
                    dict(step)
                    for step in conn.execute(
                        "SELECT * FROM pipeline_job_steps "
                        "WHERE job_id = ? ORDER BY ordinal",
                        (job_id,),
                    ).fetchall()
                ]
        return job

    def list_jobs(self, limit: int = 10, offset: int = 0) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM pipeline_jobs ORDER BY created_at DESC "
                "LIMIT ? OFFSET ?",
                (limit, offset),
            ).fetchall()
        return [dict(row) for row in rows]

    def start_job(self, job_id: str) -> bool:
        with self._connect() as conn:
            cursor = conn.execute(
                "UPDATE pipeline_jobs SET status = 'running', started_at = ? "
                "WHERE job_id = ? AND status = 'pending'",
                (utc_now(), job_id),
            )
        return cursor.rowcount == 1

    def start_step(
        self,
        job_id: str,
        ordinal: int,
        step_name: str,
        progress_percent: float,
    ) -> None:
        started_at = utc_now()
        with self._connect() as conn:
            conn.execute(
                "UPDATE pipeline_job_steps SET status = 'running', started_at = ?, "
                "progress_percent = 0, status_message = NULL "
                "WHERE job_id = ? AND ordinal = ?",
                (started_at, job_id, ordinal),
            )
            conn.execute(
                "UPDATE pipeline_jobs SET current_step = ?, progress_percent = ?, "
                "status_message = NULL "
                "WHERE job_id = ?",
                (step_name, progress_percent, job_id),
            )

    def update_step_progress(
        self,
        job_id: str,
        ordinal: int,
        step_percent: float,
        job_percent: float,
        status_message: str | None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE pipeline_job_steps SET progress_percent = ?, "
                "status_message = ? WHERE job_id = ? AND ordinal = ? "
                "AND status = 'running'",
                (step_percent, status_message, job_id, ordinal),
            )
            conn.execute(
                "UPDATE pipeline_jobs SET progress_percent = ?, "
                "status_message = ? WHERE job_id = ? AND status IN "
                "('running', 'cancelling')",
                (job_percent, status_message, job_id),
            )

    def finish_step(
        self,
        job_id: str,
        ordinal: int,
        *,
        status: str,
        duration_ms: int,
        progress_percent: float,
        step_progress_percent: float | None = None,
        result_json: str | None = None,
        error_message: str | None = None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE pipeline_job_steps SET status = ?, completed_at = ?, "
                "duration_ms = ?, result_json = ?, error_message = ?, "
                "progress_percent = ?, status_message = NULL "
                "WHERE job_id = ? AND ordinal = ?",
                (
                    status,
                    utc_now(),
                    duration_ms,
                    result_json,
                    error_message,
                    100.0 if status == "completed" else step_progress_percent or 0.0,
                    job_id,
                    ordinal,
                ),
            )
            increment = 1 if status == "completed" else 0
            conn.execute(
                "UPDATE pipeline_jobs SET completed_step_count = "
                "completed_step_count + ?, progress_percent = ? WHERE job_id = ?",
                (increment, progress_percent, job_id),
            )

    def mark_cancelling(self, job_id: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE pipeline_jobs SET status = 'cancelling', "
                "status_message = 'Cancellation requested' "
                "WHERE job_id = ? AND status IN ('pending', 'running')",
                (job_id,),
            )

    def finish_job(
        self,
        job_id: str,
        status: str,
        error_message: str | None = None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE pipeline_jobs SET status = ?, completed_at = ?, "
                "current_step = NULL, error_message = ?, status_message = NULL, "
                "progress_percent = CASE WHEN ? = 'completed' THEN 100.0 "
                "WHEN progress_percent >= 100.0 THEN 99.0 "
                "ELSE progress_percent END WHERE job_id = ?",
                (status, utc_now(), error_message, status, job_id),
            )

    def interrupt_incomplete(self) -> int:
        with self._connect() as conn:
            interrupted_at = utc_now()
            conn.execute(
                "UPDATE pipeline_job_steps SET status = 'interrupted', "
                "completed_at = ?, status_message = NULL, error_message = COALESCE("
                "error_message, 'Application stopped during step') "
                "WHERE status = 'running' AND job_id IN ("
                "SELECT job_id FROM pipeline_jobs WHERE status IN "
                "('pending', 'running', 'cancelling'))",
                (interrupted_at,),
            )
            cursor = conn.execute(
                "UPDATE pipeline_jobs SET status = 'interrupted', completed_at = ?, "
                "current_step = NULL, status_message = NULL, error_message = "
                "COALESCE(error_message, 'Application stopped before job completion') "
                "WHERE status IN ('pending', 'running', 'cancelling')",
                (interrupted_at,),
            )
        return cursor.rowcount

    def expired_job_ids(self, max_age_hours: int) -> list[str]:
        """Return terminal job IDs older than the retention cutoff."""
        cutoff = (
            datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
        ).isoformat()
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT job_id FROM pipeline_jobs WHERE status IN "
                "('cancelled', 'completed', 'failed', 'interrupted') "
                "AND completed_at < ?",
                (cutoff,),
            ).fetchall()
        return [str(row["job_id"]) for row in rows]

    def delete_jobs(self, job_ids: Iterable[str]) -> int:
        """Delete specific jobs and their cascading step records."""
        identifiers = list(job_ids)
        if not identifiers:
            return 0
        placeholders = ",".join("?" for _ in identifiers)
        with self._connect() as conn:
            cursor = conn.execute(
                f"DELETE FROM pipeline_jobs WHERE job_id IN ({placeholders})",
                identifiers,
            )
        return cursor.rowcount

    def cleanup(self, max_age_hours: int) -> int:
        return self.delete_jobs(self.expired_job_ids(max_age_hours))

    def active_count(self) -> int:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS count FROM pipeline_jobs "
                "WHERE status IN ('pending', 'running', 'cancelling')"
            ).fetchone()
        return int(row["count"])

    def status_counts(self) -> dict[str, int]:
        """Return aggregate job counts without exposing job content."""
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT status, COUNT(*) AS count FROM pipeline_jobs "
                "GROUP BY status"
            ).fetchall()
        return {str(row["status"]): int(row["count"]) for row in rows}
