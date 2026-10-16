"""One-time copy of the old pipeline job history into ``jobs.db``.

Before job workers, pipeline runs were recorded in ``app.db`` tables
``pipeline_jobs`` and ``pipeline_job_steps``. Their history is kept visible by
copying it into ``jobs``/``job_steps`` once; the old tables stay where they
are and are no longer written.
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

from src.orchestrator.common.sqlite import (
    connect_read,
    record_schema_version,
    schema_version,
    table_exists,
)

from .store import JobsStore, utc_now

logger = logging.getLogger(__name__)

IMPORT_COMPONENT = "jobs_legacy_import"
# Old status -> new status. Work that was open or running when the old
# manager stopped can never resume.
_STATUS = {"pending": "interrupted", "running": "interrupted", "cancelling": "cancelled"}


def import_legacy_pipeline_jobs(store: JobsStore, app_db_path: str | Path) -> int:
    """Copy old pipeline history once; returns how many jobs were copied."""
    app_db = Path(app_db_path)
    with store.transaction() as conn:
        if schema_version(conn, IMPORT_COMPONENT) >= 1:
            return 0
        copied = 0
        if app_db.exists():
            try:
                source = connect_read(app_db)
            except sqlite3.Error:
                logger.warning("Could not open %s to import pipeline history", app_db)
                source = None
            if source is not None:
                try:
                    copied = _copy(source, conn)
                except sqlite3.Error:
                    logger.exception("Pipeline history import failed; leaving it for the next start")
                    raise
                finally:
                    source.close()
        record_schema_version(conn, IMPORT_COMPONENT, 1, utc_now())
    if copied:
        logger.info("Imported %d pipeline jobs from the old job tables", copied)
    return copied


def _copy(source: sqlite3.Connection, target: sqlite3.Connection) -> int:
    if not table_exists(source, "pipeline_jobs"):
        return 0
    copied = 0
    for row in source.execute("SELECT * FROM pipeline_jobs").fetchall():
        old = dict(row)
        status = _STATUS.get(str(old["status"]), str(old["status"]))
        created = str(old["created_at"])
        finished = old.get("completed_at") or created
        cursor = target.execute(
            "INSERT OR IGNORE INTO jobs(job_id, kind, lane, status, title, payload_json, error_message, "
            "status_message, progress_percent, max_attempts, created_at, claimed_at, started_at, "
            "completed_at, updated_at) "
            "VALUES (?, 'pipeline', 'batch', ?, 'Pipeline', '{}', ?, NULL, ?, 1, ?, ?, ?, ?, ?)",
            (
                old["job_id"], status, old.get("error_message"),
                float(old.get("progress_percent") or 0.0), created,
                old.get("started_at"), old.get("started_at"), finished, finished,
            ),
        )
        if cursor.rowcount != 1:
            continue
        copied += 1
        if not table_exists(source, "pipeline_job_steps"):
            continue
        for step in source.execute(
            "SELECT * FROM pipeline_job_steps WHERE job_id = ? ORDER BY ordinal", (old["job_id"],)
        ).fetchall():
            data = dict(step)
            step_status = "interrupted" if data["status"] == "running" else data["status"]
            target.execute(
                "INSERT OR IGNORE INTO job_steps(job_id, ordinal, step_name, overwrite, status, "
                "started_at, completed_at, duration_ms, result_json, error_message, progress_percent, "
                "status_message) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    data["job_id"], data["ordinal"], data["step_name"], data.get("overwrite") or 0,
                    step_status, data.get("started_at"), data.get("completed_at"),
                    data.get("duration_ms"), data.get("result_json"), data.get("error_message"),
                    data.get("progress_percent") or 0.0, None,
                ),
            )
    return copied


__all__ = ["import_legacy_pipeline_jobs"]
