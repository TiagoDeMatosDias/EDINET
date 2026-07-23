"""Process-wide runtime dependencies for the pipeline API."""

from __future__ import annotations

from pathlib import Path

from src.pipeline_jobs import JobStore, PipelineJobManager
from src.web_app.security import AppSettings

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SETTINGS = AppSettings.from_env()
PIPELINE_INPUT_ROOTS = (
    PROJECT_ROOT / "data",
    PROJECT_ROOT / "assets",
    *SETTINGS.allowed_data_roots,
)
JOB_DB_PATH = PROJECT_ROOT / "config" / "state" / "pipeline_jobs.db"

job_store = JobStore(
    JOB_DB_PATH,
    busy_timeout_ms=SETTINGS.sqlite_busy_timeout_ms,
)
job_manager = PipelineJobManager(
    job_store,
    workspace_root=SETTINGS.job_workspace_root,
)


def cleanup_completed_jobs(max_age_hours: int | None = None) -> None:
    """Remove terminal jobs and workspaces older than the retention window."""
    retention = max_age_hours or SETTINGS.job_retention_hours
    job_manager.cleanup(retention)
