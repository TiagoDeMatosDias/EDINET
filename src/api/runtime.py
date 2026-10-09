"""Process-wide runtime dependencies for the pipeline API."""

from __future__ import annotations

from pathlib import Path

from src.orchestrator.common.db_config import get_app_db
from src.paths import bundle_dir, data_dir
from src.pipeline_jobs import JobStore, PipelineJobManager, PipelineScheduler
from src.web_app.security import get_settings

SETTINGS = get_settings()
PIPELINE_INPUT_ROOTS = (
    data_dir(),
    bundle_dir() / "assets",
    *SETTINGS.allowed_data_roots,
)
JOB_DB_PATH = Path(get_app_db())

job_store = JobStore(
    JOB_DB_PATH,
    busy_timeout_ms=SETTINGS.sqlite_busy_timeout_ms,
)
job_manager = PipelineJobManager(
    job_store,
    workspace_root=SETTINGS.job_workspace_root,
)
scheduler = PipelineScheduler(
    job_store,
    job_manager,
    input_roots=PIPELINE_INPUT_ROOTS,
    max_upload_bytes=SETTINGS.max_upload_bytes,
)

def cleanup_completed_jobs(max_age_hours: int | None = None) -> None:
    """Remove terminal jobs and workspaces older than the retention window."""
    retention = max_age_hours or SETTINGS.job_retention_hours
    job_manager.cleanup(retention)
