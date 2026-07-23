"""Pipeline submission endpoint."""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException

from config import Config
from src.orchestrator import validate_input
from src.orchestrator.orchestrator import (
    InvalidUploadError,
    UploadTooLargeError,
    constrain_pipeline_paths,
    resolve_file_uploads,
)

from . import runtime
from .models import JobCreateResponse, PipelineConfig

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/pipeline", tags=["pipeline"])


def _prepare_submission(config: PipelineConfig, workspace):
    resolved_config = dict(config.config)
    selected_steps = config.steps
    if selected_steps is None:
        selected_steps = validate_input(config=resolved_config, steps=None)
    if selected_steps:
        resolved_config = resolve_file_uploads(
            resolved_config,
            selected_steps,
            workspace=workspace,
            max_bytes=runtime.SETTINGS.max_upload_bytes,
        )
        resolved_config = constrain_pipeline_paths(
            resolved_config,
            selected_steps,
            workspace=workspace,
            allowed_input_roots=runtime.PIPELINE_INPUT_ROOTS,
        )
    normalized_steps = validate_input(
        config=resolved_config,
        steps=selected_steps,
    )
    return Config.from_dict(resolved_config), normalized_steps


@router.post("/run", response_model=JobCreateResponse, status_code=202)
def submit_pipeline(config: PipelineConfig) -> JobCreateResponse:
    """Validate and queue a pipeline without holding the request open."""
    manager = runtime.job_manager
    job_id = manager.new_job_id()
    workspace = manager.workspace_for(job_id, create=True)
    try:
        config_object, normalized_steps = _prepare_submission(config, workspace)
        job = manager.submit(config_object, normalized_steps, job_id=job_id)
    except UploadTooLargeError as exc:
        manager.discard_workspace(job_id)
        raise HTTPException(status_code=413, detail=str(exc)) from exc
    except InvalidUploadError as exc:
        manager.discard_workspace(job_id)
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except (RuntimeError, TypeError, ValueError) as exc:
        manager.discard_workspace(job_id)
        logger.warning("Pipeline validation failed: %s", exc)
        raise HTTPException(
            status_code=400,
            detail="Pipeline configuration is invalid",
        ) from exc

    logger.info(
        "Queued pipeline job %s with steps: %s",
        job["job_id"],
        [step["name"] for step in normalized_steps],
    )
    return JobCreateResponse(
        job_id=job["job_id"],
        status=job["status"],
        created_at=job["created_at"],
    )
