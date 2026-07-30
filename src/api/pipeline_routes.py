"""Pipeline submission endpoint."""

from __future__ import annotations

import logging
from json import JSONDecodeError

from fastapi import APIRouter, HTTPException, Request
from pydantic import ValidationError
from starlette.datastructures import UploadFile

from config import Config
from src.auth.models import AuthenticatedUser
from src.orchestrator import validate_input
from src.orchestrator.orchestrator import (
    InvalidUploadError,
    UploadTooLargeError,
    constrain_pipeline_paths,
    resolve_file_uploads,
    resolve_streamed_file_uploads,
)

from . import runtime
from .models import JobCreateResponse, PipelineConfig

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/pipeline", tags=["pipeline"])


def _require_operator(request: Request) -> AuthenticatedUser:
    user = getattr(request.state, "user", None)
    if not isinstance(user, AuthenticatedUser) or user.role not in ("admin", "operator"):
        raise HTTPException(status_code=403, detail="Operator permission required")
    return user


async def _parse_submission(request: Request) -> tuple[PipelineConfig, dict[str, UploadFile]]:
    content_type = request.headers.get("content-type", "").casefold()
    if not content_type.startswith("multipart/form-data"):
        try:
            payload = await request.json()
            return PipelineConfig.model_validate(payload), {}
        except (JSONDecodeError, TypeError, ValidationError, ValueError) as exc:
            raise HTTPException(status_code=422, detail="Invalid pipeline configuration") from exc

    form = await request.form()
    raw_config = form.get("config")
    if not isinstance(raw_config, str):
        for value in form.values():
            if isinstance(value, UploadFile):
                await value.close()
        raise HTTPException(status_code=422, detail="Multipart pipeline config is missing")

    try:
        config = PipelineConfig.model_validate_json(raw_config)
    except (TypeError, ValidationError, ValueError) as exc:
        for value in form.values():
            if isinstance(value, UploadFile):
                await value.close()
        raise HTTPException(status_code=422, detail="Invalid pipeline configuration") from exc

    uploads: dict[str, UploadFile] = {}
    for key, value in form.multi_items():
        if key == "config":
            continue
        if not key.startswith("upload:") or not isinstance(value, UploadFile):
            for upload in uploads.values():
                await upload.close()
            if isinstance(value, UploadFile):
                await value.close()
            raise HTTPException(status_code=422, detail="Invalid multipart pipeline upload")
        uploads[key.removeprefix("upload:")] = value
    return config, uploads


async def _prepare_submission(
    config: PipelineConfig,
    workspace,
    uploads: dict[str, UploadFile] | None = None,
):
    resolved_config = dict(config.config)
    selected_steps = config.steps
    if selected_steps is None:
        selected_steps = validate_input(config=resolved_config, steps=None)
    if selected_steps:
        if uploads:
            resolved_config = await resolve_streamed_file_uploads(
                resolved_config,
                selected_steps,
                uploads,
                workspace=workspace,
                max_bytes=runtime.SETTINGS.max_upload_bytes,
            )
        else:
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
async def submit_pipeline(request: Request) -> JobCreateResponse:
    """Validate and queue a pipeline without holding the request open."""
    _require_operator(request)
    config, uploads = await _parse_submission(request)
    manager = runtime.job_manager
    job_id = manager.new_job_id()
    workspace = manager.workspace_for(job_id, create=True)
    try:
        config_object, normalized_steps = await _prepare_submission(
            config,
            workspace,
            uploads,
        )
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
    finally:
        for upload in uploads.values():
            await upload.close()

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
