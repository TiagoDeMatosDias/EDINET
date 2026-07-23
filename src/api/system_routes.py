"""Discovery, capability, and health endpoints."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException

from src.orchestrator import list_available_steps
from src.version import __version__

from . import runtime
from .models import (
    AvailableStepsResponse,
    HealthResponse,
    JobHealthResponse,
    ServerConfigResponse,
    StepInfo,
)

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/api/steps", response_model=AvailableStepsResponse)
def list_steps() -> AvailableStepsResponse:
    """Return metadata for all discovered pipeline steps."""
    try:
        return AvailableStepsResponse(steps=list_available_steps())
    except Exception as exc:
        logger.exception("Failed to list pipeline steps")
        raise HTTPException(
            status_code=500,
            detail="Pipeline step metadata is unavailable",
        ) from exc


@router.get("/api/config", response_model=ServerConfigResponse)
def get_server_config() -> ServerConfigResponse:
    """Return public limits without exposing host filesystem paths."""
    return ServerConfigResponse(
        version=__version__,
        max_upload_bytes=runtime.SETTINGS.max_upload_bytes,
        max_export_bytes=runtime.SETTINGS.max_export_bytes,
        max_backtest_artifact_bytes=(
            runtime.SETTINGS.max_backtest_artifact_bytes
        ),
    )


@router.get("/api/steps/{step_name}", response_model=StepInfo)
def get_step(step_name: str) -> StepInfo:
    """Return metadata for a named pipeline step."""
    try:
        steps = list_available_steps()
    except Exception as exc:
        logger.exception("Failed to load pipeline step metadata")
        raise HTTPException(
            status_code=500,
            detail="Pipeline step metadata is unavailable",
        ) from exc

    match = next(
        (
            step
            for step in steps
            if step.get("name", "").casefold() == step_name.casefold()
        ),
        None,
    )
    if match is None:
        raise HTTPException(status_code=404, detail="Pipeline step not found")
    return StepInfo(
        name=match["name"],
        canonical_name=match.get("canonical_name", match["name"]),
        description=match.get("description"),
        parameters=match.get("parameters"),
        category=match.get("category"),
        aliases=match.get("aliases") or [],
    )


@router.get("/health", response_model=HealthResponse)
def health_check() -> HealthResponse:
    """Return bounded process health information."""
    return HealthResponse(
        status="healthy",
        version=__version__,
        timestamp=datetime.now(timezone.utc),
        jobs=JobHealthResponse(**runtime.job_manager.health_summary()),
    )
