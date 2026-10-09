"""Administrator endpoints for persisted automatic pipeline schedules."""

from __future__ import annotations

import json

from fastapi import APIRouter, HTTPException, Request, status

from src.auth.models import AuthenticatedUser
from src.orchestrator import validate_input
from src.orchestrator.common.validation import MissingSettingsError

from . import runtime
from .models import (
    PipelineSchedulePatch,
    PipelineScheduleRequest,
    PipelineScheduleResponse,
    PipelineScheduleStatusResponse,
)

router = APIRouter(prefix="/api/admin/pipeline-schedules", tags=["admin-pipeline"])


def _require_admin(request: Request) -> AuthenticatedUser:
    user = getattr(request.state, "user", None)
    if not isinstance(user, AuthenticatedUser) or user.role != "admin":
        raise HTTPException(status_code=403, detail="Administrator permission required")
    return user


def _validate_pipeline(steps: list[dict], config: dict) -> None:
    try:
        validate_input(config=config, steps=steps)
        json.dumps(config, separators=(",", ":"))
    except MissingSettingsError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except (RuntimeError, TypeError, ValueError) as exc:
        raise HTTPException(status_code=422, detail="Invalid pipeline sequence") from exc


def _response(schedule: dict) -> PipelineScheduleResponse:
    return PipelineScheduleResponse.model_validate(schedule)


@router.get("", response_model=list[PipelineScheduleResponse])
def list_schedules(request: Request) -> list[PipelineScheduleResponse]:
    _require_admin(request)
    return [_response(schedule) for schedule in runtime.job_store.list_schedules()]


@router.post("", response_model=PipelineScheduleResponse, status_code=status.HTTP_201_CREATED)
def create_schedule(
    request: Request,
    payload: PipelineScheduleRequest,
) -> PipelineScheduleResponse:
    _require_admin(request)
    _validate_pipeline(payload.steps, payload.config)
    name = payload.name.strip()
    if not name:
        raise HTTPException(status_code=422, detail="Schedule name is required")
    try:
        schedule = runtime.job_store.create_schedule(
            name=name,
            frequency=payload.frequency,
            enabled=payload.enabled,
            steps=payload.steps,
            config=payload.config,
        )
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=422, detail="Invalid schedule data") from exc
    return _response(schedule)


@router.get("/status", response_model=PipelineScheduleStatusResponse)
def scheduler_status(request: Request) -> PipelineScheduleStatusResponse:
    _require_admin(request)
    return PipelineScheduleStatusResponse.model_validate(runtime.scheduler.status())


@router.post("/check", response_model=PipelineScheduleStatusResponse)
def trigger_scheduler_check(request: Request) -> PipelineScheduleStatusResponse:
    _require_admin(request)
    triggered = runtime.scheduler.check_now()
    return PipelineScheduleStatusResponse.model_validate(
        runtime.scheduler.status(triggered)
    )


@router.post("/{schedule_id}/reset-last-run", response_model=PipelineScheduleResponse)
def reset_last_run(request: Request, schedule_id: str) -> PipelineScheduleResponse:
    _require_admin(request)
    try:
        return _response(runtime.job_store.reset_schedule_run(schedule_id))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Schedule not found") from exc

@router.patch("/{schedule_id}", response_model=PipelineScheduleResponse)
def update_schedule(
    request: Request,
    schedule_id: str,
    payload: PipelineSchedulePatch,
) -> PipelineScheduleResponse:
    _require_admin(request)
    try:
        current = runtime.job_store.get_schedule(schedule_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Schedule not found") from exc

    changes = payload.model_dump(exclude_unset=True)
    next_steps = changes.get("steps", current["steps"])
    next_config = changes.get("config", current["config"])
    if "steps" in changes or "config" in changes:
        _validate_pipeline(next_steps, next_config)
    if "name" in changes:
        changes["name"] = str(changes["name"]).strip()
        if not changes["name"]:
            raise HTTPException(status_code=422, detail="Schedule name is required")
    try:
        return _response(runtime.job_store.update_schedule(schedule_id, **changes))
    except (KeyError, TypeError, ValueError) as exc:
        if isinstance(exc, KeyError):
            raise HTTPException(status_code=404, detail="Schedule not found") from exc
        raise HTTPException(status_code=422, detail="Invalid schedule data") from exc


@router.delete("/{schedule_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_schedule(request: Request, schedule_id: str) -> None:
    _require_admin(request)
    if not runtime.job_store.delete_schedule(schedule_id):
        raise HTTPException(status_code=404, detail="Schedule not found")
