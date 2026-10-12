"""Administrator endpoints for saved pipeline setups shared by all admins."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request, status

from src.auth.models import AuthenticatedUser

from . import runtime
from .models import PipelineSetupPayload, PipelineSetupResponse
from .schedule_routes import _validate_pipeline

router = APIRouter(prefix="/api/admin/pipeline-setups", tags=["admin-pipeline"])


def _require_admin(request: Request) -> AuthenticatedUser:
    user = getattr(request.state, "user", None)
    if not isinstance(user, AuthenticatedUser) or user.role != "admin":
        raise HTTPException(status_code=403, detail="Administrator permission required")
    return user


@router.get("", response_model=list[PipelineSetupResponse])
def list_setups(request: Request) -> list[PipelineSetupResponse]:
    _require_admin(request)
    return [
        PipelineSetupResponse.model_validate(setup)
        for setup in runtime.job_store.list_setups()
    ]


@router.put("/{name}", response_model=PipelineSetupResponse)
def save_setup(
    request: Request,
    name: str,
    payload: PipelineSetupPayload,
) -> PipelineSetupResponse:
    """Create or update the shared setup stored under ``name``."""
    _require_admin(request)
    _validate_pipeline(payload.steps, payload.config)
    try:
        setup = runtime.job_store.save_setup(name, payload.steps, payload.config)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=422, detail="Invalid setup data") from exc
    return PipelineSetupResponse.model_validate(setup)


@router.delete("/{name}", status_code=status.HTTP_204_NO_CONTENT)
def delete_setup(request: Request, name: str) -> None:
    _require_admin(request)
    try:
        deleted = runtime.job_store.delete_setup(name)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    if not deleted:
        raise HTTPException(status_code=404, detail="Setup not found")
