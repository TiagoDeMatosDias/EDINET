"""Administrator API for the operator settings in ``app.db``."""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, ConfigDict

from src.auth.dependencies import require_admin
from src.auth.models import AuthenticatedUser
from src.auth.service import AuthService

from . import SettingError, describe_settings, set_setting, spec_for, unset_setting

router = APIRouter(prefix="/api/admin/settings", tags=["admin-settings"])

Admin = Annotated[AuthenticatedUser, Depends(require_admin)]


class SettingUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    value: Any


def _audit(request: Request, user: AuthenticatedUser, event: str, key: str) -> None:
    service = getattr(request.app.state, "auth_service", None)
    if isinstance(service, AuthService):
        service.store.audit(event, user.user_id, detail=key)


def _apply(request: Request, key: str) -> None:
    """Act on a setting the running server follows without a restart."""
    tunnel = getattr(request.app.state, "tunnel", None)
    if key.startswith("tunnel.") and tunnel is not None:
        tunnel.apply()


def _described(key: str) -> dict[str, Any]:
    return next(item for item in describe_settings() if item["key"] == key)


def _require_editable(key: str) -> None:
    try:
        editable = spec_for(key).editable
    except SettingError:
        editable = False
    if not editable:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Unknown setting {key!r}")


@router.get("")
def list_settings(_user: Admin) -> dict[str, Any]:
    """Every editable setting; secret values are reported only as set or not set."""
    return {"settings": describe_settings()}


@router.put("/{key}")
def update_setting(key: str, payload: SettingUpdate, request: Request, user: Admin) -> dict[str, Any]:
    _require_editable(key)
    try:
        set_setting(key, payload.value, updated_by=user.user_id)
    except SettingError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    _audit(request, user, "setting_updated", key)
    _apply(request, key)
    return _described(key)


@router.delete("/{key}")
def reset_setting(key: str, request: Request, user: Admin) -> dict[str, Any]:
    """Return a setting to its default."""
    _require_editable(key)
    try:
        unset_setting(key)
    except SettingError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    _audit(request, user, "setting_reset", key)
    _apply(request, key)
    return _described(key)
