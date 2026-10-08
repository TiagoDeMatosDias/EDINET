"""Per-user workstation preferences stored against the caller's account.

Hotkey overrides are kept in ``auth.db.user_settings`` under the
``hotkeys`` key as ``{hotkey_id: [key spec, ...]}``. Only overrides are
stored; the frontend owns the defaults and the catalogue of hotkey ids, so the
server validates shape and size but not whether an id exists.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field, StringConstraints

from src.auth.dependencies import CurrentUser
from src.auth.service import AuthService

router = APIRouter(prefix="/api/settings", tags=["settings"])

HOTKEYS_SETTING = "hotkeys"

HotkeyId = Annotated[str, StringConstraints(pattern=r"^[a-z0-9][a-z0-9._-]{0,119}$")]


class KeySpec(BaseModel):
    """One key, with the modifiers that must be held for it."""

    model_config = ConfigDict(extra="forbid")

    key: str = Field(min_length=1, max_length=30)
    shift: bool = False
    ctrl: bool = False
    alt: bool = False
    meta: bool = False


class HotkeySettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    overrides: dict[HotkeyId, Annotated[list[KeySpec], Field(min_length=1, max_length=4)]] = Field(
        default_factory=dict,
        max_length=1000,
    )


def _store(request: Request):
    service = getattr(request.app.state, "auth_service", None)
    if not isinstance(service, AuthService):
        raise HTTPException(status_code=503, detail="Settings storage is unavailable")
    return service.store


@router.get("/hotkeys", response_model=HotkeySettings)
def get_hotkeys(request: Request, user: CurrentUser) -> HotkeySettings:
    """Return the caller's hotkey overrides (empty when they use the defaults)."""
    stored = _store(request).get_user_setting(user.user_id, HOTKEYS_SETTING)
    if not isinstance(stored, dict):
        return HotkeySettings()
    try:
        return HotkeySettings.model_validate(stored)
    except ValueError:
        # A value written by an older client that no longer validates falls
        # back to the defaults rather than breaking every page.
        return HotkeySettings()


@router.put("/hotkeys", response_model=HotkeySettings)
def put_hotkeys(request: Request, user: CurrentUser, body: HotkeySettings) -> HotkeySettings:
    """Replace the caller's overrides with *body* (the full map)."""
    store = _store(request)
    if body.overrides:
        store.set_user_setting(user.user_id, HOTKEYS_SETTING, body.model_dump(exclude_defaults=False))
    else:
        store.delete_user_setting(user.user_id, HOTKEYS_SETTING)
    return body


@router.delete("/hotkeys", response_model=HotkeySettings)
def reset_hotkeys(request: Request, user: CurrentUser) -> HotkeySettings:
    """Forget every override so all hotkeys use their defaults again."""
    _store(request).delete_user_setting(user.user_id, HOTKEYS_SETTING)
    return HotkeySettings()
