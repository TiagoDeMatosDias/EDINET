"""Operator settings stored in ``app.db``.

``get_setting`` returns a stored value, or the registry default when none is
stored; it reads ``app.db`` without creating it, so it is safe before startup.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .registry import SETTINGS, SettingError, SettingSpec, parse_text, spec_for, validate
from .store import SettingsStore, read_stored_value, read_stored_values

__all__ = [
    "SETTINGS",
    "SettingError",
    "SettingSpec",
    "describe_settings",
    "edinet_api_key",
    "get_setting",
    "load_settings",
    "parse_text",
    "set_setting",
    "spec_for",
    "unset_setting",
]


def _app_db(app_db: str | Path | None) -> Path:
    if app_db is not None:
        return Path(app_db)
    # Imported late: src.orchestrator discovers the pipeline steps on import,
    # and the steps import this package.
    from src.orchestrator.common.db_config import get_app_db

    return Path(get_app_db())


def _effective(spec: SettingSpec, stored: Any) -> Any:
    if stored is None:
        return spec.default
    try:
        return validate(spec, stored)
    except SettingError as exc:
        raise SettingError(f"The stored value of {spec.key} is invalid: {exc}") from exc


def get_setting(key: str, *, app_db: str | Path | None = None) -> Any:
    spec = spec_for(key)
    return _effective(spec, read_stored_value(_app_db(app_db), key))


def load_settings(*, app_db: str | Path | None = None) -> dict[str, Any]:
    """Every editable setting's effective value."""
    stored = read_stored_values(_app_db(app_db))
    return {spec.key: _effective(spec, stored.get(spec.key)) for spec in SETTINGS if spec.editable}


def edinet_api_key(*, app_db: str | Path | None = None) -> str:
    """The EDINET API key, read at call time so a newly saved key applies at once."""
    return str(get_setting("edinet.api_key", app_db=app_db) or "")


def set_setting(key: str, value: Any, *, updated_by: str | None = None, app_db: str | Path | None = None) -> Any:
    spec = spec_for(key)
    if not spec.editable:
        raise SettingError(f"{key} is managed by the application")
    normalized = validate(spec, value)
    SettingsStore(_app_db(app_db)).set(key, normalized, updated_by=updated_by)
    return normalized


def unset_setting(key: str, *, app_db: str | Path | None = None) -> None:
    """Return a setting to its default."""
    spec = spec_for(key)
    if not spec.editable:
        raise SettingError(f"{key} is managed by the application")
    SettingsStore(_app_db(app_db)).delete(key)


def describe_settings(*, app_db: str | Path | None = None) -> list[dict[str, Any]]:
    """Every editable setting with its metadata; secret values are never included."""
    rows = SettingsStore(_app_db(app_db)).rows()
    described = []
    for spec in SETTINGS:
        if not spec.editable:
            continue
        row = rows.get(spec.key)
        value = _effective(spec, row["value"] if row else None)
        described.append(
            {
                "key": spec.key,
                "label": spec.label,
                "description": spec.description,
                "kind": spec.kind,
                "choices": list(spec.choices),
                "minimum": spec.minimum,
                "restart_required": spec.restart,
                "default": None if spec.secret else spec.default,
                "value": None if spec.secret else value,
                "is_set": row is not None and value not in ("", [], None),
                "updated_at": row["updated_at"] if row else None,
                "updated_by": row["updated_by"] if row else None,
            }
        )
    return described
