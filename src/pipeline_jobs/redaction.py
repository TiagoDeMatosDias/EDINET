"""Safe, bounded serialization for persisted pipeline output."""

from __future__ import annotations

import json
import re
from typing import Any

_SENSITIVE_PARTS = (
    "api_key",
    "apikey",
    "authorization",
    "content",
    "password",
    "secret",
    "token",
)
_WINDOWS_PATH = re.compile(r"\b[A-Za-z]:[\\/][^\r\n,;]+")
_SECRET_ASSIGNMENT = re.compile(
    r"(?i)\b(api[_-]?key|authorization|password|secret|token)\s*[:=]\s*\S+"
)


def _is_sensitive(key: str) -> bool:
    normalized = key.casefold().replace("-", "_")
    return any(part in normalized for part in _SENSITIVE_PARTS)


def redact(value: Any) -> Any:
    """Recursively remove values stored under sensitive-looking keys."""
    if isinstance(value, dict):
        return {
            str(key): "[REDACTED]" if _is_sensitive(str(key)) else redact(item)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple, set)):
        return [redact(item) for item in value]
    return value


def safe_public_text(value: str | None, max_chars: int = 256) -> str | None:
    """Bound and redact progress text before it becomes API-visible state."""
    if value is None:
        return None
    text = str(value).replace("\r", " ").replace("\n", " ").strip()
    text = _WINDOWS_PATH.sub("[path]", text)
    text = _SECRET_ASSIGNMENT.sub(lambda match: f"{match.group(1)}=[REDACTED]", text)
    return text[:max_chars] or None


def serialize_bounded(value: Any, max_bytes: int = 65_536) -> str:
    """Return redacted JSON bounded to a safe persistence size."""
    payload = json.dumps(
        redact(value),
        default=str,
        ensure_ascii=False,
        separators=(",", ":"),
    )
    encoded = payload.encode("utf-8")
    if len(encoded) <= max_bytes:
        return payload

    preview = encoded[: max(0, max_bytes - 128)].decode("utf-8", errors="ignore")
    return json.dumps(
        {"truncated": True, "preview": preview},
        ensure_ascii=False,
        separators=(",", ":"),
    )
