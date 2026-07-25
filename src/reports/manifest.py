"""Deterministic report manifests and content checksums."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any


def canonical_json(value: Any) -> bytes:
    """Serialize JSON with stable ordering and UTF-8 output."""
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")


def content_sha256(value: Any) -> str:
    """Hash canonical JSON content."""
    return hashlib.sha256(canonical_json(value)).hexdigest()


def build_manifest(
    *,
    report_id: str,
    owner_id: str,
    recipe: dict[str, Any],
    inputs: dict[str, Any],
    application_version: str,
) -> dict[str, Any]:
    """Build a manifest that identifies every report input and its digest."""
    return {
        "manifest_version": 1,
        "report_id": report_id,
        "owner_id": owner_id,
        "application_version": application_version,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "recipe": recipe,
        "inputs": inputs,
        "recipe_sha256": content_sha256(recipe),
        "inputs_sha256": content_sha256(inputs),
    }
