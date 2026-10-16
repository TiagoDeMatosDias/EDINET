"""Pipeline output redaction and the legacy history store."""

from __future__ import annotations

import json

from src.pipeline_jobs import JobStore
from src.pipeline_jobs.redaction import safe_public_text, serialize_bounded


def test_bounded_serialization_is_valid_redacted_json():
    result = serialize_bounded(
        {"api_key": "secret", "payload": '"' * 1000},
        max_bytes=200,
    )

    assert len(result.encode("utf-8")) <= 200
    decoded = json.loads(result)
    assert decoded["truncated"] is True
    assert "secret" not in result


def test_nested_secrets_are_redacted():
    decoded = json.loads(serialize_bounded({"step": "a", "nested": {"token": "x"}, "api_key": "y"}))

    assert decoded == {"step": "a", "nested": {"token": "[REDACTED]"}, "api_key": "[REDACTED]"}


def test_public_text_hides_paths_and_secrets():
    text = safe_public_text(r"failed at C:\private\operator.db with token=abc123")

    assert text is not None
    assert "private" not in text
    assert "abc123" not in text


def test_job_store_still_marks_legacy_incomplete_jobs_interrupted(tmp_path):
    store = JobStore(tmp_path / "legacy.db")
    store.create_job("stale", [{"name": "first"}])

    assert store.interrupt_incomplete() == 1
    assert store.get_job("stale")["status"] == "interrupted"
