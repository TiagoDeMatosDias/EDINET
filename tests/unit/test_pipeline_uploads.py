"""Security and ownership tests for embedded pipeline file uploads."""

from __future__ import annotations

import base64
import json
from pathlib import Path

import pytest

from src.orchestrator.common import StepDefinition, StepFieldDefinition
from src.orchestrator.orchestrator import (
    constrain_pipeline_paths,
    InvalidUploadError,
    STEP_DEFINITIONS,
    UploadTooLargeError,
    resolve_file_uploads,
)


@pytest.fixture
def upload_step(monkeypatch):
    definition = StepDefinition(
        name="upload_test",
        handler=lambda *_args, **_kwargs: None,
        config_key="upload_test_config",
        input_fields=(
            StepFieldDefinition(
                key="source_file",
                field_type="file",
                required=True,
            ),
            StepFieldDefinition(
                key="output_dir",
                field_type="str",
                default="arbitrary/client/path",
            ),
        ),
    )
    monkeypatch.setitem(STEP_DEFINITIONS, "upload_test", definition)
    return [{"name": "upload_test"}]


def _config(filename: str, content: str) -> dict:
    return {
        "upload_test_config": {
            "source_file": {"filename": filename, "content": content},
        }
    }


def test_upload_is_strict_sanitized_hashed_and_workspace_owned(
    tmp_path,
    upload_step,
):
    payload = b"ticker,price\nTEST,1\n"
    workspace = tmp_path / "job"
    resolved = resolve_file_uploads(
        _config(r"..\..\prices.csv", base64.b64encode(payload).decode("ascii")),
        upload_step,
        workspace=workspace,
        max_bytes=1024,
    )

    stored_path = Path(
        resolved["upload_test_config"]["source_file"]
    )
    assert stored_path.parent == workspace / "uploads"
    assert stored_path.name.endswith("-prices.csv")
    assert stored_path.read_bytes() == payload

    manifest_text = (workspace / "upload_manifest.json").read_text("utf-8")
    manifest = json.loads(manifest_text)
    assert manifest[0]["display_name"] == "prices.csv"
    assert manifest[0]["size_bytes"] == len(payload)
    assert len(manifest[0]["sha256"]) == 64
    assert base64.b64encode(payload).decode("ascii") not in manifest_text


def test_upload_rejects_invalid_base64_without_a_file(tmp_path, upload_step):
    workspace = tmp_path / "job"
    with pytest.raises(InvalidUploadError):
        resolve_file_uploads(
            _config("prices.csv", "not valid base64!"),
            upload_step,
            workspace=workspace,
            max_bytes=1024,
        )
    assert not list((workspace / "uploads").iterdir())


def test_upload_rejects_decoded_content_over_limit(tmp_path, upload_step):
    workspace = tmp_path / "job"
    encoded = base64.b64encode(b"12345").decode("ascii")
    with pytest.raises(UploadTooLargeError):
        resolve_file_uploads(
            _config("prices.csv", encoded),
            upload_step,
            workspace=workspace,
            max_bytes=4,
        )
    assert not list((workspace / "uploads").iterdir())


def test_pipeline_rejects_local_file_outside_allowed_roots(tmp_path, upload_step):
    source = tmp_path / "outside.csv"
    source.write_text("ticker,price\nTEST,1\n", encoding="utf-8")
    config = {"upload_test_config": {"source_file": str(source)}}

    with pytest.raises(InvalidUploadError):
        constrain_pipeline_paths(
            config,
            upload_step,
            workspace=tmp_path / "job",
            allowed_input_roots=(tmp_path / "allowed",),
        )


def test_pipeline_rewrites_output_and_allows_workspace_upload(
    tmp_path,
    upload_step,
):
    workspace = tmp_path / "job"
    upload = workspace / "uploads" / "owned.csv"
    upload.parent.mkdir(parents=True)
    upload.write_text("ticker,price\nTEST,1\n", encoding="utf-8")
    config = {
        "upload_test_config": {
            "source_file": str(upload),
            "output_dir": r"C:\private\escape",
        }
    }

    resolved = constrain_pipeline_paths(
        config,
        upload_step,
        workspace=workspace,
        allowed_input_roots=(),
    )
    step_config = resolved["upload_test_config"]
    assert Path(step_config["source_file"]) == upload
    assert Path(step_config["output_dir"]) == (
        workspace / "outputs" / "upload_test" / "backtest_set"
    )
