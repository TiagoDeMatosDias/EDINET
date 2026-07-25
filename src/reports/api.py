"""Owner-scoped reproducible report ZIP generation and download endpoints."""

from __future__ import annotations

import hashlib
import html
import json
import os
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Request, status
from fastapi.responses import FileResponse
from pydantic import BaseModel, ConfigDict, Field

from src.auth.models import AuthenticatedUser
from src.research.runtime import store

from .manifest import build_manifest, canonical_json
from .runtime import MAX_REPORT_BYTES, REPORT_ROOT

router = APIRouter(prefix="/api/reports", tags=["reports"])


class ReportRunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    recipe_id: str | None = None
    name: str = Field(default="Research report", min_length=1, max_length=120)
    as_of: str | None = Field(default=None, max_length=64)
    definition: dict[str, Any] = Field(default_factory=dict)


def _user(request: Request) -> AuthenticatedUser:
    user = getattr(request.state, "user", None)
    if not isinstance(user, AuthenticatedUser):
        raise HTTPException(status_code=401, detail="Account authentication is required")
    return user


def _as_of(value: str | None) -> str:
    return value or datetime.now(timezone.utc).isoformat(timespec="seconds")


def _zip_report(target: Path, manifest: dict[str, Any], definition: dict[str, Any]) -> tuple[int, str]:
    target.parent.mkdir(parents=True, exist_ok=True)
    descriptor = {"definition": definition, "coverage": definition.get("coverage", {}), "assumptions": definition.get("assumptions", {})}
    rendered = html.escape(json.dumps(descriptor, ensure_ascii=False, indent=2))
    html_document = f"<html><body><h1>Research report</h1><pre>{rendered}</pre></body></html>"
    fd, temporary = tempfile.mkstemp(prefix=f"{target.stem}.", suffix=".partial", dir=target.parent)
    os.close(fd)
    partial = Path(temporary)
    try:
        with zipfile.ZipFile(partial, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("manifest.json", canonical_json(manifest))
            archive.writestr("report.json", canonical_json(descriptor))
            archive.writestr("report.html", html_document.encode("utf-8"))
        size = partial.stat().st_size
        if size > MAX_REPORT_BYTES:
            raise ValueError("Generated report exceeds the configured size limit")
        digest = hashlib.sha256(partial.read_bytes()).hexdigest()
        os.replace(partial, target)
        return size, digest
    finally:
        if partial.exists():
            partial.unlink()


@router.get("/runs")
def list_runs(request: Request) -> dict[str, Any]:
    return {"runs": store.list_report_runs(_user(request).user_id)}


@router.post("/runs", status_code=status.HTTP_201_CREATED)
def create_run(request: Request, payload: ReportRunRequest) -> dict[str, Any]:
    user = _user(request)
    if len(canonical_json(payload.definition)) > 5 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="Report definition exceeds the configured size limit")
    as_of = _as_of(payload.as_of)
    manifest = build_manifest(
        report_id="pending",
        owner_id=user.user_id,
        recipe={"name": payload.name, "recipe_id": payload.recipe_id},
        inputs={"definition": payload.definition, "as_of": as_of},
        application_version="1",
    )
    manifest_json = canonical_json(manifest).decode("utf-8")
    run_id = store.create_report_run(user.user_id, payload.recipe_id, as_of, manifest_json)
    manifest["report_id"] = run_id
    manifest["recipe_sha256"] = hashlib.sha256(canonical_json(manifest["recipe"])).hexdigest()
    manifest["inputs_sha256"] = hashlib.sha256(canonical_json(manifest["inputs"])).hexdigest()
    target = (REPORT_ROOT / user.user_id / f"{run_id}.zip").resolve()
    root = REPORT_ROOT.resolve()
    try:
        target.relative_to(root)
        size, digest = _zip_report(target, manifest, payload.definition)
        store.finish_report_run(user.user_id, run_id, "completed", str(target), size, digest, manifest_json=canonical_json(manifest).decode("utf-8"))
    except Exception as exc:
        store.finish_report_run(user.user_id, run_id, "failed", None, None, None, str(exc)[:500])
        raise HTTPException(status_code=413 if "size limit" in str(exc) else 500, detail=str(exc)) from exc
    return {"run_id": run_id, "status": "completed", "size_bytes": size, "sha256": digest}


@router.get("/runs/{run_id}")
def get_run(request: Request, run_id: str) -> dict[str, Any]:
    run = store.get_report_run(_user(request).user_id, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Report run not found")
    return run


@router.get("/runs/{run_id}/manifest")
def get_manifest(request: Request, run_id: str) -> dict[str, Any]:
    run = store.get_report_run(_user(request).user_id, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Report run not found")
    try:
        return json.loads(run["manifest_json"])
    except (TypeError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=500, detail="Report manifest is invalid") from exc


@router.get("/runs/{run_id}/download")
def download_run(request: Request, run_id: str) -> FileResponse:
    run = store.get_report_run(_user(request).user_id, run_id)
    if run is None or run.get("status") != "completed":
        raise HTTPException(status_code=404, detail="Report artifact not found")
    raw_path = Path(str(run.get("artifact_path", "")))
    if raw_path.is_symlink():
        raise HTTPException(status_code=404, detail="Report artifact not found")
    path = raw_path.resolve(strict=False)
    try:
        path.relative_to(REPORT_ROOT.resolve())
    except ValueError as exc:
        raise HTTPException(status_code=404, detail="Report artifact not found") from exc
    if not path.is_file() or path.is_symlink():
        raise HTTPException(status_code=404, detail="Report artifact not found")
    return FileResponse(path, media_type="application/zip", filename=f"report-{run_id}.zip")


@router.delete("/runs/{run_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_run(request: Request, run_id: str) -> None:
    user = _user(request)
    run = store.get_report_run(user.user_id, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Report run not found")
    raw_path = Path(str(run.get("artifact_path", "")))
    candidate = raw_path.resolve(strict=False)
    path: Path | None = candidate if not raw_path.is_symlink() else None
    try:
        candidate.relative_to(REPORT_ROOT.resolve())
    except ValueError:
        path = None
    if path is not None and path.is_file() and not path.is_symlink():
        path.unlink()
    if not store.delete_report_run(user.user_id, run_id):
        raise HTTPException(status_code=404, detail="Report run not found")
