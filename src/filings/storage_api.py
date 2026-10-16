"""Administrator API for filing storage: ``/api/admin/filing-storage*``.

The answers are built from the filing index and ``os.stat``; no yearly file is
opened. Everything that changes files (split, check, compact, deleting the
retired file) is queued as a worker job, since the server never writes filing
storage.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from src.auth.dependencies import CurrentUser, require_admin
from src.jobs.service import get_job_service
from src.orchestrator.common.db_config import get_filings_db

from . import split as splitting
from .storage import (
    LAYOUT_LEGACY,
    LAYOUT_SPLIT,
    FilingStorage,
    LegacyLayoutError,
    ShardUnavailableError,
    validate_shard_id,
)

router = APIRouter(
    prefix="/api/admin/filing-storage",
    tags=["filing storage"],
    dependencies=[Depends(require_admin)],
)

STORAGE_JOB_KINDS = ("filings_split", "filing_storage_check", "filing_storage_compact", "filing_storage_cleanup")


class SplitBody(BaseModel):
    model_config = ConfigDict(extra="forbid")

    target_folder: str | None = Field(default=None, max_length=1024)


class CheckBody(BaseModel):
    model_config = ConfigDict(extra="forbid")

    repair: bool = False
    full: bool = False
    shard: str | None = Field(default=None, min_length=1, max_length=32, pattern=r"^[0-9A-Za-z][0-9A-Za-z_-]*$")


class CompactBody(BaseModel):
    model_config = ConfigDict(extra="forbid")

    shard: str = Field(min_length=1, max_length=32, pattern=r"^[0-9A-Za-z][0-9A-Za-z_-]*$")


def _queued(record: Any) -> dict[str, Any]:
    return {"job_id": record.job_id, "status": record.status, "kind": record.kind, "title": record.title}


def _jobs() -> list[dict[str, Any]]:
    records = get_job_service().list_jobs(kinds=list(STORAGE_JOB_KINDS), limit=10)
    return [
        {
            "job_id": record.job_id,
            "kind": record.kind,
            "status": record.status,
            "title": record.title,
            "progress_percent": record.progress_percent,
            "status_message": record.status_message,
            "error_message": record.error_message,
            "created_at": record.created_at,
            "completed_at": record.completed_at,
        }
        for record in records
    ]


def storage_overview(index_path: str | None = None) -> dict[str, Any]:
    """Layout, split state, yearly files, the retired file and (on the old layout) the preflight."""
    index = index_path or get_filings_db()
    storage = FilingStorage(index)
    result = splitting.status(index)
    result["index"] = {"path": str(storage.index_path), "bytes": splitting.file_bytes(storage.index_path) if storage.index_path.exists() else 0}
    result["shards_folder"] = str(storage.shards_dir)
    if result["layout"] == LAYOUT_SPLIT:
        result["shards"] = [
            {
                "shard": row["shard"],
                "path": row["path"],
                "file": row["file"],
                "state": row["state"] if row["exists"] else "missing",
                "filings": row["filing_count"],
                "archive_bytes": row["archive_bytes"],
                "file_bytes": splitting.file_bytes(storage.resolve_shard_path(str(row["path"]))) if row["exists"] else None,
                "checked_at": row["checked_at"],
                "check_result": row["check_result"],
            }
            for row in storage.shards()
        ]
    else:
        result["shards"] = []
    if result["layout"] == LAYOUT_LEGACY:
        result["preflight"] = splitting.preflight(index)
    return result


@router.get("")
def filing_storage(user: CurrentUser) -> dict[str, Any]:
    """Where filings are stored and what can be done about it."""
    result = storage_overview()
    result["jobs"] = _jobs()
    return result


def _enqueue(kind: str, payload: dict[str, Any], owner: str | None, title: str, dedupe_key: str | None = None) -> dict[str, Any]:
    record = get_job_service().enqueue(kind, payload, owner_user_id=owner, title=title, dedupe_key=dedupe_key or kind)
    return _queued(record)


@router.post("/split", status_code=status.HTTP_202_ACCEPTED)
def start_split(body: SplitBody, user: CurrentUser) -> dict[str, Any]:
    """Queue the split of the old single filings.db (``filings_split``); it resumes where an earlier run stopped."""
    state = splitting.status(get_filings_db())["state"]
    if state == "split":
        raise HTTPException(status_code=409, detail="Filing storage is already split")
    if state == "ready":
        raise HTTPException(status_code=409, detail="The split is complete; restart the server to finish it")
    payload = {"target_folder": body.target_folder} if body.target_folder else {}
    try:
        splitting.preflight(get_filings_db(), body.target_folder)
    except splitting.SplitError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _enqueue("filings_split", payload, user.user_id, "Split filing storage")


def _require_split() -> None:
    if FilingStorage(get_filings_db()).layout() != LAYOUT_SPLIT:
        raise HTTPException(status_code=409, detail=str(LegacyLayoutError()))


@router.post("/check", status_code=status.HTTP_202_ACCEPTED)
def start_check(body: CheckBody, user: CurrentUser) -> dict[str, Any]:
    """Queue a check of the yearly files (all, or ``shard``) against the index (``filing_storage_check``); ``repair`` fixes the index."""
    _require_split()
    title = "Check and repair filing storage" if body.repair else "Check filing storage"
    if body.shard:
        title = f"{title} ({body.shard})"
    return _enqueue("filing_storage_check", body.model_dump(exclude_none=True), user.user_id, title)


@router.post("/compact", status_code=status.HTTP_202_ACCEPTED)
def start_compact(body: CompactBody, user: CurrentUser) -> dict[str, Any]:
    """Queue ``VACUUM`` of one yearly file (``filing_storage_compact``)."""
    _require_split()
    storage = FilingStorage(get_filings_db())
    shard = validate_shard_id(body.shard)
    if not any(row["shard"] == shard for row in storage.shards()):
        raise HTTPException(status_code=404, detail=f"No yearly file {shard}")
    return _enqueue(
        "filing_storage_compact", {"shard": shard}, user.user_id, f"Compact filings {shard}", f"filing_storage_compact:{shard}"
    )


@router.delete("/retired", status_code=status.HTTP_202_ACCEPTED)
def delete_retired(user: CurrentUser) -> dict[str, Any]:
    """Queue the deletion of ``filings.db.migrated``, the old single file kept after the switch-over."""
    _require_split()
    if not splitting.retired_files(get_filings_db()):
        raise HTTPException(status_code=404, detail="There is no retired filings file")
    return _enqueue("filing_storage_cleanup", {}, user.user_id, "Delete the old filings file")


def install_filing_storage_error_handlers(app: FastAPI) -> None:
    """Answer a missing yearly file with 503 and a write on the old layout with 409, each with its message."""

    def _response(request: Request, status_code: int, code: str, detail: str) -> JSONResponse:
        from uuid import uuid4

        correlation_id = getattr(request.state, "correlation_id", None) or str(uuid4())
        return JSONResponse(
            status_code=status_code,
            content={"code": code, "detail": detail, "correlation_id": correlation_id},
            headers={"X-Correlation-ID": correlation_id},
        )

    @app.exception_handler(ShardUnavailableError)
    async def shard_unavailable(request: Request, exc: ShardUnavailableError) -> JSONResponse:
        return _response(request, 503, "service_unavailable", str(exc))

    @app.exception_handler(LegacyLayoutError)
    async def legacy_layout(request: Request, exc: LegacyLayoutError) -> JSONResponse:
        return _response(request, 409, "request_error", str(exc))


__all__ = ["install_filing_storage_error_handlers", "router", "storage_overview"]
