"""HTTP contracts for filing metadata, facts, narrative, and acquisition."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Request, status
from fastapi.responses import FileResponse
from pydantic import BaseModel, ConfigDict, Field

from src.auth.models import AuthenticatedUser

from .acquisition import EdinetAcquisitionError, EdinetDownloadClient
from .runtime import ARCHIVE_ROOT, catalog

router = APIRouter(prefix="/api/filings", tags=["filings"])


class AcquireRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    doc_id: str = Field(min_length=4, max_length=32, pattern=r"^[A-Za-z0-9]+$")
    edinet_code: str | None = Field(default=None, max_length=6)
    submitter_name: str | None = Field(default=None, max_length=300)
    submitted_at: str | None = Field(default=None, max_length=64)
    period_start: str | None = Field(default=None, max_length=32)
    period_end: str | None = Field(default=None, max_length=32)
    form_code: str | None = Field(default=None, max_length=32)


def _record(row: Any) -> dict[str, Any]:
    if row is None:
        return {}
    result = dict(row)
    result.pop("archive_path", None)
    return result


def _require_operator(request: Request) -> AuthenticatedUser:
    user = getattr(request.state, "user", None)
    if not isinstance(user, AuthenticatedUser) or user.role not in {"admin", "operator"}:
        raise HTTPException(status_code=403, detail="Operator permission required")
    return user


@router.get("/company/{edinet_code}")
def list_company_filings(edinet_code: str, limit: int = 100, offset: int = 0) -> dict[str, Any]:
    if not edinet_code.strip():
        raise HTTPException(status_code=400, detail="EDINET code is required")
    rows = catalog.list_company(edinet_code.strip(), limit, offset)
    return {"filings": [_record(row) for row in rows], "limit": min(max(limit, 1), 500), "offset": max(offset, 0)}


@router.get("")
def list_filings(company_code: str | None = None, limit: int = 100, offset: int = 0) -> dict[str, Any]:
    rows = catalog.list_recent(company_code.strip() if company_code else None, limit, offset)
    return {"filings": [_record(row) for row in rows], "limit": min(max(limit, 1), 500), "offset": max(offset, 0)}


@router.get("/coverage")
def filing_coverage() -> dict[str, Any]:
    return {"coverage": [_record(row) for row in catalog.coverage()]}


@router.get("/{doc_id}")
def get_filing(doc_id: str) -> dict[str, Any]:
    row = catalog.get_filing(doc_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Filing not found")
    return {"filing": _record(row), "artifacts": [_record(item) for item in catalog.list_artifacts(doc_id)]}


@router.get("/{doc_id}/facts")
def list_facts(doc_id: str, concept: str | None = None, limit: int = 500) -> dict[str, Any]:
    if catalog.get_filing(doc_id) is None:
        raise HTTPException(status_code=404, detail="Filing not found")
    rows = catalog.list_facts(doc_id, concept, limit)
    return {"facts": [_record(row) for row in rows], "count": len(rows)}


@router.get("/{doc_id}/sections")
def list_sections(doc_id: str, query: str | None = None, limit: int = 200) -> dict[str, Any]:
    if catalog.get_filing(doc_id) is None:
        raise HTTPException(status_code=404, detail="Filing not found")
    rows = catalog.list_sections(doc_id, query, limit)
    return {"sections": [_record(row) for row in rows], "count": len(rows)}


@router.get("/{doc_id}/outline")
def filing_outline(doc_id: str, limit: int = 200) -> dict[str, Any]:
    return list_sections(doc_id, limit=limit)


@router.get("/{doc_id}/statements")
def filing_statements(doc_id: str, concept: str | None = None, limit: int = 500) -> dict[str, Any]:
    return list_facts(doc_id, concept, limit)


@router.get("/{doc_id}/quality")
def list_quality_issues(doc_id: str) -> dict[str, Any]:
    if catalog.get_filing(doc_id) is None:
        raise HTTPException(status_code=404, detail="Filing not found")
    return {"issues": [_record(row) for row in catalog.list_quality_issues(doc_id)]}


@router.get("/{doc_id}/sections/{section_id}")
def get_section_detail(doc_id: str, section_id: str) -> dict[str, Any]:
    if catalog.get_filing(doc_id) is None:
        raise HTTPException(status_code=404, detail="Filing not found")
    row = catalog.get_section(doc_id, section_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Section not found")
    return {"section": _record(row)}


@router.get("/{doc_id}/taxonomy")
def filing_taxonomy(doc_id: str) -> dict[str, Any]:
    if catalog.get_filing(doc_id) is None:
        raise HTTPException(status_code=404, detail="Filing not found")
    rows = catalog.list_taxonomy(doc_id)
    return {"taxonomy": [_record(row) for row in rows], "count": len(rows)}


@router.get("/{doc_id}/audit-reports")
def filing_audit_reports(doc_id: str) -> dict[str, Any]:
    if catalog.get_filing(doc_id) is None:
        raise HTTPException(status_code=404, detail="Filing not found")
    audit_artifacts = [a for a in catalog.list_artifacts(doc_id) if "AuditDoc" in str(a["member_path"])]
    audit_sections = [
        s for s in catalog.list_sections(doc_id)
        if any(a["artifact_id"] == s["artifact_id"] for a in audit_artifacts)
    ]
    return {"audit_reports": [{"artifact": _record(a), "sections": [_record(s) for s in audit_sections if s["artifact_id"] == a["artifact_id"]]} for a in audit_artifacts]}


@router.get("/{doc_id}/parse-runs")
def list_parse_runs(doc_id: str) -> dict[str, Any]:
    if catalog.get_filing(doc_id) is None:
        raise HTTPException(status_code=404, detail="Filing not found")
    latest = catalog.latest_parse_run(doc_id)
    return {"latest": _record(latest) if latest else None}


@router.get("/{doc_id}/artifact")
def download_artifact(request: Request, doc_id: str) -> FileResponse:
    if not isinstance(getattr(request.state, "user", None), AuthenticatedUser):
        raise HTTPException(status_code=401, detail="Account authentication is required")
    filing = catalog.get_filing(doc_id)
    if filing is None:
        raise HTTPException(status_code=404, detail="Filing not found")
    raw_path = Path(str(filing["archive_path"]))
    if raw_path.is_symlink():
        raise HTTPException(status_code=404, detail="Filing artifact not found")
    path = raw_path.resolve(strict=False)
    archive_root = ARCHIVE_ROOT.resolve(strict=False)
    try:
        path.relative_to(archive_root)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail="Filing artifact not found") from exc
    if not path.is_file() or path.is_symlink():
        raise HTTPException(status_code=404, detail="Filing artifact not found")
    return FileResponse(path, media_type="application/zip", filename=f"{doc_id}.zip")


@router.post("/acquire", status_code=status.HTTP_202_ACCEPTED)
def acquire_filing(request: Request, payload: AcquireRequest) -> dict[str, Any]:
    """Acquire a type-1 archive; provider credentials are read only by the client."""
    _require_operator(request)
    metadata = payload.model_dump(exclude_none=True)
    try:
        client = EdinetDownloadClient.from_environment()
        fact_count = client.acquire_type1(
            payload.doc_id,
            ARCHIVE_ROOT,
            catalog,
            metadata,
        )
    except (ValueError, EdinetAcquisitionError) as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    return {"doc_id": payload.doc_id, "status": "parsed", "fact_count": fact_count}


# -- provenance and data quality --

from .provenance import provenance_batch, resolve_provenance  # noqa: E402


class ProvenanceRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    company_code: str | None = None
    metric_id: str | None = None
    period_end: str | None = None
    observation_id: str | None = None


class ProvenanceBatchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    requests: list[ProvenanceRequest] = Field(min_length=1, max_length=100)


@router.post("/provenance/resolve")
def resolve_provenance_endpoint(payload: ProvenanceRequest) -> dict[str, Any]:
    result = resolve_provenance(
        catalog,
        company_code=payload.company_code or "",
        metric_id=payload.metric_id or "",
        period_end=payload.period_end,
        observation_id=payload.observation_id,
    )
    if result is None:
        raise HTTPException(status_code=404, detail="No matching observation found")
    return result


@router.post("/provenance/resolve-batch")
def resolve_provenance_batch(payload: ProvenanceBatchRequest) -> dict[str, Any]:
    results = provenance_batch(
        catalog,
        [r.model_dump(exclude_none=True) for r in payload.requests],
    )
    return {"results": results}


@router.get("/data-quality/summary")
def data_quality_summary() -> dict[str, Any]:
    return {"issues_by_code": [_record(r) for r in catalog.quality_summary()]}


@router.get("/data-quality/issues")
def data_quality_issues(doc_id: str | None = None, severity: str | None = None, limit: int = 100) -> dict[str, Any]:
    if doc_id and catalog.get_filing(doc_id) is None:
        raise HTTPException(status_code=404, detail="Filing not found")
    issues = catalog.list_quality_issues(doc_id) if doc_id else []
    if severity and not doc_id:
        raise HTTPException(status_code=400, detail="Provide doc_id to filter by severity")
    filtered = [i for i in issues if not severity or i["severity"] == severity]
    return {"issues": [_record(r) for r in filtered[:limit]], "count": len(filtered)}


@router.get("/data-quality/coverage")
def data_quality_coverage() -> dict[str, Any]:
    return catalog.quality_coverage()
