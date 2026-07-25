"""HTTP contracts for filing metadata, facts, narrative, and acquisition."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Request, status
from fastapi.responses import FileResponse, Response
from pydantic import BaseModel, ConfigDict, Field

from src.auth.models import AuthenticatedUser

from .acquisition import EdinetAcquisitionError, EdinetDownloadClient
from .runtime import catalog

router = APIRouter(prefix="/api/filings", tags=["filings"])


class AcquireRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    doc_id: str = Field(min_length=4, max_length=32, pattern=r"^[A-Za-z0-9]+$")
    edinet_code: str | None = Field(default=None, max_length=6)
    submitter_name: str | None = Field(default=None, max_length=300)
    submitted_at: str | None = Field(default=None, max_length=64)
    period_start: str | None = Field(default=None, max_length=32)
    period_end: str | None = Field(default=None, max_length=32)
    doc_type_code: str | None = Field(default=None, max_length=32)


def _record(row: Any) -> dict[str, Any]:
    if row is None:
        return {}
    result = dict(row)
    for key in ("archive_path", "archive_content", "content"):
        result.pop(key, None)
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
def download_artifact(request: Request, doc_id: str) -> Response:
    if not isinstance(getattr(request.state, "user", None), AuthenticatedUser):
        raise HTTPException(status_code=401, detail="Account authentication is required")
    filing = catalog.get_filing(doc_id)
    if filing is None:
        raise HTTPException(status_code=404, detail="Filing not found")
    content = filing["archive_content"]
    if not content:
        raise HTTPException(status_code=404, detail="Filing content not available")
    return Response(
        content=bytes(content),
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename={doc_id}.zip"},
    )


@router.post("/acquire", status_code=status.HTTP_202_ACCEPTED)
def acquire_filing(request: Request, payload: AcquireRequest) -> dict[str, Any]:
    """Acquire a type-1 archive; provider credentials are read only by the client."""
    _require_operator(request)
    metadata = payload.model_dump(exclude_none=True)
    try:
        client = EdinetDownloadClient.from_environment()
        fact_count = client.acquire_type1(
            payload.doc_id,
            catalog,
            metadata,
        )
    except (ValueError, EdinetAcquisitionError) as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    return {"doc_id": payload.doc_id, "status": "parsed", "fact_count": fact_count}


@router.get("/xbrl-eligible")
def list_xbrl_eligible(
    request: Request,
    limit: int = 100,
    doc_type_code: str = "120",
) -> dict[str, Any]:
    """List documents that have CSV data but are missing XBRL archives.

    Set *doc_type_code* to filter by EDINET form code (030000=annual, 07A000=quarterly).
    Leave empty for all document types.
    """
    if not isinstance(getattr(request.state, "user", None), AuthenticatedUser):
        raise HTTPException(status_code=401, detail="Account authentication is required")
    try:
        from src.orchestrator.common.db_config import get_db1
        from src.orchestrator.common.sqlite import connect_read
        import os as _os
    except Exception:
        return {"eligible": [], "total": 0}

    db1_path = get_db1()
    if not _os.path.exists(db1_path):
        return {"eligible": [], "total": 0}

    conn = connect_read(db1_path)
    try:
        where = (
            "WHERE xbrlFlag = '1' "
            "  AND Downloaded IN ('True', 'Checked_Unavailable') "
            "  AND legalStatus IN ('1', '2')"
        )
        params: list = []
        fc = doc_type_code.strip()
        if fc:
            where += " AND docTypeCode = ?"
            params.append(fc)
        params.append(min(max(limit, 1), 500))
        rows = conn.execute(
            "SELECT docID, edinetCode, submitDateTime, periodStart, periodEnd, "
            "formCode, docDescription, filerName, xbrlFlag "
            "FROM DocumentList " + where +
            " ORDER BY submitDateTime DESC LIMIT ?",
            params,
        ).fetchall()
    finally:
        conn.close()

    eligible = []
    for row in rows:
        doc_id = str(row["docID"]).strip()
        if not doc_id:
            continue
        existing = catalog.get_filing(doc_id)
        if existing is not None and existing["status"] in ("parsed", "archived"):
            continue
        eligible.append({
            "doc_id": doc_id,
            "edinet_code": row["edinetCode"] or "",
            "submitter_name": row["filerName"] or "",
            "submitted_at": row["submitDateTime"] or "",
            "period_end": row["periodEnd"] or "",
            "doc_type_code": row["formCode"] or "",
        })

    return {"eligible": eligible, "total": len(eligible)}


@router.post("/xbrl-backfill", status_code=status.HTTP_202_ACCEPTED)
def trigger_xbrl_backfill(
    request: Request,
    limit: int = 50,
    doc_type_code: str = "120",
) -> dict[str, Any]:
    """Download and archive XBRL packages for all eligible documents.

    Set *doc_type_code* to filter by EDINET form code (030000=annual, 07A000=quarterly).
    Leave empty for all document types. Requires operator or admin permission.
    """
    _require_operator(request)
    try:
        from src.orchestrator.common.db_config import get_db1
        from src.orchestrator.common.sqlite import connect_read
        import os as _os
    except Exception:
        return {"downloaded": 0, "skipped": 0, "failed": 0, "total": 0}

    db1_path = get_db1()
    if not _os.path.exists(db1_path):
        return {"downloaded": 0, "skipped": 0, "failed": 0, "total": 0}

    conn = connect_read(db1_path)
    try:
        where = (
            "WHERE xbrlFlag = '1' "
            "  AND Downloaded IN ('True', 'Checked_Unavailable') "
            "  AND legalStatus IN ('1', '2')"
        )
        params: list = []
        fc = doc_type_code.strip()
        if fc:
            where += " AND docTypeCode = ?"
            params.append(fc)
        params.append(min(max(limit, 1), 200))
        rows = conn.execute(
            "SELECT docID, edinetCode, submitDateTime, periodStart, periodEnd, "
            "formCode, docDescription, filerName "
            "FROM DocumentList " + where +
            " ORDER BY submitDateTime DESC LIMIT ?",
            params,
        ).fetchall()
    finally:
        conn.close()

    client = EdinetDownloadClient.from_environment()
    downloaded = 0
    skipped = 0
    failed = 0

    for row in rows:
        doc_id = str(row["docID"]).strip()
        if not doc_id:
            continue
        existing = catalog.get_filing(doc_id)
        if existing is not None and existing["status"] in ("parsed", "archived"):
            skipped += 1
            continue
        try:
            client.acquire_type1(
                doc_id,
                catalog,
                {
                    "edinet_code": row["edinetCode"] or "",
                    "submitted_at": row["submitDateTime"] or "",
                    "period_start": row["periodStart"] or "",
                    "period_end": row["periodEnd"] or "",
                    "doc_type_code": row["formCode"] or "",
                },
            )
            downloaded += 1
        except Exception:
            failed += 1

    return {
        "downloaded": downloaded,
        "skipped": skipped,
        "failed": failed,
        "total": len(rows),
    }


# -- original HTM report viewer --


@router.get("/{doc_id}/html/{artifact_id}")
def get_filing_html(
    doc_id: str,
    artifact_id: str,
    translate: bool = False,
    force: bool = False,
) -> dict[str, Any]:
    """Return sanitized HTML content from an archived filing member.

    Set *translate=true* to return an English-translated version alongside
    the original Japanese HTML.  Set *force=true* to clear cached
    translations for this file before re-translating.
    """
    if catalog.get_filing(doc_id) is None:
        raise HTTPException(status_code=404, detail="Filing not found")

    artifacts = catalog.list_artifacts(doc_id)
    match = next((a for a in artifacts if a["artifact_id"] == artifact_id), None)
    if match is None:
        raise HTTPException(status_code=404, detail="Artifact not found")

    member_path = str(match["member_path"])
    if not member_path.lower().endswith((".htm", ".html")):
        raise HTTPException(status_code=400, detail="Artifact is not an HTML file")

    artifact = catalog.get_artifact_content(artifact_id)
    if artifact is None or artifact["content"] is None:
        raise HTTPException(status_code=404, detail="Artifact content not found in database")

    content = bytes(artifact["content"])

    # Sanitize: remove scripts, event handlers, and external resources
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(content, "html.parser")
    for tag in soup.find_all(["script", "style", "iframe", "object", "embed", "form", "link"]):
        tag.decompose()
    for tag in soup.find_all(True):
        attrs_to_remove = [k for k in (tag.attrs or {}).keys() if k.startswith("on")]
        for k in attrs_to_remove:
            del tag.attrs[k]
        if tag.name == "img" and tag.get("src", "").startswith(("http:", "https:")):
            tag["src"] = ""
    body = soup.body or soup

    # Wrap as a complete HTML document so the iframe renders correctly
    html_attrs = " ".join(f'{k}="{v}"' for k, v in soup.html.attrs.items()) if soup.html else ""
    full_html = f"<!DOCTYPE html><html {html_attrs}><head><meta charset=\"utf-8\"></head>{str(body)}</html>"

    result: dict[str, Any] = {
        "artifact_id": artifact_id,
        "member_path": member_path,
        "html": full_html,
        "title": (soup.title.string if soup.title else member_path.rsplit("/", 1)[-1]),
    }

    if translate:
        import hashlib
        from .translate import translate_batch

        from .translate import _needs_translation

        en_soup = BeautifulSoup(str(body), "html.parser")

        # If forcing, clear the entire translation cache
        if force:
            from src.orchestrator.common.sqlite import connect_write
            conn = connect_write(catalog.path)
            deleted = conn.execute("DELETE FROM filing_translations").rowcount
            conn.commit()
            conn.close()
            import logging
            logging.getLogger(__name__).info("Force translate: cleared %d cache entries for %s", deleted, doc_id)

        jp_nodes = []
        for node in en_soup.find_all(string=True):
            text = node.strip()
            if text and len(text) > 2 and not text.startswith("<?xml"):
                if _needs_translation(text):
                    jp_nodes.append((node, text))

        if jp_nodes:
            unique_texts = list(dict.fromkeys(t for _, t in jp_nodes))
            batch_size = 50
            all_translations: dict[str, str] = {}
            for i in range(0, len(unique_texts), batch_size):
                batch = unique_texts[i:i + batch_size]
                all_translations.update(translate_batch(batch, catalog))

            for node, text in jp_nodes:
                if text in all_translations and all_translations[text] != text:
                    node.replace_with(all_translations[text])

        en_body = en_soup.body or en_soup
        result["html_en"] = f"<!DOCTYPE html><html {html_attrs}><head><meta charset=\"utf-8\"></head>{str(en_body)}</html>"

    return result


@router.get("/{doc_id}/htm-files")
def list_htm_files(doc_id: str) -> dict[str, Any]:
    """List all HTM/HTML files available for a filing, with descriptions."""
    if catalog.get_filing(doc_id) is None:
        raise HTTPException(status_code=404, detail="Filing not found")

    artifacts = catalog.list_artifacts(doc_id)
    htm_files = []
    for a in artifacts:
        path = str(a["member_path"])
        if not path.lower().endswith((".htm", ".html")):
            continue
        # Derive a label from the filename
        name = path.rsplit("/", 1)[-1].rsplit(".", 1)[0]
        # Parse EDINET naming: 0000000_header_... or 0101010_honbun_...
        label = name
        if "_honbun_" in name:
            section_num = name.split("_honbun_")[0]
            label = f"Section {section_num}"
        elif "_header_" in name:
            label = "Cover Page"
        htm_files.append({
            "artifact_id": a["artifact_id"],
            "member_path": path,
            "label": label,
            "filename": name,
            "size_bytes": a["size_bytes"],
        })

    return {"files": htm_files}


# -- translation --


class TranslateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    texts: list[str] = Field(min_length=1, max_length=200)
    force: bool = False


@router.post("/translate")
def translate_texts(payload: TranslateRequest) -> dict[str, Any]:
    """Translate a batch of Japanese text strings to English."""
    from .translate import translate_batch

    translations = translate_batch(payload.texts, catalog, force=payload.force)
    return {"translations": [{"source": k, "translated": v} for k, v in translations.items()]}


@router.get("/{doc_id}/sections-translated")
def translated_sections(doc_id: str, bodies: bool = False, limit: int = 500) -> dict[str, Any]:
    """Return filing sections with English translations added.

    Set *bodies=true* to also translate body text (slower — may time out for large filings).
    Default is titles only for fast loading.
    """
    if catalog.get_filing(doc_id) is None:
        raise HTTPException(status_code=404, detail="Filing not found")
    from .translate import translate_filing_sections

    rows = catalog.list_sections(doc_id, limit=limit)
    translated = translate_filing_sections(
        [_record(r) for r in rows], catalog, translate_bodies=bodies,
    )
    return {"sections": translated, "count": len(translated)}


@router.get("/{doc_id}/translate-body")
def translate_section_body(
    doc_id: str,
    section_id: str,
    force: bool = False,
) -> dict[str, Any]:
    """Translate a single section's body text on demand.

    Set *force=true* to bypass the translation cache and re-translate from scratch.
    """
    if catalog.get_filing(doc_id) is None:
        raise HTTPException(status_code=404, detail="Filing not found")
    from .translate import translate_batch

    row = catalog.get_section(doc_id, section_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Section not found")

    if force:
        # Clear cache for this specific text
        import hashlib
        from src.orchestrator.common.sqlite import connect_write

        body = row["text"] or ""
        if body:
            h = hashlib.sha256(body[:3000].encode("utf-8")).hexdigest()
            conn = connect_write(catalog.path)
            conn.execute(
                "DELETE FROM filing_translations WHERE source_hash = ?",
                (h,),
            )
            conn.commit()
            conn.close()
        title = row["title"] or ""
        if title:
            h = hashlib.sha256(title.encode("utf-8")).hexdigest()
            conn = connect_write(catalog.path)
            conn.execute(
                "DELETE FROM filing_translations WHERE source_hash = ?",
                (h,),
            )
            conn.commit()
            conn.close()

    from .translate import translate_filing_sections

    translated = translate_filing_sections([_record(row)], catalog, translate_bodies=True)
    return {"section": translated[0] if translated else {}}


@router.get("/{doc_id}/facts-translated")
def translated_facts(doc_id: str, concept: str | None = None, limit: int = 2000) -> dict[str, Any]:
    """Return filing facts with English concept labels added."""
    if catalog.get_filing(doc_id) is None:
        raise HTTPException(status_code=404, detail="Filing not found")
    from .translate import translate_facts

    rows = catalog.list_facts(doc_id, concept, limit)
    facts_list = [_record(r) for r in rows]
    concept_map = translate_facts(facts_list, catalog)
    for f in facts_list:
        c = f.get("concept", "")
        if c in concept_map:
            f["concept_en"] = concept_map[c]
    return {"facts": facts_list, "count": len(facts_list)}


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
