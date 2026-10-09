"""Bond API: a company's bonds, the market of outstanding bonds, one bond in detail."""

from __future__ import annotations

import gzip
import json
import re
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Request, Response

from src.auth.models import AuthenticatedUser
from src.orchestrator.common.db_config import get_bonds_db

from . import service

router = APIRouter(prefix="/api/bonds", tags=["bonds"])
_DOC_ID = re.compile(r"^[A-Z0-9]{8}$")
_EMPTY = {"bonds": 0, "outstanding": 0, "companies": 0, "curve_date": None, "updates": {}}


def _database() -> str | None:
    path = get_bonds_db()
    return path if path and Path(path).is_file() else None


def _require(path: str | None) -> str:
    if path is None:
        raise HTTPException(status_code=503, detail="No bond data yet: run the Update bonds pipeline step.")
    return path


@router.get("/status")
def bond_status() -> dict[str, Any]:
    """How many bonds are stored and when each source was last read."""
    path = _database()
    if path is None:
        return dict(_EMPTY)
    try:
        return service.status(path)
    except Exception:  # noqa: BLE001 - tables appear on the first run of the step
        return dict(_EMPTY)


@router.get("/company/{edinet_code}")
def company(edinet_code: str) -> dict[str, Any]:
    """The company's bonds with totals, a maturity ladder, and the filings they come from."""
    return service.company_bonds(_require(_database()), edinet_code.strip())


@router.get("/market")
def market(request: Request, include_group: bool = False) -> Response:
    """Every outstanding bond; ``include_group`` adds bonds of consolidated subsidiaries.

    A few thousand bonds make a large payload, so it is gzipped when the client accepts that.
    """
    payload = json.dumps(service.universe(_require(_database()), include_group=include_group), ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    if "gzip" in request.headers.get("accept-encoding", "").lower():
        return Response(content=gzip.compress(payload, compresslevel=6), media_type="application/json", headers={"Content-Encoding": "gzip", "Vary": "Accept-Encoding"})
    return Response(content=payload, media_type="application/json", headers={"Vary": "Accept-Encoding"})


@router.get("/bond/{bond_id}")
def bond(bond_id: str) -> dict[str, Any]:
    """One bond: terms, sources, valuation, and similar bonds of other issuers."""
    detail = service.bond_detail(_require(_database()), bond_id.strip())
    if detail is None:
        raise HTTPException(status_code=404, detail="Bond not found")
    return detail


@router.get("/documents/{doc_id}")
def document(request: Request, doc_id: str) -> Response:
    """The stored EDINET archive of a bond supplement, as downloaded."""
    if not isinstance(getattr(request.state, "user", None), AuthenticatedUser):
        raise HTTPException(status_code=401, detail="Account authentication is required")
    if not _DOC_ID.match(doc_id):
        raise HTTPException(status_code=404, detail="Document not found")
    content = service.document_archive(_require(_database()), doc_id)
    if not content:
        raise HTTPException(status_code=404, detail="Document not stored")
    return Response(content=content, media_type="application/zip", headers={"Content-Disposition": f"attachment; filename={doc_id}.zip"})
