"""Bounded company-comparison endpoints built on existing analysis contracts."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field

from src.security_analysis import get_security_overview, get_security_peers, get_security_statements
from src.web_app.api.security_analysis import _resolve_db

router = APIRouter(prefix="/api/comparison", tags=["comparison"])
_MAX_COMPANIES = 12


class ComparisonRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    company_codes: list[str] = Field(min_length=2, max_length=_MAX_COMPANIES)
    metrics: list[str] = Field(default_factory=list, max_length=50)


class HistoryRequest(ComparisonRequest):
    periods: int = Field(default=12, ge=1, le=40)


def _codes(values: list[str]) -> list[str]:
    normalized = list(dict.fromkeys(item.strip() for item in values if item.strip()))
    if not 2 <= len(normalized) <= _MAX_COMPANIES:
        raise HTTPException(status_code=400, detail="Provide between 2 and 12 company codes")
    return normalized


def _overview(code: str) -> dict[str, Any] | None:
    try:
        return get_security_overview(_resolve_db(), company_code=code)
    except ValueError:
        return None


@router.get("/peers/{company_code}")
def peers(company_code: str, limit: int = Query(default=10, ge=1, le=50)) -> dict[str, Any]:
    if not company_code.strip():
        raise HTTPException(status_code=400, detail="company_code is required")
    try:
        rows = get_security_peers(_resolve_db(), company_code.strip(), limit=limit)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"company_code": company_code.strip(), "peers": rows}


@router.post("/snapshot")
def snapshot(payload: ComparisonRequest) -> dict[str, Any]:
    codes = _codes(payload.company_codes)
    companies = []
    for code in codes:
        result = _overview(code)
        if result is None:
            continue
        companies.append({
            "company_code": code,
            "company": result.get("company", {}),
            "metrics": result.get("metrics", {}),
        })
    return {"companies": companies, "requested": codes, "metrics": payload.metrics}


@router.post("/history")
def history(payload: HistoryRequest) -> dict[str, Any]:
    codes = _codes(payload.company_codes)
    output: list[dict[str, Any]] = []
    for code in codes:
        result = _overview(code)
        if result is None:
            continue
        statements = get_security_statements(_resolve_db(), code, periods=payload.periods)
        output.append({
            "company_code": code,
            "company": result.get("company", {}),
            "history": statements,
        })
    return {"companies": output, "requested": codes, "periods": payload.periods}
