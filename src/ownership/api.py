"""Ownership API: a company's shareholders and officers, and the people and organisations behind them."""

from __future__ import annotations

import gzip
import json
import logging
import re
import sqlite3
from contextvars import ContextVar
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel, ConfigDict, Field

from src.filings.remote_translation import owner_of
from src.jobs.errors import JobFailed
from src.orchestrator.common.db_config import get_filings_db, get_market_db

from . import service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/ownership", tags=["ownership"])
_ENTITY_ID = re.compile(r"^[A-Z][A-Za-z0-9]{3,15}$")
_EDINET_CODE = re.compile(r"^E\d{5}$")
_EMPTY = {"reports": 0, "companies": 0, "latest_period": None, "entities": 0, "updates": {}}
_NO_DATA = "No ownership data yet: run the Update ownership pipeline step."
_GZIP_FROM = 64 * 1024


def _database() -> str | None:
    path = get_market_db()
    return path if path and Path(path).is_file() else None


def _require(path: str | None) -> str:
    if path is None:
        raise HTTPException(status_code=503, detail=_NO_DATA)
    return path


def _market_caps(path: str) -> service.MarketCaps:
    """Market capitalisation per company, from the same calculation as the Analyze workspace."""
    def lookup(codes: list[str]) -> dict[str, float | None]:
        try:
            from src.comparison.peers import _cached_metrics
            from src.orchestrator.common.sqlite import connect_read
            from src.web_app.api.security_analysis import _compute_metrics

            conn = connect_read(path)
            try:
                marks = ",".join("?" for _ in codes)
                tickers = dict(conn.execute(f"SELECT Company_Code, Company_Ticker FROM CompanyInfo WHERE Company_Code IN ({marks})", codes).fetchall())
            finally:
                conn.close()
            metrics = _cached_metrics(path, lambda code, ticker: _compute_metrics(path, code, {}, {"ticker": ticker}))
            return {code: metrics(code, str(ticker)).get("MarketCap") for code, ticker in tickers.items() if ticker}
        except Exception as exc:  # noqa: BLE001 - stakes are still listed without market values
            logger.warning("Could not value shareholdings: %s", exc)
            return {}
    return lookup


@router.get("/status")
def ownership_status() -> dict[str, Any]:
    """How many reports have been read and how many people and organisations they name."""
    path = _database()
    if path is None:
        return dict(_EMPTY)
    try:
        return service.status(path, get_filings_db())
    except sqlite3.Error:  # tables appear on the first run of the step
        return dict(_EMPTY)


@router.get("/company/{edinet_code}")
def company(edinet_code: str) -> dict[str, Any]:
    """The company's largest shareholders and officers from its latest annual report."""
    code = edinet_code.strip()
    if not _EDINET_CODE.match(code):
        raise HTTPException(status_code=404, detail="Company not found")
    try:
        return service.company_ownership(_require(_database()), get_filings_db(), code)
    except sqlite3.OperationalError as exc:
        raise HTTPException(status_code=503, detail=_NO_DATA) from exc


@router.get("/entities")
def entities(q: str = "", kind: str = "", limit: int = service.ENTITY_PAGE, offset: int = 0) -> dict[str, Any]:
    """People and organisations by name; ``kind`` is person, individual, company, or organization."""
    try:
        return service.entities(_require(_database()), query=q[:200], kind=kind, limit=limit, offset=offset)
    except sqlite3.OperationalError as exc:
        raise HTTPException(status_code=503, detail=_NO_DATA) from exc


@router.get("/entities/{entity_id}")
def entity(request: Request, entity_id: str) -> Response:
    """One person or organisation: the companies it holds or serves, and its career.

    A custody bank is listed by thousands of companies, so the payload is gzipped when the client accepts that.
    """
    if not _ENTITY_ID.match(entity_id):
        raise HTTPException(status_code=404, detail="Not found")
    try:
        detail = service.entity_detail(_require(_database()), get_filings_db(), entity_id)
    except sqlite3.OperationalError as exc:
        raise HTTPException(status_code=503, detail=_NO_DATA) from exc
    if detail is None:
        raise HTTPException(status_code=404, detail="Not found")
    payload = json.dumps(detail, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    if len(payload) > _GZIP_FROM and "gzip" in request.headers.get("accept-encoding", "").lower():
        return Response(content=gzip.compress(payload, compresslevel=6), media_type="application/json", headers={"Content-Encoding": "gzip", "Vary": "Accept-Encoding"})
    return Response(content=payload, media_type="application/json", headers={"Vary": "Accept-Encoding"})


@router.get("/market-caps")
def market_caps(codes: str = "") -> dict[str, Any]:
    """Market capitalisation of the listed companies among ``codes`` (comma-separated EDINET codes), for valuing stakes.

    The figure is Company Analysis's own, which takes a moment per company the
    first time, so the pages ask for it after showing the stakes.
    """
    path = _require(_database())
    wanted = [code for code in (part.strip() for part in codes.split(",")) if _EDINET_CODE.match(code)]
    return {"market_caps": service.market_caps(path, wanted, _market_caps(path))}


class CareerRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    lines: list[str] = Field(min_length=1, max_length=200)
    # The company whose report tells the career: "当社" in a line is this company.
    company: str = Field(default="", max_length=6)
    period_end: str = Field(default="", max_length=10)


_translation_owner: ContextVar[str | None] = ContextVar("ownership_translation_owner", default=None)


def _machine_translation(texts: list[str]) -> dict[str, str]:
    """English for lines no rule covers: the translation cache here, the engine in a worker.

    A translation that is still running propagates as ``JobPending`` (HTTP 202) so
    the page asks again when it is done; an unavailable engine leaves the lines as filed.
    """
    from src.filings.remote_translation import server_translator
    from src.filings.runtime import catalog

    try:
        return server_translator(catalog, owner_user_id=_translation_owner.get(), title="Translating career lines")(texts)
    except JobFailed as exc:
        logger.info("Career lines were not machine-translated: %s", exc)
        return {}


@router.post("/careers/translate")
def translate_career(request: Request, payload: CareerRequest) -> dict[str, Any]:
    """A career summary's lines in English: standard lines by rule, the rest by the translation worker."""
    _translation_owner.set(owner_of(request))
    path = _require(_database())
    lines = [line[:600] for line in payload.lines]
    try:
        english = service.career_english(path, lines, company=payload.company.strip(), on=payload.period_end, translate=_machine_translation)
    except sqlite3.OperationalError as exc:
        raise HTTPException(status_code=503, detail=_NO_DATA) from exc
    return {"translations": [{"source": line, "english": english[line]} for line in dict.fromkeys(lines) if line in english]}
