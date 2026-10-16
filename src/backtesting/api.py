"""Backtesting API routes.

All routes are mounted under ``/api/backtesting``. Backtests run as jobs on
workers (``src.worker.handlers.backtest``): the routes here validate a request,
resolve what the run needs from ``app.db`` into the job payload, queue the job,
and follow it. ``src.backtesting.job_finalize`` stores finished results.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal

from fastapi import APIRouter, Body, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse
from pydantic import BaseModel, Field, ValidationError
from starlette.concurrency import run_in_threadpool

from src import backtesting as _bt
from src.auth.models import AuthenticatedUser
from src.backtesting import job_finalize, store
from src.backtesting.detail import build_single_detail
from src.backtesting.html_report import render_report
from src.backtesting.zip_export import (
    build_rolling_zip,
    build_summary,
)
from src.jobs.errors import InvalidJobState, JobFailed
from src.jobs.service import JobRecord, JobService, get_job_service
from src.orchestrator.common.db_config import get_app_db, get_market_db
from src.orchestrator.common.sqlite import connect_read
from src.portfolio.currency import get_available_display_currencies
from src.portfolio.performance import get_risk_free_rate
from src.web_app.security import get_settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/backtesting", tags=["backtesting"])

# How long /run and /run-from-csv wait for their job before answering 202.
_SYNC_WAIT_SECONDS = 300.0
_JOB_VIEW_WINDOW = timedelta(hours=24)
_JOB_KINDS = {"single": "backtest_single", "csv": "backtest_csv", "rolling": "backtest_rolling"}
# Messages of failures that are not about the request itself.
_SYSTEM_FAILURE = re.compile(
    r"^The (?:worker|server) (?:was stopped|stopped responding)|\bfailed \(\w+\)$"
    r"|could not be saved|^The backtesting database is not available"
)
_KIND_OF_JOB = {value: key for key, value in _JOB_KINDS.items()}

# Names the helpers had when they lived here (kept for callers and tests).
_BACKTEST_ROOT = store.BACKTEST_ROOT
_BACKTEST_ID = store.BACKTEST_ID
_new_backtest_id = store.new_backtest_id
_backtest_directory = store.backtest_directory
_holdings_label = store.holdings_label
_backtest_subtitle = store.backtest_subtitle
_owner_may_see = store.owner_may_see


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _resolve_db() -> str:
    """The server's configured Standardized database; requests cannot name another."""
    resolved = get_market_db()
    if not resolved or not Path(resolved).is_file():
        raise HTTPException(status_code=503, detail="The backtesting database is not available.")
    return resolved


def _resolve_db3() -> str:
    """Resolve the portfolio database (db3) path."""
    db3 = get_app_db()
    if not db3 or not Path(db3).is_file():
        raise HTTPException(
            status_code=400,
            detail="Portfolio database not found. Import transactions first."
        )
    return db3


def _request_user(http_request: Request | None) -> AuthenticatedUser | None:
    user = getattr(getattr(http_request, "state", None), "user", None)
    return user if isinstance(user, AuthenticatedUser) else None


def _write_owner(out_dir: Path, http_request: Request | None) -> None:
    """Record which account produced a saved backtest directory."""
    user = _request_user(http_request)
    store.write_owner(out_dir, user.user_id if user is not None else None)


def _portfolio_owner(http_request: Request | None) -> str:
    """Whose own portfolio a ``portfolio`` benchmark means: the signed-in account."""
    user = _request_user(http_request)
    return user.user_id if user is not None else ""


def _read_meta(directory: Path) -> dict[str, Any]:
    return store.read_meta(directory)


def _can_access(directory: Path, user: AuthenticatedUser | None) -> bool:
    return store.owner_may_see(_read_meta(directory).get("owner_user_id"), user)


def _owned_backtest_directory(backtest_id: str, http_request: Request) -> Path:
    directory = store.backtest_directory(backtest_id, require_existing=True)
    if not _can_access(directory, _request_user(http_request)):
        # 404 rather than 403 so other accounts' result ids are not confirmed.
        raise HTTPException(status_code=404, detail="Backtest not found.")
    return directory


def _enforce_export_size(content: bytes) -> bytes:
    if len(content) > get_settings().max_export_bytes:
        raise HTTPException(413, "Generated export exceeds the configured size limit")
    return content


def _resolve_risk_free_rate(explicit_rf: float, base_currency: str) -> float:
    """Return the risk-free rate: explicit if > 0, else auto-detect from inflation."""
    if explicit_rf > 0:
        return explicit_rf
    try:
        db2 = get_market_db()
        if db2:
            return get_risk_free_rate(db2, base_currency or "EUR")
    except Exception:
        pass
    return 0.02  # fallback


def _validate_base_currency(base_currency: str) -> str:
    """Validate and return the base currency code.

    Returns empty string on empty input (native currency).
    Raises HTTPException for invalid currency codes.
    """
    if not base_currency:
        return ""
    bc = base_currency.upper()
    valid = {c["code"] for c in get_available_display_currencies()}
    if bc not in valid:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid base currency: '{base_currency}'. "
                    f"Available currencies: {', '.join(sorted(valid))}"
        )
    return bc


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class AllocationSpec(BaseModel):
    mode: Literal["weight", "shares", "value"] = "weight"
    value: float


class BacktestRunRequest(BaseModel):
    portfolio: dict[str, AllocationSpec]
    start_date: str = Field(..., description="YYYY-MM-DD")
    end_date: str = Field(..., description="YYYY-MM-DD")
    benchmark_ticker: str = ""
    benchmark_mode: Literal["ticker", "portfolio"] = "ticker"
    base_currency: str = Field(default="", description="Target currency for returns (e.g. EUR, USD). Empty = native.")
    initial_capital: float = 0.0
    risk_free_rate: float = 0.0
    commission_bps: float = Field(default=0.0, ge=0, le=500)
    slippage_bps: float = Field(default=0.0, ge=0, le=500)
    spread_bps: float = Field(default=0.0, ge=0, le=500)


class CSVBacktestRequest(BaseModel):
    csv_content: str = Field(..., description="Raw CSV string")
    benchmark_ticker: str = ""
    benchmark_mode: Literal["ticker", "portfolio"] = "ticker"
    base_currency: str = Field(default="", description="Target currency for returns (e.g. EUR, USD). Empty = native.")
    durations: list[str] = ["1yr", "2yr", "3yr", "5yr", "10yr"]
    initial_capital: float = 0.0
    risk_free_rate: float = 0.0


class RollingScreeningRequest(BaseModel):
    criteria: list[dict]
    criteria_match: Literal["all", "any"] = "all"
    columns: list[str]
    computed_columns: list[dict] = []
    cadence: str = "monthly"
    durations: list[str] = ["1yr", "2yr", "3yr", "5yr", "10yr"]
    weighting_modes: list[str] = ["equal"]
    max_companies: int = 25
    ranking_algorithm: str = "none"
    ranking_rules: list[dict] = []
    benchmark_ticker: str = ""
    benchmark_mode: Literal["ticker", "portfolio"] = "ticker"
    base_currency: str = Field(default="", description="Target currency for returns (e.g. EUR, USD). Empty = native.")
    initial_capital: float = 0.0
    risk_free_rate: float = 0.0
    start_period: str | None = None
    end_period: str | None = None


class RollingExportRequest(BaseModel):
    """Request body for exporting rolling backtest results to XLSX."""
    rolling_result: dict[str, Any]


class JobStartBody(BaseModel):
    kind: Literal["single", "csv", "rolling"]
    request: dict[str, Any]


_REQUEST_MODELS: dict[str, type[BaseModel]] = {
    "single": BacktestRunRequest,
    "csv": CSVBacktestRequest,
    "rolling": RollingScreeningRequest,
}


# ---------------------------------------------------------------------------
# Jobs
# ---------------------------------------------------------------------------


def _portfolio_daily(owner: str) -> list[dict[str, Any]]:
    """The account's portfolio values from app.db, for a ``portfolio`` benchmark."""
    conn = connect_read(_resolve_db3())
    try:
        rows = conn.execute(
            "SELECT date, total_value, net_inflow FROM Portfolio_Daily WHERE owner_user_id = ? ORDER BY date",
            (owner,),
        ).fetchall()
    except sqlite3.Error:
        return []
    finally:
        conn.close()
    return [{"date": row[0], "total_value": row[1], "net_inflow": row[2]} for row in rows]


def _company_tags(owner: str) -> list[list[str]]:
    """The account's company tags as ``[edinet_code, tag]`` pairs, for screens run on a worker."""
    if not owner:
        return []
    try:
        from src.research.runtime import store as research_store

        tags = research_store.list_all_company_tags(owner)
    except Exception as exc:  # noqa: BLE001 - screens still run, without the account's tags
        logger.info("Could not read company tags for %s: %s", owner, exc)
        return []
    return [
        [str(code), str(tag["tag"])]
        for tag in tags
        if (code := tag.get("company_code") or tag.get("edinet_code")) and tag.get("tag")
    ]


def _prepare_job(kind: str, request: BaseModel, http_request: Request | None) -> tuple[dict[str, Any], str]:
    """Validate a request and build the job payload and the job's title."""
    _resolve_db()
    data = request.model_dump()
    base_currency = _validate_base_currency(data.get("base_currency", ""))
    owner = _portfolio_owner(http_request)
    portfolio_daily = None
    if data.get("benchmark_mode") == "portfolio":
        portfolio_daily = _portfolio_daily(owner)
        if not base_currency:
            base_currency = "EUR"
    if kind == "csv":
        if not data["csv_content"].strip():
            raise HTTPException(status_code=400, detail="CSV content is empty.")
        if len(data["csv_content"].encode("utf-8")) > get_settings().max_upload_bytes:
            raise HTTPException(413, "CSV content exceeds the configured size limit")
    payload: dict[str, Any] = {
        "request": data,
        "base_currency": base_currency,
        "risk_free_rate": _resolve_risk_free_rate(data.get("risk_free_rate", 0.0), base_currency),
        "portfolio_owner": owner,
        "portfolio_daily": portfolio_daily,
        "backtest_id": store.new_backtest_id(),
    }
    if kind == "rolling":
        payload["company_tags"] = _company_tags(owner)
        title = job_finalize.describe_rolling(data)[0]
    elif kind == "csv":
        title = job_finalize.describe_csv(data)[0]
    else:
        title = job_finalize.describe_single(data)[0]
    return payload, title


def _enqueue(kind: str, request: BaseModel, http_request: Request | None, service: JobService | None = None) -> JobRecord:
    payload, title = _prepare_job(kind, request, http_request)
    owner = _portfolio_owner(http_request) or None
    return (service or get_job_service()).enqueue(_JOB_KINDS[kind], payload, owner_user_id=owner, title=title)


def _epoch(stamp: str) -> float:
    try:
        parsed = datetime.fromisoformat(stamp)
    except ValueError:
        return 0.0
    return (parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)).timestamp()


def _job_view(record: JobRecord, served: dict[str, bool]) -> dict[str, Any]:
    """A backtest job as the page shows it (plan section 8.3)."""
    progress = dict(record.progress or {})
    status = {
        "open": "queued",
        "in_progress": "running",
        "cancelling": "running",
        "completed": "complete",
        "failed": "failed",
        "interrupted": "failed",
        "cancelled": "cancelled",
    }.get(record.status, "running")
    if status == "running" and progress.get("phase") == "saving":
        status = "saving"
    return {
        "job_id": record.job_id,
        "kind": _KIND_OF_JOB.get(record.kind, record.kind),
        "status": status,
        "progress": progress,
        "result_id": (record.result or {}).get("result_id") if record.status == "completed" else None,
        "error": (record.error_message or "The backtest failed.") if status == "failed" else None,
        "created_at": _epoch(record.created_at),
        "title": record.title,
        "waiting_for_worker": record.status == "open" and not served.get(record.lane, False),
    }


def _view_of(service: JobService, record: JobRecord) -> dict[str, Any]:
    return _job_view(record, {record.lane: service.lane_served(record.lane)})


def _own_job(service: JobService, job_id: str, http_request: Request, kind: str | None = None) -> JobRecord:
    try:
        record = service.get(job_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="Backtest job not found.") from None
    wanted = _JOB_KINDS[kind] if kind else None
    owner = _portfolio_owner(http_request) or None
    if record.kind not in _KIND_OF_JOB or record.owner_user_id != owner or (wanted and record.kind != wanted):
        raise HTTPException(status_code=404, detail="Backtest job not found.")
    return record


def _list_jobs(http_request: Request, kind: str | None = None) -> dict[str, Any]:
    service = get_job_service()
    kinds = [_JOB_KINDS[kind]] if kind else list(_JOB_KINDS.values())
    owner = _portfolio_owner(http_request) or None
    cutoff = (datetime.now(timezone.utc) - _JOB_VIEW_WINDOW).timestamp()
    records = [
        record for record in service.list_jobs(kinds=kinds, owner_user_id=owner, limit=100)
        if _epoch(record.created_at) >= cutoff
    ]
    served = {lane: service.lane_served(lane) for lane in ("interactive", "batch")}
    return {"jobs": [_job_view(record, served) for record in records]}


def _cancel_job(job_id: str, http_request: Request, kind: str | None = None) -> dict[str, Any]:
    service = get_job_service()
    record = _own_job(service, job_id, http_request, kind)
    try:
        record = service.cancel(record.job_id)
    except InvalidJobState:
        record = service.get(record.job_id)  # already finished: report it as it is
    return _view_of(service, record)


@router.post("/jobs", status_code=202)
def start_job(body: JobStartBody, http_request: Request) -> dict:
    """Queue a backtest (single, CSV set, or rolling screen) on a worker."""
    try:
        request = _REQUEST_MODELS[body.kind].model_validate(body.request)
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=jsonable_encoder(exc.errors(include_url=False, include_context=False))) from exc
    service = get_job_service()
    return _view_of(service, _enqueue(body.kind, request, http_request, service))


@router.get("/jobs")
def list_jobs(http_request: Request) -> dict:
    """This account's backtest jobs from the last 24 hours, newest first."""
    return _list_jobs(http_request)


@router.get("/jobs/{job_id}")
def get_job(job_id: str, http_request: Request) -> dict:
    service = get_job_service()
    return _view_of(service, _own_job(service, job_id, http_request))


@router.post("/jobs/{job_id}/cancel")
def cancel_job(job_id: str, http_request: Request) -> dict:
    return _cancel_job(job_id, http_request)


@router.post("/rolling-jobs", status_code=202)
def start_rolling_job(request: RollingScreeningRequest, http_request: Request) -> dict:
    """Alias of ``POST /jobs`` for a rolling screening backtest."""
    service = get_job_service()
    return _view_of(service, _enqueue("rolling", request, http_request, service))


@router.get("/rolling-jobs")
def list_rolling_jobs(http_request: Request) -> dict:
    return _list_jobs(http_request, "rolling")


@router.get("/rolling-jobs/{job_id}")
def get_rolling_job(job_id: str, http_request: Request) -> dict:
    service = get_job_service()
    return _view_of(service, _own_job(service, job_id, http_request, "rolling"))


@router.post("/rolling-jobs/{job_id}/cancel")
def cancel_rolling_job(job_id: str, http_request: Request) -> dict:
    return _cancel_job(job_id, http_request, "rolling")


def _read_stored_result(backtest_id: str) -> dict[str, Any]:
    directory = store.backtest_directory(backtest_id, require_existing=True)
    try:
        stored = json.loads((directory / "result.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=500, detail="Backtest result could not be read.") from exc
    if not isinstance(stored, dict):
        raise HTTPException(status_code=500, detail="Backtest result has an invalid format.")
    return stored


async def _run_and_wait(kind: str, request: BaseModel, http_request: Request) -> tuple[str, dict[str, Any]]:
    """Queue a job and wait for it; the saved result's id and content."""
    payload, title = await run_in_threadpool(_prepare_job, kind, request, http_request)
    try:
        record = await get_job_service().run(
            _JOB_KINDS[kind], payload,
            owner_user_id=_portfolio_owner(http_request) or None, title=title, timeout=_SYNC_WAIT_SECONDS,
        )
    except JobFailed as exc:
        # A run that ends on a problem with the request (no data, bad dates, bad CSV)
        # answers 400 as it always has; a worker or system failure stays a 502.
        if _SYSTEM_FAILURE.search(exc.message):
            raise
        raise HTTPException(status_code=400, detail=exc.message) from exc
    backtest_id = str((record.result or {}).get("result_id") or "")
    return backtest_id, await run_in_threadpool(_read_stored_result, backtest_id)


@router.post("/run")
async def run_backtest(
    request: BacktestRunRequest = Body(...),
    http_request: Request = None,
) -> dict:
    """Run a single backtest and wait for it (up to 5 minutes, then 202 ``job_pending``).

    Results are saved to disk; only a summary and the saved id are returned."""
    backtest_id, result = await _run_and_wait("single", request, http_request)
    return {
        "id": backtest_id,
        "status": "complete",
        "path": backtest_id,
        "summary": build_summary(result),
        "chart_data": result.get("chart_data", {}),
        "per_company": result.get("per_company", []),
        "yearly_returns": result.get("yearly_returns", []),
        "dividends_by_year": result.get("dividends_by_year", []),
        "names": result.get("names", {}),
        "warnings": result.get("warnings", []),
    }


@router.post("/run-from-csv")
async def run_from_csv(
    request: CSVBacktestRequest = Body(...),
    http_request: Request = None,
) -> dict:
    """Run a backtest set from an uploaded CSV content string and wait for it."""
    backtest_id, result = await _run_and_wait("csv", request, http_request)
    return {
        "id": backtest_id,
        "status": "complete",
        "path": backtest_id,
        "kind": "csv",
        "aggregate": result.get("aggregate", {}),
        "runs": result.get("runs", []),
        "paths": result.get("paths", {}),
        "period_holdings": result.get("period_holdings", {}),
        "run_holdings": result.get("run_holdings", {}),
        "names": result.get("names", {}),
    }




# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.get("/available-tickers")
def get_available_tickers() -> dict:
    """Return distinct tickers for autocomplete in the portfolio builder.

    Queries ``CompanyInfo.Company_Ticker`` for speed (smaller table).
    """
    resolved = _resolve_db()
    conn = connect_read(resolved)
    try:
        rows = conn.execute(
            "SELECT DISTINCT Company_Ticker FROM CompanyInfo "
            "WHERE Company_Ticker IS NOT NULL AND Company_Ticker != '' "
            "ORDER BY Company_Ticker"
        ).fetchall()
        return {"tickers": [r[0] for r in rows]}
    finally:
        conn.close()


@router.get("/base-currencies")
def get_base_currencies() -> dict:
    """Return available currencies for base currency selection."""
    currencies = get_available_display_currencies()
    return {"currencies": currencies}


@router.get("/list")
def list_backtests(http_request: Request) -> dict:
    """List the saved backtest results visible to the current account."""
    root = store.BACKTEST_ROOT
    if not root.exists():
        return {"backtests": []}
    user = _request_user(http_request)
    described = store.catalog.all()
    items = []
    for d in sorted(root.iterdir(), reverse=True):
        meta = described.get(d.name, {})
        if (
            store.BACKTEST_ID.fullmatch(d.name)
            and d.is_dir()
            and not d.is_symlink()
            and _owner_may_see(meta.get("owner_user_id"), user)
        ):
            zip_file = d / "backtest.zip"
            items.append({
                "id": d.name,
                "path": d.name,
                "created": d.name,
                "has_zip": zip_file.exists(),
                "kind": meta.get("kind"),
                "title": meta.get("title"),
                "subtitle": meta.get("subtitle"),
                "headline": meta.get("headline") or {},
            })
    return {"backtests": items}


# Candidate benchmarks: Japanese indices first, then the global index funds
# the portfolio page offers.
_BENCHMARK_CANDIDATES: tuple[tuple[str, str, str], ...] = (
    ("TPX", "TOPIX", "TOPIX index, price only"),
    ("13060", "TOPIX", "NEXT FUNDS 1306, price only"),
    ("13210", "Nikkei 225", "NEXT FUNDS 1321, price only"),
    ("VWCE", "FTSE All-World", "Vanguard VWCE, accumulating"),
    ("IWDA", "MSCI World", "iShares IWDA, accumulating"),
    ("CSPX", "S&P 500", "iShares CSPX, accumulating"),
    ("SPY", "S&P 500", "SPDR SPY, price only"),
)


@router.get("/benchmarks")
def list_benchmarks() -> dict:
    """Benchmarks with stored prices, and the dates they cover."""
    conn = connect_read(_resolve_db())
    try:
        tickers = [ticker for ticker, _label, _detail in _BENCHMARK_CANDIDATES]
        rows = conn.execute(
            f"SELECT Ticker, MIN(Date), MAX(Date), COUNT(*), MAX(Currency) FROM Stock_Prices "
            f"WHERE Ticker IN ({','.join('?' * len(tickers))}) AND Price > 0 GROUP BY Ticker",
            tickers,
        ).fetchall()
    finally:
        conn.close()
    coverage = {row[0]: row for row in rows}
    return {"benchmarks": [
        {
            "ticker": ticker, "label": label, "detail": detail,
            "available": ticker in coverage and coverage[ticker][3] > 1,
            "first_date": coverage[ticker][1][:10] if ticker in coverage else None,
            "last_date": coverage[ticker][2][:10] if ticker in coverage else None,
            "currency": coverage[ticker][4] if ticker in coverage else None,
        }
        for ticker, label, detail in _BENCHMARK_CANDIDATES
    ]}


@router.get("/download/{backtest_id}")
def download_backtest(backtest_id: str, http_request: Request):
    """Serve a previously saved backtest ZIP file."""
    zip_path = _owned_backtest_directory(backtest_id, http_request) / "backtest.zip"
    if not zip_path.is_file() or zip_path.is_symlink():
        raise HTTPException(status_code=404, detail="Backtest not found.")
    return FileResponse(
        str(zip_path),
        media_type="application/zip",
        filename=f"backtest_{backtest_id}.zip",
    )


@router.get("/result/{backtest_id}")
def get_backtest_result(backtest_id: str, http_request: Request) -> dict[str, Any]:
    """Load the lightweight result payload used by the Backtest workspace."""
    result_path = _owned_backtest_directory(backtest_id, http_request) / "result.json"
    if not result_path.is_file() or result_path.is_symlink():
        raise HTTPException(status_code=404, detail="Backtest result not found.")
    try:
        stored = json.loads(result_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=500, detail="Backtest result could not be read.") from exc
    if isinstance(stored, dict) and "metrics" in stored:
        return {
            "id": backtest_id,
            "status": "complete",
            "summary": build_summary(stored),
            "chart_data": stored.get("chart_data", {}),
            "per_company": stored.get("per_company", []),
            "yearly_returns": stored.get("yearly_returns", []),
            "dividends_by_year": stored.get("dividends_by_year", []),
            "names": stored.get("names", {}),
        }
    if isinstance(stored, dict):
        return {
            "id": backtest_id,
            "status": "complete",
            "kind": _read_meta(result_path.parent).get("kind") or ("csv" if "config" not in stored else "rolling"),
            "aggregate": stored.get("aggregate", {}),
            "config": stored.get("config", {}),
            "runs": stored.get("runs", []),
            "paths": stored.get("paths", {}),
            "period_holdings": stored.get("period_holdings", {}),
            "run_holdings": stored.get("run_holdings", {}),
            "names": stored.get("names", {}),
        }
    raise HTTPException(status_code=500, detail="Backtest result has an invalid format.")


def _stored_result(backtest_id: str, http_request: Request) -> tuple[Path, dict[str, Any]]:
    directory = _owned_backtest_directory(backtest_id, http_request)
    result_path = directory / "result.json"
    if not result_path.is_file() or result_path.is_symlink():
        raise HTTPException(status_code=404, detail="Backtest result not found.")
    try:
        stored = json.loads(result_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=500, detail="Backtest result could not be read.") from exc
    if not isinstance(stored, dict):
        raise HTTPException(status_code=500, detail="Backtest result has an invalid format.")
    return directory, stored


@router.get("/result/{backtest_id}/detail")
def get_backtest_detail(backtest_id: str, http_request: Request) -> dict[str, Any]:
    """Per-holding drill-down of a single backtest: growth, years, dividends, allocation."""
    _directory, stored = _stored_result(backtest_id, http_request)
    if "metrics" not in stored:
        raise HTTPException(status_code=400, detail="Holding detail is only kept for single backtests; open a run's holdings instead.")
    return {"id": backtest_id, **build_single_detail(stored)}


@router.get("/report/{backtest_id}")
def get_backtest_report(backtest_id: str, http_request: Request, inline: bool = Query(default=False)) -> HTMLResponse:
    """A self-contained HTML report of a saved backtest, to open or share."""
    directory, stored = _stored_result(backtest_id, http_request)
    meta = _read_meta(directory)
    html = render_report(stored, meta=meta, backtest_id=backtest_id)
    disposition = "inline" if inline else "attachment"
    return HTMLResponse(html, headers={"Content-Disposition": f'{disposition}; filename="backtest_{backtest_id}.html"'})

@router.get("/rolling-periods")
def get_rolling_periods(
    cadence: str = Query(default="monthly", description="monthly|quarterly|yearly"),
    start_period: str | None = Query(default=None, description="YYYY-MM"),
    end_period: str | None = Query(default=None, description="YYYY-MM"),
    durations: str = Query(default="1yr,2yr,3yr,5yr,10yr"),
    weighting_modes: str = Query(default="equal", description="comma-separated"),
    financial_statements_table: str = Query(default="FinancialStatements"),
) -> dict:
    """Return available screening periods and estimated backtest count."""
    db = _resolve_db()
    try:
        periods = _bt._discover_screening_periods(
            db, cadence, start_period, end_period,
            financial_statements_table=financial_statements_table,
        )
        dur_list = [d.strip() for d in durations.split(",") if d.strip()]
        wm_list = [w.strip() for w in weighting_modes.split(",") if w.strip()]
        estimated_backtests = len(periods) * len(dur_list) * len(wm_list)
        return {
            "periods": periods,
            "count": len(periods),
            "estimated_backtests": estimated_backtests,
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


def _sse(message: dict[str, Any]) -> str:
    return f"data: {json.dumps(message, default=str)}\n\n"


@router.post("/run-rolling")
async def run_rolling(
    request: RollingScreeningRequest,
    http_request: Request,
) -> StreamingResponse:
    """Run a rolling screening backtest, streaming its progress as SSE.

    Prefer ``/rolling-jobs``: streamed responses do not pass through every
    proxy. The run is a job; closing the page cancels it.
    """
    payload, title = await run_in_threadpool(_prepare_job, "rolling", request, http_request)
    service = get_job_service()
    record = await run_in_threadpool(
        lambda: service.enqueue("backtest_rolling", payload, owner_user_id=_portfolio_owner(http_request) or None, title=title)
    )

    async def event_generator():
        last_progress: dict[str, Any] | None = None
        announced_wait = False
        try:
            current = record
            while not current.is_terminal:
                if await http_request.is_disconnected():
                    return
                current = await service.wait(record.job_id, 0.5)
                if current.status == "open" and not await run_in_threadpool(service.lane_served, current.lane):
                    if not announced_wait:
                        announced_wait = True
                        yield _sse({"type": "progress", "phase": "Waiting for a worker to start the backtest…"})
                elif current.progress and current.progress != last_progress:
                    last_progress = current.progress
                    yield _sse({"type": "progress", **current.progress})
            if current.status == "completed":
                backtest_id = str((current.result or {}).get("result_id") or "")
                try:
                    stored = await run_in_threadpool(_read_stored_result, backtest_id)
                except HTTPException:
                    yield _sse({"type": "error", "message": "Failed to save results"})
                    return
                yield _sse({
                    "type": "result", "id": backtest_id, "path": backtest_id,
                    "aggregate": stored.get("aggregate", {}), "config": stored.get("config", {}),
                })
            elif current.status == "cancelled":
                yield _sse({"type": "error", "message": "cancelled"})
            else:
                yield _sse({"type": "error", "message": current.error_message or "Backtest failed"})
        finally:
            try:
                latest = await run_in_threadpool(service.get, record.job_id)
                if not latest.is_terminal:
                    await run_in_threadpool(service.cancel, record.job_id)
            except Exception:  # noqa: BLE001 - the stream is ending either way
                logger.debug("Could not cancel rolling backtest job %s", record.job_id, exc_info=True)

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@router.post("/export-rolling-xlsx")
async def export_rolling_xlsx(request: RollingExportRequest) -> StreamingResponse:
    """Export rolling backtest results as a multi-sheet XLSX workbook.

    Sheet 1: "Summary" — aggregate statistics, by-weighting, benchmark.
    Subsequent sheets: one per screening period, containing per-backtest
    data for each duration × weighting combination.
    """
    import io
    from datetime import datetime

    import openpyxl
    from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
    from openpyxl.utils import get_column_letter

    request_bytes = json.dumps(request.rolling_result, default=str).encode("utf-8")
    _enforce_export_size(request_bytes)
    wb = openpyxl.Workbook()

    # ── Styles ────────────────────────────────────────────────────
    header_font = Font(bold=True, size=11)
    header_fill = PatternFill(start_color="D9E2F2", end_color="D9E2F2", fill_type="solid")
    pct_fmt = '0.00%'
    num_fmt = '0.000'
    thin_border = Border(
        left=Side(style='thin'), right=Side(style='thin'),
        top=Side(style='thin'), bottom=Side(style='thin'),
    )

    def style_header(ws, row, cols):
        for c in range(1, cols + 1):
            cell = ws.cell(row=row, column=c)
            cell.font = header_font
            cell.fill = header_fill
            cell.border = thin_border
            cell.alignment = Alignment(horizontal='center')

    def auto_width(ws, min_w=10, max_w=40):
        for col_cells in ws.columns:
            max_len = 0
            col_letter = get_column_letter(col_cells[0].column)
            for cell in col_cells:
                if cell.value:
                    max_len = max(max_len, len(str(cell.value)))
            ws.column_dimensions[col_letter].width = max(min_w, min(max_len + 2, max_w))

    result = request.rolling_result
    agg = result.get("aggregate", {})
    config = result.get("config", {})
    results_list = result.get("results", [])

    # ── Summary sheet ─────────────────────────────────────────────
    ws_sum = wb.active
    ws_sum.title = "Summary"

    # Config info
    ws_sum.cell(row=1, column=1, value="Rolling Screening Backtest").font = Font(bold=True, size=14)
    ws_sum.cell(row=2, column=1, value=f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    row = 4
    for key, label in [("cadence", "Cadence"), ("durations", "Durations"),
                       ("weighting_modes", "Weighting"), ("max_companies", "Max Companies"),
                       ("benchmark_ticker", "Benchmark"), ("start_period", "Start"),
                       ("end_period", "End")]:
        ws_sum.cell(row=row, column=1, value=label).font = Font(bold=True)
        val = config.get(key, "")
        ws_sum.cell(row=row, column=2, value=str(val) if not isinstance(val, list) else ", ".join(val))
        row += 1

    row += 1
    ws_sum.cell(row=row, column=1, value=f"Total Backtests: {agg.get('total_runs', 0)}").font = Font(bold=True)
    ws_sum.cell(row=row + 1, column=1, value=f"Successful: {agg.get('successful', 0)}")
    ws_sum.cell(row=row + 2, column=1, value=f"Failed: {agg.get('failed', 0)}")
    ws_sum.cell(row=row + 3, column=1, value=f"Periods: {agg.get('periods', 0)}")

    # Summary by duration/weighting
    row += 5
    by_w = agg.get("by_weighting", {})
    if by_w:
        ws_sum.cell(row=row, column=1, value="Returns by Duration & Weighting").font = Font(bold=True, size=12)
        row += 1
        headers = ["Weighting", "Duration", "Count", "Mean Ann. Return", "Median Ann. Return", "Mean Sharpe"]
        for c, h in enumerate(headers, 1):
            ws_sum.cell(row=row, column=c, value=h)
        style_header(ws_sum, row, len(headers))
        row += 1
        for wm, dur_data in by_w.items():
            for dur, stats in dur_data.items():
                ws_sum.cell(row=row, column=1, value=wm)
                ws_sum.cell(row=row, column=2, value=dur)
                ws_sum.cell(row=row, column=3, value=stats.get("count", 0))
                ws_sum.cell(row=row, column=4, value=stats.get("mean_return", 0))
                ws_sum.cell(row=row, column=5, value=stats.get("median_return", 0))
                ws_sum.cell(row=row, column=6, value=stats.get("mean_sharpe", 0))
                for c in range(1, 7):
                    ws_sum.cell(row=row, column=c).border = thin_border
                ws_sum.cell(row=row, column=4).number_format = pct_fmt
                ws_sum.cell(row=row, column=5).number_format = pct_fmt
                ws_sum.cell(row=row, column=6).number_format = num_fmt
                row += 1

        # Benchmark comparison
        bc = agg.get("benchmark_comparison")
        if bc:
            row += 1
            ws_sum.cell(row=row, column=1, value="Benchmark Comparison").font = Font(bold=True, size=12)
            row += 1
            ws_sum.cell(row=row, column=1, value=f"Outperformed: {bc.get('outperformed', 0)}")
            ws_sum.cell(row=row + 1, column=1, value=f"Underperformed: {bc.get('underperformed', 0)}")
            ws_sum.cell(row=row + 2, column=1, value=f"Win Rate: {bc.get('win_rate', 0) * 100:.1f}%")

        auto_width(ws_sum)

    # ── Per-period sheets ──────────────────────────────────────────
    for period_result in results_list:
        period = period_result.get("period", "unknown")
        # Sheet names max 31 chars
        sheet_name = period[:10].replace("-", "")[:31]
        if sheet_name in wb.sheetnames:
            sheet_name = sheet_name[:28] + f"_{len(wb.sheetnames)}"
        ws = wb.create_sheet(title=sheet_name)

        ws.cell(row=1, column=1, value=f"Period: {period}").font = Font(bold=True, size=13)
        ws.cell(row=2, column=1,
                value=f"Tickers ({period_result.get('ticker_count', 0)}): " +
                      ", ".join(period_result.get("tickers", [])))

        backtests = period_result.get("backtests", {})
        for wm, dur_data in backtests.items():
            row = 4
            ws.cell(row=row, column=1, value=f"Weighting: {wm}").font = Font(bold=True, size=12)
            row += 1

            headers = ["Duration", "Total Return", "Ann. Return", "Sharpe",
                       "Max Drawdown", "Benchmark Return", "Excess Return",
                       "Volatility", "Info Ratio", "Start", "End"]
            for c, h in enumerate(headers, 1):
                ws.cell(row=row, column=c, value=h)
            style_header(ws, row, len(headers))
            row += 1

            for dur, bt in dur_data.items():
                m = bt.get("metrics") if isinstance(bt, dict) else None
                if m is None:
                    ws.cell(row=row, column=1, value=dur)
                    ws.cell(row=row, column=2, value="Failed")
                    row += 1
                    continue

                ws.cell(row=row, column=1, value=dur)
                ws.cell(row=row, column=2, value=m.get("total_return", 0))
                ws.cell(row=row, column=3, value=m.get("annualized_return", 0))
                ws.cell(row=row, column=4, value=m.get("sharpe_ratio", 0))
                ws.cell(row=row, column=5, value=m.get("max_drawdown", 0))
                bench = m.get("benchmark_total_return")
                ws.cell(row=row, column=6, value=bench if bench is not None else "N/A")
                if bench is not None and m.get("total_return") is not None:
                    ws.cell(row=row, column=7, value=m["total_return"] - bench)
                else:
                    ws.cell(row=row, column=7, value="N/A")
                ws.cell(row=row, column=8, value=m.get("volatility", 0))
                info = m.get("information_ratio")
                ws.cell(row=row, column=9, value=info if info is not None else "N/A")
                ws.cell(row=row, column=10, value=m.get("start_date", ""))
                ws.cell(row=row, column=11, value=m.get("end_date", ""))

                for c in range(1, 12):
                    ws.cell(row=row, column=c).border = thin_border
                ws.cell(row=row, column=2).number_format = pct_fmt
                ws.cell(row=row, column=3).number_format = pct_fmt
                ws.cell(row=row, column=4).number_format = num_fmt
                ws.cell(row=row, column=5).number_format = pct_fmt
                if bench is not None:
                    ws.cell(row=row, column=6).number_format = pct_fmt
                    ws.cell(row=row, column=7).number_format = pct_fmt
                ws.cell(row=row, column=8).number_format = pct_fmt
                if info is not None:
                    ws.cell(row=row, column=9).number_format = num_fmt
                row += 1

            # Per-company breakdown if available
            for dur, bt in dur_data.items():
                per_co = bt.get("per_company") if isinstance(bt, dict) else None
                if per_co and len(per_co) > 0:
                    row += 1
                    ws.cell(row=row, column=1,
                            value=f"{dur} — Per-Company Breakdown").font = Font(bold=True)
                    row += 1
                    co_headers = ["Ticker", "Total Return", "Price Return",
                                  "Dividend Return", "Weight",
                                  "Wtd Price", "Wtd Div", "Wtd Total",
                                  "Start Price", "End Price"]
                    # Add cash columns if available
                    has_capital = any(
                        co.get("capital_invested") is not None for co in per_co
                    )
                    if has_capital:
                        co_headers += ["Capital", "Shares", "Divs Received", "Market Value"]
                    for c, h in enumerate(co_headers, 1):
                        ws.cell(row=row, column=c, value=h)
                    style_header(ws, row, len(co_headers))
                    row += 1
                    co_keys = [
                        "total_return", "price_return", "dividend_return", "weight",
                        "weighted_price", "weighted_dividend", "weighted_total",
                        "start_price", "end_price",
                    ]
                    if has_capital:
                        co_keys += [
                            "capital_invested", "shares_purchased",
                            "dividends_received", "market_value",
                        ]
                    for co in per_co:
                        values = [co.get("Ticker", "")] + [co.get(key, 0) for key in co_keys]
                        for col, value in enumerate(values, start=1):
                            cell = ws.cell(row=row, column=col, value=value)
                            cell.border = thin_border
                        for c in [2, 3, 4, 5, 6, 7, 8]:
                            ws.cell(row=row, column=c).number_format = pct_fmt
                        row += 1
                    break  # Only show per-company for first duration

        auto_width(ws)

    # ── Write to bytes ───────────────────────────────────────────
    output = io.BytesIO()
    wb.save(output)
    content = _enforce_export_size(output.getvalue())

    filename = f"rolling_backtest_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
    return StreamingResponse(
        io.BytesIO(content),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.post("/export-rolling-zip")
async def export_rolling_zip(request: RollingExportRequest) -> StreamingResponse:
    """Export rolling backtest results as a ZIP archive.

    The ZIP contains:

    - ``summary.txt`` / ``summary.csv`` — aggregate statistics
    - ``heatmap.csv`` — returns matrix (periods × durations × weightings)
    - ``backtests/{period}_{weighting}_{duration}/`` — per‑backtest files:
      ``report.txt``, chart PNGs, ``per_company.csv``,
      ``yearly_returns.csv``, ``dividends.csv``
    """
    import io as _io
    from datetime import datetime as _datetime

    request_bytes = json.dumps(request.rolling_result, default=str).encode("utf-8")
    _enforce_export_size(request_bytes)
    zip_bytes = await asyncio.to_thread(
        build_rolling_zip, request.rolling_result,
    )
    _enforce_export_size(zip_bytes)

    filename = f"rolling_backtest_{_datetime.now().strftime('%Y%m%d_%H%M')}.zip"
    return StreamingResponse(
        _io.BytesIO(zip_bytes),
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
