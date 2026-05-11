"""Backtesting API routes.

All routes are mounted under ``/api/backtesting``.
Heavy computation is offloaded via ``asyncio.to_thread`` with a
120-second timeout guard.
"""

from __future__ import annotations

import asyncio
import json
import logging
import queue
import sqlite3
import threading
from pathlib import Path
from typing import Literal

from fastapi import APIRouter, Body, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from src import backtesting as _bt
from src.orchestrator.common.db_config import get_db2
from src.orchestrator.common.backtesting import _sql_ident

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/backtesting", tags=["backtesting"])

# ---------------------------------------------------------------------------
# Concurrency guard
# ---------------------------------------------------------------------------
_MAX_CONCURRENT = 2
_semaphore = asyncio.Semaphore(_MAX_CONCURRENT)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _resolve_db(db_path: str = "") -> str:
    """Resolve a database path, falling back to the configured DB2."""
    if db_path:
        p = Path(db_path)
        if not p.exists():
            raise HTTPException(
                status_code=400,
                detail=f"Database not found: {db_path}",
            )
        return str(p.resolve())

    resolved = get_db2()
    if not resolved:
        raise HTTPException(status_code=503, detail="No database configured.")
    p = Path(resolved)
    if not p.exists():
        raise HTTPException(status_code=503, detail="Database not found.")
    return str(p.resolve())


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class AllocationSpec(BaseModel):
    mode: Literal["weight", "shares", "value"] = "weight"
    value: float


class BacktestRunRequest(BaseModel):
    db_path: str = ""
    portfolio: dict[str, AllocationSpec]
    start_date: str = Field(..., description="YYYY-MM-DD")
    end_date: str = Field(..., description="YYYY-MM-DD")
    benchmark_ticker: str = ""
    initial_capital: float = 0.0
    risk_free_rate: float = 0.0


class CSVBacktestRequest(BaseModel):
    db_path: str = ""
    csv_content: str = Field(..., description="Raw CSV string")
    benchmark_ticker: str = ""
    durations: list[str] = ["1yr", "2yr", "3yr", "5yr", "10yr"]
    initial_capital: float = 0.0
    risk_free_rate: float = 0.0


class RollingScreeningRequest(BaseModel):
    db_path: str = ""
    criteria: list[dict]
    columns: list[str]
    computed_columns: list[dict] = []
    cadence: str = "monthly"
    durations: list[str] = ["1yr", "2yr", "3yr", "5yr", "10yr"]
    weighting_modes: list[str] = ["equal"]
    max_companies: int = 25
    ranking_algorithm: str = "none"
    ranking_rules: list[dict] = []
    benchmark_ticker: str = ""
    initial_capital: float = 0.0
    risk_free_rate: float = 0.0
    start_period: str | None = None
    end_period: str | None = None


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.get("/db-path")
def get_db_path() -> dict:
    """Return the default database path."""
    try:
        return {"db_path": _resolve_db()}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/available-tickers")
def get_available_tickers(
    db_path: str = Query(default="", description="Database path"),
) -> dict:
    """Return distinct tickers for autocomplete in the portfolio builder.

    Queries ``CompanyInfo.Company_Ticker`` for speed (smaller table).
    """
    try:
        resolved = _resolve_db(db_path)
        conn = sqlite3.connect(resolved)
        try:
            rows = conn.execute(
                "SELECT DISTINCT Company_Ticker FROM CompanyInfo "
                "WHERE Company_Ticker IS NOT NULL AND Company_Ticker != '' "
                "ORDER BY Company_Ticker"
            ).fetchall()
            return {"tickers": [r[0] for r in rows]}
        finally:
            conn.close()
    except HTTPException:
        raise
    except Exception as e:
        logger.error("available-tickers failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/run")
async def run_backtest(request: BacktestRunRequest = Body(...)) -> dict:
    """Run a single backtest with a manual portfolio."""
    db = _resolve_db(request.db_path)

    # Convert AllocationSpec → plain dicts for internal use
    portfolio: dict[str, dict] = {
        tk: {"mode": spec.mode, "value": spec.value}
        for tk, spec in request.portfolio.items()
    }

    async with _semaphore:
        try:
            result = await asyncio.wait_for(
                asyncio.to_thread(
                    _bt.run_backtest_web,
                    db_path=db,
                    portfolio=portfolio,
                    start_date=request.start_date,
                    end_date=request.end_date,
                    benchmark_ticker=request.benchmark_ticker,
                    initial_capital=request.initial_capital,
                    risk_free_rate=request.risk_free_rate,
                ),
                timeout=120,
            )
            return result
        except asyncio.TimeoutError:
            raise HTTPException(
                status_code=504,
                detail="Backtest timed out after 120 seconds.",
            )
        except HTTPException:
            raise
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            logger.error("Backtest run failed: %s", e)
            raise HTTPException(status_code=500, detail=str(e))


@router.post("/run-from-csv")
async def run_from_csv(request: CSVBacktestRequest = Body(...)) -> dict:
    """Run a backtest set from an uploaded CSV content string."""
    db = _resolve_db(request.db_path)

    if not request.csv_content.strip():
        raise HTTPException(status_code=400, detail="CSV content is empty.")

    async with _semaphore:
        try:
            result = await asyncio.wait_for(
                asyncio.to_thread(
                    _bt.run_backtest_set_web,
                    db_path=db,
                    csv_content=request.csv_content,
                    durations=request.durations,
                    benchmark_ticker=request.benchmark_ticker,
                    initial_capital=request.initial_capital,
                    risk_free_rate=request.risk_free_rate,
                ),
                timeout=120,
            )
            return result
        except asyncio.TimeoutError:
            raise HTTPException(
                status_code=504,
                detail="Backtest set timed out after 120 seconds.",
            )
        except HTTPException:
            raise
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            logger.error("CSV backtest set failed: %s", e)
            raise HTTPException(status_code=500, detail=str(e))


@router.get("/rolling-periods")
def get_rolling_periods(
    db_path: str = Query(default="", description="Database path"),
    cadence: str = Query(default="monthly", description="monthly|quarterly|yearly"),
    start_period: str | None = Query(default=None, description="YYYY-MM"),
    end_period: str | None = Query(default=None, description="YYYY-MM"),
    durations: str = Query(default="1yr,2yr,3yr,5yr,10yr"),
    weighting_modes: str = Query(default="equal", description="comma-separated"),
) -> dict:
    """Return available screening periods and estimated backtest count."""
    db = _resolve_db(db_path)
    try:
        periods = _bt._discover_screening_periods(
            db, cadence, start_period, end_period,
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
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error("rolling-periods failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/run-rolling")
async def run_rolling(
    request: RollingScreeningRequest,
    http_request: Request,
) -> StreamingResponse:
    """Run a rolling screening backtest with SSE progress streaming."""
    db = _resolve_db(request.db_path)
    progress_queue: queue.Queue = queue.Queue()
    cancel_event = threading.Event()

    async def event_generator():
        loop = asyncio.get_event_loop()
        task = asyncio.ensure_future(
            asyncio.to_thread(
                _bt.run_screening_backtest_rolling,
                db_path=db,
                criteria=request.criteria,
                columns=request.columns,
                cadence=request.cadence,
                durations=request.durations,
                weighting_modes=request.weighting_modes,
                max_companies=request.max_companies,
                ranking_algorithm=request.ranking_algorithm,
                ranking_rules=request.ranking_rules,
                computed_columns=request.computed_columns,
                benchmark_ticker=request.benchmark_ticker,
                initial_capital=request.initial_capital,
                risk_free_rate=request.risk_free_rate,
                start_period=request.start_period,
                end_period=request.end_period,
                progress_queue=progress_queue,
                cancel_event=cancel_event,
            )
        )
        try:
            while True:
                # Check if client disconnected
                if await http_request.is_disconnected():
                    cancel_event.set()
                    break

                try:
                    msg = await loop.run_in_executor(
                        None, progress_queue.get, True, 1.0,
                    )
                except queue.Empty:
                    if task.done():
                        break
                    continue

                yield f"data: {json.dumps(msg)}\n\n"
                if msg.get("type") in ("result", "error"):
                    break
        finally:
            cancel_event.set()
            if not task.done():
                task.cancel()

        # After loop, check for thread exception
        if task.done() and not task.cancelled():
            exc = task.exception()
            if exc is not None:
                error_msg = str(exc)
                if "cancelled" in error_msg.lower():
                    yield f"data: {json.dumps({'type': 'error', 'message': 'cancelled'})}\n\n"
                else:
                    yield f"data: {json.dumps({'type': 'error', 'message': error_msg})}\n\n"

    async with _semaphore:
        return StreamingResponse(
            event_generator(),
            media_type="text/event-stream",
        )
