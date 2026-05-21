"""Portfolio Management API routes.

All routes are mounted under ``/api/portfolio``.  Discovered automatically
by ``src/web_app/api/__init__.py`` via the ``router`` export.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, UploadFile, File, Query, HTTPException

from src.portfolio.schema import (
    UploadResponse, TransactionEntry, HoldingItem, PerformanceResponse,
    DateRangeResponse, ActivitySummaryResponse, RebuildResponse,
)
from src.portfolio.ibkr_parser import parse_ibkr_xml, normalize_entries
from src.portfolio.transactions import (
    insert_entries, get_transactions, get_unique_symbols, get_date_range,
    get_activity_summary, delete_by_source,
)
from src.portfolio.price_fetcher import ensure_prices_for_tickers, _build_currency_map
from src.portfolio.portfolio_state import (
    build_portfolio_state, get_daily_values, get_current_holdings,
    get_holdings_at_date, get_holding_performance, get_closed_positions,
)
from src.portfolio.performance import calculate_metrics, get_risk_free_rate
from src.orchestrator.common.db_config import get_db2, get_db3

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/portfolio", tags=["portfolio"])


# ---------------------------------------------------------------------------
# Upload
# ---------------------------------------------------------------------------

@router.post("/upload", response_model=UploadResponse)
async def upload_xml(file: UploadFile = File(...)):
    """Upload an IBKR FlexQuery XML file.  Parses, fetches missing ticker
    prices, and inserts all entries into the Transactions table."""
    if not file.filename or not file.filename.lower().endswith(".xml"):
        raise HTTPException(400, "Only .xml files are accepted")

    raw = await file.read()
    content = raw.decode("utf-8") if isinstance(raw, bytes) else raw

    # Parse
    try:
        xml_data = parse_ibkr_xml(content)
    except Exception as e:
        logger.error("Failed to parse XML: %s", e)
        raise HTTPException(400, f"XML parse error: {e}")

    entries = normalize_entries(xml_data)
    if not entries:
        return UploadResponse(
            source_file=file.filename,
            total_entries=0, inserted=0, skipped=0,
        )

    # Fetch missing prices
    ticker_map = _build_currency_map(entries)
    db2_path = get_db2()
    db3_path = get_db3()
    source = file.filename

    price_result = await asyncio.to_thread(
        ensure_prices_for_tickers, db2_path, ticker_map
    )

    # Insert
    result = await asyncio.to_thread(
        insert_entries, db3_path, entries, source
    )

    return UploadResponse(
        source_file=file.filename,
        total_entries=len(entries),
        inserted=result["inserted"],
        skipped=result["skipped"],
        by_activity=result["by_activity"],
        new_tickers_fetched=price_result["fetched"],
        ticker_fetch_failures=price_result["failed"],
    )


# ---------------------------------------------------------------------------
# Transactions
# ---------------------------------------------------------------------------

@router.get("/transactions")
async def transactions_list(
    symbol: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    activity_type: Optional[str] = Query(None),
    limit: int = Query(1000, ge=1, le=10000),
    offset: int = Query(0, ge=0),
):
    """List transactions with optional filters."""
    return await asyncio.to_thread(
        get_transactions, get_db3(),
        symbol=symbol, start_date=start_date,
        end_date=end_date, activity_type=activity_type,
        limit=limit, offset=offset,
    )


@router.get("/symbols")
async def list_symbols():
    """Return distinct symbols with asset categories."""
    return await asyncio.to_thread(get_unique_symbols, get_db3())


@router.get("/date-range", response_model=DateRangeResponse)
async def transactions_date_range():
    """Return min and max trade_date."""
    result = await asyncio.to_thread(get_date_range, get_db3())
    return DateRangeResponse(**result)


@router.delete("/transactions/{source_file}")
async def delete_transactions(source_file: str):
    """Delete all transactions from a given source file."""
    deleted = await asyncio.to_thread(delete_by_source, get_db3(), source_file)
    return {"deleted": deleted}


@router.get("/activity-summary", response_model=ActivitySummaryResponse)
async def activity_summary():
    """Return counts by activity_type."""
    result = await asyncio.to_thread(get_activity_summary, get_db3())
    return ActivitySummaryResponse(by_activity=result)


# ---------------------------------------------------------------------------
# Portfolio State
# ---------------------------------------------------------------------------

@router.get("/holdings")
async def holdings():
    """Current portfolio holdings with market values."""
    return await asyncio.to_thread(get_current_holdings, get_db3())


@router.get("/holdings/closed")
async def holdings_closed():
    """Positions that were fully closed and are no longer held."""
    return await asyncio.to_thread(get_closed_positions, get_db3())


@router.get("/holdings/history")
async def holdings_history(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
):
    """Daily portfolio value series."""
    return await asyncio.to_thread(
        get_daily_values, get_db3(), start_date, end_date
    )


@router.get("/holdings/at/{date}")
async def holdings_at_date(date: str):
    """Holdings snapshot at a specific date."""
    return await asyncio.to_thread(get_holdings_at_date, get_db3(), date)


@router.get("/holdings/{symbol}/performance")
async def holding_performance(symbol: str):
    """Performance metrics for a single holding."""
    result = await asyncio.to_thread(get_holding_performance, symbol, get_db3())
    if result is None:
        raise HTTPException(404, f"No data found for symbol {symbol}")
    return result


@router.get("/holdings/{symbol}/history")
async def holding_history(symbol: str):
    """Daily market value and price history for a single holding."""
    db3_path = get_db3()
    import sqlite3
    conn = sqlite3.connect(db3_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT date, market_price, market_value, market_value_native "
        "FROM Holdings_History WHERE symbol = ? ORDER BY date",
        (symbol,),
    ).fetchall()
    conn.close()
    if not rows:
        raise HTTPException(404, f"No history found for symbol {symbol}")
    return [dict(r) for r in rows]


@router.get("/holdings/performance")
async def holdings_with_performance():
    """Current holdings enriched with per-symbol performance metrics."""
    holdings = await asyncio.to_thread(get_current_holdings, get_db3())
    result = []
    for h in holdings:
        sym = h["symbol"]
        # Skip cash rows
        if h["asset_category"] == "CASH" or sym.startswith("CASH"):
            result.append({**h, "performance": None})
            continue
        try:
            perf = await asyncio.to_thread(get_holding_performance, sym, get_db3())
        except Exception:
            perf = None
        result.append({**h, "performance": perf})
    return result


@router.post("/rebuild", response_model=RebuildResponse)
async def rebuild_state(
    base_currency: str = Query("EUR"),
):
    """Rebuild portfolio state from scratch."""
    result = await asyncio.to_thread(
        build_portfolio_state, get_db3(), get_db2(),
        base_currency=base_currency,
    )
    return RebuildResponse(
        message="Portfolio state rebuilt successfully",
        daily_rows=result["daily_rows"],
        holdings_count=result["holdings_count"],
    )


# ---------------------------------------------------------------------------
# Performance
# ---------------------------------------------------------------------------

@router.get("/performance", response_model=PerformanceResponse)
async def portfolio_performance(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    benchmark_ticker: Optional[str] = Query(None),
    risk_free_rate: Optional[float] = Query(None),
    base_currency: str = Query("EUR"),
):
    """Compute portfolio performance metrics."""
    result = await asyncio.to_thread(
        calculate_metrics,
        get_db3(), get_db2(), start_date, end_date,
        risk_free_rate, benchmark_ticker, base_currency,
    )
    return PerformanceResponse(**result)


@router.get("/risk-free-rate")
async def detect_risk_free_rate(base_currency: str = Query("EUR")):
    """Get auto-detected risk-free rate for a currency."""
    return {
        "base_currency": base_currency,
        "risk_free_rate": get_risk_free_rate(get_db2(), base_currency),
    }


# ---------------------------------------------------------------------------
# Backtesting
# ---------------------------------------------------------------------------

@router.post("/backtest/compare")
async def backtest_compare(request: dict):
    """Compare portfolio performance against a model portfolio.

    Expects JSON body with: ``model_ticker``, optional ``start_date``,
    ``end_date``, ``risk_free_rate``, ``base_currency``.
    """
    model_ticker = request.get("model_ticker")
    if not model_ticker:
        raise HTTPException(400, "model_ticker is required")

    result = await asyncio.to_thread(
        calculate_metrics,
        get_db3(), get_db2(),
        start_date=request.get("start_date"),
        end_date=request.get("end_date"),
        risk_free_rate=request.get("risk_free_rate"),
        benchmark_ticker=model_ticker,
        base_currency=request.get("base_currency", "EUR"),
    )
    return result


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@router.get("/db-path")
async def db_path():
    """Return the db3 path (for info/debugging)."""
    return {"db3": get_db3(), "db2": get_db2()}
