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


@router.get("/holdings/history/constituents")
async def holdings_constituents():
    """Daily market value per holding (for stacked breakdown chart).

    Returns a dict with ``dates`` (ordered list) and ``series`` (symbol → value
    array).  Non-stock items (cash, options) are excluded.

    For positions that were fully sold, values after the last known holding
    date are set to 0 rather than null so the stacked chart fill drops to
    zero instead of bridging across the gap.
    """
    import sqlite3
    from collections import defaultdict
    db3_path = get_db3()
    conn = sqlite3.connect(db3_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """SELECT date, symbol, market_value
           FROM Holdings_History
           WHERE is_option = 0
             AND symbol NOT LIKE 'CASH%'
             AND market_value IS NOT NULL
           ORDER BY date, symbol"""
    ).fetchall()

    # Also get currently-held symbols so we know which are closed
    cur_held = set()
    ch_rows = conn.execute(
        "SELECT symbol FROM Portfolio_Holdings WHERE quantity > 0"
        " AND asset_category != 'CASH'"
    ).fetchall()
    cur_held = {r["symbol"] for r in ch_rows}
    conn.close()

    if not rows:
        return {"dates": [], "series": {}}
    # Group: find all unique dates, then build per-symbol arrays
    date_list: list[str] = []
    seen_dates: set[str] = set()
    series_by_symbol: dict[str, dict[str, float]] = defaultdict(dict)
    for r in rows:
        d = r["date"]
        sym = r["symbol"]
        val = r["market_value"] or 0
        if d not in seen_dates:
            seen_dates.add(d)
            date_list.append(d)
        series_by_symbol[sym][d] = val
    # Build aligned arrays
    result_series: dict[str, list[float | None]] = {}
    n_dates = len(date_list)
    for sym, day_map in series_by_symbol.items():
        vals = [day_map.get(d) for d in date_list]
        # Find last index with a non-null value
        last_idx = -1
        for i in range(n_dates - 1, -1, -1):
            if vals[i] is not None:
                last_idx = i
                break
        if last_idx >= 0 and last_idx < n_dates - 1:
            # Position was sold — fill trailing nulls with 0
            for i in range(last_idx + 1, n_dates):
                vals[i] = 0.0
        # If the position is still held (in cur_held) but has no entry
        # for the latest dates (pricing gap), forward-fill the last value
        elif last_idx >= 0 and sym in cur_held:
            last_val = vals[last_idx]
            for i in range(last_idx + 1, n_dates):
                vals[i] = last_val
        result_series[sym] = vals
    return {"dates": date_list, "series": result_series}


@router.get("/dividends/history")
async def dividends_history(
    period: str = Query("monthly", description="Aggregation: monthly, quarterly, or yearly"),
):
    """Dividend income aggregated by period.

    Reads ``dividend_income`` from ``Portfolio_Daily`` and buckets by
    month, quarter, or year.  Returns ``[{period, gross, tax, net}, ...]``.
    """
    import sqlite3
    db3_path = get_db3()
    conn = sqlite3.connect(db3_path)
    conn.row_factory = sqlite3.Row

    # Read daily dividend income
    rows = conn.execute(
        "SELECT date, dividend_income FROM Portfolio_Daily ORDER BY date"
    ).fetchall()
    conn.close()

    if not rows:
        return []

    # Also read individual dividend/tax transactions for gross/tax split
    conn2 = sqlite3.connect(db3_path)
    conn2.row_factory = sqlite3.Row
    txn_rows = conn2.execute(
        "SELECT trade_date, activity_type, amount, fx_rate_to_base FROM Transactions "
        "WHERE activity_type IN ('DIVIDEND', 'PIL_DIVIDEND', 'WITHHOLDING_TAX') "
        "ORDER BY trade_date"
    ).fetchall()
    conn2.close()

    # Build daily gross/tax maps
    daily_gross: dict[str, float] = {}
    daily_tax: dict[str, float] = {}
    for tr in txn_rows:
        d = tr["trade_date"]
        fx = tr["fx_rate_to_base"] or 1.0
        amt = (tr["amount"] or 0) * fx
        if tr["activity_type"] in ("DIVIDEND", "PIL_DIVIDEND"):
            daily_gross[d] = daily_gross.get(d, 0) + abs(amt)
        elif tr["activity_type"] == "WITHHOLDING_TAX":
            daily_tax[d] = daily_tax.get(d, 0) + abs(amt)

    # Aggregate
    from collections import defaultdict
    buckets: dict[str, dict[str, float]] = defaultdict(lambda: {"gross": 0, "tax": 0, "net": 0})

    for r in rows:
        d = r["date"]
        if not d:
            continue
        y, m, _day = d[:4], d[5:7], d[8:10]
        if period == "yearly":
            key = y
        elif period == "quarterly":
            q = str((int(m) - 1) // 3 + 1)
            key = f"{y}-Q{q}"
        else:  # monthly
            key = f"{y}-{m}"

        gross = daily_gross.get(d, 0)
        tax = daily_tax.get(d, 0)
        net = r["dividend_income"] or 0
        buckets[key]["gross"] += gross
        buckets[key]["tax"] += tax
        buckets[key]["net"] += net

    result = []
    for k in sorted(buckets.keys()):
        b = buckets[k]
        result.append({
            "period": k,
            "gross": round(b["gross"], 2),
            "tax": round(b["tax"], 2),
            "net": round(b["net"], 2),
        })
    return result


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
