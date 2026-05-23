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
# Analytical charts
# ---------------------------------------------------------------------------

@router.get("/dividends/yoy")
async def dividends_yoy(
    base_currency: str = Query("EUR"),
):
    """Yearly dividend totals with YoY growth for the dividend growth chart."""
    import sqlite3
    from collections import defaultdict

    db3_path = get_db3()
    conn = sqlite3.connect(db3_path)
    conn.row_factory = sqlite3.Row

    # Aggregate dividend_income from Portfolio_Daily by year
    rows = conn.execute(
        "SELECT date, dividend_income, cash_ccy_json FROM Portfolio_Daily ORDER BY date"
    ).fetchall()
    conn.close()

    # First pass: collect raw yearly dividends and per-currency cash for FX
    import json
    yearly: dict[int, float] = defaultdict(float)
    latest_ccy_json = "{}"
    for r in rows:
        year = int(r["date"][:4])
        yearly[year] += r["dividend_income"] or 0
        if r["cash_ccy_json"]:
            latest_ccy_json = r["cash_ccy_json"]

    years = sorted(yearly.keys())
    dividends = [round(yearly[y], 2) for y in years]

    # YoY growth: (this_year / prev_year - 1) * 100
    yoy_growth: list[float | None] = [None]
    for i in range(1, len(dividends)):
        prev = dividends[i - 1]
        curr = dividends[i]
        if prev > 0:
            yoy_growth.append(round((curr / prev - 1) * 100, 2))
        else:
            yoy_growth.append(None)

    return {
        "years": years,
        "dividends": dividends,
        "yoy_growth": yoy_growth,
        "currency": base_currency,
    }


@router.get("/dividends/yoy/per-company")
async def dividends_per_company_yoy():
    """Dividend per share per company per year with YoY growth."""
    import sqlite3
    from collections import defaultdict

    db3_path = get_db3()
    conn = sqlite3.connect(db3_path)
    conn.row_factory = sqlite3.Row

    # Get dividend transactions: dividend income and withholding tax per company per year
    div_rows = conn.execute("""
        SELECT
            symbol,
            CAST(substr(trade_date, 1, 4) AS INTEGER) AS year,
            activity_type,
            SUM(CASE WHEN activity_type IN ('DIVIDEND','PIL_DIVIDEND') THEN ABS(amount) * COALESCE(fx_rate_to_base, 1) ELSE 0 END) AS gross_eur,
            SUM(CASE WHEN activity_type = 'WITHHOLDING_TAX' THEN ABS(amount) * COALESCE(fx_rate_to_base, 1) ELSE 0 END) AS tax_eur,
            SUM(CASE WHEN activity_type IN ('DIVIDEND','PIL_DIVIDEND') THEN ABS(amount) ELSE 0 END) AS gross_native,
            SUM(CASE WHEN activity_type = 'WITHHOLDING_TAX' THEN ABS(amount) ELSE 0 END) AS tax_native,
            MAX(currency) AS currency
        FROM Transactions
        WHERE activity_type IN ('DIVIDEND', 'PIL_DIVIDEND', 'WITHHOLDING_TAX')
          AND symbol NOT LIKE 'CASH%'
        GROUP BY symbol, year
        ORDER BY symbol, year
    """).fetchall()

    if not div_rows:
        conn.close()
        return {"years": [], "companies": {}}

    # Get avg shares per year from Holdings_History (approximate DPS = total_div / avg_shares)
    hh_rows = conn.execute("""
        SELECT symbol, CAST(substr(date, 1, 4) AS INTEGER) AS year,
               AVG(quantity) AS avg_qty
        FROM Holdings_History
        WHERE quantity > 0 AND symbol NOT LIKE 'CASH%'
        GROUP BY symbol, year
        ORDER BY symbol, year
    """).fetchall()
    conn.close()

    # Build shares map: {symbol: {year: avg_qty}}
    shares_map: dict[str, dict[int, float]] = defaultdict(dict)
    for r in hh_rows:
        shares_map[r["symbol"]][r["year"]] = r["avg_qty"]

    # Collect all years
    all_years: set[int] = set()
    for r in div_rows:
        all_years.add(r["year"])
    for sym, ym in shares_map.items():
        all_years.update(ym.keys())
    years = sorted(all_years)
    if not years:
        return {"years": [], "companies": {}}

    # Build per-company series
    companies: dict[str, dict] = {}
    for r in div_rows:
        sym = r["symbol"]
        y = r["year"]
        shares = shares_map.get(sym, {}).get(y, 0)
        gross = round(r["gross_native"] or 0, 2)
        tax = round(r["tax_native"] or 0, 2)
        net_native = gross - tax
        dps = round(net_native / shares, 4) if shares > 0 else 0

        if sym not in companies:
            companies[sym] = {
                "currency": r["currency"] or "EUR",
                "year_data": {},
            }
        companies[sym]["year_data"][y] = {
            "gross": gross,
            "tax": tax,
            "net": net_native,
            "shares": round(shares, 2),
            "dps": dps,
        }

    # Build aligned arrays per company
    result: dict[str, dict] = {}
    for sym, cdata in companies.items():
        dps_arr: list[float | None] = []
        growth_arr: list[float | None] = []
        prev_dps = None
        for y in years:
            yd = cdata["year_data"].get(y)
            if yd and yd["shares"] > 0:
                dps = yd["dps"]
                dps_arr.append(dps)
                if prev_dps is not None and prev_dps > 0:
                    growth_arr.append(round((dps / prev_dps - 1) * 100, 2))
                else:
                    growth_arr.append(None)
                prev_dps = dps
            else:
                dps_arr.append(None)
                growth_arr.append(None)
        result[sym] = {
            "currency": cdata["currency"],
            "dps": dps_arr,
            "yoy_growth": growth_arr,
        }

    return {"years": years, "companies": result}


@router.get("/returns/by-company")
async def returns_by_company():
    """Yearly total return per company with decomposition into capital
    gain and dividend return.

    Uses price-per-share (market_value / quantity) to isolate true
    market movement from the effect of buying/selling more shares.

    For each company and year:
    - capital_gain  = (end_price - start_price) / start_price * 100
    - dividend_yield = (dividends_per_share / avg_price) * 100
    - total_return   = capital_gain + dividend_yield

    All computed in EUR (market_value / quantity = EUR-per-share).
    Positions with zero quantity at start/end of year are skipped.
    """
    import sqlite3
    from collections import defaultdict

    db3_path = get_db3()
    conn = sqlite3.connect(db3_path)
    conn.row_factory = sqlite3.Row

    # 1. Holdings_History: per-symbol daily market values + quantities
    hh_rows = conn.execute("""
        SELECT symbol, date, market_value, quantity
        FROM Holdings_History
        WHERE symbol NOT LIKE 'CASH%'
          AND is_option = 0
          AND market_value IS NOT NULL
          AND quantity > 0
        ORDER BY symbol, date
    """).fetchall()

    # 2. Dividend income per symbol per year (net EUR)
    div_rows = conn.execute("""
        SELECT
            symbol,
            CAST(substr(trade_date, 1, 4) AS INTEGER) AS year,
            SUM(
                CASE WHEN activity_type IN ('DIVIDEND','PIL_DIVIDEND')
                     THEN ABS(amount) * COALESCE(fx_rate_to_base, 1)
                     ELSE 0 END
            ) AS gross_eur,
            SUM(
                CASE WHEN activity_type = 'WITHHOLDING_TAX'
                     THEN ABS(amount) * COALESCE(fx_rate_to_base, 1)
                     ELSE 0 END
            ) AS tax_eur
        FROM Transactions
        WHERE activity_type IN ('DIVIDEND', 'PIL_DIVIDEND', 'WITHHOLDING_TAX')
          AND symbol NOT LIKE 'CASH%'
        GROUP BY symbol, year
    """).fetchall()

    conn.close()

    if not hh_rows:
        return {"years": [], "companies": {}}

    # --- Group entries by symbol ---
    sym_entries: dict[str, list] = defaultdict(list)
    for r in hh_rows:
        sym_entries[r["symbol"]].append(r)

    # Collect years
    all_years: set[int] = set()
    for r in hh_rows:
        all_years.add(int(r["date"][:4]))
    for r in div_rows:
        all_years.add(r["year"])
    years = sorted(all_years)
    if not years:
        return {"years": [], "companies": {}}

    # Dividend map: {symbol: {year: net_eur}}
    div_map: dict[str, dict[int, float]] = defaultdict(lambda: defaultdict(float))
    for r in div_rows:
        div_map[r["symbol"]][r["year"]] += (r["gross_eur"] or 0) - (r["tax_eur"] or 0)

    # --- For each symbol, compute per-year price-based returns ---
    result: dict[str, dict] = {}

    for sym, entries in sym_entries.items():
        entries.sort(key=lambda x: x["date"])

        total_arr: list[float | None] = []
        capital_arr: list[float | None] = []
        dividend_arr: list[float | None] = []

        for y in years:
            y_str = str(y)
            year_entries = [e for e in entries if e["date"].startswith(y_str)]
            if len(year_entries) < 2:
                total_arr.append(None)
                capital_arr.append(None)
                dividend_arr.append(None)
                continue

            # EUR price per share: market_value / quantity
            first = year_entries[0]
            last = year_entries[-1]

            if first["quantity"] <= 0 or last["quantity"] <= 0:
                total_arr.append(None)
                capital_arr.append(None)
                dividend_arr.append(None)
                continue

            start_price = first["market_value"] / first["quantity"]
            end_price = last["market_value"] / last["quantity"]

            if start_price <= 0:
                total_arr.append(None)
                capital_arr.append(None)
                dividend_arr.append(None)
                continue

            # Capital gain from price change (in EUR)
            cap_pct = round((end_price - start_price) / start_price * 100, 2)

            # Dividend yield: DPS / avg_price * 100
            total_div = div_map[sym].get(y, 0)
            avg_qty = sum(e["quantity"] for e in year_entries) / len(year_entries)
            avg_price = sum(
                e["market_value"] / e["quantity"] for e in year_entries
                if e["quantity"] > 0
            ) / len(year_entries)
            dps = total_div / avg_qty if avg_qty > 0 else 0
            div_pct = round(dps / avg_price * 100, 2) if avg_price > 0 and dps > 0 else 0

            total_pct = round(cap_pct + div_pct, 2)

            total_arr.append(total_pct)
            capital_arr.append(cap_pct)
            dividend_arr.append(div_pct)

        result[sym] = {
            "total_return": total_arr,
            "capital_gain": capital_arr,
            "dividend_return": dividend_arr,
        }

    # Compute total_return across all years for sorting
    for sym in result:
        r = result[sym]
        total_all = sum(v for v in r["total_return"] if v is not None)
        r["_total_all_years"] = round(total_all, 2)

    return {"years": years, "companies": result}


@router.get("/returns/money-weighted")
async def returns_money_weighted():
    """Money-weighted return per company per year using Modified Dietz.

    Accounts for intra-year cash flows (buys add capital, sells remove
    it) so additional purchases don't inflate the return.

    Modified Dietz:
        Return = (end_val - start_val - net_cf) / (start_val + Σ w_i × cf_i)
    where w_i = days remaining in year / total days in year.
    """
    import sqlite3
    from collections import defaultdict
    from datetime import date as D, timedelta

    db3_path = get_db3()
    conn = sqlite3.connect(db3_path)
    conn.row_factory = sqlite3.Row

    # Holdings_History: first/last value per symbol per year
    hh_rows = conn.execute("""
        SELECT symbol, date, market_value
        FROM Holdings_History
        WHERE symbol NOT LIKE 'CASH%'
          AND is_option = 0
          AND market_value IS NOT NULL
        ORDER BY symbol, date
    """).fetchall()

    # Trade transactions: buys and sells per symbol per year (EUR)
    trade_rows = conn.execute("""
        SELECT symbol, trade_date, buy_sell,
               ABS(trade_money) * COALESCE(fx_rate_to_base, 1) AS amount_eur
        FROM Transactions
        WHERE activity_type = 'TRADE'
          AND buy_sell IN ('BUY', 'SELL')
          AND symbol NOT LIKE 'CASH%'
        ORDER BY symbol, trade_date
    """).fetchall()

    conn.close()

    if not hh_rows:
        return {"years": [], "companies": {}}

    # Group holdings by symbol
    sym_entries: dict[str, list] = defaultdict(list)
    for r in hh_rows:
        sym_entries[r["symbol"]].append(r)

    # Group trades by (symbol, year)
    sym_trades: dict[str, dict[int, list]] = defaultdict(lambda: defaultdict(list))
    for r in trade_rows:
        d = r["trade_date"]
        if not d:
            continue
        y = int(d[:4])
        sym_trades[r["symbol"]][y].append(r)

    # Collect years
    all_years: set[int] = set()
    for r in hh_rows:
        all_years.add(int(r["date"][:4]))
    for sym, ym in sym_trades.items():
        all_years.update(ym.keys())
    years = sorted(all_years)
    if not years:
        return {"years": [], "companies": {}}

    result: dict[str, dict] = {}

    for sym, entries in sym_entries.items():
        entries.sort(key=lambda x: x["date"])

        ret_arr: list[float | None] = []

        for y in years:
            y_str = str(y)
            year_entries = [e for e in entries if e["date"].startswith(y_str)]
            if not year_entries:
                ret_arr.append(None)
                continue

            start_val = year_entries[0]["market_value"] or 0
            end_val = year_entries[-1]["market_value"] or 0

            if start_val <= 0:
                ret_arr.append(None)
                continue

            # Collect cash flows from trades in this year
            trades = sym_trades.get(sym, {}).get(y, [])
            net_cf = 0.0
            weighted_cf = 0.0

            year_end = D(y, 12, 31)
            days_in_year = 366 if (y % 4 == 0 and y % 100 != 0) or y % 400 == 0 else 365

            for t in trades:
                td = t["trade_date"]
                tm = t["amount_eur"] or 0
                bs = t["buy_sell"]

                # BUY = money goes INTO the position → positive CF for denominator
                # SELL = money LEAVES the position → negative CF
                if bs == "BUY":
                    cf_val = tm
                else:
                    cf_val = -tm

                net_cf += cf_val

                # Weight: fraction of year remaining after cash flow
                try:
                    cf_date = D.fromisoformat(td)
                except ValueError:
                    continue
                days_remaining = (year_end - cf_date).days
                weight = max(days_remaining, 0) / days_in_year
                weighted_cf += weight * cf_val

            denominator = start_val + weighted_cf
            if denominator <= 0:
                ret_arr.append(None)
                continue

            mw_return = round((end_val - start_val - net_cf) / denominator * 100, 2)
            ret_arr.append(mw_return)

        result[sym] = {
            "return_pct": ret_arr,
        }

    return {"years": years, "companies": result}


@router.get("/returns/contribution")
async def returns_contribution():
    """Contribution of each company to the portfolio's total return per year.

    For each company-year:
    - Absolute contribution (EUR) = end_val - start_val - net_invested + dividends
    - Percentage contribution = absolute / portfolio_start_value × 100

    This shows which companies drove (or dragged) the portfolio each year.
    """
    import sqlite3
    from collections import defaultdict

    db3_path = get_db3()
    conn = sqlite3.connect(db3_path)
    conn.row_factory = sqlite3.Row

    # Per-symbol daily market values
    hh_rows = conn.execute("""
        SELECT symbol, date, market_value
        FROM Holdings_History
        WHERE symbol NOT LIKE 'CASH%'
          AND is_option = 0
          AND market_value IS NOT NULL
        ORDER BY symbol, date
    """).fetchall()

    # Portfolio daily totals for denominator
    pf_rows = conn.execute(
        "SELECT date, total_value FROM Portfolio_Daily ORDER BY date"
    ).fetchall()

    # Trade transactions per symbol per year
    trade_rows = conn.execute("""
        SELECT symbol,
               CAST(substr(trade_date, 1, 4) AS INTEGER) AS year,
               SUM(CASE WHEN buy_sell = 'BUY'
                        THEN ABS(trade_money) * COALESCE(fx_rate_to_base, 1)
                        ELSE 0 END) AS total_bought_eur,
               SUM(CASE WHEN buy_sell = 'SELL'
                        THEN COALESCE(proceeds, ABS(trade_money)) * COALESCE(fx_rate_to_base, 1)
                        ELSE 0 END) AS total_sold_eur
        FROM Transactions
        WHERE activity_type = 'TRADE'
          AND buy_sell IN ('BUY', 'SELL')
          AND symbol NOT LIKE 'CASH%'
        GROUP BY symbol, year
    """).fetchall()

    # Dividend income per symbol per year (net EUR)
    div_rows = conn.execute("""
        SELECT symbol,
               CAST(substr(trade_date, 1, 4) AS INTEGER) AS year,
               SUM(
                   CASE WHEN activity_type IN ('DIVIDEND','PIL_DIVIDEND')
                        THEN ABS(amount) * COALESCE(fx_rate_to_base, 1)
                        ELSE 0 END
               ) AS gross_eur,
               SUM(
                   CASE WHEN activity_type = 'WITHHOLDING_TAX'
                        THEN ABS(amount) * COALESCE(fx_rate_to_base, 1)
                        ELSE 0 END
               ) AS tax_eur
        FROM Transactions
        WHERE activity_type IN ('DIVIDEND', 'PIL_DIVIDEND', 'WITHHOLDING_TAX')
          AND symbol NOT LIKE 'CASH%'
        GROUP BY symbol, year
    """).fetchall()

    conn.close()

    if not hh_rows:
        return {"years": [], "companies": {}, "portfolio_total": []}

    # Group holdings by symbol
    sym_entries: dict[str, list] = defaultdict(list)
    for r in hh_rows:
        sym_entries[r["symbol"]].append(r)

    # Build portfolio total map by year-start
    pf_total_map: dict[int, float] = {}
    for r in pf_rows:
        y = int(r["date"][:4])
        if y not in pf_total_map:
            pf_total_map[y] = r["total_value"] or 0

    # Trade map: {symbol: {year: net_invested}}
    trade_map: dict[str, dict[int, float]] = defaultdict(lambda: defaultdict(float))
    for r in trade_rows:
        net = (r["total_bought_eur"] or 0) - (r["total_sold_eur"] or 0)
        trade_map[r["symbol"]][r["year"]] += net

    # Dividend map: {symbol: {year: net_eur}}
    div_map: dict[str, dict[int, float]] = defaultdict(lambda: defaultdict(float))
    for r in div_rows:
        div_map[r["symbol"]][r["year"]] += (r["gross_eur"] or 0) - (r["tax_eur"] or 0)

    # Collect years
    all_years: set[int] = set()
    for r in hh_rows:
        all_years.add(int(r["date"][:4]))
    for r in trade_rows:
        all_years.add(r["year"])
    for r in div_rows:
        all_years.add(r["year"])
    years = sorted(all_years)
    if not years:
        return {"years": [], "companies": {}, "portfolio_total": []}

    pf_arr = [pf_total_map.get(y) for y in years]

    result: dict[str, dict] = {}

    for sym, entries in sym_entries.items():
        entries.sort(key=lambda x: x["date"])
        eur_arr: list[float | None] = []
        pct_arr: list[float | None] = []

        for y in years:
            y_str = str(y)
            year_entries = [e for e in entries if e["date"].startswith(y_str)]
            if not year_entries:
                eur_arr.append(None)
                pct_arr.append(None)
                continue

            start_val = year_entries[0]["market_value"] or 0
            end_val = year_entries[-1]["market_value"] or 0
            net_invested = trade_map.get(sym, {}).get(y, 0)
            dividends = div_map.get(sym, {}).get(y, 0)

            # Contribution = value change minus net new money plus dividends
            contrib = end_val - start_val - net_invested + dividends
            contrib = round(contrib, 2)

            pf_start = pf_total_map.get(y)
            contrib_pct = round(contrib / pf_start * 100, 2) if pf_start and pf_start > 0 else None

            eur_arr.append(contrib)
            pct_arr.append(contrib_pct)

        result[sym] = {
            "contribution_eur": eur_arr,
            "contribution_pct": pct_arr,
        }

    return {"years": years, "companies": result, "portfolio_start": pf_arr}


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@router.get("/db-path")
async def db_path():
    """Return the db3 path (for info/debugging)."""
    return {"db3": get_db3(), "db2": get_db2()}
