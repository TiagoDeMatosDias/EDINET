"""Security Analysis API routes.

All database interactions go through ``src.security_analysis`` functions.
Database resolution is server-side via DB2_PATH — never exposed to clients.
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Body, HTTPException, Query
from pydantic import BaseModel, Field

from src import security_analysis as _security
from src.orchestrator.common.db_config import get_db2

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/security", tags=["security_analysis"])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _resolve_db() -> str:
    db_path = get_db2()
    if not db_path:
        raise HTTPException(status_code=503, detail="No database configured.")
    path = Path(db_path)
    if not path.exists():
        raise HTTPException(status_code=503, detail=f"Database not found.")
    return str(path.resolve())


def _safe_float(value: Any) -> float | None:
    if value is None: return None
    try: return float(value)
    except (TypeError, ValueError): return None


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class UpdatePriceRequest(BaseModel):
    ticker: str = Field(..., description="Ticker to refresh")


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------

@router.get("/search")
def search_securities(
    q: str = Query(default="", description="Search query"),
    limit: int = Query(default=25),
) -> dict:
    query = q.strip()
    if not query: return {"results": []}
    try:
        return {"results": _security.search_securities(_resolve_db(), query, limit=limit)}
    except HTTPException: raise
    except Exception as e:
        logger.error("Search failed: %s", str(e))
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# Overview — company summary with computed metrics
# ---------------------------------------------------------------------------

@router.get("/overview")
def get_overview(company_code: str = Query(..., description="Company code")) -> dict:
    """Company summary with metrics computed from the actual DB tables."""
    if not company_code.strip():
        raise HTTPException(status_code=400, detail="company_code is required")
    try:
        db = _resolve_db()
        result = _security.get_security_overview(db, company_code.strip())
        result["metrics"] = _compute_metrics(db, company_code.strip(),
                                              result.get("market", {}),
                                              result.get("company", {}))
        return result
    except HTTPException: raise
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error("Overview failed: %s", str(e))
        raise HTTPException(status_code=500, detail=str(e))


def _compute_metrics(db: str, code: str, market: dict, company: dict) -> dict:
    """Compute key metrics using direct queries against the actual tables."""
    ticker = company.get("ticker", "")
    if not ticker:
        return _empty_metrics()

    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    try:
        tables = {r[0].lower(): r[0] for r in
                  conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}

        # Get latest stock price
        price = None
        if "stock_prices" in tables:
            r = conn.execute(
                f'SELECT Price FROM "{tables["stock_prices"]}" '
                f'WHERE Ticker=? ORDER BY Date DESC LIMIT 1', (ticker,)
            ).fetchone()
            if r: price = _safe_float(r["Price"])

        # Find the latest docID with actual data in ShareMetrics
        doc_id = _find_doc_with_data(conn, tables, code,
                                      ["ShareMetrics", "PerShare_Metrics",
                                       "Financial_Ratios", "Financial_Ratios_Rolling"])

        if not doc_id:
            return {"LatestPrice": price}

        # Query each table for this docID
        share = _query_row(conn, tables, "ShareMetrics", doc_id)
        ps_metrics = _query_row(conn, tables, "PerShare_Metrics", doc_id)
        fin_ratios = _query_row(conn, tables, "Financial_Ratios", doc_id)
        fin_rolling = _query_row(conn, tables, "Financial_Ratios_Rolling", doc_id)

        eps = _col(share, "Basic earnings (loss) per share")
        bvps = _col(share, "Net assets per share")
        dps = _col(share, "Dividend paid per share")
        shares = _col(share, "Number of issued shares as of filing date")
        sps = _col(ps_metrics, "Sales Per Share")
        cr = _col(fin_ratios, "Current Ratio")
        roa = _col(fin_rolling, "Return on Assets_Average_3_Year")
        roe = _col(fin_rolling, "Return on Equity_Average_3_Year")

        return {
            "LatestPrice": price,
            "MarketCap": (price * shares) if (price and shares) else None,
            "PERatio": (price / eps) if (price and eps and eps != 0) else None,
            "PriceToBook": (price / bvps) if (price and bvps and bvps != 0) else None,
            "PriceToSales": (price / sps) if (price and sps and sps != 0) else None,
            "DividendsYield": (dps / price) if (dps and price and price != 0) else None,
            "PayoutRatio": (dps / eps) if (dps and eps and eps != 0) else None,
            "ReturnOnAssets": roa,
            "ReturnOnEquity": roe,
            "CurrentRatio": cr,
        }
    finally:
        conn.close()


def _find_doc_with_data(conn, tables, code, table_names):
    """Find the latest docID where at least one of the given tables has data."""
    # Resolve the actual edinet/company code column in FinancialStatements
    fs_info = conn.execute("PRAGMA table_info(FinancialStatements)").fetchall()
    fs_cols = {row[1] for row in fs_info}
    fs_code_col = None
    for candidate in ("Company_Code", "edinetCode", "EdinetCode"):
        if candidate in fs_cols:
            fs_code_col = candidate
            break
    if not fs_code_col:
        fs_code_col = "Company_Code"

    for tname in table_names:
        actual = tables.get(tname.lower())
        if not actual: continue
        rows = conn.execute(
            f'SELECT fs.docID FROM FinancialStatements fs '
            f'JOIN "{actual}" m ON m.docID = fs.docID '
            f'WHERE fs."{fs_code_col}"=? ORDER BY fs.periodEnd DESC LIMIT 5',
            (code,)
        ).fetchall()
        for r in rows:
            mrow = conn.execute(
                f'SELECT * FROM "{actual}" WHERE docID=?', (r["docID"],)
            ).fetchone()
            if mrow:
                nn = sum(1 for k in mrow.keys()
                         if k.lower() != "docid" and mrow[k] is not None)
                if nn > 0: return r["docID"]
    # Fallback: latest docID
    r = conn.execute(
        f"SELECT docID FROM FinancialStatements "
        f"WHERE \"{fs_code_col}\"=? ORDER BY periodEnd DESC LIMIT 1", (code,)
    ).fetchone()
    return r["docID"] if r else None


def _query_row(conn, tables, tname, doc_id):
    actual = tables.get(tname.lower())
    if not actual or not doc_id: return {}
    r = conn.execute(f'SELECT * FROM "{actual}" WHERE docID=?', (doc_id,)).fetchone()
    return dict(r) if r else {}


def _col(row, name):
    """Get a column value from a row dict by exact name."""
    if not row: return None
    return _safe_float(row.get(name))


def _empty_metrics():
    return {k: None for k in (
        "LatestPrice", "MarketCap", "PERatio", "PriceToBook",
        "PriceToSales", "DividendsYield", "PayoutRatio",
        "ReturnOnAssets", "ReturnOnEquity", "CurrentRatio",
    )}


# ---------------------------------------------------------------------------
# Formulas — metric definitions for the frontend tile labels
# ---------------------------------------------------------------------------

@router.get("/formulas")
def get_formulas() -> dict:
    return {"formulas": [
        {"name": "Latest Price",    "id": "LatestPrice",    "format": "price"},
        {"name": "Market Cap",      "id": "MarketCap",      "format": "currency"},
        {"name": "P/E Ratio",       "id": "PERatio",        "format": "ratio"},
        {"name": "P/B Ratio",       "id": "PriceToBook",    "format": "ratio"},
        {"name": "P/S Ratio",       "id": "PriceToSales",   "format": "ratio"},
        {"name": "Dividend Yield",  "id": "DividendsYield", "format": "percent"},
        {"name": "Payout Ratio",    "id": "PayoutRatio",    "format": "percent"},
        {"name": "Return on Assets","id": "ReturnOnAssets", "format": "percent"},
        {"name": "Return on Equity","id": "ReturnOnEquity", "format": "percent"},
        {"name": "Current Ratio",   "id": "CurrentRatio",   "format": "ratio"},
    ]}


# ---------------------------------------------------------------------------
# Price history
# ---------------------------------------------------------------------------

@router.get("/price-history")
def get_price_history(
    ticker: str = Query(...),
    start_date: str | None = Query(default=None),
    end_date: str | None = Query(default=None),
) -> dict:
    if not ticker.strip():
        raise HTTPException(status_code=400, detail="ticker is required")
    try:
        return {"prices": _security.get_security_price_history(
            _resolve_db(), ticker.strip(), start_date, end_date)}
    except HTTPException: raise
    except Exception as e:
        logger.error("Price history failed: %s", str(e))
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# Update price
# ---------------------------------------------------------------------------

@router.post("/update-price")
def update_price(request: UpdatePriceRequest = Body(...)) -> dict:
    if not request.ticker.strip():
        raise HTTPException(status_code=400, detail="ticker is required")
    try:
        return _security.update_security_price(_resolve_db(), request.ticker.strip())
    except HTTPException: raise
    except Exception as e:
        logger.error("Update price failed: %s", str(e))
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# History — all table data as metric-rows
# ---------------------------------------------------------------------------

@router.get("/history")
def get_history(
    company_code: str = Query(...),
    periods: int = Query(default=20),
) -> dict:
    """All historical statement data as metric-rows grouped by table."""
    if not company_code.strip():
        raise HTTPException(status_code=400, detail="company_code is required")
    try:
        db = _resolve_db()

        # Discover all joinable tables
        conn = sqlite3.connect(db)
        try:
            table_map = {}
            for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'"):
                name = r[0]
                if name.lower() in ("companyinfo", "financialstatements",
                                     "stock_prices", "documentlist", "sqlite_sequence"):
                    continue
                cols = [c[1] for c in conn.execute(f'PRAGMA table_info("{name}")')]
                cl = {c.lower() for c in cols}
                if "docid" in cl or ("company_code" in cl or "edinetcode" in cl) and "periodend" in cl:
                    table_map[name] = name
        finally:
            conn.close()

        sources = {name: name for name in table_map.values()}
        statements = _security.get_security_statements(
            db, company_code.strip(),
            periods=max(1, int(periods)),
            statement_sources=sources if sources else None,
        )

        display_names = {
            "IncomeStatement": "Income Statement",
            "BalanceSheet": "Balance Sheet",
            "CashflowStatement": "Cashflow Statement",
            "PerShare": "Share Metrics", "ShareMetrics": "Share Metrics",
            "Valuation": "Financial Ratios", "Financial_Ratios": "Financial Ratios",
            "Financial_Ratios_Rolling": "Financial Ratios (Rolling)",
            "PerShare_Metrics": "Per Share Metrics",
            "PerShare_Metrics_Rolling": "Per Share Metrics (Rolling)",
            "BalanceSheet_Rolling": "Balance Sheet (Rolling)",
            "IncomeStatement_Rolling": "Income Statement (Rolling)",
            "CashflowStatement_Rolling": "Cashflow Statement (Rolling)",
            "ShareMetrics_Rolling": "Share Metrics (Rolling)",
        }

        tables_out = {}
        for key, rows in statements.items():
            if key in ("periods", "records"): continue
            if not rows or not isinstance(rows, list) or not len(rows): continue
            metrics = []
            for row in rows:
                f = row.get("field", row.get("record_field", ""))
                if not f: continue
                metrics.append({
                    "field": f,
                    "display_name": row.get("metric", f),
                    "values": row.get("values", []),
                })
            if metrics:
                tables_out[key] = {
                    "display_name": display_names.get(key, key.replace("_", " ").title()),
                    "metrics": metrics,
                }
        return {"periods": statements.get("periods", []), "tables": tables_out}
    except HTTPException: raise
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error("History failed: %s", str(e))
        raise HTTPException(status_code=500, detail=str(e))
