"""The signed-in overview: one request for the dashboard's portfolio, research, and data panels."""

from __future__ import annotations

import logging
import sqlite3
from datetime import date
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Request

from src.auth.models import AuthenticatedUser
from src.orchestrator.common.db_config import get_db2, get_db3
from src.orchestrator.common.sqlite import connect_read

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/overview", tags=["overview"])

# Series stored alongside prices that are not securities.
_NOT_SECURITIES = "Ticker <> 'EUR' AND Ticker NOT LIKE 'RiskFree\\_%' ESCAPE '\\' AND Ticker NOT LIKE 'Inflation\\_%' ESCAPE '\\'"


def _user(request: Request) -> AuthenticatedUser:
    user = getattr(request.state, "user", None)
    if not isinstance(user, AuthenticatedUser):
        raise HTTPException(status_code=401, detail="Account authentication is required")
    return user


def _read(path: str) -> sqlite3.Connection | None:
    try:
        return connect_read(path) if Path(path).is_file() else None
    except (OSError, sqlite3.Error):
        return None


def portfolio_summary(db3_path: str, owner_user_id: str, top: int = 8) -> dict[str, Any] | None:
    """Latest value, its changes, cash, and the largest holdings, all in the portfolio's base currency (EUR)."""
    conn = _read(db3_path)
    if conn is None:
        return None
    try:
        rows = conn.execute(
            "SELECT date, total_value, cash_balance, daily_return, cumulative_return FROM Portfolio_Daily "
            "WHERE owner_user_id = ? ORDER BY date DESC LIMIT 2",
            (owner_user_id,),
        ).fetchall()
        if not rows:
            return None
        latest = rows[0]
        year_start = conn.execute(
            "SELECT cumulative_return FROM Portfolio_Daily WHERE owner_user_id = ? AND date < ? ORDER BY date DESC LIMIT 1",
            (owner_user_id, f"{latest['date'][:4]}-01-01"),
        ).fetchone()
        first = conn.execute("SELECT MIN(date) FROM Portfolio_Daily WHERE owner_user_id = ?", (owner_user_id,)).fetchone()[0]
        holdings = conn.execute(
            "SELECT symbol, asset_category, quantity, market_value, currency, is_option FROM Portfolio_Holdings "
            "WHERE owner_user_id = ? AND quantity > 0 ORDER BY COALESCE(market_value, 0) DESC",
            (owner_user_id,),
        ).fetchall()
    except sqlite3.Error as exc:
        logger.warning("Overview could not read the portfolio: %s", exc)
        return None
    finally:
        conn.close()
    total = latest["total_value"] or 0.0
    cumulative = latest["cumulative_return"]
    ytd = None
    if cumulative is not None and year_start is not None and year_start["cumulative_return"] is not None:
        ytd = (1 + cumulative) / (1 + year_start["cumulative_return"]) - 1
    return {
        "currency": "EUR",
        "valuation_date": latest["date"],
        "first_date": first,
        "total_value": total,
        "cash": latest["cash_balance"],
        "day_return": latest["daily_return"],
        "ytd_return": ytd,
        "total_return": cumulative,
        "holdings_count": len(holdings),
        "top_holdings": [
            {
                "symbol": row["symbol"],
                "asset_category": row["asset_category"],
                "value": row["market_value"],
                "weight": (row["market_value"] or 0) / total if total else None,
                "currency": row["currency"],
            }
            for row in holdings[:top]
        ],
    }


def research_summary(owner_user_id: str) -> dict[str, Any]:
    from src.research.runtime import store

    today = date.today().isoformat()
    conn = store._connection()
    try:
        followed = conn.execute(
            "SELECT COUNT(*) FROM (SELECT edinet_code FROM company_research WHERE user_id = ? "
            "UNION SELECT edinet_code FROM company_tags WHERE user_id = ?)",
            (owner_user_id, owner_user_id),
        ).fetchone()[0]
        due = conn.execute(
            "SELECT edinet_code, review_on, thesis_status FROM company_research WHERE user_id = ? AND review_on IS NOT NULL "
            "AND review_on <> '' AND review_on <= ? ORDER BY review_on LIMIT 8",
            (owner_user_id, today),
        ).fetchall()
        alerts = conn.execute("SELECT COUNT(*) FROM alert_rules WHERE user_id = ? AND enabled = 1", (owner_user_id,)).fetchone()[0]
        notes = conn.execute(
            "SELECT note_id, title, edinet_code, updated_at FROM research_notes WHERE user_id = ? ORDER BY updated_at DESC LIMIT 5",
            (owner_user_id,),
        ).fetchall()
        note_count = conn.execute("SELECT COUNT(*) FROM research_notes WHERE user_id = ?", (owner_user_id,)).fetchone()[0]
        statuses = conn.execute(
            "SELECT thesis_status, COUNT(*) AS companies FROM company_research WHERE user_id = ? AND thesis_status IS NOT NULL "
            "GROUP BY thesis_status ORDER BY companies DESC",
            (owner_user_id,),
        ).fetchall()
    finally:
        conn.close()
    return {
        "followed": followed,
        "alerts": alerts,
        "notes": note_count,
        "reviews_due": [dict(row) for row in due],
        "recent_notes": [dict(row) for row in notes],
        "theses": {row["thesis_status"]: row["companies"] for row in statuses},
    }


def data_summary() -> dict[str, Any]:
    """How current the shared market and filing data are, for everyone."""
    result: dict[str, Any] = {"latest_price_date": None, "priced_securities": None}
    conn = _read(get_db2())
    if conn is not None:
        try:
            # The Date index makes the latest date instant; the count reads one day's rows.
            latest = conn.execute(f"SELECT MAX(Date) FROM Stock_Prices WHERE {_NOT_SECURITIES}").fetchone()[0]
            result["latest_price_date"] = latest
            if latest:
                result["priced_securities"] = conn.execute(
                    f"SELECT COUNT(DISTINCT Ticker) FROM Stock_Prices WHERE Date = ? AND {_NOT_SECURITIES}", (latest,)
                ).fetchone()[0]
        except sqlite3.Error as exc:
            logger.info("Overview could not read prices: %s", exc)
        finally:
            conn.close()
    try:
        from src.filings.runtime import catalog

        result["filings"] = catalog.coverage_summary()
    except Exception:  # noqa: BLE001 - the filing catalog is optional
        result["filings"] = None
    return result


@router.get("")
def overview(request: Request) -> dict[str, Any]:
    user = _user(request)
    return {
        "portfolio": portfolio_summary(get_db3(), user.user_id),
        "research": research_summary(user.user_id),
        "data": data_summary(),
        "today": date.today().isoformat(),
    }
