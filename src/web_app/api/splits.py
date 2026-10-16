"""REST API for stock split events — detection, listing, and manual review."""

from __future__ import annotations

import logging

from fastapi import APIRouter, Body, Depends, HTTPException, Query

from src.auth.dependencies import require_operator
from src.auth.models import AuthenticatedUser
from src.jobs.service import get_job_service
from src.orchestrator.common.db_config import get_market_db
from src.orchestrator.common.sqlite import connect_read, table_exists
from src.portfolio.portfolio_state import _invalidate_split_cache

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/splits", tags=["splits"])

# ── read helpers ────────────────────────────────────────────────────────────


def _serialize_row(row) -> dict:
    """Convert a sqlite3.Row to a JSON-safe dict."""
    if row is None:
        return {}
    return {k: row[k] for k in row.keys()}


def _has_split_table(conn) -> bool:
    """Whether Stock_Splits exists; a missing table reads as no splits (workers create it)."""
    return table_exists(conn, "Stock_Splits")


# ── endpoints ───────────────────────────────────────────────────────────────


@router.get("/")
def list_splits(
    ticker: str | None = Query(default=None),
    confirmation: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> dict:
    """List split events, optionally filtered.

    Args:
        ticker: Filter by ticker symbol.
        confirmation: Filter by status (``confirmed``, ``pending``, ``rejected``).
        limit: Max rows to return.
        offset: Pagination offset.
    """
    db2_path = get_market_db()
    conn = connect_read(db2_path)
    try:
        if not _has_split_table(conn):
            return {"splits": [], "total": 0}
        where: list[str] = []
        params: list = []
        if ticker:
            where.append("ticker = ?")
            params.append(ticker.strip())
        if confirmation:
            where.append("confirmation = ?")
            params.append(confirmation.strip().lower())

        where_clause = ""
        if where:
            where_clause = "WHERE " + " AND ".join(where)

        rows = conn.execute(
            f"SELECT * FROM Stock_Splits {where_clause} "
            "ORDER BY split_date DESC, ticker ASC "
            "LIMIT ? OFFSET ?",
            [*params, limit, offset],
        ).fetchall()

        count_row = conn.execute(
            f"SELECT COUNT(*) FROM Stock_Splits {where_clause}",
            params,
        ).fetchone()

        return {
            "splits": [_serialize_row(r) for r in rows],
            "total": count_row[0] if count_row else 0,
        }
    except Exception as exc:
        logger.error("Could not list split events: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail="Could not read split events") from exc
    finally:
        conn.close()


@router.get("/{split_id}")
def get_split(split_id: int) -> dict:
    """Get a single split event by its database ID."""
    db2_path = get_market_db()
    conn = connect_read(db2_path)
    try:
        if not _has_split_table(conn):
            raise HTTPException(status_code=404, detail="Split not found")
        row = conn.execute(
            "SELECT * FROM Stock_Splits WHERE id = ?", (split_id,)
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Split not found")
        return {"split": _serialize_row(row)}
    finally:
        conn.close()


@router.put("/{split_id}")
def update_split(
    split_id: int,
    confirmation: str = Body(..., embed=True),
    reason: str | None = Body(default=None, embed=True),
    _operator: AuthenticatedUser = Depends(require_operator),  # noqa: B008
) -> dict:
    """Update a split event's confirmation status.

    Valid values: ``confirmed``, ``pending``, ``rejected``.

    After updating to *confirmed*, call ``build_portfolio_state()``
    (portfolio rebuild) to correct historical values using the new
    split adjustment.
    """
    confirmation = confirmation.strip().lower()
    if confirmation not in ("confirmed", "pending", "rejected"):
        raise HTTPException(
            status_code=400,
            detail="confirmation must be 'confirmed', 'pending', or 'rejected'",
        )

    conn = connect_read(get_market_db())
    try:
        exists = _has_split_table(conn) and conn.execute(
            "SELECT 1 FROM Stock_Splits WHERE id = ?", (split_id,)
        ).fetchone() is not None
    finally:
        conn.close()
    if not exists:
        raise HTTPException(status_code=404, detail="Split not found")

    record = get_job_service().run_sync(
        "split_review",
        {
            "split_id": split_id,
            "confirmation": confirmation,
            "reason": reason,
            "reviewer": _operator.user_id,
        },
        owner_user_id=_operator.user_id,
        title=f"Split review · {confirmation}",
    )
    # Drop the in-process split-factor cache so the next price lookup sees the change.
    _invalidate_split_cache()
    return dict(record.result or {})


@router.post("/detect")
def trigger_detection(
    ticker: str | None = Body(default=None, embed=True),
    mode: str = Body(default="incremental", embed=True),
    _operator: AuthenticatedUser = Depends(require_operator),  # noqa: B008
) -> dict:
    """Run split detection on-demand.

    Args:
        ticker: Limit to a single ticker.  None = all tickers.
        mode: ``incremental``, ``full``, or ``verify_pending``.
    """
    mode = mode.strip().lower()
    if mode not in ("incremental", "full", "verify_pending"):
        raise HTTPException(
            status_code=400,
            detail="mode must be 'incremental', 'full', or 'verify_pending'",
        )

    tickers = ticker.strip() if ticker and ticker.strip() else None
    record = get_job_service().run_sync(
        "split_detection",
        {"ticker": tickers, "mode": mode},
        owner_user_id=_operator.user_id,
        title="Split detection" + (f" · {tickers}" if tickers else ""),
    )
    results = dict(record.result or {})
    _invalidate_split_cache()

    # Add a hint that the user should rebuild portfolio state
    if results.get("new_pending", 0) > 0 or results.get("confirmed", 0) > 0:
        results["rebuild_recommended"] = True

    return results
