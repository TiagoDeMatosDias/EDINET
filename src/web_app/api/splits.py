"""REST API for stock split events — detection, listing, and manual review."""

from __future__ import annotations

import logging

from fastapi import APIRouter, Body, Depends, HTTPException, Query

from src.auth.dependencies import require_operator
from src.auth.models import AuthenticatedUser
from src.orchestrator.common.db_config import get_db2
from src.orchestrator.common.sqlite import connect_read, connect_write
from src.portfolio.portfolio_state import _invalidate_split_cache
from src.utilities.price_provenance import refresh_split_adjusted_prices

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/splits", tags=["splits"])

# ── read helpers ────────────────────────────────────────────────────────────


def _serialize_row(row) -> dict:
    """Convert a sqlite3.Row to a JSON-safe dict."""
    if row is None:
        return {}
    return {k: row[k] for k in row.keys()}


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
    db2_path = get_db2()
    from src.portfolio.split_schema import ensure_split_tables
    ensure_split_tables(db2_path)
    conn = connect_read(db2_path)
    try:
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
    db2_path = get_db2()
    from src.portfolio.split_schema import ensure_split_tables
    ensure_split_tables(db2_path)
    conn = connect_read(db2_path)
    try:
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

    db2_path = get_db2()
    from src.portfolio.split_schema import ensure_split_tables
    ensure_split_tables(db2_path)
    conn = connect_write(db2_path)
    try:
        row = conn.execute(
            "SELECT id FROM Stock_Splits WHERE id = ?", (split_id,)
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Split not found")

        detail_suffix = f"; manual review by {_operator.user_id}"
        if reason and reason.strip():
            detail_suffix += f": {reason.strip()}"
        conn.execute(
            "UPDATE Stock_Splits SET confirmation = ?, "
            "confirmed_by = CASE WHEN ? = 'confirmed' THEN 'manual' "
            "ELSE confirmed_by END, "
            "price_basis = CASE WHEN ? = 'confirmed' THEN 'raw' ELSE price_basis END, "
            "source_detail = COALESCE(source_detail, '') || ?, "
            "updated_at = datetime('now') "
            "WHERE id = ?",
            (confirmation, confirmation, confirmation, detail_suffix, split_id),
        )
        refresh_split_adjusted_prices(conn)
        conn.commit()

        # Invalidate the split-factor cache so the next price lookup
        # sees the updated confirmation status.
        _invalidate_split_cache()

        updated = conn.execute(
            "SELECT * FROM Stock_Splits WHERE id = ?", (split_id,)
        ).fetchone()
        return {"split": _serialize_row(updated)}
    finally:
        conn.close()


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

    from src.portfolio.split_detection import run_split_detection

    db2_path = get_db2()
    tickers = [ticker.strip()] if ticker and ticker.strip() else None

    results = run_split_detection(
        db2_path=db2_path,
        tickers=tickers,
        mode=mode,
    )

    _invalidate_split_cache()

    # Add a hint that the user should rebuild portfolio state
    if results.get("new_pending", 0) > 0 or results.get("confirmed", 0) > 0:
        results["rebuild_recommended"] = True

    return results
