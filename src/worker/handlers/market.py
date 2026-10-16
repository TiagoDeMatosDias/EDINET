"""Market-data handlers: price refreshes, portfolio prices, splits and company descriptions.

Everything the web server used to write into market.db inside a request runs
here. Payloads are built by the server (it alone reads app.db).
"""

from __future__ import annotations

import logging
from typing import Any

from src import security_analysis as _security
from src.orchestrator.common.db_config import get_market_db
from src.orchestrator.common.sqlite import connect_write
from src.portfolio.price_fetcher import ensure_prices_for_tickers, refresh_market_prices
from src.worker.context import JobContext, JobError

logger = logging.getLogger(__name__)


def _strings(value: Any) -> list[str]:
    return [str(item).strip() for item in value or [] if str(item).strip()]


def price_refresh(context: JobContext) -> dict:
    """Refresh each ticker from the price provider; one failure does not stop the rest."""
    tickers = _strings(context.payload.get("tickers"))
    if not tickers:
        raise JobError("No tickers were given.")
    db_path = str(get_market_db())
    results: dict[str, dict] = {}
    for index, ticker in enumerate(tickers):
        context.checkpoint()
        context.report_progress(100.0 * index / len(tickers), f"Updating {ticker}")
        try:
            results[ticker] = dict(_security.update_security_price(db_path, ticker))
        except Exception as exc:  # noqa: BLE001 - one failed ticker must not abort the batch
            logger.warning("Update price failed for %s: %s", ticker, exc)
            results[ticker] = {"ok": False, "rows_inserted": 0, "message": str(exc)}
    return {"results": results}


def portfolio_prices(context: JobContext) -> dict:
    """Fetch prices for tickers a portfolio import mentions and the market database lacks."""
    ticker_map = {str(k): str(v) for k, v in (context.payload.get("ticker_map") or {}).items()}
    context.report_progress(None, "Fetching prices")
    result = ensure_prices_for_tickers(str(get_market_db()), ticker_map)
    return {"fetched": list(result.get("fetched", [])), "failed": list(result.get("failed", []))}


def portfolio_market_refresh(context: JobContext) -> dict:
    """Prices, FX and interest rates for a portfolio (the market.db part of a refresh)."""
    tickers = _strings(context.payload.get("tickers"))
    currency = str(context.payload.get("base_currency") or "EUR")
    context.report_progress(None, "Refreshing market data")
    return refresh_market_prices(str(get_market_db()), tickers, currency)


def split_review(context: JobContext) -> dict:
    """Confirm, reject or reset a detected split and refresh split-adjusted prices."""
    from src.portfolio.portfolio_state import _invalidate_split_cache
    from src.portfolio.split_schema import ensure_split_tables
    from src.utilities.price_provenance import refresh_split_adjusted_prices

    payload = context.payload
    split_id = int(payload["split_id"])
    confirmation = str(payload.get("confirmation") or "").strip().lower()
    if confirmation not in ("confirmed", "pending", "rejected"):
        raise JobError("confirmation must be 'confirmed', 'pending', or 'rejected'")
    reason = str(payload.get("reason") or "").strip()
    suffix = f"; manual review by {payload.get('reviewer') or 'unknown'}"
    if reason:
        suffix += f": {reason}"

    db_path = str(get_market_db())
    ensure_split_tables(db_path)
    conn = connect_write(db_path)
    try:
        if conn.execute("SELECT id FROM Stock_Splits WHERE id = ?", (split_id,)).fetchone() is None:
            raise JobError("Split not found")
        conn.execute(
            "UPDATE Stock_Splits SET confirmation = ?, "
            "confirmed_by = CASE WHEN ? = 'confirmed' THEN 'manual' ELSE confirmed_by END, "
            "price_basis = CASE WHEN ? = 'confirmed' THEN 'raw' ELSE price_basis END, "
            "source_detail = COALESCE(source_detail, '') || ?, "
            "updated_at = datetime('now') WHERE id = ?",
            (confirmation, confirmation, confirmation, suffix, split_id),
        )
        context.report_progress(None, "Refreshing split-adjusted prices")
        refresh_split_adjusted_prices(conn)
        conn.commit()
        updated = conn.execute("SELECT * FROM Stock_Splits WHERE id = ?", (split_id,)).fetchone()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    _invalidate_split_cache()
    return {"split": {key: updated[key] for key in updated.keys()}}


def split_detection(context: JobContext) -> dict:
    """Scan stored prices for stock splits (one ticker or all)."""
    from src.portfolio.portfolio_state import _invalidate_split_cache
    from src.portfolio.split_detection import run_split_detection

    payload = context.payload
    ticker = str(payload.get("ticker") or "").strip()

    def progress(done: int, total: int, message: str) -> None:
        context.checkpoint()
        context.report_progress(100.0 * done / total if total else None, message)

    try:
        result = run_split_detection(
            db2_path=str(get_market_db()),
            tickers=[ticker] if ticker else None,
            mode=str(payload.get("mode") or "incremental"),
            progress=progress,
        )
    except ValueError as exc:
        raise JobError(str(exc)) from exc
    _invalidate_split_cache()
    return dict(result)


def description_fetch(context: JobContext) -> dict:
    """Fetch a company's external description once and cache it."""
    from src.security_analysis.company_descriptions import get_or_fetch_description

    payload = context.payload
    description = get_or_fetch_description(
        str(get_market_db()),
        str(payload.get("company_code") or "").strip() or None,
        str(payload.get("ticker") or "").strip() or None,
    )
    return {"description": description or None}
