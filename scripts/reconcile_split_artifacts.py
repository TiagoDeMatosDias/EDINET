"""Reconcile mis-dated stock split artifacts with per-ticker verification.

For each candidate ticker:
  1. Snapshot its Stock_Prices and Stock_Splits rows.
  2. Run reconcile_ticker_price_basis (restores fetched rows to raw, records
     splits at their true dates).
  3. Verify the ticker's ADJUSTED price view is continuous across every raw
     boundary (>40% adjacent move).  If any boundary still shows a >40%
     adjusted return, the reconcile over-applied (common on tickers with
     multiple historical splits) — the ticker is reverted to its snapshot so
     its genuine historical data is never damaged.

Only tickers that reconcile cleanly (continuous adjusted view) are kept.

Usage:
    python scripts/reconcile_split_artifacts.py [--ticker X]
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from datetime import date as Date

from src.orchestrator.common.db_config import get_db2
from src.portfolio.portfolio_state import _get_adjusted_price, _invalidate_split_cache
from src.portfolio.split_schema import ensure_split_tables
from src.utilities.stock_prices import reconcile_ticker_price_basis

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("reconcile")

_BASIS_CHANGE_LOW = 0.6
_BASIS_CHANGE_HIGH = 1.67
_ADJUSTED_TOLERANCE = 0.40


def _quote_identifier(name: str) -> str:
    """Quote an identifier read from the database schema."""
    return '"' + str(name).replace('"', '""') + '"'


def find_candidates(conn: sqlite3.Connection) -> list[str]:
    """Tickers with a heuristic split record."""
    try:
        return [
            r[0]
            for r in conn.execute(
                "SELECT DISTINCT ticker FROM Stock_Splits "
                "WHERE detection_method = 'price_heuristic'"
            ).fetchall()
        ]
    except sqlite3.OperationalError:
        return []


def _snapshot_ticker(conn: sqlite3.Connection, ticker: str) -> dict:
    """Capture the ticker's prices and split records."""
    return {
        "prices": conn.execute(
            "SELECT * FROM Stock_Prices WHERE Ticker = ?",
            (ticker,),
        ).fetchall(),
        "splits": conn.execute(
            "SELECT * FROM Stock_Splits WHERE ticker = ?", (ticker,),
        ).fetchall(),
    }


def _restore_ticker(conn: sqlite3.Connection, ticker: str, snap: dict) -> None:
    """Revert a ticker's prices and splits to a snapshot."""
    conn.execute("DELETE FROM Stock_Prices WHERE Ticker = ?", (ticker,))
    if snap["prices"]:
        price_columns = list(snap["prices"][0].keys())
        quoted_columns = ", ".join(_quote_identifier(column) for column in price_columns)
        placeholders = ", ".join("?" for _ in price_columns)
        conn.executemany(
            f"INSERT INTO Stock_Prices ({quoted_columns}) VALUES ({placeholders})",
            [tuple(row[column] for column in price_columns) for row in snap["prices"]],
        )
    conn.execute("DELETE FROM Stock_Splits WHERE ticker = ?", (ticker,))
    if snap["splits"]:
        cols = [k for k in snap["splits"][0].keys()]
        quoted_cols = ", ".join(_quote_identifier(column) for column in cols)
        placeholders = ", ".join("?" for _ in cols)
        for row in snap["splits"]:
            conn.execute(
                f"INSERT INTO Stock_Splits ({quoted_cols}) VALUES ({placeholders})",
                [row[k] for k in cols],
            )


def _adjusted_is_continuous(conn: sqlite3.Connection, ticker: str) -> bool:
    """True if every >40% raw boundary has a continuous adjusted return."""
    rows = conn.execute(
        "SELECT Date, Price FROM Stock_Prices WHERE Ticker = ? ORDER BY Date",
        (ticker,),
    ).fetchall()
    prev = None
    for r in rows:
        if prev is not None:
            p, c = prev[1], r[1]
            if p and c and p > 0:
                ratio = c / p
                if ratio < _BASIS_CHANGE_LOW or ratio > _BASIS_CHANGE_HIGH:
                    try:
                        gap = (Date.fromisoformat(r[0]) - Date.fromisoformat(prev[0])).days
                    except ValueError:
                        gap = 999
                    if gap <= 10:
                        p1 = _get_adjusted_price(conn, ticker, prev[0])
                        p2 = _get_adjusted_price(conn, ticker, r[0])
                        if p1 and p2 and p1 > 0 and abs(p2 / p1 - 1) > _ADJUSTED_TOLERANCE:
                            return False
        prev = r
    return True


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ticker", type=str, default=None)
    args = parser.parse_args()

    db_path = get_db2()
    ensure_split_tables(db_path)
    _invalidate_split_cache()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        candidates = [args.ticker] if args.ticker else find_candidates(conn)
    finally:
        conn.close()

    if not candidates:
        logger.info("No candidates.")
        return 0
    logger.info("Processing %d candidate ticker(s).", len(candidates))

    kept: list[str] = []
    reverted: list[str] = []

    for i, ticker in enumerate(candidates, 1):
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        try:
            snap = _snapshot_ticker(conn, ticker)
            result = reconcile_ticker_price_basis(conn, "Stock_Prices", ticker)
            if result.get("status") == "reconciled":
                if _adjusted_is_continuous(conn, ticker):
                    kept.append(ticker)
                    logger.info("[%d/%d] %s -> reconciled + verified continuous", i, len(candidates), ticker)
                else:
                    _restore_ticker(conn, ticker, snap)
                    conn.commit()
                    reverted.append(ticker)
                    logger.warning(
                        "[%d/%d] %s -> reconciled but NOT continuous — reverted "
                        "(complex multi-split history, review manually)",
                        i, len(candidates), ticker,
                    )
            else:
                logger.info("[%d/%d] %s -> %s", i, len(candidates), ticker, result.get("status"))
        except Exception as exc:  # noqa: BLE001  # restore snapshot before aborting
            logger.error("[%d/%d] %s -> ERROR %s", i, len(candidates), ticker, exc)
            try:
                _restore_ticker(conn, ticker, snap)
                conn.commit()
            except Exception:  # noqa: BLE001  # best-effort rollback cleanup
                pass
        finally:
            conn.close()

    logger.info("\n=== Summary ===")
    logger.info("Reconciled + verified: %d", len(kept))
    logger.info("Reverted (ambiguous): %d: %s", len(reverted), ", ".join(reverted))
    return 0


if __name__ == "__main__":
    sys.exit(main())
