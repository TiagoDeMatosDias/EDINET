"""Fetches stock prices for tickers encountered in IBKR XML and stores them in db2.

Uses the now multi-currency-aware ``load_ticker_data()`` from
``src.utilities.stock_prices`` to fetch from Stooq/Yahoo with the correct
currency per ticker (derived from the IBKR XML ``currency`` field).
"""

from __future__ import annotations

import sqlite3
import logging
from collections import defaultdict

import pandas as pd

from src.utilities.stock_prices import load_ticker_data, _create_prices_table
from src.orchestrator.common.db_config import get_db2
from src.portfolio.etf_data import fetch_etf_history, is_etf, get_etf_info

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Forex detection
# ---------------------------------------------------------------------------

def _is_forex_pair(symbol: str) -> bool:
    """Check if a symbol looks like a forex pair (e.g. EUR.USD, USD.JPY).

    Avoids false positives on Japanese tickers like 7575.T by requiring
    the symbol to contain a currency-like dot between short alphabetic codes.
    """
    if not symbol:
        return False
    s = symbol.strip()
    # Must contain a dot and be short
    if "." not in s or "/" not in s:
        # Only dot-pairs like EUR.USD — not .T suffix
        if "." in s and len(s) <= 7:
            parts = s.split(".")
            # Both parts must be 3-letter alphabetic currency codes
            if len(parts) == 2 and parts[0].isalpha() and parts[1].isalpha():
                return True
        if "/" in s and len(s) <= 7:
            parts = s.split("/")
            if len(parts) == 2 and parts[0].isalpha() and parts[1].isalpha():
                return True
        return False
    return False


def _build_currency_map(entries: list[dict]) -> dict[str, str]:
    """Extract {ticker: currency} mapping from parsed IBKR entries.

    Only uses TRADE entries (STK + OPT: real securities). CASH forex trades
    (EUR.USD, USD.JPY) are ignored — they're not equities and don't need
    price fetching.

    For option trades, the ``symbol`` field is the option symbol (e.g.
    'JXN 250620P00050000'), not the underlying. We extract both the option
    symbol and the underlying for completeness, mapped to the same currency.
    
    Option symbols (containing spaces and numbers after the underlying)
    are NOT included in the returned map — they can't be priced via
    Stooq/Yahoo. Only the underlying stock ticker is included.
    """
    mapping: dict[str, str] = {}
    for entry in entries:
        if entry.get("activity_type") != "TRADE":
            continue
        sym = (entry.get("symbol") or "").strip()
        cur = (entry.get("currency") or "").strip()
        if not sym or not cur:
            continue
        # Skip forex pairs
        if _is_forex_pair(sym):
            continue
        
        asset_cat = entry.get("asset_category", "")
        if asset_cat == "OPT":
            # Option symbol — only map the underlying, not the option symbol itself
            underlying = (entry.get("underlying_symbol") or "").strip()
            if underlying and underlying != sym:
                mapping[underlying] = cur
        else:
            # STK — map the stock symbol directly
            mapping[sym] = cur
    return mapping


def ensure_prices_for_tickers(
    db2_path: str | None = None,
    ticker_currency_map: dict[str, str] | None = None,
) -> dict:
    """Ensure every ticker in the map has price data in db2 ``Stock_Prices``.

    For each ticker not already present, calls ``load_ticker_data`` with the
    correct currency.  Tickers already in the table are skipped (unless the
    stored currency mismatches — then a warning is logged).

    European ETF suffix fallback is handled by ``load_ticker_data`` itself.

    Args:
        db2_path: Path to the Standardized DB (defaults to ``get_db2()``).
        ticker_currency_map: ``{ticker: currency}`` dict from parsed XML.

    Returns:
        ``{'fetched': [...], 'already_present': [...], 'failed': [...]}``
    """
    db2_path = db2_path or get_db2()
    ticker_currency_map = ticker_currency_map or {}

    fetched: list[str] = []
    already_present: list[str] = []
    failed: list[str] = []

    conn = sqlite3.connect(db2_path)
    try:
        _create_prices_table(conn, "Stock_Prices")

        for ticker, currency in ticker_currency_map.items():
            if not ticker or ticker == currency:  # skip forex tokens
                continue

            # Check if ticker already has data (and verify currency)
            existing_cur = _get_stored_currency(conn, ticker)
            if existing_cur:
                if existing_cur != currency:
                    logger.warning(
                        "Currency mismatch for %s: stored=%s, expected=%s — skipping",
                        ticker, existing_cur, currency,
                    )
                    continue
                logger.debug("Ticker %s already present in Stock_Prices", ticker)
                already_present.append(ticker)
                continue

            # Fetch — prefer ETF issuer data for known UCITS ETFs
            if is_etf(ticker):
                logger.info("Fetching ETF %s from exchange-listed data", ticker)
                ok = _fetch_etf_to_db(conn, ticker, currency)
            else:
                logger.info("Fetching prices for %s (currency=%s)", ticker, currency)
                ok = load_ticker_data(ticker, "Stock_Prices", conn, currency=currency)
            if ok:
                fetched.append(ticker)
            else:
                failed.append(ticker)
                logger.warning("Failed to fetch prices for %s", ticker)
    finally:
        conn.close()

    logger.info(
        "Price fetch complete: fetched=%d, present=%d, failed=%d",
        len(fetched), len(already_present), len(failed),
    )
    return {
        "fetched": fetched,
        "already_present": already_present,
        "failed": failed,
    }


def _get_stored_currency(conn: sqlite3.Connection, ticker: str) -> str | None:
    """Return the Currency stored in Stock_Prices for a ticker, or None."""
    try:
        cur = conn.execute(
            "SELECT DISTINCT Currency FROM Stock_Prices WHERE Ticker = ? LIMIT 1",
            (ticker,),
        ).fetchone()
        return cur[0] if cur else None
    except sqlite3.OperationalError:
        return None


def _fetch_etf_to_db(conn: sqlite3.Connection, ticker: str, expected_currency: str) -> bool:
    """Fetch ETF data from exchange-listed source and store in Stock_Prices.

    Uses the ETF registry to determine the correct exchange ticker and currency.
    Data is stored under the original (unsuffixed) ticker name.
    """
    df_to_store = None

    # Try ETF-specific fetcher first (uses registered exchange tickers)
    result = fetch_etf_history(ticker)
    if result:
        source_label, raw_df = result
        if not raw_df.empty:
            # Normalize column names to match Stock_Prices table
            df = raw_df.copy()
            if "Close" in df.columns:
                df["Price"] = df["Close"]
            elif "Price" in df.columns:
                pass
            else:
                logger.warning("ETF data for %s has no Close/Price column", ticker)
                return False

            # Check if data is up to date
            today = pd.Timestamp.today().strftime("%Y-%m-%d")
            last_stored = conn.execute(
                "SELECT MAX(Date) FROM Stock_Prices WHERE Ticker = ?", (ticker,),
            ).fetchone()[0]
            if last_stored:
                days_since = (pd.Timestamp(today) - pd.Timestamp(last_stored)).days
                if days_since <= 5:
                    logger.debug("ETF %s already up to date", ticker)
                    return True

            df["Ticker"] = ticker
            df["Currency"] = expected_currency
            # Keep only needed columns
            cols = [c for c in ["Date", "Ticker", "Currency", "Price"] if c in df.columns]
            df = df[cols]

            # Remove duplicates by date
            df = df.drop_duplicates(subset=["Date"], keep="last")
            df = df.sort_values("Date")
            df_to_store = df
            logger.info(
                "ETF %s: fetched %d rows via %s", ticker, len(df), source_label,
            )

    # Fallback to generic load_ticker_data with suffix support
    if df_to_store is None:
        logger.info("ETF %s: falling back to generic fetcher", ticker)
        ok = load_ticker_data(ticker, "Stock_Prices", conn, currency=expected_currency)
        if ok:
            # Verify currency of what was stored
            stored_cur = _get_stored_currency(conn, ticker)
            if stored_cur and stored_cur != expected_currency:
                logger.warning(
                    "ETF %s: stored currency %s differs from expected %s",
                    ticker, stored_cur, expected_currency,
                )
            return ok
        return False

    if df_to_store is not None and not df_to_store.empty:
        try:
            df_to_store.to_sql("Stock_Prices", conn, if_exists="append", index=False)
            conn.commit()
            return True
        except Exception as exc:
            logger.error("Failed to store ETF data for %s: %s", ticker, exc)
            return False

    return False
