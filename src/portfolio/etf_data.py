"""ETF price data source — maps ETF tickers to correct exchange-listed symbols
and fetches from Yahoo Finance with verified currency handling.

Preferred over generic Stooq/Yahoo fallback because it:
1. Uses the correct exchange listing (e.g. SXR8.DE for EUR CSPX)
2. Verifies the returned currency matches expectations
3. Falls back to alternate exchanges if primary fails
"""

from __future__ import annotations

import logging
import pandas as pd
import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# ETF Registry — maps portfolio ticker → {exchange_ticker, currency, name, isin}
# ---------------------------------------------------------------------------

_ETF_REGISTRY: dict[str, dict] = {
    # iShares Core S&P 500 UCITS ETF (Acc)
    "CSPX": {
        "isin": "IE00B5BMR087",
        "name": "iShares Core S&P 500 UCITS ETF",
        "primary": "SXR8.DE",      # Xetra — EUR
        "currency": "EUR",
        "fallbacks": ["CSSPX.MI", "CSPX.AS"],  # Milan EUR, Amsterdam EUR
    },
    "SXR8": {
        "isin": "IE00B5BMR087",
        "name": "iShares Core S&P 500 UCITS ETF",
        "primary": "SXR8.DE",
        "currency": "EUR",
        "fallbacks": ["CSSPX.MI", "CSPX.AS"],
    },
    # Vanguard FTSE All-World UCITS ETF (Acc)
    "VWCE": {
        "isin": "IE00BK5BQT80",
        "name": "Vanguard FTSE All-World UCITS ETF",
        "primary": "VWCE.DE",      # Xetra — EUR
        "currency": "EUR",
        "fallbacks": ["VWCE.MI", "VWCE.AS"],
    },
    # iShares Core MSCI World UCITS ETF (Acc)
    "IWDA": {
        "isin": "IE00B4L5Y983",
        "name": "iShares Core MSCI World UCITS ETF",
        "primary": "IWDA.AS",      # Amsterdam — EUR
        "currency": "EUR",
        "fallbacks": ["EUNL.DE", "SWDA.L"],
    },
    "SWDA": {
        "isin": "IE00B4L5Y983",
        "name": "iShares Core MSCI World UCITS ETF",
        "primary": "SWDA.L",       # London — USD
        "currency": "USD",
        "fallbacks": ["EUNL.DE", "IWDA.AS"],
    },
    "EUNL": {
        "isin": "IE00B4L5Y983",
        "name": "iShares Core MSCI World UCITS ETF",
        "primary": "EUNL.DE",      # Xetra — EUR
        "currency": "EUR",
        "fallbacks": ["IWDA.AS", "SWDA.L"],
    },
}


# List of exchange suffixes known to work with Yahoo Finance
_YAHOO_EXCHANGE_SUFFIXES = [".DE", ".L", ".MI", ".AS", ".PA", ".SW"]


def is_etf(ticker: str) -> bool:
    """Check if a ticker is a known UCITS ETF."""
    return ticker.upper() in _ETF_REGISTRY


def get_etf_info(ticker: str) -> dict | None:
    """Return registry info for an ETF ticker."""
    return _ETF_REGISTRY.get(ticker.upper())


def fetch_etf_history(
    ticker: str,
    start_date: str | None = None,
) -> tuple[str, pd.DataFrame] | None:
    """Fetch historical price data for a UCITS ETF from Yahoo Finance.

    Uses the registered primary exchange ticker (e.g. SXR8.DE for CSPX)
    to get correctly denominated prices. Falls back to alternate exchanges
    if the primary fails.

    Args:
        ticker: Portfolio ticker (e.g. 'CSPX', 'VWCE').
        start_date: Optional YYYY-MM-DD start.

    Returns:
        ``(source_label, DataFrame with Date/Close columns)`` or None.
    """
    info = _ETF_REGISTRY.get(ticker.upper())
    if not info:
        return None

    def _try_fetch(yticker: str) -> pd.DataFrame | None:
        """Try Yahoo Finance chart API for a single ticker."""
        params = {
            "interval": "1d",
            "includePrePost": "false",
            "events": "div,splits",
        }
        if start_date:
            import time
            start_ts = int(pd.Timestamp(start_date).timestamp())
            end_ts = int((pd.Timestamp.utcnow().normalize()
                         + pd.Timedelta(days=1)).timestamp())
            params["period1"] = start_ts
            params["period2"] = end_ts
        else:
            params["range"] = "max"

        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{yticker}"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        }
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            result = data["chart"]["result"][0]
            timestamps = result["timestamp"]
            quotes = result["indicators"]["quote"][0]
            closes = quotes["close"]
            meta = result["meta"]

            # Verify currency
            actual_currency = (meta.get("currency") or "").upper()
            expected_currency = info["currency"].upper()
            if actual_currency and actual_currency != expected_currency:
                logger.warning(
                    "ETF %s (%s): expected currency %s but Yahoo returned %s — "
                    "conversion may be needed",
                    ticker, yticker, expected_currency, actual_currency,
                )

            # Build DataFrame
            df = pd.DataFrame({
                "Date": pd.to_datetime(timestamps, unit="s").strftime("%Y-%m-%d"),
                "Close": closes,
            })
            df = df.dropna(subset=["Date", "Close"])
            return df

        except Exception as exc:
            logger.debug("ETF fetch failed for %s: %s", yticker, exc)
            return None

    # Try primary
    primary = info["primary"]
    logger.info("Fetching ETF %s as %s (%s)", ticker, primary, info["currency"])
    df = _try_fetch(primary)
    if df is not None and not df.empty:
        return f"Yahoo ({primary})", df

    # Try fallbacks
    for fb in info.get("fallbacks", []):
        logger.info("ETF %s: trying fallback %s", ticker, fb)
        df = _try_fetch(fb)
        if df is not None and not df.empty:
            return f"Yahoo ({fb})", df

    return None
