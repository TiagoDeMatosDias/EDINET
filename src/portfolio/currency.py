"""Shared currency conversion utilities for portfolio chart endpoints.

Loads FX rates from db2 (Stock_Prices table) where all FX data is stored as
**Ticker = "EUR", Currency = target currency** with Price = units of target
per 1 EUR.  Example: Date=2024-01-02, Ticker=EUR, Currency=USD, Price=1.10
means 1 EUR = 1.10 USD on that date.

Non-EUR ↔ non-EUR conversions triangulate through EUR (A → EUR → B).
"""

from __future__ import annotations

import sqlite3
import logging

from src.orchestrator.common.db_config import get_db2

logger = logging.getLogger(__name__)


def _load_eur_ccy_series(
    target_currency: str,
    db2_path: str,
) -> dict[str, float]:
    """Load the EUR→{target_currency} FX series, forward-filled.

    Returns ``{date_str: rate}`` where *rate* = units of *target_currency*
    per 1 EUR.  Multiply a EUR value by this rate to get the target currency.

    Returns empty dict if no data.
    """
    conn = sqlite3.connect(db2_path)
    rows = conn.execute(
        "SELECT Date, Price FROM Stock_Prices "
        "WHERE Ticker = 'EUR' AND Currency = ? ORDER BY Date",
        (target_currency.upper(),),
    ).fetchall()
    conn.close()

    if not rows or len(rows) < 2:
        logger.warning("No FX data for EUR→%s", target_currency)
        # Fallback: try old EUR{XXX}_FX format (backward compat during migration)
        return _load_legacy_series(f"EUR{target_currency.upper()}_FX", db2_path)

    fx_series: dict[str, float] = {}
    last_rate: float | None = None
    for date_str, price in rows:
        if price and price > 0:
            last_rate = price
        if last_rate is not None:
            fx_series[date_str] = last_rate
    return fx_series


def _load_legacy_series(
    ticker: str,
    db2_path: str,
) -> dict[str, float]:
    """Fallback: load FX series from old EUR{XXX}_FX ticker format.

    Only used when the new Ticker=EUR/Currency=XXX format has no data.
    """
    conn = sqlite3.connect(db2_path)
    rows = conn.execute(
        "SELECT Date, Price FROM Stock_Prices WHERE Ticker = ? ORDER BY Date",
        (ticker,),
    ).fetchall()
    conn.close()

    if not rows or len(rows) < 2:
        return {}

    fx_series: dict[str, float] = {}
    last_rate: float | None = None
    for date_str, price in rows:
        if price and price > 0:
            last_rate = price
        if last_rate is not None:
            fx_series[date_str] = last_rate
    return fx_series


def get_fx_series(
    from_currency: str,
    to_currency: str,
    db2_path: str | None = None,
) -> dict[str, float]:
    """Build a forward-filled FX series that converts *from_currency* to
    *to_currency*.

    Returns ``{date_str: multiplier}`` — multiply a value in *from_currency*
    by the returned rate to get *to_currency*.  Returns an empty dict when
    no conversion is needed (same currency) or FX data is unavailable.

    All FX rates are stored as EUR→{target} (Ticker=EUR, Currency={target}).
    Non-EUR cross pairs triangulate through EUR:
    ``USD_value × (EURJPY / EURUSD) = JPY_value``.
    """
    db2_path = db2_path or get_db2()
    fc = str(from_currency).upper()
    tc = str(to_currency).upper()

    if fc == tc:
        return {}

    # Case 1: from EUR — use EUR→{tc} series directly (multiply)
    if fc == "EUR":
        return _load_eur_ccy_series(tc, db2_path)

    # Case 2: to EUR — use EUR→{fc} series and invert (divide)
    if tc == "EUR":
        eur_fc = _load_eur_ccy_series(fc, db2_path)
        if not eur_fc:
            return {}
        inverted: dict[str, float] = {}
        for d, rate in eur_fc.items():
            if rate > 0:
                inverted[d] = 1.0 / rate
        return inverted

    # Case 3: non-EUR → non-EUR — triangulate via EUR
    # fc → EUR (divide by EUR→{fc})
    # EUR → tc (multiply by EUR→{tc})
    # Combined: rate = EUR→{tc} / EUR→{fc}
    eur_fc = _load_eur_ccy_series(fc, db2_path)
    eur_tc = _load_eur_ccy_series(tc, db2_path)
    if not eur_fc or not eur_tc:
        logger.warning("Cannot triangulate %s→%s — missing FX data", fc, tc)
        return {}

    all_dates = sorted(set(eur_fc.keys()) | set(eur_tc.keys()))
    cross: dict[str, float] = {}
    last_fc: float | None = None
    last_tc: float | None = None
    for d in all_dates:
        rf = eur_fc.get(d)
        rt = eur_tc.get(d)
        if rf is not None and rf > 0:
            last_fc = rf
        if rt is not None and rt > 0:
            last_tc = rt
        if last_fc is not None and last_tc is not None and last_fc > 0:
            cross[d] = last_tc / last_fc
    return cross


def convert_series(
    values: list[float | None],
    dates: list[str],
    fx_series: dict[str, float],
) -> list[float | None]:
    """Convert a list of values in the source currency to the target currency.

    Multiplies each value by the FX rate active on the corresponding date.
    None entries pass through.  If *fx_series* is empty, values are returned
    unchanged.
    """
    if not fx_series:
        return list(values)

    result: list[float | None] = []
    for i, v in enumerate(values):
        if v is None:
            result.append(None)
        else:
            rate = fx_series.get(dates[i]) if i < len(dates) else None
            result.append(round(v * rate, 2) if rate else v)
    return result


def get_rate_at_date(
    date_str: str,
    fx_series: dict[str, float],
) -> float | None:
    """Return the FX multiplier for a specific date.

    If no rate is available for *date_str*, walks backwards through
    the series to find the most recent known rate.  Returns None if
    no rate at all is available.
    """
    if not fx_series:
        return None

    # Exact match
    if date_str in fx_series:
        return fx_series[date_str]

    # Walk backwards
    candidates = [(d, r) for d, r in fx_series.items() if d <= date_str]
    if candidates:
        candidates.sort(key=lambda x: x[0])
        return candidates[-1][1]

    return None


# ---------------------------------------------------------------------------
# Display currency helpers
# ---------------------------------------------------------------------------

def get_available_display_currencies(
    db2_path: str | None = None,
) -> list[dict]:
    """Return all currencies available as display currency.

    Scans Stock_Prices for distinct Currency values where Ticker='EUR'
    (FX data).  Also includes EUR itself.
    """
    db2_path = db2_path or get_db2()
    conn = sqlite3.connect(db2_path)
    try:
        rows = conn.execute(
            "SELECT DISTINCT Currency FROM Stock_Prices "
            "WHERE Ticker = 'EUR' AND Currency IS NOT NULL AND Currency != '' "
            "ORDER BY Currency"
        ).fetchall()
    finally:
        conn.close()

    currencies: list[dict] = [{"code": "EUR", "label": "EUR — Euro"}]
    seen = {"EUR"}
    for (ccy,) in rows:
        ccy = str(ccy).strip()
        if ccy and ccy not in seen and len(ccy) == 3:
            seen.add(ccy)
            currencies.append({"code": ccy, "label": f"{ccy}"})
    return currencies


def get_asset_native_currency(
    ticker: str,
    db2_path: str | None = None,
) -> str | None:
    """Return the native currency of any ticker from Stock_Prices.

    For stocks/ETFs, returns the Currency column value.
    """
    db2_path = db2_path or get_db2()
    conn = sqlite3.connect(db2_path)
    try:
        row = conn.execute(
            "SELECT DISTINCT Currency FROM Stock_Prices WHERE Ticker = ? LIMIT 1",
            (ticker,),
        ).fetchone()
        if row and row[0]:
            return row[0]
    finally:
        conn.close()
    return None


def get_rate_at_date_any(
    from_currency: str,
    to_currency: str,
    date_str: str,
    db2_path: str | None = None,
) -> float | None:
    """Get the FX conversion rate from *from_currency* to *to_currency*
    on a specific date.

    Uses the same triangulation logic as ``get_fx_series`` but returns
    a single rate for one date rather than a full series.
    """
    fc = str(from_currency).upper()
    tc = str(to_currency).upper()
    if fc == tc:
        return 1.0
    series = get_fx_series(fc, tc, db2_path)
    if not series:
        return None
    return get_rate_at_date(date_str, series)


def convert_native_to_display(
    value_native: float | None,
    native_currency: str,
    display_currency: str,
    date_str: str,
    db2_path: str | None = None,
) -> float | None:
    """Convert a single native-currency value to the display currency.

    Returns None if *value_native* is None or conversion is impossible.
    """
    if value_native is None:
        return None
    rate = get_rate_at_date_any(native_currency, display_currency, date_str, db2_path)
    if rate is None:
        return None
    return round(value_native * rate, 2)
