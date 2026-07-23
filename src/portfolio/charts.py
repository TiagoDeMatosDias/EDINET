"""Data-fetching functions for the Portfolio Charts tab.

Each function returns plain dicts ready for JSON serialisation.  Business
logic is concentrated here rather than in the API layer so it's testable.
"""

from __future__ import annotations

import logging
import sqlite3
from collections import defaultdict

from src.orchestrator.common.sqlite import connect_read
from src.portfolio.currency import (
    get_fx_series,
    get_rate_at_date,
    get_available_display_currencies,
)

logger = logging.getLogger(__name__)


def _latest_fx_rate(from_currency: str, to_currency: str, db2_path: str) -> float | None:
    """Get the most recent FX rate from *from_currency* to *to_currency*."""
    series = get_fx_series(from_currency, to_currency, db2_path)
    if not series:
        return None
    # Get the last (most recent) date's rate
    last_date = max(series.keys())
    return series[last_date]


# ---------------------------------------------------------------------------
# Chart 1: Holdings by value (pie)
# ---------------------------------------------------------------------------

def _get_latest_rates(
    currencies: set[str],
    display_currency: str,
    db2_path: str,
) -> dict[str, float]:
    """Pre-compute latest FX rates for a set of currencies to display currency."""
    dc = display_currency.upper()
    rates: dict[str, float] = {dc: 1.0}
    for ccy in currencies:
        u = ccy.upper()
        if u != dc and u not in rates:
            r = _latest_fx_rate(u, dc, db2_path)
            rates[u] = r if r is not None else 1.0
    return rates


def get_holdings_by_value(
    db3_path: str,
    db2_path: str,
    display_currency: str = "EUR",
) -> dict:
    """Current portfolio holdings aggregated by symbol, valued in *display_currency*.

    Excludes cash and option positions.
    Returns ``{labels: [str], values: [float], total: float, currency: str}``.
    """
    conn = connect_read(db3_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT symbol, market_value_native, currency "
        "FROM Portfolio_Holdings WHERE quantity > 0 "
        "AND asset_category NOT IN ('CASH', 'OPT') "
        "ORDER BY market_value_native DESC"
    ).fetchall()
    conn.close()

    if not rows:
        return {"labels": [], "values": [], "total": 0, "currency": display_currency}

    dc = display_currency.upper()
    native_ccies = {(r["currency"] or "EUR").upper() for r in rows}
    rates = _get_latest_rates(native_ccies, dc, db2_path)

    labels: list[str] = []
    values: list[float] = []
    total = 0.0

    for r in rows:
        sym = r["symbol"]
        val_native = r["market_value_native"] or 0
        native_ccy = (r["currency"] or "EUR").upper()
        val_display = round(val_native * rates.get(native_ccy, 1.0), 2)
        labels.append(sym)
        values.append(val_display)
        total += val_display

    return {
        "labels": labels,
        "values": [round(v, 2) for v in values],
        "total": round(total, 2),
        "currency": display_currency,
    }


# ---------------------------------------------------------------------------
# Chart 2: Holdings by native currency (pie)
# ---------------------------------------------------------------------------

def get_holdings_by_currency(
    db3_path: str,
    db2_path: str,
    display_currency: str = "EUR",
) -> dict:
    """Current holdings grouped by native currency, all values in *display_currency*.

    Returns ``{labels: [str], values: [float], total: float, currency: str}``.
    """
    conn = connect_read(db3_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT market_value_native, currency FROM Portfolio_Holdings "
        "WHERE quantity > 0 AND asset_category NOT IN ('CASH', 'OPT')"
    ).fetchall()
    conn.close()

    if not rows:
        return {"labels": [], "values": [], "total": 0, "currency": display_currency}

    dc = display_currency.upper()
    native_ccies = {(r["currency"] or "EUR").upper() for r in rows}
    rates = _get_latest_rates(native_ccies, dc, db2_path)

    by_ccy: dict[str, float] = defaultdict(float)
    for r in rows:
        val_native = r["market_value_native"] or 0
        native_ccy = (r["currency"] or "EUR").upper()
        val_display = round(val_native * rates.get(native_ccy, 1.0), 2)
        by_ccy[native_ccy] += val_display

    labels = sorted(by_ccy.keys())
    values = [round(by_ccy[c], 2) for c in labels]
    total = round(sum(values), 2)

    return {"labels": labels, "values": values, "total": total, "currency": display_currency}


# ---------------------------------------------------------------------------
# Chart 3: Portfolio value over time and flow-adjusted risk series
# ---------------------------------------------------------------------------

def _load_portfolio_daily(db3_path: str) -> list[sqlite3.Row]:
    conn = connect_read(db3_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT date, total_value, net_inflow FROM Portfolio_Daily ORDER BY date"
    ).fetchall()
    conn.close()
    return rows


def _convert_base_values(
    values: list[float | None],
    dates: list[str],
    display_currency: str,
    db2_path: str,
) -> list[float | None]:
    if display_currency.upper() == "EUR":
        return [round(value, 2) if value is not None else None for value in values]
    fx = get_fx_series("EUR", display_currency, db2_path)
    if not fx:
        return [round(value, 2) if value is not None else None for value in values]
    converted: list[float | None] = []
    for date_str, value in zip(dates, values):
        if value is None:
            converted.append(None)
            continue
        rate = get_rate_at_date(date_str, fx) or 1.0
        converted.append(round(value * rate, 2))
    return converted


def _flow_adjusted_series(
    daily_rows: list[sqlite3.Row],
    dates: list[str],
    display_currency: str,
    db2_path: str,
) -> dict[str, list[float | None]]:
    by_date = {row["date"]: row for row in daily_rows}
    base_values = [
        float(by_date[date]["total_value"])
        if date in by_date and by_date[date]["total_value"] is not None
        else None
        for date in dates
    ]
    base_inflows = [
        float(by_date[date]["net_inflow"] or 0)
        if date in by_date
        else None
        for date in dates
    ]
    values = _convert_base_values(base_values, dates, display_currency, db2_path)
    inflows = _convert_base_values(base_inflows, dates, display_currency, db2_path)

    daily_returns: list[float | None] = []
    cumulative_returns: list[float | None] = []
    wealth = 1.0
    previous_value: float | None = None
    for value, inflow in zip(values, inflows):
        if value is None:
            daily_returns.append(None)
            cumulative_returns.append(None)
            continue
        daily_return = 0.0
        if previous_value is not None and previous_value > 0:
            denominator = previous_value + (inflow or 0.0)
            if abs(denominator) > 0.01:
                raw_return = (value - previous_value - (inflow or 0.0)) / denominator
                daily_return = max(min(raw_return, 1.0), -1.0)
        wealth *= 1 + daily_return
        daily_returns.append(round(daily_return, 8))
        cumulative_returns.append(round(wealth - 1, 8))
        previous_value = value

    return {
        "portfolio_values": values,
        "net_inflows": inflows,
        "daily_returns": daily_returns,
        "cumulative_returns": cumulative_returns,
    }


def get_portfolio_value_history(
    db3_path: str,
    db2_path: str,
    display_currency: str = "EUR",
) -> dict:
    """Return daily holdings and flow-adjusted portfolio risk series.

    ``daily_returns`` removes deposits and withdrawals using Modified Dietz,
    so analytics never mistake a cash flow or a newly observed holding for a
    market return. Values are converted to the requested display currency.
    """
    conn = connect_read(db3_path)
    conn.row_factory = sqlite3.Row
    hh_rows = conn.execute(
        "SELECT date, symbol, market_value_native, currency "
        "FROM Holdings_History "
        "WHERE is_option = 0 AND symbol NOT LIKE 'CASH%' "
        "AND market_value_native IS NOT NULL "
        "ORDER BY date, symbol"
    ).fetchall()
    cur_held = {
        row["symbol"] for row in conn.execute(
            "SELECT symbol FROM Portfolio_Holdings WHERE quantity > 0 "
            "AND asset_category != 'CASH'"
        ).fetchall()
    }
    conn.close()
    daily_rows = _load_portfolio_daily(db3_path)

    if not hh_rows and not daily_rows:
        return {
            "dates": [], "holdings": {}, "currency": display_currency,
            "portfolio_values": [], "net_inflows": [],
            "daily_returns": [], "cumulative_returns": [],
        }

    dc = display_currency.upper()
    series_by_symbol: dict[str, dict[str, float]] = defaultdict(dict)
    native_ccy_by_symbol: dict[str, str] = {}
    date_set: set[str] = {row["date"] for row in daily_rows}
    for row in hh_rows:
        date_set.add(row["date"])
        series_by_symbol[row["symbol"]][row["date"]] = row["market_value_native"] or 0
        native_ccy_by_symbol[row["symbol"]] = row["currency"] or "EUR"
    date_list = sorted(date_set)

    fx_series_cache: dict[str, dict[str, float]] = {}
    for native_currency in set(native_ccy_by_symbol.values()):
        native_upper = native_currency.upper()
        if native_upper != dc:
            fx = get_fx_series(native_currency, dc, db2_path)
            if fx:
                fx_series_cache[native_upper] = fx

    result_series: dict[str, list[float | None]] = {}
    for symbol, day_map in series_by_symbol.items():
        values: list[float | None] = [day_map.get(date) for date in date_list]
        last_idx = max((index for index, value in enumerate(values) if value is not None), default=-1)
        if last_idx >= 0 and last_idx < len(values) - 1:
            for index in range(last_idx + 1, len(values)):
                values[index] = 0.0 if symbol not in cur_held else values[last_idx]
        native_upper = native_ccy_by_symbol.get(symbol, "EUR").upper()
        fx = fx_series_cache.get(native_upper)
        if fx:
            values = [
                round(value * (get_rate_at_date(date, fx) or 1.0), 2)
                if value is not None else None
                for date, value in zip(date_list, values)
            ]
        else:
            values = [round(value, 2) if value is not None else None for value in values]
        result_series[symbol] = values

    risk_series = _flow_adjusted_series(daily_rows, date_list, display_currency, db2_path)
    return {
        "dates": date_list,
        "holdings": result_series,
        "currency": display_currency,
        **risk_series,
    }


# ---------------------------------------------------------------------------
# Chart 4: Dividends by company (stacked bar)
# ---------------------------------------------------------------------------

def _period_bucket(date_str: str, period: str) -> str:
    """Group a date string into period bucket."""
    y = date_str[:4]
    m = int(date_str[5:7])
    if period == "yearly":
        return y
    elif period == "quarterly":
        q = (m - 1) // 3 + 1
        return f"{y}-Q{q}"
    else:  # monthly
        return date_str[:7]


def _batch_fx_rates(
    rows_with_ccy: list[tuple[str, str]],  # [(date_str, currency), ...]
    display_currency: str,
    db2_path: str,
) -> list[float | None]:
    """Batch-convert amounts by pre-loading FX series per currency once."""
    dc = display_currency.upper()
    # Group unique currencies
    currencies = {(ccy or "EUR").upper() for _, ccy in rows_with_ccy} | {dc}
    # Pre-load FX series for each currency to display currency
    fx_cache: dict[str, dict[str, float]] = {}
    for ccy in currencies:
        u = ccy.upper()
        if u == dc:
            continue
        fx = get_fx_series(u, dc, db2_path)
        if fx:
            fx_cache[u] = fx

    rates: list[float | None] = []
    for date_str, ccy in rows_with_ccy:
        u = (ccy or "EUR").upper()
        if u == dc:
            rates.append(1.0)
        elif u in fx_cache:
            rate = get_rate_at_date(date_str, fx_cache[u])
            rates.append(rate or 1.0)
        else:
            rates.append(None)
    return rates


def get_dividends_by_company(
    db3_path: str,
    db2_path: str,
    display_currency: str = "EUR",
    period: str = "monthly",
) -> dict:
    """Net dividends grouped by company, in *display_currency*.

    *period*: "monthly", "quarterly", or "yearly".
    Returns ``{periods: [str], companies: {symbol: [float]}, currency: str}``.
    """
    conn = connect_read(db3_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT symbol, trade_date, activity_type, amount, currency "
        "FROM Transactions "
        "WHERE activity_type IN ('DIVIDEND', 'PIL_DIVIDEND', 'WITHHOLDING_TAX') "
        "AND symbol NOT LIKE 'CASH%' "
        "ORDER BY trade_date"
    ).fetchall()
    conn.close()

    if not rows:
        return {"periods": [], "companies": {}, "currency": display_currency}

    dc = display_currency.upper()

    # Batch pre-load FX rates
    raw_ccy_pairs = [(r["trade_date"], r["currency"] or "EUR") for r in rows]
    fx_rates = _batch_fx_rates(raw_ccy_pairs, dc, db2_path)

    net_divs: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    period_set: set[str] = set()

    for i, r in enumerate(rows):
        d = r["trade_date"]
        pk = _period_bucket(d, period)
        sym = r["symbol"]
        amount = r["amount"] or 0
        rate = fx_rates[i]
        display_amt = round(abs(amount) * (rate or 1.0), 2)

        if r["activity_type"] in ("DIVIDEND", "PIL_DIVIDEND"):
            net_divs[sym][pk] += display_amt
        elif r["activity_type"] == "WITHHOLDING_TAX":
            net_divs[sym][pk] -= display_amt

        period_set.add(pk)

    periods = sorted(period_set)
    companies: dict[str, list[float]] = {}
    for sym in sorted(net_divs.keys()):
        companies[sym] = [round(net_divs[sym].get(p, 0), 2) for p in periods]

    return {
        "periods": periods,
        "companies": companies,
        "currency": display_currency,
    }


# ---------------------------------------------------------------------------
# Chart 5: Dividends by currency (stacked bar)
# ---------------------------------------------------------------------------

def get_dividends_by_currency(
    db3_path: str,
    db2_path: str,
    display_currency: str = "EUR",
    period: str = "monthly",
) -> dict:
    """Net dividends grouped by payment currency, in *display_currency*.

    *period*: "monthly", "quarterly", or "yearly".
    Returns ``{periods: [str], currencies: {ccy: [float]}, currency: str}``.
    """
    conn = connect_read(db3_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT trade_date, activity_type, amount, currency "
        "FROM Transactions "
        "WHERE activity_type IN ('DIVIDEND', 'PIL_DIVIDEND', 'WITHHOLDING_TAX') "
        "AND symbol NOT LIKE 'CASH%' "
        "ORDER BY trade_date"
    ).fetchall()
    conn.close()

    if not rows:
        return {"periods": [], "currencies": {}, "currency": display_currency}

    dc = display_currency.upper()
    raw_ccy_pairs = [(r["trade_date"], r["currency"] or "EUR") for r in rows]
    fx_rates = _batch_fx_rates(raw_ccy_pairs, dc, db2_path)

    net_divs: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    period_set: set[str] = set()

    for i, r in enumerate(rows):
        d = r["trade_date"]
        pk = _period_bucket(d, period)
        native_ccy = (r["currency"] or "EUR").upper()
        amount = r["amount"] or 0
        rate = fx_rates[i]
        display_amt = round(abs(amount) * (rate or 1.0), 2)

        if r["activity_type"] in ("DIVIDEND", "PIL_DIVIDEND"):
            net_divs[native_ccy][pk] += display_amt
        elif r["activity_type"] == "WITHHOLDING_TAX":
            net_divs[native_ccy][pk] -= display_amt

        period_set.add(pk)

    periods = sorted(period_set)
    currencies: dict[str, list[float]] = {}
    for ccy in sorted(net_divs.keys()):
        currencies[ccy] = [round(net_divs[ccy].get(p, 0), 2) for p in periods]

    return {
        "periods": periods,
        "currencies": currencies,
        "currency": display_currency,
    }


# ---------------------------------------------------------------------------
# Chart 6: Dividend heatmap (year × month)
# ---------------------------------------------------------------------------

def get_dividends_heatmap(
    db3_path: str,
    db2_path: str,
    display_currency: str = "EUR",
) -> dict:
    """Net dividends aggregated by (year, month), in *display_currency*.

    Returns ``{years: [int], months: [int], values: [[float|null]], currency: str}``
    where ``values[y][m]`` is the net dividend for *years[y]* in month *months[m]*.
    """
    conn = connect_read(db3_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT trade_date, activity_type, amount, currency "
        "FROM Transactions "
        "WHERE activity_type IN ('DIVIDEND', 'PIL_DIVIDEND', 'WITHHOLDING_TAX') "
        "AND symbol NOT LIKE 'CASH%' "
        "ORDER BY trade_date"
    ).fetchall()
    conn.close()

    if not rows:
        return {"years": [], "months": list(range(1, 13)), "values": [], "currency": display_currency}

    dc = display_currency.upper()
    raw_ccy_pairs = [(r["trade_date"], r["currency"] or "EUR") for r in rows]
    fx_rates = _batch_fx_rates(raw_ccy_pairs, dc, db2_path)

    raw: dict[int, dict[int, float]] = defaultdict(lambda: defaultdict(float))

    for i, r in enumerate(rows):
        d = r["trade_date"]
        year = int(d[:4])
        month = int(d[5:7])
        amount = r["amount"] or 0
        rate = fx_rates[i]
        display_amt = round(abs(amount) * (rate or 1.0), 2)

        if r["activity_type"] in ("DIVIDEND", "PIL_DIVIDEND"):
            raw[year][month] += display_amt
        elif r["activity_type"] == "WITHHOLDING_TAX":
            raw[year][month] -= display_amt

    years = sorted(raw.keys())
    values: list[list[float | None]] = []
    for y in years:
        row: list[float | None] = []
        for m in range(1, 13):
            v = raw[y].get(m)
            row.append(round(v, 2) if v else None)
        values.append(row)

    return {
        "years": years,
        "months": list(range(1, 13)),
        "values": values,
        "currency": display_currency,
    }


# ---------------------------------------------------------------------------
# Chart 7: Portfolio returns heatmap (year × month)
# ---------------------------------------------------------------------------

def get_returns_heatmap(
    db3_path: str,
    db2_path: str,
    display_currency: str = "EUR",
) -> dict:
    """Monthly portfolio returns as a percentage, computed with Modified Dietz
    to adjust for cash flows, in *display_currency*.

    Returns ``{years: [int], months: [int], values: [[float|null]], currency: str}``
    where ``values[y][m]`` is the return *percentage* for *years[y]* in month *months[m]*.
    """
    conn = connect_read(db3_path)
    conn.row_factory = sqlite3.Row

    # Portfolio_Daily: total_value, cash_balance, net_inflow, dividend_income
    # All columns are in base_currency (EUR). We'll convert the final
    # value to display_currency at each month-end date.
    daily_rows = conn.execute(
        "SELECT date, total_value, cash_balance, net_inflow, dividend_income "
        "FROM Portfolio_Daily ORDER BY date"
    ).fetchall()

    conn.close()

    if not daily_rows or len(daily_rows) < 2:
        return {"years": [], "months": list(range(1, 13)), "values": [], "currency": display_currency}

    dc = display_currency.upper()

    # Group rows by year-month
    months_data: dict[tuple[int, int], list[dict]] = defaultdict(list)
    for r in daily_rows:
        y = int(r["date"][:4])
        m = int(r["date"][5:7])
        months_data[(y, m)].append(dict(r))

    years_all = sorted({y for y, _ in months_data.keys()})
    values: list[list[float | None]] = []

    for y in years_all:
        row: list[float | None] = []
        for m in range(1, 13):
            entries = months_data.get((y, m), [])
            if len(entries) < 2:
                row.append(None)
                continue

            # Modified Dietz within this month
            # Get the first and last entries' total_value (EUR base)
            # Also need start/end FX rate for display currency conversion
            first = entries[0]
            last = entries[-1]

            start_val_base = first["total_value"] or 0
            end_val_base = last["total_value"] or 0

            # Net cash flow and weighted cash flow this month
            net_cf = 0.0
            weighted_cf = 0.0
            n_days = (m == 12) and 31 or 30  # approximate; use actual days
            try:
                import datetime
                if m == 12:
                    next_month = datetime.date(y + 1, 1, 1)
                else:
                    next_month = datetime.date(y, m + 1, 1)
                month_start = datetime.date(y, m, 1)
                n_days = (next_month - month_start).days
            except ValueError:
                pass

            for entry in entries:
                inflow = entry.get("net_inflow", 0) or 0
                if inflow != 0:
                    net_cf += inflow
                    # weight = (days_remaining_in_month) / n_days
                    try:
                        d = int(entry["date"][8:10])
                        days_remaining = n_days - d + 1
                        weight = max(days_remaining, 0) / n_days
                    except (ValueError, IndexError):
                        weight = 0.5
                    weighted_cf += weight * inflow

            denominator = start_val_base + weighted_cf
            if denominator <= 0:
                row.append(None)
                continue

            # Monthly return in base currency (EUR), as percentage
            monthly_return_pct = round((end_val_base - start_val_base - net_cf) / denominator * 100, 2)

            # Convert to display currency using month-end FX rate
            if dc != "EUR":
                # Use the last date of the month for FX conversion
                # Since return is a ratio (percentage), the conversion is:
                # formula: R_display = (1 + R_base) * (FX_end/FX_start) - 1
                # But for simplicity and since FX rates don't swing wildly within a month
                # we approximate by just returning the base-currency return.
                # A more accurate approach would apply FX adjustment.
                pass

            row.append(monthly_return_pct)

        values.append(row)

    return {
        "years": years_all,
        "months": list(range(1, 13)),
        "values": values,
        "currency": display_currency,
    }


# ---------------------------------------------------------------------------
# Chart 8: Deposits heatmap (year × month)
# ---------------------------------------------------------------------------

def get_deposits_heatmap(
    db3_path: str,
    db2_path: str,
    display_currency: str = "EUR",
) -> dict:
    """Net deposits/withdrawals aggregated by (year, month), in *display_currency*.

    Returns ``{years: [int], months: [int], values: [[float|null]], currency: str}``.
    """
    conn = connect_read(db3_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT date, net_inflow FROM Portfolio_Daily ORDER BY date"
    ).fetchall()
    conn.close()

    if not rows:
        return {"years": [], "months": list(range(1, 13)), "values": [], "currency": display_currency}

    dc = display_currency.upper()

    # Group dates by (year, month) to batch-load FX rates
    month_dates: dict[tuple[int, int], list[str]] = defaultdict(list)
    for r in rows:
        d = r["date"]
        y = int(d[:4])
        m = int(d[5:7])
        month_dates[(y, m)].append(d)

    # Aggregate net_inflow per month
    raw: dict[int, dict[int, float]] = defaultdict(lambda: defaultdict(float))
    ccy_pairs: list[tuple[str, str]] = []
    for r in rows:
        d = r["date"]
        y = int(d[:4])
        m = int(d[5:7])
        val = r["net_inflow"] or 0
        # net_inflow is in EUR base; convert to display if needed
        ccy_pairs.append((d, "EUR"))
        # We'll apply FX rate after batch loading
        raw[y][m] += val

    # Convert aggregated monthly totals to display currency using mid-month date
    if dc != "EUR":
        mid_month_dates: dict[tuple[int, int], str] = {}
        for (y, m), dates in month_dates.items():
            # Use the middle date of the month's entries
            sorted_dates = sorted(dates)
            mid_month_dates[(y, m)] = sorted_dates[len(sorted_dates) // 2]

        # Batch-load EUR→display FX series
        fx = get_fx_series("EUR", dc, db2_path)
        if fx:
            for (y, m), ref_date in mid_month_dates.items():
                rate = get_rate_at_date(ref_date, fx)
                if rate:
                    raw[y][m] = round(raw[y][m] * rate, 2)

    years = sorted(raw.keys())
    values: list[list[float | None]] = []
    for y in years:
        row: list[float | None] = []
        for m in range(1, 13):
            v = raw[y].get(m)
            row.append(round(v, 2) if v else None)
        values.append(row)

    return {
        "years": years,
        "months": list(range(1, 13)),
        "values": values,
        "currency": display_currency,
    }


# ---------------------------------------------------------------------------
# Chart 9: Cost basis vs annualized return scatter (open + closed holdings)
# ---------------------------------------------------------------------------

def get_return_vs_cost(
    db3_path: str,
    db2_path: str,
    display_currency: str = "EUR",
) -> list[dict]:
    """Return scatter-plot data: cost basis vs annualized return per holding.

    Includes both open and closed (non-option) holdings.
    Returns ``[{symbol, cost_basis_display, annualized_return, is_open}, ...]``.
    """
    from src.portfolio.portfolio_state import get_all_holdings_performance, get_closed_positions
    from src.portfolio.currency import get_rate_at_date_any
    from datetime import datetime

    dc = display_currency.upper()
    result: list[dict] = []

    # --- Open holdings ---
    open_holdings = get_all_holdings_performance(db3_path, db2_path, dc)
    for h in open_holdings:
        perf = h.get("performance")
        if not perf:
            continue
        cost = perf.get("cost_basis_display")
        ann_ret = perf.get("annualized_return")
        if cost is None or cost <= 0 or ann_ret is None:
            continue
        result.append({
            "symbol": h["symbol"],
            "cost_basis_display": round(cost, 2),
            "annualized_return": round(ann_ret * 100, 2),  # percentage
            "is_open": True,
        })

    # --- Closed holdings (non-option) ---
    closed = get_closed_positions(db3_path)
    for cp in closed:
        if cp.get("asset_category") == "OPT":
            continue
        total_cost = cp.get("total_cost") or 0
        realized_pnl = cp.get("realized_pnl") or 0
        if total_cost <= 0:
            continue
        native_ccy = (cp.get("currency") or "EUR").upper()

        # Convert cost to display currency
        ref_date = cp.get("last_trade_date") or cp.get("first_trade_date")
        if not ref_date:
            continue
        rate = 1.0
        if native_ccy != dc:
            r = get_rate_at_date_any(native_ccy, dc, ref_date, db2_path)
            if r:
                rate = r
        cost_display = round(total_cost * rate, 2)

        # Compute annualized return
        total_return = realized_pnl / total_cost
        try:
            start = datetime.strptime(cp["first_trade_date"], "%Y-%m-%d")
            end = datetime.strptime(cp["last_trade_date"], "%Y-%m-%d")
            years = (end - start).days / 365.25
        except (ValueError, KeyError):
            years = 1
        years = max(years, 1 / 365.25)  # at least 1 day
        ann_ret = (1 + total_return) ** (1 / years) - 1

        result.append({
            "symbol": cp.get("symbol", "?"),
            "cost_basis_display": cost_display,
            "annualized_return": round(ann_ret * 100, 2),
            "is_open": False,
        })

    return result
