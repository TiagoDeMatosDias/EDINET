"""Portfolio performance metrics: Sharpe, Sortino, drawdowns, benchmark comparison.

Reads daily return series from ``Portfolio_Daily`` (produced by
``portfolio_state.build_portfolio_state``) and computes risk/return
statistics.  Also compares against a user-selected benchmark ticker.
"""

from __future__ import annotations

import logging
import sqlite3
import numpy as np
from scipy import stats as scipy_stats

from src.orchestrator.common.db_config import get_db2, get_db3
from src.orchestrator.common.sqlite import connect_read
from src.portfolio.portfolio_state import get_daily_values

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Risk-free rate
# ---------------------------------------------------------------------------

def get_risk_free_rate(db2_path: str | None = None, base_currency: str = "EUR") -> float:
    """Look up the risk-free rate from inflation data in db2.

    Tries ``Inflation_{CUR}`` ticker (e.g. ``Inflation_EUR``). Computes the
    latest 12-month annualised inflation rate. Falls back to 2%.
    """
    db2_path = db2_path or get_db2()
    table = "Stock_Prices"
    ticker = f"Inflation_{base_currency.upper()}"

    try:
        conn = connect_read(db2_path)
        row = conn.execute(
            f"SELECT Date, Price FROM {table} WHERE Ticker = ? ORDER BY Date DESC LIMIT 13",
            (ticker,),
        ).fetchall()
        conn.close()

        if len(row) < 13:
            logger.info("Not enough inflation data for %s, using 2%% default", ticker)
            return 0.02

        latest_price = row[0][1]
        year_ago_price = row[12][1]
        if year_ago_price and year_ago_price > 0:
            annual_rate = (latest_price / year_ago_price) - 1
            logger.info("Auto-detected risk-free rate: %.4f%%", annual_rate * 100)
            return max(annual_rate, 0.0)  # negative inflation = 0% floor
    except (sqlite3.OperationalError, IndexError):
        pass

    logger.info("Could not compute risk-free rate from db2, using 2%% default")
    return 0.02


# ---------------------------------------------------------------------------
# Individual metrics
# ---------------------------------------------------------------------------

def _annualize_return(total_return: float, years: float) -> float:
    """Convert cumulative total return to annualised."""
    if years <= 0:
        return total_return
    if total_return <= -1:
        return -1.0
    return (1 + total_return) ** (1 / years) - 1


def sharpe_ratio(daily_returns: list[float], rf_annual: float) -> float:
    """Annualised Sharpe ratio."""
    if len(daily_returns) < 2:
        return 0.0
    rf_daily = (1 + rf_annual) ** (1 / 252) - 1
    excess = np.array(daily_returns) - rf_daily
    mean_excess = np.mean(excess)
    std_excess = np.std(excess, ddof=1)
    if std_excess == 0:
        return 0.0
    return float(mean_excess / std_excess * np.sqrt(252))


def sortino_ratio(daily_returns: list[float], rf_annual: float) -> float:
    """Annualised Sortino ratio (downside deviation only)."""
    if len(daily_returns) < 2:
        return 0.0
    rf_daily = (1 + rf_annual) ** (1 / 252) - 1
    returns = np.array(daily_returns)
    excess = returns - rf_daily
    downside = np.minimum(excess, 0)
    downside_std = np.std(downside, ddof=1)
    if downside_std == 0:
        return 0.0
    mean_excess = np.mean(excess)
    return float(mean_excess / downside_std * np.sqrt(252))


def max_drawdown(cumulative: list[float]) -> tuple[float, str | None, str | None]:
    """Compute maximum drawdown and peak/trough dates from a cumulative value series.

    Args:
        cumulative: list of cumulative portfolio values.

    Returns:
        ``(max_drawdown, peak_date, trough_date)`` where max_drawdown is negative.
    """
    if not cumulative or len(cumulative) < 2:
        return 0.0, None, None

    max_dd = 0.0
    peak_idx = 0
    trough_idx = 0
    current_peak_idx = 0

    for i in range(1, len(cumulative)):
        if cumulative[i] > cumulative[current_peak_idx]:
            current_peak_idx = i
        dd = (cumulative[i] - cumulative[current_peak_idx]) / cumulative[current_peak_idx]
        if dd < max_dd:
            max_dd = dd
            peak_idx = current_peak_idx
            trough_idx = i

    return max_dd, str(peak_idx), str(trough_idx)


def max_drawdown_with_dates(values: list[float], dates: list[str]) -> tuple[float, str | None, str | None]:
    """Compute max drawdown and return actual date strings."""
    dd_val, peak_i_str, trough_i_str = max_drawdown(values)
    peak_date = dates[int(peak_i_str)] if peak_i_str is not None else None
    trough_date = dates[int(trough_i_str)] if trough_i_str is not None else None
    return dd_val, peak_date, trough_date


def calmar_ratio(ann_return: float, max_dd: float) -> float:
    """Annualised return / |max drawdown|."""
    if max_dd == 0 or max_dd >= 0:
        return 0.0
    return ann_return / abs(max_dd)


def win_rate(daily_returns: list[float]) -> float:
    """Fraction of days with positive returns (excluding zero-return days)."""
    if not daily_returns:
        return 0.0
    wins = sum(1 for r in daily_returns if r > 0)
    losses = sum(1 for r in daily_returns if r < 0)
    total = wins + losses
    return wins / total if total > 0 else 0.0


def avg_win(daily_returns: list[float]) -> float:
    """Average positive daily return."""
    wins = [r for r in daily_returns if r > 0]
    return float(np.mean(wins)) if wins else 0.0


def avg_loss(daily_returns: list[float]) -> float:
    """Average negative daily return."""
    losses = [r for r in daily_returns if r < 0]
    return float(np.mean(losses)) if losses else 0.0


def profit_factor(daily_returns: list[float]) -> float:
    """Gross profit / |gross loss|."""
    wins = sum(r for r in daily_returns if r > 0)
    losses = abs(sum(r for r in daily_returns if r < 0))
    return wins / losses if losses > 0 else float("inf") if wins > 0 else 0.0


def var_historical(daily_returns: list[float], confidence: float = 0.95) -> float:
    """Historical VaR at given confidence level (returns negative number)."""
    if len(daily_returns) < 2:
        return 0.0
    return float(np.percentile(daily_returns, (1 - confidence) * 100))


def cvar_historical(daily_returns: list[float], confidence: float = 0.95) -> float:
    """Historical CVaR (expected shortfall beyond VaR)."""
    if len(daily_returns) < 2:
        return 0.0
    var_val = var_historical(daily_returns, confidence)
    tail = [r for r in daily_returns if r <= var_val]
    return float(np.mean(tail)) if tail else var_val


# ---------------------------------------------------------------------------
# Benchmark comparison
# ---------------------------------------------------------------------------

def _get_benchmark_returns(
    db2_path: str,
    benchmark_ticker: str,
    dates: list[str],
    base_currency: str = "EUR",
) -> tuple[list[float] | None, str | None]:
    """Retrieve price series for a benchmark ticker, convert to base
    currency, and compute daily returns.

    Returns:
        ``(returns_list, currency_used)`` — returns_list is None if
        insufficient data.  currency_used is the currency the returns
        are denominated in (after conversion to base_currency).
    """
    if not dates:
        return None, None

    conn = connect_read(db2_path)
    min_date = dates[0]
    max_date = dates[-1]

    rows = conn.execute(
        "SELECT Date, Price, Currency FROM Stock_Prices WHERE Ticker = ? "
        "AND Date >= ? AND Date <= ? ORDER BY Date",
        (benchmark_ticker, min_date, max_date),
    ).fetchall()

    if len(rows) < 2:
        conn.close()
        logger.warning("Insufficient benchmark data for %s", benchmark_ticker)
        return None, None

    bench_currency = rows[0][2] or "???"

    # Build price map
    price_map: dict[str, float] = {row[0]: row[1] for row in rows}

    # If benchmark currency differs from base, fetch FX rates and convert.
    # FX data stored as Ticker='EUR', Currency=XXX with Price = XXX per 1 EUR.
    if bench_currency != base_currency:
        if base_currency == "EUR":
            # bench_price / EUR{bench} = EUR_price
            fx_rows = conn.execute(
                "SELECT Date, Price FROM Stock_Prices "
                "WHERE Ticker = 'EUR' AND Currency = ? "
                "AND Date >= ? AND Date <= ? ORDER BY Date",
                (bench_currency, min_date, max_date),
            ).fetchall()
            fx_map: dict[str, float] = {row[0]: row[1] for row in fx_rows}
        else:
            # bench_ccy → EUR → base_ccy: cross_rate = EUR{base} / EUR{bench}
            fx_bench_rows = conn.execute(
                "SELECT Date, Price FROM Stock_Prices "
                "WHERE Ticker = 'EUR' AND Currency = ? "
                "AND Date >= ? AND Date <= ? ORDER BY Date",
                (bench_currency, min_date, max_date),
            ).fetchall()
            fx_base_rows = conn.execute(
                "SELECT Date, Price FROM Stock_Prices "
                "WHERE Ticker = 'EUR' AND Currency = ? "
                "AND Date >= ? AND Date <= ? ORDER BY Date",
                (base_currency, min_date, max_date),
            ).fetchall()
            fx_bench_map = {row[0]: row[1] for row in fx_bench_rows}
            fx_base_map = {row[0]: row[1] for row in fx_base_rows}
            all_d = sorted(set(fx_bench_map.keys()) | set(fx_base_map.keys()))
            fx_map = {}
            last_b: float | None = None
            last_b2: float | None = None
            for d in all_d:
                if d in fx_bench_map:
                    last_b = fx_bench_map[d]
                if d in fx_base_map:
                    last_b2 = fx_base_map[d]
                if last_b and last_b2 and last_b > 0:
                    fx_map[d] = last_b2 / last_b
    else:
        fx_map = {}

    conn.close()

    # Build price series in base currency with forward-fill
    prices_base: list[float] = []
    last_price = None
    last_fx = None
    for d in dates:
        p = price_map.get(d)
        if p is not None:
            last_price = p
        if last_price is None:
            prices_base.append(0.0)
            continue

        # Apply FX conversion if needed
        if fx_map:
            fx = fx_map.get(d)
            if fx is not None:
                last_fx = fx
            if last_fx and last_fx > 0:
                # ECB rate: 1 EUR = X USD, so USD price / rate = EUR price
                p_conv = last_price / last_fx
            else:
                p_conv = last_price  # no FX data yet, use raw
        else:
            p_conv = last_price
        prices_base.append(p_conv)

    # Compute daily returns from converted prices
    bench_returns = []
    for i in range(1, len(prices_base)):
        if prices_base[i-1] > 0:
            bench_returns.append(prices_base[i] / prices_base[i-1] - 1)
        else:
            bench_returns.append(0.0)

    return bench_returns, base_currency


def compare_to_benchmark(
    portfolio_returns: list[float],
    benchmark_returns: list[float],
    rf_annual: float = 0.0,
) -> dict:
    """Compare portfolio vs benchmark: excess return, alpha, beta, IR, TE."""
    if not benchmark_returns or len(portfolio_returns) != len(benchmark_returns):
        return {}

    pr = np.array(portfolio_returns)
    br = np.array(benchmark_returns)

    excess_daily = pr - br
    ann_excess = np.mean(excess_daily) * 252

    # Beta
    cov = np.cov(pr, br)
    if cov.shape == (2, 2) and cov[1, 1] > 0:
        beta = cov[0, 1] / cov[1, 1]
    else:
        beta = 1.0

    # Alpha (annualised, Jensen's Alpha): α = R_p - R_f - β*(R_m - R_f)
    # ann_excess = R_p - R_m (annualised)
    # Expanding: α = (R_p - R_m) - (β-1)*R_m - (1-β)*R_f
    #          = ann_excess - (β-1)*R_m_ann - (1-β)*R_f
    alpha = ann_excess - (beta - 1) * np.mean(br) * 252 - (1 - beta) * rf_annual

    # Tracking error
    if len(excess_daily) >= 2:
        te = float(np.std(excess_daily, ddof=1) * np.sqrt(252))
    else:
        te = 0.0

    # Information ratio
    ir = ann_excess / te if te > 0 else 0.0

    return {
        "excess_return": float(ann_excess),
        "alpha": float(alpha),
        "beta": float(beta),
        "information_ratio": float(ir),
        "tracking_error": float(te),
    }


# ---------------------------------------------------------------------------
# Main calculation
# ---------------------------------------------------------------------------

def calculate_metrics(
    db3_path: str | None = None,
    db2_path: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    risk_free_rate: float | None = None,
    benchmark_ticker: str | None = None,
    base_currency: str = "EUR",
) -> dict:
    """Compute comprehensive performance metrics for the portfolio.

    Args:
        db3_path, db2_path: Database paths (defaults from config).
        start_date, end_date: Date range (defaults to full history).
        risk_free_rate: Annual decimal rate (None = auto-detect from inflation).
        benchmark_ticker: Ticker to compare against (None = skip benchmark).
        base_currency: Currency for risk-free rate detection.

    Returns:
        Performance dict matching ``PerformanceResponse`` Pydantic shape.
    """
    db3_path = db3_path or get_db3()
    db2_path = db2_path or get_db2()

    daily = get_daily_values(db3_path, start_date, end_date)
    if not daily:
        return {
            "start_date": start_date or "", "end_date": end_date or "",
            "base_currency": base_currency,
        }

    dates = [d["date"] for d in daily]
    values = [d["total_value"] or 0 for d in daily]
    cash_flows = [d.get("net_inflow") or 0 for d in daily]
    crs = [d.get("cumulative_return") or 0 for d in daily]
    returns = [d.get("daily_return") or 0 for d in daily]

    # ── Currency conversion ──
    # Portfolio_Daily stores everything in EUR. When the user requests a
    # different base currency, convert each day's total_value and net_inflow
    # using the FX rate on that day, then recompute daily/cumulative returns.
    if base_currency != "EUR":
        import sqlite3 as _sql_fx
        _fx_conn = _sql_fx.connect(db2_path)
        _fx_rows = _fx_conn.execute(
            "SELECT Date, Price FROM Stock_Prices "
            "WHERE Ticker = 'EUR' AND Currency = ? ORDER BY Date",
            (base_currency,),
        ).fetchall()
        _fx_conn.close()
        if _fx_rows and len(_fx_rows) >= 2:
            # Build forward-fill FX map
            _fx_dates = [r[0] for r in _fx_rows]
            _fx_prices = [r[1] for r in _fx_rows]
            _last_fx = _fx_prices[0] if _fx_prices[0] and _fx_prices[0] > 0 else 1.0
            _fx_idx = 0
            _values_converted: list[float] = []
            _inflows_converted: list[float] = []
            for d in dates:
                while _fx_idx < len(_fx_dates) and _fx_dates[_fx_idx] <= d:
                    _candidate = _fx_prices[_fx_idx]
                    if _candidate and _candidate > 0:
                        _last_fx = _candidate
                    _fx_idx += 1
                _fx = _last_fx if _last_fx > 0 else 1.0
                _values_converted.append(values[len(_values_converted)] * _fx)
                _inflows_converted.append(cash_flows[len(_inflows_converted)] * _fx)
            # Recompute returns from converted values (Modified Dietz)
            _converted_returns: list[float] = []
            _cum_series: list[float] = []
            _running_cum = 1.0
            _prev_value = 0.0
            for _i, _val in enumerate(_values_converted):
                if _prev_value > 0:
                    _denom = _prev_value + _inflows_converted[_i]
                    if abs(_denom) > 0.01:
                        _dr = (_val - _prev_value - _inflows_converted[_i]) / _denom
                        _dr = max(min(_dr, 1.0), -1.0)
                    else:
                        _dr = 0.0
                    _running_cum *= (1 + _dr)
                else:
                    _dr = 0.0
                _converted_returns.append(_dr)
                _cum_series.append(round(_running_cum - 1, 6))
                _prev_value = _val
            values = _values_converted
            cash_flows = _inflows_converted
            crs = _cum_series
            returns = _converted_returns
            logger.info("Converted portfolio values from EUR to %s using %s",
                        base_currency, _fx_ticker)

    # Total return from stored cumulative return (time-weighted).
    # crs[-1] = Π(1+r_i) - 1 → total_return = crs[-1]
    total_return = crs[-1] if crs else 0.0

    # Clean daily returns: skip days where portfolio hasn't started yet
    # (i.e. first days with all zeros before first transaction)
    first_real = next((i for i, v in enumerate(values) if v > 0), None)
    if first_real is not None and first_real + 1 < len(returns):
        returns_clean = returns[first_real + 1:]  # skip first real day (no return)
    else:
        returns_clean = returns

    if not returns_clean:
        returns_clean = [0.0]

    rf = risk_free_rate if risk_free_rate is not None else get_risk_free_rate(db2_path, base_currency)

    # Time-based metrics
    years = len(returns_clean) / 252 if returns_clean else 0
    # total_return already computed above from cumulative_return
    ann_return = _annualize_return(total_return, max(years, 0.01))
    vol = float(np.std(returns_clean, ddof=1) * np.sqrt(252)) if len(returns_clean) >= 2 else 0

    # Drawdown
    dd_val, peak_date, trough_date = max_drawdown_with_dates(values, dates)

    # Dividend breakdown — from raw transactions for accuracy
    # (Portfolio_Daily nets gross+tax, so we need source data)
    import sqlite3 as _sql
    conn3 = _sql.connect(db3_path)
    conn3.row_factory = _sql.Row
    div_rows = conn3.execute(
        "SELECT activity_type, amount, fx_rate_to_base FROM Transactions "
        "WHERE activity_type IN ('DIVIDEND', 'PIL_DIVIDEND', 'WITHHOLDING_TAX')"
        + (" AND trade_date >= ? AND trade_date <= ?" if start_date and end_date else ""),
        tuple(filter(None, [start_date, end_date]))
    ).fetchall()
    conn3.close()
    div_gross = sum(
        abs((r["amount"] or 0) * (r["fx_rate_to_base"] or 1))
        for r in div_rows if r["activity_type"] in ("DIVIDEND", "PIL_DIVIDEND")
    )
    div_tax = sum(
        (r["amount"] or 0) * (r["fx_rate_to_base"] or 1)
        for r in div_rows if r["activity_type"] == "WITHHOLDING_TAX"
    )
    div_net = div_gross + div_tax

    # ── Convert dividends to target currency ──
    # Transaction amounts were converted to EUR via fx_rate_to_base.
    # If the user wants a different base currency, convert the totals.
    if base_currency != "EUR":
        # Use the last FX rate (latest available) as conversion factor.
        # This is an approximation — ideally each dividend would be
        # converted at its payment-date FX rate, but the error from
        # using a single rate on total sums is within ±1% for moderate
        # currency moves over typical portfolio lifetimes.
        import sqlite3 as _sql_fx2
        _fx_conn2 = _sql_fx2.connect(db2_path)
        _fx_latest = _fx_conn2.execute(
            "SELECT Price FROM Stock_Prices "
            "WHERE Ticker = 'EUR' AND Currency = ? ORDER BY Date DESC LIMIT 1",
            (base_currency,),
        ).fetchone()
        _fx_conn2.close()
        _fx_factor = (_fx_latest[0] if _fx_latest and _fx_latest[0] and _fx_latest[0] > 0 else 1.0)
        div_gross *= _fx_factor
        div_tax *= _fx_factor
        div_net *= _fx_factor

    result = {
        "start_date": dates[0],
        "end_date": dates[-1],
        "base_currency": base_currency,
        "total_return": float(total_return),
        "annualized_return": float(ann_return),
        "volatility": float(vol),
        "sharpe_ratio": float(sharpe_ratio(returns_clean, rf)),
        "sortino_ratio": float(sortino_ratio(returns_clean, rf)),
        "max_drawdown": float(dd_val),
        "max_dd_peak_date": peak_date,
        "max_dd_trough_date": trough_date,
        "calmar_ratio": float(calmar_ratio(ann_return, dd_val)),
        "win_rate": float(win_rate(returns_clean)),
        "avg_win": float(avg_win(returns_clean)),
        "avg_loss": float(avg_loss(returns_clean)),
        "profit_factor": float(profit_factor(returns_clean)) if profit_factor(returns_clean) != float("inf") else 999.0,
        "var_95": float(var_historical(returns_clean)),
        "cvar_95": float(cvar_historical(returns_clean)),
        "total_dividend_income": float(div_net),
        "risk_free_rate": float(rf),
        "dividend_breakdown": {
            "total_gross": float(div_gross),
            "total_tax": float(div_tax),
            "total_net": float(div_net),
        },
    }

    # Benchmark comparison
    bench_series: list[dict] = []
    if benchmark_ticker:
        bench_returns, bench_ccy = _get_benchmark_returns(
            db2_path, benchmark_ticker, dates, base_currency,
        )
        if bench_returns and len(bench_returns) > len(returns_clean):
            bench_returns = bench_returns[-len(returns_clean):]
        if bench_returns and len(bench_returns) == len(returns_clean):
            cmp = compare_to_benchmark(returns_clean, bench_returns, rf)
            # Compute benchmark cumulative return series (aligned with dates)
            bench_cum = 0.0
            # Skip dates[0] — benchmark returns start from dates[1]
            for i, br in enumerate(bench_returns):
                bench_cum = (1 + bench_cum) * (1 + br) - 1
                date_idx = first_real + 1 + i if first_real is not None else 1 + i
                if date_idx < len(dates):
                    bench_series.append({
                        "date": dates[date_idx],
                        "cumulative_return": round(bench_cum, 6),
                    })
            total_bench_return = float(
                np.prod([1 + r for r in bench_returns]) - 1
                if bench_returns else 0
            )
            result["benchmark"] = {
                "ticker": benchmark_ticker,
                "total_return": float(total_bench_return),
                "series": bench_series,
                **cmp,
            }
        else:
            result["benchmark"] = {"ticker": benchmark_ticker, "series": []}

    # --- Portfolio cumulative return series (for equity chart) ---
    # Already computed in target currency above (crs).  Align with dates
    # so the frontend equity chart can use a single data source for
    # portfolio, benchmark, and inflation lines.
    portfolio_series: list[dict] = []
    for i, d in enumerate(dates):
        cr_val = crs[i] if i < len(crs) else 0.0
        # Skip leading zeros before portfolio has any value
        if i == 0 or values[i] > 0:
            portfolio_series.append({
                "date": d,
                "cumulative_return": round(float(cr_val), 6),
                "total_value": round(float(values[i]), 2),
            })
    result["series"] = portfolio_series

    # --- Inflation series (for chart) ---
    import sqlite3 as _sql2
    from datetime import date as _date
    inflation_series: list[dict] = []
    inf_ticker = f"Inflation_{base_currency.upper()}"
    try:
        conn_inf = _sql2.connect(db2_path)
        inf_rows = conn_inf.execute(
            "SELECT Date, Price FROM Stock_Prices WHERE Ticker = ? ORDER BY Date",
            (inf_ticker,),
        ).fetchall()
        conn_inf.close()
        if inf_rows and len(inf_rows) >= 2:
            inf_dates = [r[0] for r in inf_rows]
            inf_prices = [r[1] for r in inf_rows]
            # Pin base to the portfolio's first date (forward-fill to find
            # the inflation level when the portfolio started).
            base_price: float | None = None
            inf_idx = 0
            for d in dates:
                while inf_idx < len(inf_dates) and inf_dates[inf_idx] <= d:
                    base_price = inf_prices[inf_idx]
                    inf_idx += 1
                if base_price is not None:
                    break
            if base_price and base_price > 0:
                inf_idx = 0
                last_cpi_price = base_price
                last_cpi_date_str: str | None = None
                for d in dates:
                    current_d = _date.fromisoformat(d)
                    # Advance to most recent CPI data point <= d
                    while inf_idx < len(inf_dates) and inf_dates[inf_idx] <= d:
                        last_cpi_price = inf_prices[inf_idx]
                        last_cpi_date_str = inf_dates[inf_idx]
                        inf_idx += 1
                    # Check staleness: if last CPI data is >6 months behind
                    # today's date, compound forward using the risk-free rate
                    # as expected monthly inflation (instead of forward-fill
                    # which would show near-zero inflation).
                    if last_cpi_date_str:
                        last_cpi_d = _date.fromisoformat(last_cpi_date_str)
                        months_gap = (current_d.year - last_cpi_d.year) * 12 + (current_d.month - last_cpi_d.month)
                        if months_gap > 6 and current_d > last_cpi_d:
                            # Compound forward at monthly expected inflation.
                            # Use rf as the best estimate, but floor at 1%/yr
                            # for countries with stale/deflationary CPI data.
                            rf_effective = max(rf, 0.01)
                            rf_monthly = (1 + rf_effective) ** (1 / 12) - 1
                            proj_price = last_cpi_price * (1 + rf_monthly) ** max(months_gap, 0)
                            infl_cum = proj_price / base_price - 1
                        else:
                            infl_cum = last_cpi_price / base_price - 1
                    else:
                        infl_cum = last_cpi_price / base_price - 1
                    inflation_series.append({
                        "date": d,
                        "cumulative": round(infl_cum, 6),
                    })
    except Exception as exc:
        logger.warning("Could not compute inflation series for %s: %s", base_currency, exc)
    result["inflation_series"] = inflation_series

    # --- Inflation total (for real return) ---
    inflation_total = 0.0
    if inflation_series:
        inflation_total = inflation_series[-1]["cumulative"] if inflation_series else 0.0
    else:
        # Fallback: use risk-free rate compounded over the period
        if years > 0:
            inflation_total = (1 + rf) ** years - 1

    # --- Return distribution ---
    returns_arr = np.array(returns_clean)
    result["return_distribution"] = {
        "min": float(np.min(returns_arr)) if len(returns_arr) else 0,
        "p25": float(np.percentile(returns_arr, 25)) if len(returns_arr) else 0,
        "median": float(np.median(returns_arr)) if len(returns_arr) else 0,
        "p75": float(np.percentile(returns_arr, 75)) if len(returns_arr) else 0,
        "max": float(np.max(returns_arr)) if len(returns_arr) else 0,
        "skewness": float(scipy_stats.skew(returns_arr)) if len(returns_arr) >= 3 else 0,
        "kurtosis": float(scipy_stats.kurtosis(returns_arr)) if len(returns_arr) >= 4 else 0,
        "positive_days": int(sum(1 for r in returns_clean if r > 0)),
        "negative_days": int(sum(1 for r in returns_clean if r < 0)),
        "zero_days": int(sum(1 for r in returns_clean if r == 0)),
    }

    # --- Return attribution ---
    # Dividend yield = total dividends / average portfolio value
    avg_value = float(np.mean(values)) if values and sum(values) > 0 else 0
    div_yield = div_net / avg_value if avg_value > 0 else 0
    # Annualize dividend yield
    div_yield_ann = (1 + div_yield) ** (1 / max(years, 0.01)) - 1 if years > 0 and div_yield > 0 else div_yield
    capital_appreciation = total_return - div_yield if total_return else 0
    # Real return = (1 + nominal) / (1 + inflation) - 1 (Fisher equation)
    real_return = ((1 + total_return) / (1 + inflation_total) - 1) if total_return and (1 + inflation_total) > 0 else total_return
    result["return_attribution"] = {
        "total_return": float(total_return),
        "dividend_yield": float(div_yield),
        "capital_appreciation": float(capital_appreciation),
        "real_return": float(real_return),
        "inflation_total": float(inflation_total),
    }

    return result
