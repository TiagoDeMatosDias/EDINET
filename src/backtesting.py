"""
Backtesting module for portfolio strategy evaluation.

Receives a date range and a portfolio (tickers + weights), calculates total
returns (price + dividends) over the period, and compares against a benchmark.

Data sources
------------
* ``stock_prices`` table  — daily prices (Date, Ticker, Currency, Price).
* ``Standard_Data_Ratios`` table — per-share dividends (``PerShare_Dividends``)
  linked to companies via ``edinetCode``.
* ``companyInfo`` table — maps ``edinetCode`` ↔ ``Company_Ticker``.

The entry point :func:`run_backtest` is called from the orchestrator as a
workflow step.
"""

from __future__ import annotations

import logging
import os
import sqlite3
from datetime import datetime

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Portfolio allocation modes
# ---------------------------------------------------------------------------

# Each portfolio entry can be:
#   - a plain float  (backward compat, treated as a weight fraction)
#   - a dict with {"mode": "weight"|"shares"|"value", "value": <number>}
ALLOCATION_MODES = ("weight", "shares", "value")


def _normalise_portfolio_entry(
    spec,
) -> tuple[str, float]:
    """Return ``(mode, numeric_value)`` from a portfolio entry.

    Handles both the legacy plain-float format (treated as ``weight``) and the
    new ``{"mode": ..., "value": ...}`` format.
    """
    if isinstance(spec, (int, float)):
        return ("weight", float(spec))
    if isinstance(spec, dict):
        mode = spec.get("mode", "weight")
        val = float(spec.get("value", 0))
        if mode not in ALLOCATION_MODES:
            logger.warning(
                "Unknown allocation mode '%s', falling back to 'weight'.",
                mode,
            )
            mode = "weight"
        return (mode, val)
    raise ValueError(f"Unexpected portfolio entry type: {type(spec)}")


def resolve_portfolio_allocations(
    portfolio_config: dict,
    start_prices: dict[str, float],
    initial_capital: float = 0.0,
) -> tuple[dict[str, float], float, list[str]]:
    """Resolve a mixed-mode portfolio config into normalised weights.

    Supports three allocation modes per ticker:

    * ``weight`` – fraction of total portfolio (e.g. 0.5 = 50 %).
    * ``shares`` – fixed number of shares (requires start price).
    * ``value``  – fixed currency amount (e.g. 10 000 JPY).

    When the portfolio contains *only* weight-mode entries **and** no
    ``initial_capital`` is provided, the weights are returned as-is
    (classic behaviour).  Otherwise all allocations are converted to
    capital amounts and then normalised.

    Args:
        portfolio_config: Mapping of ticker → allocation spec.
        start_prices: Mapping of ticker → opening price on the first day.
        initial_capital: User-specified starting capital.  ``0`` means
            "derive automatically".

    Returns:
        A 3-tuple ``(portfolio_weights, effective_capital, warnings)``:

        * **portfolio_weights** – ``dict[str, float]`` normalised weights
          that sum to 1.0.
        * **effective_capital** – the total capital implied by the
          resolved allocations (may differ from *initial_capital*
          depending on the input configuration).
        * **warnings** – list of human-readable warning strings.
    """
    weight_entries: dict[str, float] = {}   # ticker → weight fraction
    fixed_entries: dict[str, float] = {}    # ticker → capital amount
    warnings: list[str] = []

    for ticker, spec in portfolio_config.items():
        mode, val = _normalise_portfolio_entry(spec)

        if mode == "weight":
            weight_entries[ticker] = val
        elif mode == "shares":
            price = start_prices.get(ticker, 0.0)
            if price > 0:
                fixed_entries[ticker] = val * price
            else:
                warnings.append(
                    f"No start-price data for '{ticker}' — "
                    f"cannot convert {val:.0f} shares to capital; skipping."
                )
        elif mode == "value":
            fixed_entries[ticker] = val

    total_weight_frac = sum(weight_entries.values())
    total_fixed_capital = sum(fixed_entries.values())

    # ── Fast path: pure weight-only portfolio, no initial_capital ─────
    all_weight_only = (not fixed_entries)
    if all_weight_only and initial_capital <= 0:
        # Classic behaviour — return weights directly, normalised
        w_sum = total_weight_frac or 1.0
        weights = {t: w / w_sum for t, w in weight_entries.items()}
        return weights, 0.0, warnings

    # ── Mixed or fixed allocations: convert everything to capital ─────
    if initial_capital > 0:
        effective_capital = initial_capital
    elif total_weight_frac < 1.0 and total_fixed_capital > 0:
        # Derive: weight_frac * C + fixed = C  →  C = fixed / (1 - W)
        effective_capital = total_fixed_capital / (1.0 - total_weight_frac)
    elif total_fixed_capital > 0:
        # Weights already ≥ 100 %, treat fixed as additional
        effective_capital = total_fixed_capital
        if total_weight_frac > 0:
            warnings.append(
                f"Weight-mode tickers sum to {total_weight_frac * 100:.1f}% "
                f"(≥ 100 %); fixed allocations are added on top."
            )
    else:
        # Only weight-mode with explicit initial_capital=0 — use nominal
        effective_capital = 1_000_000.0

    allocations: dict[str, float] = {}
    for ticker, frac in weight_entries.items():
        allocations[ticker] = frac * effective_capital
    for ticker, cap in fixed_entries.items():
        allocations[ticker] = cap

    total_allocated = sum(allocations.values())

    if initial_capital > 0 and abs(total_allocated - initial_capital) > 0.01:
        warnings.append(
            f"Total allocated capital ({total_allocated:,.0f}) differs from "
            f"initial capital ({initial_capital:,.0f}). "
            f"Weights will be normalised to the allocated total."
        )

    if total_allocated > 0:
        portfolio_weights = {
            t: c / total_allocated for t, c in allocations.items()
        }
    else:
        portfolio_weights = {}

    return portfolio_weights, total_allocated, warnings


# ---------------------------------------------------------------------------
# Data retrieval
# ---------------------------------------------------------------------------


def get_portfolio_prices(
    db_path: str,
    prices_table: str,
    tickers: list[str],
    start_date: str,
    end_date: str,
    *,
    conn: sqlite3.Connection | None = None,
) -> pd.DataFrame:
    """Fetch daily prices for the given tickers within a date range.

    Args:
        db_path: Path to the SQLite database file.
        prices_table: Name of the stock-prices table.
        tickers: List of ticker symbols to retrieve.
        start_date: Start date (inclusive) in ``YYYY-MM-DD`` format.
        end_date: End date (inclusive) in ``YYYY-MM-DD`` format.
        conn: Optional existing database connection.

    Returns:
        DataFrame with columns ``Date``, ``Ticker``, ``Price`` sorted by date.
    """
    own_conn = conn is None
    if own_conn:
        conn = sqlite3.connect(db_path)
    try:
        placeholders = ",".join(["?"] * len(tickers))
        query = (
            f"SELECT Date, Ticker, Price FROM {prices_table} "
            f"WHERE Ticker IN ({placeholders}) "
            f"AND Date >= ? AND Date <= ? "
            f"ORDER BY Date"
        )
        params = [*tickers, start_date, end_date]
        df = pd.read_sql_query(query, conn, params=params)
        df["Date"] = pd.to_datetime(df["Date"])
        df["Price"] = pd.to_numeric(df["Price"], errors="coerce")
        return df
    finally:
        if own_conn:
            conn.close()


def get_dividend_data(
    db_path: str,
    ratios_table: str,
    company_table: str,
    tickers: list[str],
    start_date: str,
    end_date: str,
    *,
    conn: sqlite3.Connection | None = None,
) -> pd.DataFrame:
    """Fetch per-share dividends mapped to tickers for the given period.

    Joins the ratios table with the company-info table so that dividend
    data (stored by ``edinetCode``) can be looked up by ``Ticker``.

    Args:
        db_path: Path to the SQLite database file.
        ratios_table: Name of the standardised-ratios table.
        company_table: Name of the company-info table.
        tickers: List of ticker symbols.
        start_date: Start date (inclusive) ``YYYY-MM-DD``.
        end_date: End date (inclusive) ``YYYY-MM-DD``.
        conn: Optional existing database connection.

    Returns:
        DataFrame with columns ``Ticker``, ``periodEnd``, ``PerShare_Dividends``.
    """
    own_conn = conn is None
    if own_conn:
        conn = sqlite3.connect(db_path)
    try:
        placeholders = ",".join(["?"] * len(tickers))
        query = (
            f"SELECT c.Company_Ticker AS Ticker, r.periodEnd, "
            f"r.PerShare_Dividends "
            f"FROM {ratios_table} r "
            f"JOIN {company_table} c ON c.edinetCode = r.edinetCode "
            f"WHERE c.Company_Ticker IN ({placeholders}) "
            f"AND r.periodEnd >= ? AND r.periodEnd <= ? "
            f"ORDER BY r.periodEnd"
        )
        params = [*tickers, start_date, end_date]
        df = pd.read_sql_query(query, conn, params=params)
        df["periodEnd"] = pd.to_datetime(df["periodEnd"])
        df["PerShare_Dividends"] = pd.to_numeric(
            df["PerShare_Dividends"], errors="coerce"
        ).fillna(0.0)
        return df
    finally:
        if own_conn:
            conn.close()


# ---------------------------------------------------------------------------
# Return calculations
# ---------------------------------------------------------------------------


def calculate_portfolio_returns(
    prices_df: pd.DataFrame,
    portfolio_weights: dict[str, float],
    dividends_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Calculate weighted daily portfolio returns including dividends.

    Pivots the long-format *prices_df* into a wide price matrix, computes
    daily percentage returns for each ticker, applies *portfolio_weights*,
    and adds any per-share dividend contributions on the dates they were paid.

    Args:
        prices_df: Long DataFrame with ``Date``, ``Ticker``, ``Price``.
        portfolio_weights: Mapping of ticker → weight (should sum to 1.0).
        dividends_df: Optional DataFrame with ``Ticker``, ``periodEnd``,
            ``PerShare_Dividends``.

    Returns:
        DataFrame indexed by ``Date`` with columns:

        * ``portfolio_return`` — weighted daily return.
        * ``cumulative_return`` — cumulative product of (1 + daily return).
    """
    # Pivot to wide format: one column per ticker
    price_matrix = prices_df.pivot_table(
        index="Date", columns="Ticker", values="Price", aggfunc="last"
    )
    price_matrix = price_matrix.sort_index()
    price_matrix = price_matrix.ffill()  # forward-fill gaps

    # Buy-and-hold: only consider tickers present in data
    tickers_in_data = [t for t in portfolio_weights if t in price_matrix.columns]
    if not tickers_in_data:
        return pd.DataFrame(
            {
                "portfolio_return": pd.Series(dtype=float),
                "cumulative_return": pd.Series(dtype=float),
            }
        )

    weights = pd.Series(
        {t: portfolio_weights[t] for t in tickers_in_data}
    )
    # Normalise weights in case some tickers are missing
    if weights.sum() > 0:
        weights = weights / weights.sum()

    # Normalise each ticker's price to its initial value
    initial_prices = price_matrix[tickers_in_data].iloc[0]
    normalised = price_matrix[tickers_in_data].div(initial_prices, axis=1)

    # Portfolio value = weighted sum of normalised price series
    portfolio_value = normalised.mul(weights, axis=1).sum(axis=1)

    # Save the price-only initial value (before adding dividends) so that
    # cumulative_return is always measured relative to the original
    # investment, not an inflated base that includes day-0 dividend cash.
    initial_portfolio_value = portfolio_value.iloc[0]

    # Add cumulative dividend cash flows (held as cash, not reinvested)
    if dividends_df is not None and not dividends_df.empty:
        div_cash = pd.Series(0.0, index=portfolio_value.index)
        for _, row in dividends_df.iterrows():
            ticker = row["Ticker"]
            pay_date = row["periodEnd"]
            div_amount = row["PerShare_Dividends"]
            if ticker in tickers_in_data and pay_date in portfolio_value.index:
                init_price = initial_prices[ticker]
                if init_price > 0:
                    # Cash per unit portfolio = weight * dividend / initial_price
                    cash = weights[ticker] * div_amount / init_price
                    div_cash.loc[div_cash.index >= pay_date] += cash
        portfolio_value = portfolio_value + div_cash

    # Derive daily returns and cumulative return from the value series
    daily_returns = portfolio_value.pct_change()
    cumulative_return = portfolio_value / initial_portfolio_value

    result = pd.DataFrame(
        {
            "portfolio_return": daily_returns,
            "cumulative_return": cumulative_return,
        }
    )
    # Drop the first NaN row from pct_change
    result = result.iloc[1:]
    return result


def calculate_return_decomposition(
    prices_df: pd.DataFrame,
    portfolio_weights: dict[str, float],
    dividends_df: pd.DataFrame | None = None,
) -> dict[str, pd.DataFrame]:
    """Decompose portfolio returns into price-only and dividend components.

    Computes three cumulative time series:
    * **total** — the full return including dividends.
    * **price_only** — return from share-price changes alone.
    * **dividend_only** — the cumulative contribution of dividends.

    Args:
        prices_df: Long DataFrame with ``Date``, ``Ticker``, ``Price``.
        portfolio_weights: Mapping of ticker → weight (should sum to 1.0).
        dividends_df: Optional DataFrame with ``Ticker``, ``periodEnd``,
            ``PerShare_Dividends``.

    Returns:
        Dictionary with keys ``total``, ``price_only``, ``dividend_only``
        each mapping to a DataFrame indexed by Date with
        ``daily_return`` and ``cumulative_return`` columns.
    """
    # Total return (price + dividends)
    total_df = calculate_portfolio_returns(prices_df, portfolio_weights, dividends_df)

    # Price-only return (no dividends)
    price_only_df = calculate_portfolio_returns(prices_df, portfolio_weights, None)

    # Additive decomposition: dividend contribution = total - price_only
    if not total_df.empty and not price_only_df.empty:
        common_idx = total_df.index.intersection(price_only_df.index)
        total_cum = total_df.loc[common_idx, "cumulative_return"]
        price_cum = price_only_df.loc[common_idx, "cumulative_return"]

        # Dividend contribution in cumulative-return terms
        div_contribution = total_cum - price_cum
        div_cum = 1.0 + div_contribution
        div_daily = div_contribution.diff().fillna(
            div_contribution.iloc[0] if len(div_contribution) > 0 else 0.0
        )

        dividend_only_df = pd.DataFrame(
            {"daily_return": div_daily, "cumulative_return": div_cum}
        )
    else:
        dividend_only_df = pd.DataFrame(
            {"daily_return": pd.Series(dtype=float),
             "cumulative_return": pd.Series(dtype=float)}
        )

    return {
        "total": total_df.rename(
            columns={"portfolio_return": "daily_return"}
        ),
        "price_only": price_only_df.rename(
            columns={"portfolio_return": "daily_return"}
        ),
        "dividend_only": dividend_only_df,
    }


def calculate_per_company_returns(
    prices_df: pd.DataFrame,
    portfolio_weights: dict[str, float],
    dividends_df: pd.DataFrame | None = None,
    initial_capital: float = 0.0,
) -> pd.DataFrame:
    """Compute a per-company breakdown of returns.

    For each ticker in the portfolio produces price return, dividend return,
    total return, portfolio weight, and weighted contribution.  When
    *initial_capital* is positive, also computes concrete quantities:
    ``capital_invested``, ``shares_purchased``, and
    ``dividends_received`` (total cash dividends for the position).

    Args:
        prices_df: Long DataFrame with ``Date``, ``Ticker``, ``Price``.
        portfolio_weights: Mapping of ticker → weight.
        dividends_df: Optional dividends DataFrame.
        initial_capital: Total starting capital.  When > 0 the result
            includes investment-size columns.

    Returns:
        DataFrame with columns ``Ticker``, ``start_price``, ``end_price``,
        ``price_return``, ``dividend_return``, ``total_return``, ``weight``,
        ``weighted_price``, ``weighted_dividend``, ``weighted_total``
        and, when *initial_capital* > 0, ``capital_invested``,
        ``shares_purchased``, ``dividends_received``.
    """
    records: list[dict] = []

    # Normalise weights to available tickers
    available = prices_df["Ticker"].unique()
    active = {t: w for t, w in portfolio_weights.items() if t in available}
    w_sum = sum(active.values())
    if w_sum > 0:
        active = {t: w / w_sum for t, w in active.items()}

    for ticker, weight in active.items():
        tk_prices = (
            prices_df[prices_df["Ticker"] == ticker]
            .sort_values("Date")
            .reset_index(drop=True)
        )
        if tk_prices.empty:
            continue

        start_price = float(tk_prices["Price"].iloc[0])
        end_price = float(tk_prices["Price"].iloc[-1])
        price_return = (end_price / start_price - 1) if start_price else 0.0

        # Dividend return for this ticker (relative to initial price)
        div_return = 0.0
        if dividends_df is not None and not dividends_df.empty:
            tk_divs = dividends_df[dividends_df["Ticker"] == ticker]
            for _, drow in tk_divs.iterrows():
                div_amount = drow["PerShare_Dividends"]
                if div_amount > 0 and start_price > 0:
                    div_return += div_amount / start_price

        total_return = price_return + div_return

        rec: dict = {
            "Ticker": ticker,
            "start_price": start_price,
            "end_price": end_price,
            "price_return": price_return,
            "dividend_return": div_return,
            "total_return": total_return,
            "weight": weight,
            "weighted_price": price_return * weight,
            "weighted_dividend": div_return * weight,
            "weighted_total": total_return * weight,
        }

        if initial_capital > 0:
            capital_invested = weight * initial_capital
            shares = capital_invested / start_price if start_price else 0.0
            rec["capital_invested"] = capital_invested
            rec["shares_purchased"] = shares
            rec["dividends_received"] = shares * (div_return * start_price)
            rec["market_value"] = shares * end_price

        records.append(rec)

    if records:
        return pd.DataFrame(records)
    cols = [
        "Ticker", "start_price", "end_price",
        "price_return", "dividend_return", "total_return",
        "weight", "weighted_price", "weighted_dividend", "weighted_total",
    ]
    if initial_capital > 0:
        cols += ["capital_invested", "shares_purchased", "dividends_received",
                 "market_value"]
    return pd.DataFrame(columns=cols)


def calculate_yearly_returns(
    decomposition: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """Break down portfolio returns by calendar year.

    For each calendar year covered by the backtest, computes the price-only
    return, dividend-only return, and total return.

    Args:
        decomposition: Output of :func:`calculate_return_decomposition`.

    Returns:
        DataFrame with columns ``Year``, ``Price Return``,
        ``Dividend Return``, ``Total Return``.
    """
    total_df = decomposition.get("total")
    price_df = decomposition.get("price_only")

    if total_df is None or total_df.empty:
        return pd.DataFrame(
            columns=["Year", "Price Return", "Dividend Return", "Total Return"]
        )

    div_df = decomposition.get("dividend_only")

    years = sorted(total_df.index.year.unique())
    records: list[dict] = []

    prev_total_cum = 1.0
    prev_div_cum = 1.0

    for year in years:
        year_total = total_df[total_df.index.year == year]
        if year_total.empty:
            continue

        end_total_cum = float(year_total["cumulative_return"].iloc[-1])
        total_ret = end_total_cum / prev_total_cum - 1

        # Dividend contribution: new dividend cash received this year,
        # expressed as a return relative to portfolio value at year start.
        div_ret = 0.0
        if div_df is not None and not div_df.empty:
            year_div = div_df[div_df.index.year == year]
            if not year_div.empty:
                end_div_cum = float(year_div["cumulative_return"].iloc[-1])
                new_div_cash = end_div_cum - prev_div_cum
                if prev_total_cum > 0:
                    div_ret = new_div_cash / prev_total_cum
                prev_div_cum = end_div_cum

        price_ret = total_ret - div_ret
        prev_total_cum = end_total_cum

        records.append({
            "Year": year,
            "Price Return": price_ret,
            "Dividend Return": div_ret,
            "Total Return": total_ret,
        })

    return pd.DataFrame(records)


def calculate_dividends_by_company_year(
    dividends_df: pd.DataFrame | None,
    shares_purchased: dict[str, float] | None = None,
) -> pd.DataFrame:
    """Pivot dividends into a Year × Ticker table.

    When *shares_purchased* is provided the values represent total cash
    dividends received (per-share dividend × shares held).  Otherwise the
    raw per-share dividend amounts are shown.

    Args:
        dividends_df: DataFrame with ``Ticker``, ``periodEnd``,
            ``PerShare_Dividends``.
        shares_purchased: Optional mapping of ticker → number of shares
            held.  When given, each per-share dividend is multiplied by
            the corresponding share count so that values (and the
            ``Total`` column) represent actual cash received.

    Returns:
        DataFrame with years as the index, one column per ticker, and a
        ``Total`` column.
    """
    if dividends_df is None or dividends_df.empty:
        return pd.DataFrame()

    df = dividends_df.copy()
    df["Year"] = df["periodEnd"].dt.year

    if shares_purchased:
        shares_series = df["Ticker"].map(shares_purchased).fillna(0.0)
        df["DividendCash"] = df["PerShare_Dividends"] * shares_series
        value_col = "DividendCash"
    else:
        value_col = "PerShare_Dividends"

    pivot = df.pivot_table(
        index="Year",
        columns="Ticker",
        values=value_col,
        aggfunc="sum",
        fill_value=0.0,
    )
    pivot["Total"] = pivot.sum(axis=1)
    pivot.index.name = "Year"
    return pivot


def calculate_benchmark_returns(
    prices_df: pd.DataFrame,
    benchmark_ticker: str,
    dividends_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Calculate daily returns for a single benchmark ticker.

    Args:
        prices_df: Long DataFrame with ``Date``, ``Ticker``, ``Price``.
        benchmark_ticker: The ticker symbol of the benchmark.
        dividends_df: Optional dividends DataFrame for the benchmark.

    Returns:
        DataFrame indexed by ``Date`` with columns:

        * ``benchmark_return`` — daily total return (price + dividends).
        * ``cumulative_return`` — cumulative product of (1 + daily return).
        * ``price_return`` — daily price-only return.
        * ``cum_price_return`` — cumulative price-only return.
        * ``dividend_return`` — daily dividend contribution.
        * ``cum_dividend_return`` — cumulative dividend contribution.
    """
    bench = prices_df[prices_df["Ticker"] == benchmark_ticker].copy()
    bench = bench.sort_values("Date").set_index("Date")
    bench = bench[~bench.index.duplicated(keep="last")]

    # Price-only returns
    bench["price_return"] = bench["Price"].pct_change()
    bench["dividend_return"] = 0.0

    # Add dividend yield on payment dates (using previous day's price)
    if dividends_df is not None and not dividends_df.empty:
        bench_divs = dividends_df[dividends_df["Ticker"] == benchmark_ticker]
        for _, row in bench_divs.iterrows():
            pay_date = row["periodEnd"]
            div_amount = row["PerShare_Dividends"]
            if pay_date in bench.index:
                loc = bench.index.get_loc(pay_date)
                prev_price = bench["Price"].iloc[loc - 1] if loc > 0 else bench["Price"].iloc[0]
                if prev_price and prev_price > 0:
                    bench.loc[pay_date, "dividend_return"] = div_amount / prev_price

    bench["benchmark_return"] = bench["price_return"] + bench["dividend_return"]
    bench["cumulative_return"] = (1 + bench["benchmark_return"]).cumprod()
    bench["cum_price_return"] = (1 + bench["price_return"]).cumprod()
    bench["cum_dividend_return"] = (1 + bench["dividend_return"]).cumprod()

    bench = bench.iloc[1:]  # drop the first NaN row
    return bench[[
        "benchmark_return", "cumulative_return",
        "price_return", "cum_price_return",
        "dividend_return", "cum_dividend_return",
    ]]


# ---------------------------------------------------------------------------
# Performance metrics
# ---------------------------------------------------------------------------


def calculate_metrics(
    portfolio_df: pd.DataFrame,
    benchmark_df: pd.DataFrame | None,
    start_date: str,
    end_date: str,
    risk_free_rate: float = 0.0,
) -> dict:
    """Compute summary performance metrics for the backtest.

    Args:
        portfolio_df: Output of :func:`calculate_portfolio_returns`.
        benchmark_df: Output of :func:`calculate_benchmark_returns`, or None.
        start_date: Backtest start date ``YYYY-MM-DD``.
        end_date: Backtest end date ``YYYY-MM-DD``.
        risk_free_rate: Annual risk-free rate as a decimal (e.g. 0.02 for 2%).
            Used in the Sharpe ratio calculation.

    Returns:
        Dictionary containing:

        * ``start_date``, ``end_date``
        * ``total_return`` — cumulative portfolio return as a fraction.
        * ``annualized_return``
        * ``volatility`` — annualised standard deviation of daily returns.
        * ``sharpe_ratio`` — annualised Sharpe ratio.
        * ``max_drawdown``
        * ``risk_free_rate``
        * ``benchmark_total_return``, ``benchmark_annualized_return``
        * ``excess_return`` — portfolio minus benchmark total return.
    """
    dt_start = pd.to_datetime(start_date)
    dt_end = pd.to_datetime(end_date)
    years = max((dt_end - dt_start).days / 365.25, 1 / 365.25)

    total_return = float(portfolio_df["cumulative_return"].iloc[-1] - 1) if len(portfolio_df) else 0.0
    annualized_return = (1 + total_return) ** (1 / years) - 1

    daily_vol = portfolio_df["portfolio_return"].std() if len(portfolio_df) else 0.0
    volatility = daily_vol * np.sqrt(252)

    sharpe_ratio = (annualized_return - risk_free_rate) / volatility if volatility > 0 else 0.0

    # Max drawdown
    cum = portfolio_df["cumulative_return"]
    running_max = cum.cummax()
    drawdowns = (cum - running_max) / running_max
    max_drawdown = float(drawdowns.min()) if len(drawdowns) else 0.0

    metrics: dict = {
        "start_date": start_date,
        "end_date": end_date,
        "total_return": total_return,
        "annualized_return": annualized_return,
        "volatility": volatility,
        "sharpe_ratio": sharpe_ratio,
        "max_drawdown": max_drawdown,
        "risk_free_rate": risk_free_rate,
    }

    # Benchmark metrics
    if benchmark_df is not None and len(benchmark_df):
        bench_total = float(benchmark_df["cumulative_return"].iloc[-1] - 1)
        bench_ann = (1 + bench_total) ** (1 / years) - 1
        metrics["benchmark_total_return"] = bench_total
        metrics["benchmark_annualized_return"] = bench_ann
        metrics["excess_return"] = total_return - bench_total

        # Benchmark price vs dividend decomposition
        if "cum_price_return" in benchmark_df.columns:
            bench_price = float(
                benchmark_df["cum_price_return"].iloc[-1] - 1
            )
            metrics["benchmark_price_return"] = bench_price
            # Additive: dividend return = total - price
            metrics["benchmark_dividend_return"] = bench_total - bench_price
        else:
            metrics["benchmark_price_return"] = bench_total
            metrics["benchmark_dividend_return"] = 0.0
    else:
        metrics["benchmark_total_return"] = None
        metrics["benchmark_annualized_return"] = None
        metrics["excess_return"] = None
        metrics["benchmark_price_return"] = None
        metrics["benchmark_dividend_return"] = None

    return metrics


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------


def generate_report(
    metrics: dict,
    output_file: str,
    decomposition: dict | None = None,
    per_company: pd.DataFrame | None = None,
    benchmark_df: pd.DataFrame | None = None,
    yearly_returns: pd.DataFrame | None = None,
    dividends_by_year: pd.DataFrame | None = None,
) -> str:
    """Write a human-readable performance report to *output_file*.

    Args:
        metrics: Dictionary produced by :func:`calculate_metrics`.
        output_file: Destination file path.
        decomposition: Optional return decomposition from
            :func:`calculate_return_decomposition`.
        per_company: Optional per-company breakdown from
            :func:`calculate_per_company_returns`.
        benchmark_df: Optional benchmark DataFrame with decomposition columns.

    Returns:
        The report text that was written.
    """
    lines: list[str] = []
    lines.append("=" * 60)
    lines.append("  BACKTESTING REPORT")
    lines.append("=" * 60)
    lines.append("")
    lines.append(f"Period:              {metrics['start_date']}  →  {metrics['end_date']}")
    lines.append(f"Total Return:        {metrics['total_return']:+.2%}")
    lines.append(f"Annualized Return:   {metrics['annualized_return']:+.2%}")
    lines.append(f"Volatility (ann.):   {metrics['volatility']:.2%}")
    risk_free = metrics.get('risk_free_rate', 0.0)
    lines.append(f"Sharpe Ratio:        {metrics['sharpe_ratio']:.4f}  (rf={risk_free:.2%})")
    lines.append(f"Max Drawdown:        {metrics['max_drawdown']:.2%}")

    # ── Capital allocation ────────────────────────────────────────────
    initial_capital = metrics.get("initial_capital", 0.0)
    if initial_capital > 0:
        lines.append("")
        lines.append("--- Capital Allocation ---")
        lines.append(f"Initial Capital:     {initial_capital:,.0f}")

        # Per-company shares purchased
        if per_company is not None and not per_company.empty and "shares_purchased" in per_company.columns:
            cap_header = (
                f"{'Ticker':<10} {'Weight':>8} {'Capital':>14} "
                f"{'Price':>10} {'Shares':>12} {'Divs Rcvd':>12}"
            )
            lines.append(cap_header)
            lines.append("-" * len(cap_header))
            for _, row in per_company.iterrows():
                lines.append(
                    f"{row['Ticker']:<10} {row['weight']:>7.1%} "
                    f"{row['capital_invested']:>13,.0f} "
                    f"{row['start_price']:>10,.2f} "
                    f"{row['shares_purchased']:>11,.2f} "
                    f"{row['dividends_received']:>11,.2f}"
                )
            lines.append("-" * len(cap_header))
            lines.append(
                f"{'TOTAL':<10} {'':>8} "
                f"{per_company['capital_invested'].sum():>13,.0f} "
                f"{'':>10} {'':>12} "
                f"{per_company['dividends_received'].sum():>11,.2f}"
            )

        # Benchmark shares
        bench_info = metrics.get("benchmark_shares_info")
        if bench_info:
            lines.append("")
            lines.append(
                f"Benchmark ({bench_info['ticker']}):  "
                f"{bench_info['capital']:,.0f} capital  →  "
                f"{bench_info['shares']:,.2f} shares @ {bench_info['start_price']:,.2f}"
            )

    # ── Return decomposition (portfolio) ──────────────────────────────
    if decomposition is not None:
        price_cum = decomposition["price_only"]
        div_cum = decomposition["dividend_only"]
        if not price_cum.empty:
            price_ret = float(price_cum["cumulative_return"].iloc[-1] - 1)
            div_ret = float(div_cum["cumulative_return"].iloc[-1] - 1)
            lines.append("")
            lines.append("--- Portfolio Return Decomposition ---")
            lines.append(f"  Price Return:      {price_ret:+.2%}")
            lines.append(f"  Dividend Return:   {div_ret:+.2%}")
            lines.append(f"  Total Return:      {metrics['total_return']:+.2%}")

    # ── Benchmark ─────────────────────────────────────────────────────
    if metrics.get("benchmark_total_return") is not None:
        lines.append("")
        lines.append("--- Benchmark ---")
        lines.append(f"Benchmark Return:    {metrics['benchmark_total_return']:+.2%}")
        lines.append(f"Benchmark Ann. Ret.: {metrics['benchmark_annualized_return']:+.2%}")
        lines.append(f"Excess Return:       {metrics['excess_return']:+.2%}")

        if metrics.get("benchmark_price_return") is not None:
            lines.append("")
            lines.append("--- Benchmark Return Decomposition ---")
            lines.append(f"  Price Return:      {metrics['benchmark_price_return']:+.2%}")
            lines.append(f"  Dividend Return:   {metrics['benchmark_dividend_return']:+.2%}")

    # ── Per-company breakdown ─────────────────────────────────────────
    if per_company is not None and not per_company.empty:
        has_capital = "market_value" in per_company.columns
        lines.append("")
        lines.append("--- Per-Company Breakdown ---")

        if has_capital:
            header = (
                f"{'Ticker':<10} {'Weight':>8} "
                f"{'Start':>10} {'Cost Basis':>14} "
                f"{'End':>10} {'Mkt Value':>14} "
                f"{'Cap Gains':>14} {'Cum Divs':>12} {'Total Ret':>14} "
                f"{'Price':>10} {'Dividend':>10} {'Total':>10} {'Wtd Cont.':>10}"
            )
            lines.append(header)
            lines.append("-" * len(header))
            for _, row in per_company.iterrows():
                cap_gains = row['market_value'] - row['capital_invested']
                total_ret_cash = cap_gains + row['dividends_received']
                lines.append(
                    f"{row['Ticker']:<10} {row['weight']:>7.1%} "
                    f"{row['start_price']:>10,.2f} "
                    f"{row['capital_invested']:>14,.0f} "
                    f"{row['end_price']:>10,.2f} "
                    f"{row['market_value']:>14,.0f} "
                    f"{cap_gains:>+14,.0f} "
                    f"{row['dividends_received']:>12,.0f} "
                    f"{total_ret_cash:>+14,.0f} "
                    f"{row['price_return']:>+9.2%} "
                    f"{row['dividend_return']:>+9.2%} "
                    f"{row['total_return']:>+9.2%} "
                    f"{row['weighted_total']:>+9.2%}"
                )
            lines.append("-" * len(header))
            total_cap_gains = per_company['market_value'].sum() - per_company['capital_invested'].sum()
            total_divs = per_company['dividends_received'].sum()
            total_ret_cash_all = total_cap_gains + total_divs
            lines.append(
                f"{'TOTAL':<10} {per_company['weight'].sum():>7.1%} "
                f"{'':>10} "
                f"{per_company['capital_invested'].sum():>14,.0f} "
                f"{'':>10} "
                f"{per_company['market_value'].sum():>14,.0f} "
                f"{total_cap_gains:>+14,.0f} "
                f"{total_divs:>12,.0f} "
                f"{total_ret_cash_all:>+14,.0f} "
                f"{per_company['weighted_price'].sum():>+9.2%} "
                f"{per_company['weighted_dividend'].sum():>+9.2%} "
                f"{per_company['weighted_total'].sum():>+9.2%} "
                f"{per_company['weighted_total'].sum():>+9.2%}"
            )
        else:
            header = (
                f"{'Ticker':<10} {'Weight':>8} {'Price':>10} "
                f"{'Dividend':>10} {'Total':>10} {'Wtd Cont.':>10}"
            )
            lines.append(header)
            lines.append("-" * len(header))
            for _, row in per_company.iterrows():
                lines.append(
                    f"{row['Ticker']:<10} {row['weight']:>7.1%} "
                    f"{row['price_return']:>+9.2%} "
                    f"{row['dividend_return']:>+9.2%} "
                    f"{row['total_return']:>+9.2%} "
                    f"{row['weighted_total']:>+9.2%}"
                )
            lines.append("-" * len(header))
            lines.append(
                f"{'TOTAL':<10} {per_company['weight'].sum():>7.1%} "
                f"{per_company['weighted_price'].sum():>+9.2%} "
                f"{per_company['weighted_dividend'].sum():>+9.2%} "
                f"{per_company['weighted_total'].sum():>+9.2%} "
                f"{per_company['weighted_total'].sum():>+9.2%}"
            )

    # ── Yearly returns ────────────────────────────────────────────────
    if yearly_returns is not None and not yearly_returns.empty:
        lines.append("")
        lines.append("--- Yearly Returns ---")
        yr_header = (
            f"{'Year':<6} {'Price':>10} "
            f"{'Dividend':>10} {'Total':>10}"
        )
        lines.append(yr_header)
        lines.append("-" * len(yr_header))
        for _, row in yearly_returns.iterrows():
            lines.append(
                f"{int(row['Year']):<6} "
                f"{row['Price Return']:>+9.2%} "
                f"{row['Dividend Return']:>+9.2%} "
                f"{row['Total Return']:>+9.2%}"
            )

    # ── Dividends per company per year ────────────────────────────────
    if dividends_by_year is not None and not dividends_by_year.empty:
        lines.append("")
        lines.append("--- Dividends Per Company Per Year ---")
        ticker_cols = [c for c in dividends_by_year.columns if c != "Total"]
        col_w = 10
        div_hdr = f"{'Year':<6}"
        for col in ticker_cols:
            div_hdr += f" {col:>{col_w}}"
        div_hdr += f" {'Total':>{col_w}}"
        lines.append(div_hdr)
        lines.append("-" * len(div_hdr))
        for year, row in dividends_by_year.iterrows():
            line = f"{int(year):<6}"
            for col in ticker_cols:
                line += f" {row[col]:>{col_w},.2f}"
            line += f" {row['Total']:>{col_w},.2f}"
            lines.append(line)

    lines.append("")
    lines.append("=" * 60)

    report_text = "\n".join(lines)

    output_dir = os.path.dirname(output_file)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(report_text)

    logger.info("Backtest report written to %s", output_file)
    return report_text


# ---------------------------------------------------------------------------
# Chart generation
# ---------------------------------------------------------------------------


def generate_backtest_charts(
    decomposition: dict[str, pd.DataFrame],
    benchmark_df: pd.DataFrame | None,
    per_company: pd.DataFrame | None,
    output_dir: str,
    start_date: str,
    end_date: str,
    dividends_by_year: pd.DataFrame | None = None,
) -> list[str]:
    """Generate performance visualisation charts and save as PNG files.

    Creates up to five charts:

    1. **cumulative_returns.png** — Portfolio total return vs benchmark.
    2. **drawdown.png** — Portfolio drawdown over time.
    3. **return_decomposition.png** — Stacked area of price vs dividend
       contribution over time.
    4. **per_company_breakdown.png** — Horizontal bar chart of each
       company's price and dividend return contribution.
    5. **dividends_by_year.png** — Stacked bar chart of per-share
       dividends paid per year, broken down by company.

    Args:
        decomposition: Output of :func:`calculate_return_decomposition`.
        benchmark_df: Output of :func:`calculate_benchmark_returns` or None.
        per_company: Output of :func:`calculate_per_company_returns` or None.
        output_dir: Directory to write PNG files into.
        start_date: Backtest start date (for titles).
        end_date: Backtest end date (for titles).
        dividends_by_year: Output of :func:`calculate_dividends_by_company_year`
            or None.

    Returns:
        List of file paths that were created.
    """
    try:
        import matplotlib
        matplotlib.use("Agg")  # non-interactive backend
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
    except ImportError:
        logger.warning(
            "matplotlib is not installed — skipping chart generation. "
            "Install it with: pip install matplotlib"
        )
        return []

    os.makedirs(output_dir, exist_ok=True)
    created: list[str] = []
    period_label = f"{start_date} → {end_date}"

    # ── 1. Cumulative returns: portfolio vs benchmark ─────────────────
    total_df = decomposition.get("total")
    if total_df is not None and not total_df.empty:
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(
            total_df.index,
            (total_df["cumulative_return"] - 1) * 100,
            label="Portfolio (Total)",
            linewidth=2,
            color="#2196F3",
        )

        price_df = decomposition.get("price_only")
        if price_df is not None and not price_df.empty:
            ax.plot(
                price_df.index,
                (price_df["cumulative_return"] - 1) * 100,
                label="Portfolio (Price Only)",
                linewidth=1.5,
                linestyle="--",
                color="#64B5F6",
            )

        if benchmark_df is not None and not benchmark_df.empty:
            ax.plot(
                benchmark_df.index,
                (benchmark_df["cumulative_return"] - 1) * 100,
                label="Benchmark (Total)",
                linewidth=2,
                color="#FF9800",
            )
            if "cum_price_return" in benchmark_df.columns:
                ax.plot(
                    benchmark_df.index,
                    (benchmark_df["cum_price_return"] - 1) * 100,
                    label="Benchmark (Price Only)",
                    linewidth=1.5,
                    linestyle="--",
                    color="#FFB74D",
                )

        ax.set_title(f"Cumulative Returns — {period_label}", fontsize=14)
        ax.set_ylabel("Return (%)")
        ax.set_xlabel("Date")
        ax.legend(loc="best")
        ax.grid(True, alpha=0.3)
        ax.axhline(y=0, color="grey", linewidth=0.8, linestyle="-")
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
        fig.autofmt_xdate()
        fig.tight_layout()
        path = os.path.join(output_dir, "cumulative_returns.png")
        fig.savefig(path, dpi=150)
        plt.close(fig)
        created.append(path)
        logger.info("Chart saved: %s", path)

    # ── 2. Drawdown chart ─────────────────────────────────────────────
    if total_df is not None and not total_df.empty:
        cum = total_df["cumulative_return"]
        running_max = cum.cummax()
        drawdown = ((cum - running_max) / running_max) * 100

        fig, ax = plt.subplots(figsize=(12, 4))
        ax.fill_between(
            drawdown.index, drawdown.values, 0,
            color="#EF5350", alpha=0.4, label="Drawdown",
        )
        ax.plot(drawdown.index, drawdown.values, color="#C62828", linewidth=1)

        if benchmark_df is not None and not benchmark_df.empty:
            b_cum = benchmark_df["cumulative_return"]
            b_max = b_cum.cummax()
            b_dd = ((b_cum - b_max) / b_max) * 100
            ax.plot(
                b_dd.index, b_dd.values,
                color="#FF9800", linewidth=1, linestyle="--",
                label="Benchmark Drawdown",
            )

        ax.set_title(f"Drawdown — {period_label}", fontsize=14)
        ax.set_ylabel("Drawdown (%)")
        ax.set_xlabel("Date")
        ax.legend(loc="best")
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
        fig.autofmt_xdate()
        fig.tight_layout()
        path = os.path.join(output_dir, "drawdown.png")
        fig.savefig(path, dpi=150)
        plt.close(fig)
        created.append(path)
        logger.info("Chart saved: %s", path)

    # ── 3. Return decomposition (stacked area) ───────────────────────
    price_only = decomposition.get("price_only")
    div_only = decomposition.get("dividend_only")
    if (
        price_only is not None
        and not price_only.empty
        and div_only is not None
        and not div_only.empty
    ):
        fig, ax = plt.subplots(figsize=(12, 6))

        common_idx = price_only.index.intersection(div_only.index)
        price_vals = (price_only.loc[common_idx, "cumulative_return"] - 1) * 100
        div_vals = (div_only.loc[common_idx, "cumulative_return"] - 1) * 100

        ax.fill_between(
            common_idx, 0, price_vals,
            alpha=0.5, color="#42A5F5", label="Price Return",
        )
        ax.fill_between(
            common_idx, price_vals, price_vals + div_vals,
            alpha=0.5, color="#66BB6A", label="Dividend Return",
        )
        ax.plot(
            common_idx, price_vals + div_vals,
            color="#1B5E20", linewidth=1.5, label="Total Return",
        )

        ax.set_title(
            f"Portfolio Return Decomposition — {period_label}", fontsize=14
        )
        ax.set_ylabel("Cumulative Return (%)")
        ax.set_xlabel("Date")
        ax.legend(loc="best")
        ax.grid(True, alpha=0.3)
        ax.axhline(y=0, color="grey", linewidth=0.8, linestyle="-")
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
        fig.autofmt_xdate()
        fig.tight_layout()
        path = os.path.join(output_dir, "return_decomposition.png")
        fig.savefig(path, dpi=150)
        plt.close(fig)
        created.append(path)
        logger.info("Chart saved: %s", path)

    # ── 4. Per-company breakdown (horizontal bar) ─────────────────────
    if per_company is not None and not per_company.empty:
        fig, ax = plt.subplots(
            figsize=(10, max(4, len(per_company) * 0.8 + 2))
        )

        tickers = per_company["Ticker"].values
        y_pos = np.arange(len(tickers))
        price_pct = per_company["weighted_price"].values * 100
        div_pct = per_company["weighted_dividend"].values * 100

        bars_price = ax.barh(
            y_pos, price_pct, height=0.5,
            color="#42A5F5", label="Price Contribution",
        )
        bars_div = ax.barh(
            y_pos, div_pct, height=0.5, left=price_pct,
            color="#66BB6A", label="Dividend Contribution",
        )

        ax.set_yticks(y_pos)
        ax.set_yticklabels(tickers)
        ax.set_xlabel("Weighted Return Contribution (%)")
        ax.set_title(
            f"Per-Company Return Contribution — {period_label}", fontsize=14
        )
        ax.legend(loc="best")
        ax.grid(True, axis="x", alpha=0.3)
        ax.axvline(x=0, color="grey", linewidth=0.8, linestyle="-")
        fig.tight_layout()
        path = os.path.join(output_dir, "per_company_breakdown.png")
        fig.savefig(path, dpi=150)
        plt.close(fig)
        created.append(path)
        logger.info("Chart saved: %s", path)

    # ── 5. Dividends by company by year (stacked bar) ─────────────────
    if dividends_by_year is not None and not dividends_by_year.empty:
        ticker_cols = [c for c in dividends_by_year.columns if c != "Total"]
        if ticker_cols:
            fig, ax = plt.subplots(figsize=(10, 6))

            years = dividends_by_year.index.astype(str)
            x = np.arange(len(years))
            bottom = np.zeros(len(years))

            cmap = plt.cm.get_cmap("Set2", max(len(ticker_cols), 1))

            for i, ticker in enumerate(ticker_cols):
                values = dividends_by_year[ticker].values.astype(float)
                ax.bar(
                    x, values, bottom=bottom,
                    label=ticker, color=cmap(i),
                )
                bottom += values

            ax.set_xticks(x)
            ax.set_xticklabels(years)
            ax.set_xlabel("Year")
            ax.set_ylabel("Dividends Per Share")
            ax.set_title(
                f"Dividends Per Company Per Year — {period_label}",
                fontsize=14,
            )
            ax.legend(loc="best")
            ax.grid(True, axis="y", alpha=0.3)
            fig.tight_layout()
            path = os.path.join(output_dir, "dividends_by_year.png")
            fig.savefig(path, dpi=150)
            plt.close(fig)
            created.append(path)
            logger.info("Chart saved: %s", path)

    return created


# ---------------------------------------------------------------------------
# Orchestrator entry point
# ---------------------------------------------------------------------------


def run_backtest(
    backtesting_config: dict,
    db_path: str,
    prices_table: str = "stock_prices",
    ratios_table: str = "Standard_Data_Ratios",
    company_table: str = "companyInfo",
) -> dict:
    """Run a full backtest from a config dictionary.

    This is the function called by the orchestrator step.

    Args:
        backtesting_config: Dictionary with keys ``start_date``, ``end_date``,
            ``portfolio`` (dict of ticker→weight), ``benchmark_ticker``
            (optional), ``output_file`` (optional).
        db_path: Path to the SQLite database.
        prices_table: Name of the stock-prices table.
        ratios_table: Name of the standardised-ratios table.
        company_table: Name of the company-info table.

    Returns:
        Metrics dictionary produced by :func:`calculate_metrics`.
    """
    start_date = backtesting_config.get("start_date")
    end_date = backtesting_config.get("end_date")
    portfolio_config = backtesting_config.get("portfolio", {})
    benchmark_ticker = backtesting_config.get("benchmark_ticker")
    output_file = backtesting_config.get(
        "output_file", "data/backtest_results/backtest_report.txt"
    )
    risk_free_rate = backtesting_config.get("risk_free_rate", 0.0)
    initial_capital = backtesting_config.get("initial_capital", 0.0)

    # ── Validate configuration ────────────────────────────────────────────
    if not start_date or not end_date:
        raise ValueError(
            "Backtesting config must include 'start_date' and 'end_date'."
        )

    if not portfolio_config:
        raise ValueError(
            "Backtesting config 'portfolio' is empty. "
            "Add at least one ticker with an allocation "
            "(e.g. {\"7203\": 0.5} or "
            "{\"7203\": {\"mode\": \"shares\", \"value\": 100}})."
        )

    # Detect whether the portfolio uses only plain weight-mode entries
    # so we can provide the legacy strict validation as a warning.
    has_non_weight = any(
        isinstance(v, dict) and v.get("mode", "weight") != "weight"
        for v in portfolio_config.values()
    )

    if not has_non_weight:
        # Legacy / pure-weight portfolio — validate sum (warning only)
        raw_weights = {
            t: float(v) if isinstance(v, (int, float))
            else float(v.get("value", 0))
            for t, v in portfolio_config.items()
        }
        weight_sum = sum(raw_weights.values())
        if abs(weight_sum - 1.0) > 0.01:
            logger.warning(
                "Portfolio weights sum to %.4f (%.1f%%), not 100%%. "
                "Weights will be normalised automatically.",
                weight_sum, weight_sum * 100,
            )

    # Portfolio weights will be resolved after price data is available.
    # For now, extract ticker symbols so we can fetch prices.
    tickers = list(portfolio_config.keys())
    all_tickers = list(tickers)
    if benchmark_ticker and benchmark_ticker not in all_tickers:
        all_tickers.append(benchmark_ticker)

    logger.info(
        "Running backtest from %s to %s with %d ticker(s).",
        start_date, end_date, len(tickers),
    )

    # Derive chart / output directory from output_file
    output_dir = os.path.dirname(output_file) or "data/backtest_results"

    conn = sqlite3.connect(db_path)
    try:
        # 1. Fetch prices
        prices_df = get_portfolio_prices(
            db_path, prices_table, all_tickers, start_date, end_date, conn=conn
        )

        if prices_df.empty:
            logger.warning("No price data found for the specified tickers/period.")
            empty_metrics = calculate_metrics(
                pd.DataFrame({"portfolio_return": [], "cumulative_return": []}),
                None, start_date, end_date, risk_free_rate,
            )
            generate_report(empty_metrics, output_file)
            return empty_metrics

        # 1b. Resolve mixed portfolio allocations into weights
        start_prices: dict[str, float] = {}
        for tk in tickers:
            tk_prices = prices_df[prices_df["Ticker"] == tk].sort_values("Date")
            if not tk_prices.empty:
                start_prices[tk] = float(tk_prices["Price"].iloc[0])

        portfolio_weights, effective_capital, alloc_warnings = (
            resolve_portfolio_allocations(
                portfolio_config, start_prices, initial_capital,
            )
        )
        for w in alloc_warnings:
            logger.warning(w)

        # Use the resolved effective capital when one was derived and the
        # user did not supply an explicit value.
        if effective_capital > 0 and initial_capital <= 0:
            initial_capital = effective_capital
        elif effective_capital > 0 and initial_capital > 0:
            # Keep user-supplied capital, but log if it differs
            pass  # warning already emitted by resolve_portfolio_allocations

        # 2. Fetch dividends (for portfolio tickers)
        dividends_df = get_dividend_data(
            db_path, ratios_table, company_table, tickers, start_date, end_date,
            conn=conn,
        )

        # Also fetch dividends for the benchmark ticker (if any)
        all_dividends_df = dividends_df
        if benchmark_ticker:
            bench_divs = get_dividend_data(
                db_path, ratios_table, company_table,
                [benchmark_ticker], start_date, end_date, conn=conn,
            )
            if not bench_divs.empty:
                all_dividends_df = pd.concat(
                    [dividends_df, bench_divs], ignore_index=True
                )

        portfolio_prices = prices_df[prices_df["Ticker"].isin(tickers)]

        # 3. Portfolio returns
        portfolio_df = calculate_portfolio_returns(
            portfolio_prices, portfolio_weights, dividends_df,
        )

        # 4. Return decomposition (price vs dividend)
        decomposition = calculate_return_decomposition(
            portfolio_prices, portfolio_weights, dividends_df,
        )

        # 5. Per-company breakdown
        per_company = calculate_per_company_returns(
            portfolio_prices, portfolio_weights, dividends_df,
            initial_capital=initial_capital,
        )

        # 5a. Yearly returns breakdown
        yearly_returns = calculate_yearly_returns(decomposition)

        # 5b. Dividends by company by year (use cash amounts when possible)
        shares_map = None
        if per_company is not None and "shares_purchased" in per_company.columns:
            shares_map = dict(
                zip(per_company["Ticker"], per_company["shares_purchased"])
            )
        dividends_by_year = calculate_dividends_by_company_year(
            dividends_df, shares_purchased=shares_map,
        )

        # 6. Benchmark returns (with dividend decomposition)
        benchmark_df = None
        if benchmark_ticker:
            bench_prices = prices_df[prices_df["Ticker"] == benchmark_ticker]
            if not bench_prices.empty:
                benchmark_df = calculate_benchmark_returns(
                    prices_df, benchmark_ticker, all_dividends_df,
                )

        # 7. Metrics
        metrics = calculate_metrics(portfolio_df, benchmark_df, start_date, end_date, risk_free_rate)

        # Attach decomposition summary to metrics
        if decomposition and not decomposition["price_only"].empty:
            metrics["portfolio_price_return"] = float(
                decomposition["price_only"]["cumulative_return"].iloc[-1] - 1
            )
            metrics["portfolio_dividend_return"] = float(
                decomposition["dividend_only"]["cumulative_return"].iloc[-1] - 1
            )
        else:
            metrics["portfolio_price_return"] = metrics["total_return"]
            metrics["portfolio_dividend_return"] = 0.0

        # Attach per-company data as list of dicts for easy consumption
        if per_company is not None and not per_company.empty:
            metrics["per_company"] = per_company.to_dict(orient="records")
        else:
            metrics["per_company"] = []

        # Capital allocation info
        metrics["initial_capital"] = initial_capital
        if initial_capital > 0 and benchmark_ticker and benchmark_df is not None:
            bench_prices_data = prices_df[prices_df["Ticker"] == benchmark_ticker]
            if not bench_prices_data.empty:
                bench_start_price = float(
                    bench_prices_data.sort_values("Date")["Price"].iloc[0]
                )
                bench_shares = initial_capital / bench_start_price if bench_start_price else 0.0
                metrics["benchmark_shares_info"] = {
                    "ticker": benchmark_ticker,
                    "capital": initial_capital,
                    "start_price": bench_start_price,
                    "shares": bench_shares,
                }

        # 8. Report
        generate_report(
            metrics, output_file,
            decomposition=decomposition,
            per_company=per_company,
            benchmark_df=benchmark_df,
            yearly_returns=yearly_returns,
            dividends_by_year=dividends_by_year,
        )

        # 9. Charts
        chart_files = generate_backtest_charts(
            decomposition, benchmark_df, per_company,
            output_dir, start_date, end_date,
            dividends_by_year=dividends_by_year,
        )
        metrics["chart_files"] = chart_files

        logger.info("Backtest complete. Total return: %.2f%%", metrics["total_return"] * 100)
        return metrics

    finally:
        conn.close() 



