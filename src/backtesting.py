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

    # Daily simple returns
    daily_returns = price_matrix.pct_change()

    # Add dividend yield on payment dates
    if dividends_df is not None and not dividends_df.empty:
        for _, row in dividends_df.iterrows():
            ticker = row["Ticker"]
            pay_date = row["periodEnd"]
            div_amount = row["PerShare_Dividends"]
            if ticker in price_matrix.columns and pay_date in price_matrix.index:
                price_on_date = price_matrix.loc[pay_date, ticker]
                if price_on_date and price_on_date > 0:
                    daily_returns.loc[pay_date, ticker] += div_amount / price_on_date

    # Weighted portfolio return
    tickers_in_data = [t for t in portfolio_weights if t in daily_returns.columns]
    weights = pd.Series(
        {t: portfolio_weights[t] for t in tickers_in_data}
    )
    # Normalise weights in case some tickers are missing
    if weights.sum() > 0:
        weights = weights / weights.sum()

    weighted_returns = daily_returns[tickers_in_data].mul(weights, axis=1)
    portfolio_return = weighted_returns.sum(axis=1)

    result = pd.DataFrame(
        {
            "portfolio_return": portfolio_return,
            "cumulative_return": (1 + portfolio_return).cumprod(),
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

    # Dividend-only contribution: difference between total and price-only
    if not total_df.empty and not price_only_df.empty:
        # Align indices
        common_idx = total_df.index.intersection(price_only_df.index)
        div_daily = (
            total_df.loc[common_idx, "portfolio_return"]
            - price_only_df.loc[common_idx, "portfolio_return"]
        )
        div_cumulative = (1 + div_daily).cumprod()
        dividend_only_df = pd.DataFrame(
            {"daily_return": div_daily, "cumulative_return": div_cumulative}
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
) -> pd.DataFrame:
    """Compute a per-company breakdown of returns.

    For each ticker in the portfolio produces price return, dividend return,
    total return, portfolio weight, and weighted contribution.

    Args:
        prices_df: Long DataFrame with ``Date``, ``Ticker``, ``Price``.
        portfolio_weights: Mapping of ticker → weight.
        dividends_df: Optional dividends DataFrame.

    Returns:
        DataFrame with columns ``Ticker``, ``start_price``, ``end_price``,
        ``price_return``, ``dividend_return``, ``total_return``, ``weight``,
        ``weighted_price``, ``weighted_dividend``, ``weighted_total``.
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

        # Dividend return for this ticker
        div_return = 0.0
        if dividends_df is not None and not dividends_df.empty:
            tk_divs = dividends_df[dividends_df["Ticker"] == ticker]
            for _, drow in tk_divs.iterrows():
                pay_date = drow["periodEnd"]
                div_amount = drow["PerShare_Dividends"]
                # Use price on dividend date (or closest prior) as base
                prior = tk_prices[tk_prices["Date"] <= pay_date]
                if not prior.empty and div_amount > 0:
                    base_price = float(prior["Price"].iloc[-1])
                    if base_price > 0:
                        div_return += div_amount / base_price

        total_return = price_return + div_return

        records.append(
            {
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
        )

    if records:
        return pd.DataFrame(records)
    return pd.DataFrame(
        columns=[
            "Ticker", "start_price", "end_price",
            "price_return", "dividend_return", "total_return",
            "weight", "weighted_price", "weighted_dividend", "weighted_total",
        ]
    )


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

    # Add dividend yield on payment dates
    if dividends_df is not None and not dividends_df.empty:
        bench_divs = dividends_df[dividends_df["Ticker"] == benchmark_ticker]
        for _, row in bench_divs.iterrows():
            pay_date = row["periodEnd"]
            div_amount = row["PerShare_Dividends"]
            if pay_date in bench.index:
                price_on_date = bench.loc[pay_date, "Price"]
                if price_on_date and price_on_date > 0:
                    bench.loc[pay_date, "dividend_return"] = div_amount / price_on_date

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
) -> dict:
    """Compute summary performance metrics for the backtest.

    Args:
        portfolio_df: Output of :func:`calculate_portfolio_returns`.
        benchmark_df: Output of :func:`calculate_benchmark_returns`, or None.
        start_date: Backtest start date ``YYYY-MM-DD``.
        end_date: Backtest end date ``YYYY-MM-DD``.

    Returns:
        Dictionary containing:

        * ``start_date``, ``end_date``
        * ``total_return`` — cumulative portfolio return as a fraction.
        * ``annualized_return``
        * ``volatility`` — annualised standard deviation of daily returns.
        * ``sharpe_ratio`` — annualised Sharpe (assuming 0 % risk-free rate).
        * ``max_drawdown``
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

    sharpe_ratio = annualized_return / volatility if volatility > 0 else 0.0

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
            metrics["benchmark_price_return"] = float(
                benchmark_df["cum_price_return"].iloc[-1] - 1
            )
            metrics["benchmark_dividend_return"] = float(
                benchmark_df["cum_dividend_return"].iloc[-1] - 1
            )
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
    lines.append(f"Sharpe Ratio:        {metrics['sharpe_ratio']:.4f}")
    lines.append(f"Max Drawdown:        {metrics['max_drawdown']:.2%}")

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
        lines.append("")
        lines.append("--- Per-Company Breakdown ---")
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
        # Totals row
        lines.append("-" * len(header))
        lines.append(
            f"{'TOTAL':<10} {per_company['weight'].sum():>7.1%} "
            f"{per_company['weighted_price'].sum():>+9.2%} "
            f"{per_company['weighted_dividend'].sum():>+9.2%} "
            f"{per_company['weighted_total'].sum():>+9.2%} "
            f"{per_company['weighted_total'].sum():>+9.2%}"
        )

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
) -> list[str]:
    """Generate performance visualisation charts and save as PNG files.

    Creates up to four charts:

    1. **cumulative_returns.png** — Portfolio total return vs benchmark.
    2. **drawdown.png** — Portfolio drawdown over time.
    3. **return_decomposition.png** — Stacked area of price vs dividend
       contribution over time.
    4. **per_company_breakdown.png** — Horizontal bar chart of each
       company's price and dividend return contribution.

    Args:
        decomposition: Output of :func:`calculate_return_decomposition`.
        benchmark_df: Output of :func:`calculate_benchmark_returns` or None.
        per_company: Output of :func:`calculate_per_company_returns` or None.
        output_dir: Directory to write PNG files into.
        start_date: Backtest start date (for titles).
        end_date: Backtest end date (for titles).

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
    portfolio_weights = backtesting_config.get("portfolio", {})
    benchmark_ticker = backtesting_config.get("benchmark_ticker")
    output_file = backtesting_config.get(
        "output_file", "data/backtest_results/backtest_report.txt"
    )

    # ── Validate configuration ────────────────────────────────────────────
    if not start_date or not end_date:
        raise ValueError(
            "Backtesting config must include 'start_date' and 'end_date'."
        )

    if not portfolio_weights:
        raise ValueError(
            "Backtesting config 'portfolio' is empty. "
            "Add at least one ticker with a weight (e.g. {\"7203\": 0.5, \"6758\": 0.5})."
        )

    weight_sum = sum(portfolio_weights.values())
    if abs(weight_sum - 1.0) > 0.01:
        raise ValueError(
            f"Portfolio weights must sum to 1.0 (100%), "
            f"but they sum to {weight_sum:.4f} ({weight_sum * 100:.1f}%). "
            f"Adjust the weights and try again."
        )

    tickers = list(portfolio_weights.keys())
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
                None, start_date, end_date,
            )
            generate_report(empty_metrics, output_file)
            return empty_metrics

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
        metrics = calculate_metrics(portfolio_df, benchmark_df, start_date, end_date)

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

        # 8. Report
        generate_report(
            metrics, output_file,
            decomposition=decomposition,
            per_company=per_company,
            benchmark_df=benchmark_df,
        )

        # 9. Charts
        chart_files = generate_backtest_charts(
            decomposition, benchmark_df, per_company,
            output_dir, start_date, end_date,
        )
        metrics["chart_files"] = chart_files

        logger.info("Backtest complete. Total return: %.2f%%", metrics["total_return"] * 100)
        return metrics

    finally:
        conn.close() 



