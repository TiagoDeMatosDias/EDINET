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


def calculate_benchmark_returns(
    prices_df: pd.DataFrame,
    benchmark_ticker: str,
) -> pd.DataFrame:
    """Calculate daily returns for a single benchmark ticker.

    Args:
        prices_df: Long DataFrame with ``Date``, ``Ticker``, ``Price``.
        benchmark_ticker: The ticker symbol of the benchmark.

    Returns:
        DataFrame indexed by ``Date`` with columns:

        * ``benchmark_return`` — daily simple return.
        * ``cumulative_return`` — cumulative product of (1 + daily return).
    """
    bench = prices_df[prices_df["Ticker"] == benchmark_ticker].copy()
    bench = bench.sort_values("Date").set_index("Date")
    bench = bench[~bench.index.duplicated(keep="last")]
    bench["benchmark_return"] = bench["Price"].pct_change()
    bench["cumulative_return"] = (1 + bench["benchmark_return"]).cumprod()
    bench = bench.iloc[1:]  # drop the first NaN row
    return bench[["benchmark_return", "cumulative_return"]]


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
    else:
        metrics["benchmark_total_return"] = None
        metrics["benchmark_annualized_return"] = None
        metrics["excess_return"] = None

    return metrics


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------


def generate_report(metrics: dict, output_file: str) -> str:
    """Write a human-readable performance report to *output_file*.

    Args:
        metrics: Dictionary produced by :func:`calculate_metrics`.
        output_file: Destination file path.

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

    if metrics.get("benchmark_total_return") is not None:
        lines.append("")
        lines.append("--- Benchmark ---")
        lines.append(f"Benchmark Return:    {metrics['benchmark_total_return']:+.2%}")
        lines.append(f"Benchmark Ann. Ret.: {metrics['benchmark_annualized_return']:+.2%}")
        lines.append(f"Excess Return:       {metrics['excess_return']:+.2%}")

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

        # 2. Fetch dividends
        dividends_df = get_dividend_data(
            db_path, ratios_table, company_table, tickers, start_date, end_date,
            conn=conn,
        )

        # 3. Portfolio returns
        portfolio_df = calculate_portfolio_returns(
            prices_df[prices_df["Ticker"].isin(tickers)],
            portfolio_weights,
            dividends_df,
        )

        # 4. Benchmark returns
        benchmark_df = None
        if benchmark_ticker:
            bench_prices = prices_df[prices_df["Ticker"] == benchmark_ticker]
            if not bench_prices.empty:
                benchmark_df = calculate_benchmark_returns(prices_df, benchmark_ticker)

        # 5. Metrics
        metrics = calculate_metrics(portfolio_df, benchmark_df, start_date, end_date)

        # 6. Report
        generate_report(metrics, output_file)

        logger.info("Backtest complete. Total return: %.2f%%", metrics["total_return"] * 100)
        return metrics

    finally:
        conn.close() 



