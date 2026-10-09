"""
Web-facing backtesting logic.

Wraps the orchestrator-level backtesting functions
(:mod:`src.orchestrator.common.backtesting`) to produce JSON-serializable
chart data and metrics for the browser frontend.  Does **not** write
PNGs or text reports — Chart.js handles visualisation client-side.

Sections:
  1. Single backtest runner + helpers
  2. Backtest set runner (CSV-based)
  3. Screening backtest set
  4. Rolling screening backtest
"""

from __future__ import annotations

import io
import logging
from datetime import datetime
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

from src.orchestrator.common.backtesting import (
    _BACKTEST_DURATIONS,
    _sql_ident,
    build_daily_portfolio_tracker,
    calculate_benchmark_returns,
    calculate_dividends_by_company_year,
    calculate_yearly_returns,
    convert_dividends_to_base_currency,
    convert_prices_to_base_currency,
    get_dividend_data,
    get_portfolio_benchmark_returns,
    get_portfolio_prices,
    resolve_portfolio_allocations,
)
from src.orchestrator.common.sqlite import connect_read

if TYPE_CHECKING:
    import queue
    import threading

logger = logging.getLogger(__name__)

# ── Module-level helpers ─────────────────────────────────────────────────


def company_names(db_path: str, tickers: list[str], company_table: str = "CompanyInfo") -> dict[str, str]:
    """Company names for tickers, for labelling holdings in results and reports."""
    tickers = sorted({str(ticker) for ticker in tickers if ticker})
    if not tickers or not db_path:
        return {}
    try:
        conn = connect_read(db_path, busy_timeout_ms=10_000)
    except Exception:  # noqa: BLE001 - names are a label, never required
        return {}
    try:
        placeholders = ",".join("?" for _ in tickers)
        rows = conn.execute(
            f"SELECT Company_Ticker, Company_Name FROM {_sql_ident(company_table)} WHERE Company_Ticker IN ({placeholders})",
            tickers,
        ).fetchall()
        return {str(ticker): str(name) for ticker, name in rows if name}
    except Exception:  # noqa: BLE001
        return {}
    finally:
        conn.close()


_DUR_YEARS: dict[str, int] = {
    "1yr": 1, "2yr": 2, "3yr": 3, "5yr": 5, "10yr": 10,
}


def _annualize_return(total_return: float, dur: str) -> float:
    """Convert a total-period return to annualized (CAGR)."""
    yrs = _DUR_YEARS.get(dur, 1)
    if yrs <= 1:
        return total_return
    if total_return <= -1.0:
        return -1.0
    return (1.0 + total_return) ** (1.0 / yrs) - 1.0


# ==========================================================================
#  1. SINGLE BACKTEST RUNNER + HELPERS
# ==========================================================================


def _derive_per_company_from_tracker(
    daily_df: pd.DataFrame,
    tickers: list[str],
    portfolio_weights: dict[str, float],
    initial_capital: float = 0.0,
    dividends_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Derive per-company summary from the daily portfolio tracker.

    Returns a DataFrame with the same columns as
    ``calculate_per_company_returns`` for ZIP export / report compatibility.

    Note: *daily_df* has its first row dropped (pct_change), so start
    prices are derived from ``weight * initial_capital / shares`` rather
    than read from the truncated price columns.
    """
    records: list[dict] = []
    for t in tickers:
        price_col = f"price_{t}"
        shares_col = f"shares_{t}"
        div_cash_col = f"div_cash_{t}"
        if price_col not in daily_df.columns:
            continue
        shares = float(daily_df[shares_col].iloc[0]) if shares_col in daily_df.columns else 0.0
        weight = portfolio_weights.get(t, 0.0)
        capital_invested = weight * initial_capital if initial_capital > 0 else shares * float(daily_df[price_col].iloc[0])
        # Derive start price from capital and shares (daily_df starts at day 2)
        start_price = capital_invested / shares if shares > 0 else 0.0
        end_price = float(daily_df[price_col].iloc[-1])
        market_value = shares * end_price
        price_return = (end_price / start_price - 1.0) if start_price > 0 else 0.0
        total_divs_received = (
            float(daily_df[div_cash_col].iloc[-1])
            if div_cash_col in daily_df.columns else 0.0
        )
        dividend_return = total_divs_received / capital_invested if capital_invested > 0 else 0.0
        total_return = price_return + dividend_return
        records.append({
            "Ticker": t,
            "start_price": start_price,
            "end_price": end_price,
            "price_return": price_return,
            "dividend_return": dividend_return,
            "total_return": total_return,
            "weight": weight,
            "weighted_price": price_return * weight,
            "weighted_dividend": dividend_return * weight,
            "weighted_total": total_return * weight,
            "capital_invested": capital_invested,
            "shares_purchased": shares,
            "dividends_received": total_divs_received,
            "market_value": market_value,
        })
    cols = [
        "Ticker", "start_price", "end_price", "price_return",
        "dividend_return", "total_return", "weight",
        "weighted_price", "weighted_dividend", "weighted_total",
        "capital_invested", "shares_purchased",
        "dividends_received", "market_value",
    ]
    return pd.DataFrame(records, columns=cols) if records else pd.DataFrame(columns=cols)


def _swap_native_prices_in_pyp(
    per_company_per_year: pd.DataFrame,
    native_prices: dict[str, pd.DataFrame],
    ticker_native_currency: dict[str, str],
    tickers: list[str],
    base_currency: str,
) -> None:
    """Swap base-currency prices with native prices in per_company_per_year.

    Saves base-currency values with ``Base_`` prefix, then replaces
    Start_Price / End_Price / Starting_Market_Value / Ending_Market_Value
    with their native-currency equivalents.  Modifies *per_company_per_year*
    in place.
    """
    if per_company_per_year.empty:
        return

    if "Dividend_Currency" in per_company_per_year.columns:
        per_company_per_year["Dividend_Currency"] = (
            per_company_per_year["Ticker"].map(
                lambda t: ticker_native_currency.get(t, base_currency)
            )
        )

    # Save base-currency values before overwriting
    per_company_per_year["Base_Start_Price"] = per_company_per_year["Start_Price"]
    per_company_per_year["Base_End_Price"] = per_company_per_year["End_Price"]
    per_company_per_year["Base_Starting_Market_Value"] = per_company_per_year["Starting_Market_Value"]
    per_company_per_year["Base_Ending_Market_Value"] = per_company_per_year["Ending_Market_Value"]

    # Native prices on each row's own start and end dates (the previous
    # year's close and this year's close), not the whole period's.
    native_series: dict[str, pd.Series] = {}
    for tk in tickers:
        if tk in native_prices and len(native_prices[tk]) > 0:
            npx = native_prices[tk].sort_values("Date")
            native_series[tk] = pd.Series(npx[f"native_price_{tk}"].to_numpy(dtype=float), index=pd.DatetimeIndex(npx["Date"]))

    def _native_at(ticker: str, day: object) -> float | None:
        series = native_series.get(ticker)
        if series is None or not day:
            return None
        value = series.asof(pd.Timestamp(str(day)))
        return None if pd.isna(value) else float(value)

    has_dates = "Start_Date" in per_company_per_year.columns
    for idx, row in per_company_per_year.iterrows():
        tk = row["Ticker"]
        if tk not in native_series:
            per_company_per_year.at[idx, "Start_Price"] = 1.0 if tk == base_currency else None
            per_company_per_year.at[idx, "End_Price"] = 1.0 if tk == base_currency else None
            continue
        start = _native_at(tk, row["Start_Date"]) if has_dates else float(native_series[tk].iloc[0])
        end = _native_at(tk, row["End_Date"]) if has_dates else float(native_series[tk].iloc[-1])
        per_company_per_year.at[idx, "Start_Price"] = start
        per_company_per_year.at[idx, "End_Price"] = end
        shares_val = row["Starting_Shares"] if row["Starting_Shares"] > 0 else 0
        if shares_val > 0 and start is not None and end is not None:
            per_company_per_year.at[idx, "Starting_Market_Value"] = shares_val * start
            per_company_per_year.at[idx, "Ending_Market_Value"] = shares_val * end

    # Cash row prices are always 1.0
    cash_mask = per_company_per_year["Start_Price"].isna()
    per_company_per_year.loc[cash_mask, "Start_Price"] = 1.0
    per_company_per_year.loc[cash_mask, "End_Price"] = 1.0


def run_backtest_web(
    db_path: str,
    portfolio: dict[str, dict],
    start_date: str,
    end_date: str,
    *,
    benchmark_ticker: str = "",
    benchmark_mode: str = "ticker",
    base_currency: str = "",
    db3_path: str = "",
    initial_capital: float = 0.0,
    risk_free_rate: float = 0.0,
    commission_bps: float = 0.0,
    slippage_bps: float = 0.0,
    spread_bps: float = 0.0,
    portfolio_owner: str = "",
    prices_table: str = "Stock_Prices",
    ratios_table: str = "ShareMetrics",
    company_table: str = "CompanyInfo",
    financial_statements_table: str = "FinancialStatements",
) -> dict:
    """Run a single portfolio backtest and return JSON-serializable results.

    ``portfolio_owner`` names the account whose own portfolio is the
    benchmark when ``benchmark_mode`` is ``"portfolio"``.

    See :ref:`BacktestResult` in the implementation plan for the return
    type schema.
    """
    # ── Validate inputs ───────────────────────────────────────────────
    if not portfolio:
        raise ValueError("Portfolio is empty — add at least one ticker.")
    if not start_date or not end_date:
        raise ValueError("start_date and end_date are required.")
    if start_date >= end_date:
        raise ValueError("start_date must be before end_date.")

    tickers = list(portfolio.keys())
    warnings: list[str] = []

    # ── Resolve base currency early so conversion happens once ─────────
    if benchmark_mode == "portfolio" and not base_currency:
        base_currency = "EUR"

    all_tickers = list(tickers)
    if benchmark_mode == "ticker" and benchmark_ticker and benchmark_ticker not in all_tickers:
        all_tickers.append(benchmark_ticker)

    # ── Fetch data ────────────────────────────────────────────────────
    prices_df = get_portfolio_prices(
        db_path, prices_table, all_tickers, start_date, end_date,
    )

    if prices_df.empty:
        return _empty_result(start_date, end_date, initial_capital,
                             has_benchmark=bool(benchmark_ticker) or benchmark_mode == "portfolio",
                             warnings=["No price data available for the "
                                       "selected tickers in this date range."])

    # ── Capture native currencies and prices before conversion ──────────
    ticker_native_currency: dict[str, str] = {}
    native_prices: dict[str, pd.DataFrame] = {}  # ticker → native-price DataFrame
    for tk in tickers:
        tk_rows = prices_df[prices_df["Ticker"] == tk][["Date", "Price", "Currency"]].copy()
        if len(tk_rows) > 0:
            ticker_native_currency[tk] = str(tk_rows["Currency"].iloc[0]) if "Currency" in tk_rows.columns else ""
            native_prices[tk] = tk_rows.rename(columns={"Price": f"native_price_{tk}"})
    if benchmark_ticker:
        bench_rows = prices_df[prices_df["Ticker"] == benchmark_ticker][["Date", "Price", "Currency"]].copy()
        if len(bench_rows) > 0:
            if "Currency" in bench_rows.columns:
                ticker_native_currency[benchmark_ticker] = str(bench_rows["Currency"].iloc[0])
            native_prices[benchmark_ticker] = bench_rows.rename(columns={"Price": f"native_price_{benchmark_ticker}"})

    # ── Currency conversion (prices) — portfolio math needs single currency ──
    fx_rates_used: dict[str, dict[str, float]] = {}
    if base_currency and base_currency != "":
        prices_df = convert_prices_to_base_currency(
            prices_df, base_currency, db_path,
        )
        for tk in tickers:
            native = ticker_native_currency.get(tk, "")
            if native and native != base_currency:
                try:
                    from src.portfolio.currency import get_fx_series
                    fx_rates_used[tk] = get_fx_series(native, base_currency, db_path)
                except Exception:
                    pass

    # Check missing tickers
    available = set(prices_df["Ticker"].unique())
    for tk in tickers:
        if tk not in available:
            warnings.append(f"Ticker '{tk}' not found in price data.")

    # Resolve allocations
    start_prices: dict[str, float] = {}
    for tk in tickers:
        tk_prices = prices_df[prices_df["Ticker"] == tk].sort_values("Date")
        if not tk_prices.empty:
            start_prices[tk] = float(tk_prices["Price"].iloc[0])

    portfolio_weights, effective_capital, alloc_warnings = (
        resolve_portfolio_allocations(portfolio, start_prices, initial_capital)
    )
    warnings.extend(alloc_warnings)

    if effective_capital > 0 and initial_capital <= 0:
        initial_capital = effective_capital

    if not portfolio_weights:
        return _empty_result(start_date, end_date, initial_capital,
                             has_benchmark=bool(benchmark_ticker) or benchmark_mode == "portfolio",
                             warnings=["No valid tickers with price data "
                                       "in the selected date range."])

    # Fetch dividends (portfolio tickers only)
    dividends_df = get_dividend_data(
        db_path, ratios_table, company_table, tickers,
        start_date, end_date,
        financial_statements_table=financial_statements_table,
    )

    all_dividends_df = dividends_df
    if benchmark_mode == "ticker" and benchmark_ticker:
        bench_divs = get_dividend_data(
            db_path, ratios_table, company_table,
            [benchmark_ticker], start_date, end_date,
            financial_statements_table=financial_statements_table,
        )
        if not bench_divs.empty:
            all_dividends_df = pd.concat(
                [dividends_df, bench_divs], ignore_index=True,
            )

    # ── Dividend currency conversion (after ALL dividends are fetched) ──
    if base_currency and base_currency != "" and not all_dividends_df.empty:
        all_dividends_df = convert_dividends_to_base_currency(
            all_dividends_df, base_currency, db_path,
        )
        dividends_df = all_dividends_df[
            all_dividends_df["Ticker"].isin(tickers)
        ]

    portfolio_prices = prices_df[prices_df["Ticker"].isin(tickers)]

    # ── Default initial capital so shares and cash dividends are always visible ──
    effective_initial_capital = initial_capital if initial_capital > 0 else 1_000_000.0

    # ── 1. PRIMARY: per-company per-year breakdown ──────────────────────
    # This single call builds the daily tracker, yearly aggregate, and
    # all performance metrics in one pass — guaranteeing consistency.
    tracker = build_daily_portfolio_tracker(
        portfolio_prices, portfolio_weights, dividends_df,
        initial_capital=effective_initial_capital,
        base_currency=base_currency or "",
        risk_free_rate=risk_free_rate,
        commission_bps=commission_bps,
        slippage_bps=slippage_bps,
        spread_bps=spread_bps,
    )
    per_company_per_year = tracker["per_company_per_year"]
    daily_df = tracker["daily"]
    metrics = tracker["metrics"]
    warnings.extend(tracker.get("notes", []))
    metrics["base_currency"] = base_currency or ticker_native_currency.get(tickers[0] if tickers else "", "")

    # Swap native/base prices for display (native prices become primary)
    _swap_native_prices_in_pyp(per_company_per_year, native_prices,
                               ticker_native_currency, tickers,
                               base_currency or "")

    # Set dates from the daily tracker (actual data range, not requested);
    # the tracker's start is the purchase day, before the first daily return.
    if len(daily_df) > 0:
        metrics["start_date"] = metrics.get("start_date") or str(daily_df.index[0].date())
        metrics["end_date"] = str(daily_df.index[-1].date())
    else:
        metrics["start_date"] = start_date
        metrics["end_date"] = end_date

    # Build chart-compatible DataFrames from the daily tracker
    portfolio_df = pd.DataFrame({
        "portfolio_return": daily_df["daily_return"],
        "cumulative_return": daily_df["cumulative_return"],
    })
    decomposition = {
        "total": pd.DataFrame({
            "daily_return": daily_df["daily_return"],
            "cumulative_return": daily_df["cumulative_return"],
        }),
        "price_only": pd.DataFrame({
            "daily_return": daily_df["price_only_daily_return"],
            "cumulative_return": daily_df["price_only_cum_return"],
        }),
        "dividend_only": pd.DataFrame({
            "daily_return": daily_df["dividend_cum_contribution"].diff().fillna(0.0),
            "cumulative_return": 1.0 + daily_df["dividend_cum_contribution"],
        }),
    }

    # ── 4. Per-company (derived from tracker — single source of truth) ─
    per_company = _derive_per_company_from_tracker(
        daily_df, tickers, portfolio_weights,
        initial_capital=effective_initial_capital,
        dividends_df=dividends_df,
    )
    # Extract share counts directly from the tracker columns
    shares_map: dict[str, float] = {}
    for t in tickers:
        shares_col = f"shares_{t}"
        if shares_col in daily_df.columns:
            shares_map[t] = float(daily_df[shares_col].iloc[0])
    dividends_by_year = calculate_dividends_by_company_year(
        dividends_df, shares_purchased=shares_map or None,
    )

    # ── 5. Yearly returns (from per-company-per-year) ───────────────────
    # Legacy yearly_returns feed the frontend's yearly table
    yearly_returns = calculate_yearly_returns(decomposition) if decomposition else None

    # ── Benchmark ──
    benchmark_df = None
    if benchmark_mode == "ticker" and benchmark_ticker:
        bench_prices = prices_df[prices_df["Ticker"] == benchmark_ticker]
        if not bench_prices.empty:
            benchmark_df = calculate_benchmark_returns(
                prices_df, benchmark_ticker, all_dividends_df,
            )
        else:
            warnings.append(f"Benchmark '{benchmark_ticker}' has no stored prices in this period; results have no benchmark.")
    elif benchmark_mode == "portfolio" and db3_path:
        # Currency conversion already handled above (base_currency defaults
        # to "EUR" for portfolio benchmarks).  No need to recompute tracker.
        benchmark_df = get_portfolio_benchmark_returns(
            db3_path, start_date, end_date, base_currency, db_path, owner_user_id=portfolio_owner,
        )

    # ── Compare portfolio and benchmark over the days both have ──
    # The portfolio's own metrics keep its full period; the comparison
    # (benchmark return, excess, tracking) uses the overlap only.
    compare_df = portfolio_df
    if benchmark_df is not None and not benchmark_df.empty:
        common_idx = portfolio_df.index.intersection(benchmark_df.index)
        if len(common_idx) > 1:
            if common_idx[0] > portfolio_df.index[0] + pd.Timedelta(days=7) or common_idx[-1] < portfolio_df.index[-1] - pd.Timedelta(days=7):
                label = "Portfolio benchmark" if benchmark_mode == "portfolio" else f"Benchmark '{benchmark_ticker}'"
                warnings.append(
                    f"{label} covers only {common_idx[0]:%Y-%m-%d} → {common_idx[-1]:%Y-%m-%d}; "
                    "the comparison uses that period."
                )
            compare_df = portfolio_df.loc[common_idx]
            benchmark_df = benchmark_df.loc[common_idx]
            if benchmark_mode == "portfolio":
                portfolio_df = compare_df
        else:
            warnings.append("The benchmark has too little data overlapping the portfolio's; results have no benchmark.")
            benchmark_df = None

    # ── Benchmark metrics ───────────────────────────────────────────────
    if benchmark_df is not None and not benchmark_df.empty:
        bench_cum = benchmark_df["cumulative_return"]
        # Rebase both to the first common day, so each is the return over the same window.
        bench_total = float(bench_cum.iloc[-1] / (bench_cum.iloc[0] / (1 + benchmark_df["benchmark_return"].iloc[0])) - 1)
        port_cum = compare_df["cumulative_return"]
        port_base = port_cum.iloc[0] / (1 + compare_df["portfolio_return"].iloc[0])
        port_window_total = float(port_cum.iloc[-1] / port_base - 1) if port_base else metrics["total_return"]
        window_days = (benchmark_df.index[-1] - benchmark_df.index[0]).days + 1
        yrs = max(window_days / 365.25, 1 / 365.25)
        bench_ann = (1 + bench_total) ** (1 / yrs) - 1 if bench_total > -1 else -1.0
        port_window_ann = (1 + port_window_total) ** (1 / yrs) - 1 if port_window_total > -1 else -1.0
        metrics["benchmark_total_return"] = bench_total
        metrics["benchmark_annualized_return"] = bench_ann
        metrics["excess_return"] = port_window_total - bench_total
        metrics["excess_annualized_return"] = port_window_ann - bench_ann
        metrics["comparison_start"] = f"{benchmark_df.index[0]:%Y-%m-%d}"
        metrics["comparison_end"] = f"{benchmark_df.index[-1]:%Y-%m-%d}"

        bench_daily = benchmark_df["benchmark_return"].dropna()
        bd_std = float(bench_daily.std()) if len(bench_daily) > 1 else 0.0
        bench_vol = float(bd_std * np.sqrt(252))
        running_max_b = bench_cum.cummax()
        drawdowns_b = (bench_cum - running_max_b) / running_max_b
        bench_max_dd = float(drawdowns_b.min()) if len(drawdowns_b) > 0 else 0.0

        portfolio_daily = portfolio_df["portfolio_return"].dropna() if len(portfolio_df) else pd.Series(dtype=float)
        excess_daily = None
        if len(portfolio_daily) > 0 and len(bench_daily) > 0:
            common_idx = portfolio_daily.index.intersection(bench_daily.index)
            if len(common_idx) > 1:
                excess_daily = portfolio_daily.loc[common_idx] - bench_daily.loc[common_idx]
        te_std = float(excess_daily.std()) if excess_daily is not None and len(excess_daily) > 1 else 0.0
        tracking_error = float(te_std * np.sqrt(252))
        # Annualised active return over annualised tracking error.
        active_ann = float(excess_daily.mean() * 252) if excess_daily is not None and len(excess_daily) > 1 else 0.0
        info_ratio = active_ann / tracking_error if tracking_error > 0 else 0.0
        metrics["tracking_error"] = tracking_error

        metrics["benchmark_volatility"] = bench_vol
        metrics["benchmark_max_drawdown"] = bench_max_dd
        metrics["information_ratio"] = info_ratio
        rf = metrics.get("risk_free_rate", 0.0)
        metrics["benchmark_sharpe_ratio"] = (bench_ann - rf) / bench_vol if bench_vol > 0 else 0.0
    else:
        metrics["benchmark_total_return"] = None
        metrics["benchmark_annualized_return"] = None
        metrics["benchmark_volatility"] = None
        metrics["benchmark_max_drawdown"] = None
        metrics["excess_return"] = None
        metrics["excess_annualized_return"] = None
        metrics["information_ratio"] = None
        metrics["benchmark_sharpe_ratio"] = None
        metrics["tracking_error"] = None

    # ── Build chart data ──────────────────────────────────────────────
    chart_data = _build_chart_data(
        decomposition, benchmark_df, portfolio_df,
        initial_capital=effective_initial_capital,
    )

    # ── Per-company records ───────────────────────────────────────────
    per_company_records = (
        per_company.to_dict(orient="records")
        if per_company is not None and not per_company.empty
        else []
    )
    # Add native currency to each capital allocation record
    for rec in per_company_records:
        tk = rec.get("Ticker", "")
        rec["Currency"] = ticker_native_currency.get(tk, "")

    # ── Per-company per-year records (new primary table) ──────────────
    per_company_per_year_records = (
        per_company_per_year.to_dict(orient="records")
        if not per_company_per_year.empty
        else []
    )

    # ── Legacy yearly returns ─────────────────────────────────────────
    legacy_yearly_records = (
        yearly_returns.to_dict(orient="records")
        if yearly_returns is not None and not yearly_returns.empty
        else []
    )

    # ── Dividends by year pivot ───────────────────────────────────────
    div_records = _pivot_dividends(dividends_by_year)

    # ── Daily per-company breakdown (the source of truth) ──────────────
    daily_records: list[dict] = []
    if len(daily_df) > 0:
        daily_reset = daily_df.reset_index()
        daily_reset["Date"] = daily_reset["Date"].dt.strftime("%Y-%m-%d")
        # Merge benchmark price + returns into daily data
        if benchmark_ticker and benchmark_mode == "ticker":
            bench_px = prices_df[prices_df["Ticker"] == benchmark_ticker][["Date", "Price"]].copy()
            if len(bench_px) > 0:
                bench_px["Date"] = bench_px["Date"].dt.strftime("%Y-%m-%d")
                bench_px = bench_px.rename(columns={"Price": "bench_price"})
                daily_reset = daily_reset.merge(bench_px, on="Date", how="left")
                daily_reset["bench_price"] = daily_reset["bench_price"].ffill()
        if benchmark_df is not None and not benchmark_df.empty:
            bench_daily = benchmark_df.reset_index()
            bench_daily = bench_daily.rename(columns={bench_daily.columns[0]: "Date"})
            bench_daily["Date"] = pd.to_datetime(bench_daily["Date"]).dt.strftime("%Y-%m-%d")
            columns = [col for col in ("benchmark_return", "cumulative_return", "price_return", "cum_price_return",
                                       "dividend_return", "cum_dividend_return") if col in bench_daily.columns]
            # Joined by date: the two series need not share every day.
            daily_reset = daily_reset.merge(
                bench_daily[["Date", *columns]].rename(columns={col: f"bench_{col}" for col in columns}),
                on="Date", how="left",
            )
        # Swap: native prices become primary, base-currency becomes suffixed
        for tk in tickers:
            daily_reset[f"currency_{tk}"] = ticker_native_currency.get(tk, "")
            if tk in native_prices:
                np_df = native_prices[tk][["Date", f"native_price_{tk}"]].copy()
                np_df["Date"] = np_df["Date"].dt.strftime("%Y-%m-%d")
                daily_reset = daily_reset.merge(np_df, on="Date", how="left")
                daily_reset[f"native_price_{tk}"] = daily_reset[f"native_price_{tk}"].ffill()
                # Swap: price_{t} → base_price_{t}, native_price_{t} → price_{t}
                if f"price_{tk}" in daily_reset.columns:
                    daily_reset[f"base_price_{tk}"] = daily_reset[f"price_{tk}"]
                    daily_reset[f"price_{tk}"] = daily_reset[f"native_price_{tk}"]
                    daily_reset.drop(columns=[f"native_price_{tk}"], inplace=True)
                # Native market value: shares × native price
                if f"shares_{tk}" in daily_reset.columns:
                    daily_reset[f"native_mktval_{tk}"] = (
                        daily_reset[f"shares_{tk}"] * daily_reset[f"price_{tk}"]
                    )
            if tk in fx_rates_used:
                fx_map = fx_rates_used[tk]
                daily_reset[f"fx_rate_{tk}"] = daily_reset["Date"].map(fx_map)
        if benchmark_ticker:
            daily_reset[f"currency_{benchmark_ticker}"] = ticker_native_currency.get(benchmark_ticker, "")
        daily_reset["base_currency"] = base_currency or ticker_native_currency.get(tickers[0] if tickers else "", "")
        daily_records = daily_reset.to_dict(orient="records")

    # ── Add benchmark rows to per-company-per-year ────────────────────
    if benchmark_ticker and benchmark_mode == "ticker" and not per_company_per_year.empty:
        bench_px_all = prices_df[prices_df["Ticker"] == benchmark_ticker].copy()
        if len(bench_px_all) > 0:
            bench_px_all["Year"] = bench_px_all["Date"].dt.year
            bench_divs_all = all_dividends_df[all_dividends_df["Ticker"] == benchmark_ticker].copy() if len(all_dividends_df) > 0 else pd.DataFrame()
            bench_yearly_rows = []
            bench_px_all = bench_px_all.sort_values("Date")
            bench_native = native_prices.get(benchmark_ticker)
            bench_native_series = (
                pd.Series(bench_native[f"native_price_{benchmark_ticker}"].to_numpy(dtype=float), index=pd.DatetimeIndex(bench_native["Date"])).sort_index()
                if bench_native is not None and len(bench_native) else None
            )
            bench_div_years: dict[int, float] = {}
            if len(bench_divs_all) > 0:
                for record_date, amount in zip(pd.to_datetime(bench_divs_all["periodEnd"]), bench_divs_all["PerShare_Dividends"], strict=True):
                    if bench_px_all["Date"].iloc[0] <= record_date <= bench_px_all["Date"].iloc[-1]:
                        bench_div_years[record_date.year] = bench_div_years.get(record_date.year, 0.0) + float(amount or 0.0)
            for year in sorted(per_company_per_year["Year"].unique()):
                yr_px = bench_px_all[bench_px_all["Year"] == year]
                if len(yr_px) == 0:
                    continue
                # Like the holdings: from the previous year's close.
                before = bench_px_all[bench_px_all["Year"] < year]
                start_row = before.iloc[-1] if len(before) else yr_px.iloc[0]
                s_price = float(start_row["Price"])
                e_price = float(yr_px["Price"].iloc[-1])
                start_day, end_day = start_row["Date"], yr_px["Date"].iloc[-1]
                bench_native_start = float(bench_native_series.asof(start_day)) if bench_native_series is not None else None
                bench_native_end = float(bench_native_series.asof(end_day)) if bench_native_series is not None else None
                div_ps = bench_div_years.get(int(year), 0.0)
                price_ret = (e_price - s_price) / s_price if s_price > 0 else 0.0
                div_ret = div_ps / s_price if s_price > 0 else 0.0
                bench_start_mkt = effective_initial_capital  # same capital invested
                bench_end_mkt = bench_start_mkt * (1.0 + price_ret)
                bench_yearly_rows.append({
                    "Year": year,
                    "Ticker": f"BENCH:{benchmark_ticker}",
                    "Start_Date": start_day.strftime("%Y-%m-%d"),
                    "End_Date": end_day.strftime("%Y-%m-%d"),
                    "Starting_Shares": 0,
                    "Dividend_Per_Share": div_ps,
                    "Start_Price": bench_native_start if bench_native_start is not None else s_price,
                    "End_Price": bench_native_end if bench_native_end is not None else e_price,
                    "Total_Dividends_Received": 0,
                    "Dividend_Currency": ticker_native_currency.get(benchmark_ticker, ""),
                    "Ending_Shares": 0,
                    "Starting_Market_Value": effective_initial_capital,
                    "Ending_Market_Value": effective_initial_capital * (1.0 + price_ret),
                    "Price_Return_Pct": price_ret,
                    "Dividend_Return_Pct": div_ret,
                    "Total_Return_Pct": price_ret + div_ret,
                    "Weighted_Value_Start": 0.0,
                    "Weighted_Value_End": 0.0,
                    "Weighted_Return": 0.0,
                    "Base_Start_Price": s_price,
                    "Base_End_Price": e_price,
                    "Base_Starting_Market_Value": bench_start_mkt,
                    "Base_Ending_Market_Value": bench_end_mkt,
                })
            if bench_yearly_rows:
                per_company_per_year = pd.concat([
                    per_company_per_year,
                    pd.DataFrame(bench_yearly_rows),
                ], ignore_index=True)
                per_company_per_year_records = per_company_per_year.to_dict(orient="records")

    for payment in tracker.get("dividend_ledger") or []:
        payment["currency"] = ticker_native_currency.get(payment["ticker"], "")

    return {
        "metrics": metrics,
        "chart_data": chart_data,
        "per_company": per_company_records,
        "per_company_per_year": per_company_per_year_records,
        "daily": daily_records,
        "yearly_returns": legacy_yearly_records,
        "dividends_by_year": div_records,
        "dividend_payments": tracker.get("dividend_ledger") or [],
        "names": company_names(db_path, [*tickers, benchmark_ticker], company_table),
        "warnings": warnings,
    }


def _empty_result(
    start_date: str,
    end_date: str,
    initial_capital: float,
    *,
    has_benchmark: bool = False,
    warnings: list[str] | None = None,
) -> dict:
    """Return a BacktestResult with empty chart data and zero metrics."""
    bm = {
        "benchmark_total_return": None,
        "benchmark_annualized_return": None,
        "benchmark_volatility": None,
        "benchmark_max_drawdown": None,
        "excess_return": None,
        "information_ratio": None,
    } if has_benchmark else {}
    return {
        "no_data": True,
        "metrics": {
            "no_data": True,
            "total_return": 0.0,
            "annualized_return": 0.0,
            "volatility": 0.0,
            "sharpe_ratio": 0.0,
            "max_drawdown": 0.0,
            "start_date": start_date,
            "end_date": end_date,
            "portfolio_price_return": 0.0,
            "portfolio_dividend_return": 0.0,
            "initial_capital": initial_capital,
            **bm,
        },
        "chart_data": {
            "cumulative": [],
            "drawdown": [],
            "decomposition": [],
        },
        "per_company": [],
        "yearly_returns": [],
        "dividends_by_year": [],
        "warnings": warnings or [],
    }


def _build_chart_data(
    decomposition: dict[str, pd.DataFrame],
    benchmark_df: pd.DataFrame | None,
    portfolio_df: pd.DataFrame,
    *,
    initial_capital: float = 1_000_000.0,
) -> dict:
    """Convert pandas DataFrames to chart.js-friendly record arrays.

    Includes VAMI (Value Added Monthly Index) = cumulative_return ×
    *initial_capital* in the cumulative data points.
    """
    cumulative: list[dict] = []
    drawdown: list[dict] = []
    decomposition_data: list[dict] = []

    total_df = decomposition.get("total")
    price_only = decomposition.get("price_only")
    div_only = decomposition.get("dividend_only")

    if total_df is not None and not total_df.empty:
        total_cum = total_df["cumulative_return"]

        # Cumulative + drawdown
        running_max = total_cum.cummax()
        dd = (total_cum - running_max) / running_max

        bench_cum_map: dict = {}
        bench_dd_map: dict = {}
        if benchmark_df is not None and not benchmark_df.empty:
            b_cum = benchmark_df["cumulative_return"]
            # A benchmark that starts later is drawn from the portfolio's level
            # on its first day, so the two lines diverge from a common point.
            first = b_cum.index[0]
            if first in total_cum.index and first > total_cum.index[0]:
                start_level = float(total_cum.loc[first]) / (1 + float(total_df.loc[first, "daily_return"] or 0.0))
                b_base = float(b_cum.iloc[0]) / (1 + float(benchmark_df["benchmark_return"].iloc[0] or 0.0))
                if b_base:
                    b_cum = b_cum / b_base * start_level
            b_max = b_cum.cummax()
            b_dd = (b_cum - b_max) / b_max
            bench_cum_map = {d.strftime("%Y-%m-%d"): float(v) - 1
                             for d, v in b_cum.items()}
            bench_dd_map = {d.strftime("%Y-%m-%d"): float(v)
                            for d, v in b_dd.items()}

        for d in total_cum.index:
            ds = d.strftime("%Y-%m-%d")
            cum_val = float(total_cum.loc[d])
            cumulative.append({
                "date": ds,
                "portfolio": cum_val - 1,
                "vami": cum_val * initial_capital,
                "benchmark": bench_cum_map.get(ds),
            })
            drawdown.append({
                "date": ds,
                "portfolio": float(dd.loc[d]) if d in dd.index else 0.0,
                "benchmark": bench_dd_map.get(ds),
            })

        # Decomposition: price_only + dividend_only on common index
        if price_only is not None and not price_only.empty and div_only is not None and not div_only.empty:
            common_idx = price_only.index.intersection(div_only.index)
            for d in common_idx:
                ds = d.strftime("%Y-%m-%d")
                decomposition_data.append({
                    "date": ds,
                    "price_only": float(price_only.loc[d, "cumulative_return"]) - 1,
                    "dividend_only": float(div_only.loc[d, "cumulative_return"]) - 1,
                    "total": float(total_df.loc[d, "cumulative_return"]) - 1 if d in total_df.index else 0.0,
                })

    return {
        "cumulative": cumulative,
        "drawdown": drawdown,
        "decomposition": decomposition_data,
    }


def _pivot_dividends(div_df: pd.DataFrame | None) -> list[dict]:
    """Convert dividends-by-year pivot to a list of records."""
    if div_df is None or div_df.empty:
        return []
    records = []
    for year_val, row in div_df.iterrows():
        rec = {"year": int(year_val)}
        for col in div_df.columns:
            rec[col] = float(row[col])
        records.append(rec)
    return records


# ==========================================================================
#  2. BACKTEST SET RUNNER (CSV-BASED)
# ==========================================================================


def run_backtest_set_web(
    db_path: str,
    csv_content: str,
    *,
    durations: list[str] | None = None,
    benchmark_ticker: str = "",
    benchmark_mode: str = "ticker",
    base_currency: str = "",
    db3_path: str = "",
    initial_capital: float = 0.0,
    risk_free_rate: float = 0.0,
    portfolio_owner: str = "",
    prices_table: str = "Stock_Prices",
    ratios_table: str = "ShareMetrics",
    company_table: str = "CompanyInfo",
    financial_statements_table: str = "FinancialStatements",
) -> dict:
    """Run multiple backtests from a CSV content string.

    The CSV format follows the backtest-set convention:

    * Columns: ``Year``, ``Tickers``, ``Type``, ``Amount``; ``weight``
      amounts may be fractions or percentages (a year's weights summing to
      more than 1.5 are read as percentages)
    * Optional comment headers: ``# Benchmark:``, ``# Discount Rate:``

    For each ``Year`` row, one backtest is run per requested ``duration``,
    anchored at that year's start (e.g. ``2020-01-01`` for 1yr →
    ``2021-01-01``).

    Args:
        db_path: Path to the SQLite database.
        csv_content: Raw CSV string (the full file content).
        durations: List of duration labels (e.g. ``["1yr","2yr","3yr","5yr","10yr"]``).
            Defaults to all five durations.
        benchmark_ticker: Optional benchmark ticker.
        initial_capital: Starting capital (0 = derive).
        risk_free_rate: Annual risk-free rate.

    Returns:
        A ``BacktestSetResult`` dict with ``aggregate`` and ``results`` keys.
    """
    if durations is None:
        durations = ["1yr", "2yr", "3yr", "5yr", "10yr"]

    # ── Parse CSV comments for embedded config ────────────────────────
    csv_benchmark = ""
    csv_discount = ""
    for line in io.StringIO(csv_content):
        stripped = line.strip()
        if not stripped.startswith("#"):
            break
        if stripped.startswith("# Benchmark:"):
            csv_benchmark = stripped[len("# Benchmark:"):].strip()
        elif stripped.startswith("# Discount Rate:"):
            csv_discount = stripped[len("# Discount Rate:"):].strip()

    if not benchmark_ticker:
        benchmark_ticker = csv_benchmark
    if not risk_free_rate and csv_discount:
        try:
            risk_free_rate = float(csv_discount)
        except ValueError:
            logger.warning("Invalid Discount Rate in CSV: %r", csv_discount)

    # ── Parse CSV data ────────────────────────────────────────────────
    try:
        df = pd.read_csv(io.StringIO(csv_content), comment="#")
    except Exception as e:
        raise ValueError(f"Failed to parse CSV: {e}") from e

    required = {"Year", "Tickers", "Type", "Amount"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"CSV is missing required columns: {missing}. "
            f"Expected: {required}"
        )

    df["Year"] = df["Year"].astype(str).str.strip()
    years = sorted(df["Year"].unique())

    if not years:
        return {
            "aggregate": {
                "total_runs": 0,
                "successful": 0,
                "failed": 0,
                "benchmark_comparison": None,
                "stats": None,
            },
            "results": [],
        }

    all_results: list[dict] = []
    successful = 0
    failed = 0

    for year_str in years:
        year_df = df[df["Year"] == year_str]

        portfolio: dict[str, dict] = {}
        for _, row in year_df.iterrows():
            ticker = str(row["Tickers"]).strip()
            mode = str(row["Type"]).strip().lower()
            amount = float(row["Amount"])
            portfolio[ticker] = {"mode": mode, "value": amount}

        if not portfolio:
            continue
        # Weights may be written as fractions (0.25) or percentages (25).
        weights = [spec["value"] for spec in portfolio.values() if spec["mode"] == "weight"]
        if weights and sum(weights) > 1.5:
            for spec in portfolio.values():
                if spec["mode"] == "weight":
                    spec["value"] /= 100.0

        for dur_label in durations:
            dur_years = _BACKTEST_DURATIONS.get(dur_label)
            if dur_years is None:
                logger.warning("Unknown duration '%s', skipping.", dur_label)
                continue

            bt_start = f"{year_str}-01-01"
            end_year = int(year_str) + dur_years
            bt_end = f"{end_year}-01-01"

            result_entry: dict = {
                "year": year_str,
                "duration": dur_label,
                "start_date": bt_start,
                "end_date": bt_end,
                "tickers": list(portfolio.keys()),
                "metrics": None,
                "chart_data": {},
                "per_company": [],
                "yearly_returns": [],
                "dividends_by_year": [],
                "warnings": [],
            }

            try:
                bt_result = run_backtest_web(
                    db_path=db_path,
                    portfolio=portfolio,
                    start_date=bt_start,
                    end_date=bt_end,
                    benchmark_ticker=benchmark_ticker,
                    benchmark_mode=benchmark_mode,
                    base_currency=base_currency,
                    db3_path=db3_path,
                    initial_capital=initial_capital,
                    risk_free_rate=risk_free_rate,
                    portfolio_owner=portfolio_owner,
                    prices_table=prices_table,
                    ratios_table=ratios_table,
                    company_table=company_table,
                    financial_statements_table=financial_statements_table,
                )
                if bt_result.get("no_data"):
                    raise ValueError("; ".join(bt_result.get("warnings") or ["No price data"]))
                result_entry["metrics"] = bt_result["metrics"]
                last_date = bt_result["metrics"].get("end_date") or ""
                if last_date and last_date < (datetime.strptime(bt_end, "%Y-%m-%d") - pd.Timedelta(days=10)).strftime("%Y-%m-%d"):
                    result_entry["truncated"] = True
                result_entry["requested_end"] = bt_end
                result_entry["chart_data"] = bt_result["chart_data"]
                result_entry["per_company"] = bt_result["per_company"]
                result_entry["yearly_returns"] = bt_result["yearly_returns"]
                result_entry["dividends_by_year"] = bt_result["dividends_by_year"]
                result_entry["warnings"] = bt_result["warnings"]
                successful += 1
            except Exception as e:
                result_entry["warnings"].append(str(e))
                failed += 1

            all_results.append(result_entry)

    # ── Aggregate summary ─────────────────────────────────────────────
    aggregate = _build_aggregate_summary(all_results, successful, failed,
                                         benchmark_ticker, durations,
                                         benchmark_mode=benchmark_mode,
                                         base_currency=base_currency)

    # The same per-run rows and paths as a rolling run, keyed by year.
    as_rolling = [
        {"period": f"{year}-01-01", "tickers": next((r["tickers"] for r in all_results if r["year"] == year), []),
         "backtests": {"csv": {r["duration"]: r for r in all_results if r["year"] == year}}}
        for year in years
    ]
    return {
        "aggregate": aggregate,
        "runs": rolling_run_rows(as_rolling, durations, ["csv"]),
        "paths": rolling_paths(as_rolling),
        "run_holdings": rolling_run_holdings(as_rolling),
        "names": company_names(db_path, [ticker for item in as_rolling for ticker in item["tickers"]], company_table),
        "period_holdings": {item["period"]: item["tickers"] for item in as_rolling},
        "results": all_results,
    }


def _build_aggregate_summary(
    all_results: list[dict],
    successful: int,
    failed: int,
    benchmark_ticker: str,
    durations: list[str],
    benchmark_mode: str = "ticker",
    base_currency: str = "",
) -> dict:
    """Build the aggregate statistics for a backtest set."""
    total_runs = len(all_results)
    # Truncated runs (prices end before the holding period) are not results for their duration.
    success_results = [r for r in all_results if r.get("metrics") and not r.get("truncated")]

    # Benchmark comparison
    has_bench = [
        r for r in success_results
        if r["metrics"].get("benchmark_total_return") is not None
    ]
    outperformed = len([
        r for r in has_bench
        if r["metrics"]["total_return"] > r["metrics"]["benchmark_total_return"]
    ])
    underperformed = len(has_bench) - outperformed

    by_duration: dict[str, dict] = {}
    if has_bench:
        for dur in durations:
            dur_results = [r for r in has_bench if r["duration"] == dur]
            dur_out = len([
                r for r in dur_results
                if r["metrics"]["total_return"] > r["metrics"]["benchmark_total_return"]
            ])
            by_duration[dur] = {"out": dur_out, "total": len(dur_results)}

    benchmark_comparison = {
        "outperformed": outperformed,
        "underperformed": underperformed,
        "by_duration": by_duration,
    } if has_bench else None

    # Aggregate stats
    stats = None
    if success_results:
        returns = [r["metrics"]["total_return"] for r in success_results]
        ann_returns = [r["metrics"]["annualized_return"] for r in success_results]
        price_returns = [r["metrics"].get("portfolio_price_return", 0) for r in success_results]
        div_returns = [r["metrics"].get("portfolio_dividend_return", 0) for r in success_results]
        sharpes = [r["metrics"]["sharpe_ratio"] for r in success_results]
        drawdowns = [r["metrics"]["max_drawdown"] for r in success_results]

        stats = {
            "total_return": _stat_summary(returns),
            "annualized_return": _stat_summary(ann_returns),
            "price_return": _stat_summary(price_returns),
            "dividend_return": _stat_summary(div_returns),
            "sharpe_ratio": _stat_summary(sharpes),
            "max_drawdown": _stat_summary(drawdowns),
            # Pooling one-year and ten-year totals says little; these do not mix durations.
            "by_duration": {
                dur: {
                    "count": len(in_duration),
                    "annualized_return": _stat_summary([r["metrics"]["annualized_return"] for r in in_duration]),
                    "total_return": _stat_summary([r["metrics"]["total_return"] for r in in_duration]),
                }
                for dur in durations
                if (in_duration := [r for r in success_results if r["duration"] == dur])
            },
        }

    return {
        "total_runs": total_runs,
        "successful": successful,
        "failed": failed,
        "benchmark_comparison": benchmark_comparison,
        "stats": stats,
    }


def _stat_summary(values: list[float]) -> dict:
    """Compute mean/median/min/max/std for a list of floats."""
    if not values:
        return {"mean": 0.0, "median": 0.0, "min": 0.0, "max": 0.0, "std": 0.0}
    arr = np.array(values)
    return {
        "mean": float(np.mean(arr)),
        "median": float(np.median(arr)),
        "min": float(np.min(arr)),
        "max": float(np.max(arr)),
        "std": float(np.std(arr)),
    }


# ==========================================================================
#  3. SCREENING BACKTEST SET
# ==========================================================================


def run_screening_backtest_set(
    db_path: str,
    criteria: list[dict],
    columns: list[str],
    screening_date: str,
    *,
    max_companies: int = 25,
    ranking_algorithm: str = "none",
    ranking_rules: list[dict] | None = None,
    computed_columns: list[dict] | None = None,
    criteria_match: str = "all",
    durations: list[str] | None = None,
    benchmark_ticker: str = "",
    benchmark_mode: str = "ticker",
    base_currency: str = "",
    db3_path: str = "",
    initial_capital: float = 0.0,
    risk_free_rate: float = 0.0,
    prices_table: str = "Stock_Prices",
    ratios_table: str = "ShareMetrics",
    company_table: str = "CompanyInfo",
    financial_statements_table: str = "FinancialStatements",
) -> dict:
    """Run a screen once at *screening_date*, then backtest the resulting
    ticker set across multiple durations anchored at that date.

    This does **not** re-screen for each historical year — it screens
    once on *screening_date* and uses that ticker list for all durations.
    All screened tickers receive equal weight.

    NOTE: This function depends on ``src.screening.run_screening()``
    returning a DataFrame with a ``Company_Ticker`` or ``Ticker`` column.
    If that column name changes, this function will break.

    Returns:
        A ``BacktestSetResult`` dict.
    """
    from src.screening import run_screening as _run_screening

    if durations is None:
        durations = ["1yr", "2yr", "3yr", "5yr", "10yr"]

    # ── Run screening ─────────────────────────────────────────────────
    screen_df = _run_screening(
        db_path=db_path,
        criteria=criteria,
        columns=columns,
        screening_date=screening_date,
        ranking_algorithm=ranking_algorithm,
        ranking_rules=ranking_rules,
        computed_columns=computed_columns,
        criteria_match=criteria_match,
    )

    if screen_df is None or screen_df.empty:
        raise ValueError(
            "Screening returned no results for the given date and criteria."
        )

    # Extract ticker column
    ticker_col = None
    for candidate in ("Company_Ticker", "Ticker", "ticker"):
        if candidate in screen_df.columns:
            ticker_col = candidate
            break

    if ticker_col is None:
        raise ValueError(
            "Screening result does not contain a ticker column. "
            "Expected one of: Company_Ticker, Ticker, ticker."
        )

    tickers, _skipped = _investable_tickers(screen_df, ticker_col, screening_date, max_companies)

    if not tickers:
        raise ValueError("No tradeable tickers (with a ticker and a recent price) in the screening results.")

    # Equal-weight portfolio
    weight = 1.0 / len(tickers)

    # ── Build CSV-like input for run_backtest_set_web ─────────────────
    # We build a CSV string for a single year = screening_date year
    screening_year = screening_date[:4]

    csv_lines = ["Year,Tickers,Type,Amount"]
    if benchmark_ticker:
        csv_lines.insert(0, f"# Benchmark: {benchmark_ticker}")
    if risk_free_rate:
        csv_lines.insert(0, f"# Discount Rate: {risk_free_rate}")
    for t in tickers:
        csv_lines.append(f"{screening_year},{t},weight,{weight:.6f}")

    csv_content = "\n".join(csv_lines) + "\n"

    return run_backtest_set_web(
        db_path=db_path,
        csv_content=csv_content,
        durations=durations,
        benchmark_ticker=benchmark_ticker,
        benchmark_mode=benchmark_mode,
        base_currency=base_currency,
        db3_path=db3_path,
        initial_capital=initial_capital,
        risk_free_rate=risk_free_rate,
        prices_table=prices_table,
        ratios_table=ratios_table,
        company_table=company_table,
        financial_statements_table=financial_statements_table,
    )


# ==========================================================================
#  4. ROLLING SCREENING BACKTEST
# ==========================================================================


def _investable_tickers(
    screen_df: pd.DataFrame,
    ticker_col: str,
    screening_date: str,
    max_companies: int | None,
    *,
    stale_days: int = 31,
) -> tuple[list[str], int]:
    """The first ``max_companies`` matches that could be bought on ``screening_date``.

    A match without a ticker (unlisted, or delisted since and dropped from the
    code list) or without a price in the month before the screening date
    cannot be bought; letting it take a slot would leave that slot in cash.
    Returns the tickers, in screen order, and how many matches were skipped.
    """
    frame = screen_df
    tickers = frame[ticker_col].astype("string").str.strip()
    usable = tickers.notna() & (tickers != "") & (tickers.str.lower() != "nan")
    if "LatestPrice" in frame.columns:
        usable &= pd.to_numeric(frame["LatestPrice"], errors="coerce").fillna(0) > 0
    if "PriceDate" in frame.columns and screening_date:
        price_dates = pd.to_datetime(frame["PriceDate"], errors="coerce")
        cutoff = pd.Timestamp(screening_date) - pd.Timedelta(days=stale_days)
        usable &= price_dates.notna() & (price_dates >= cutoff)
    selected = list(dict.fromkeys(tickers[usable].tolist()))
    skipped = int((~usable).sum())
    if max_companies and len(selected) > max_companies:
        selected = selected[:max_companies]
    return selected, skipped


def _discover_screening_periods(
    db_path: str,
    cadence: str,
    start_period: str | None = None,
    end_period: str | None = None,
    *,
    financial_statements_table: str = "FinancialStatements",
) -> list[str]:
    """Return screening dates (YYYY-MM-01) for the given cadence.

    Queries the financial-statements table for distinct ``periodEnd``
    months, then samples according to *cadence*:

    * ``"monthly"`` — all months.
    * ``"quarterly"`` — every 3rd month relative to the first available.
    * ``"yearly"`` — one month per year, relative to the first available.

    Respects optional *start_period* / *end_period* bounds (``"YYYY-MM"``).
    """
    conn = connect_read(db_path, busy_timeout_ms=10_000)
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT DISTINCT substr(periodEnd, 1, 7) AS ym "
            f"FROM {_sql_ident(financial_statements_table)} "
            "WHERE periodEnd IS NOT NULL "
            "ORDER BY ym"
        )
        all_months = [row[0] for row in cursor.fetchall() if row[0]]
    finally:
        conn.close()

    if not all_months:
        return []

    # Apply period bounds
    if start_period:
        all_months = [m for m in all_months if m >= start_period]
    if end_period:
        all_months = [m for m in all_months if m <= end_period]

    if not all_months:
        return []

    # Sample according to cadence (relative to first available month)
    step: int
    if cadence == "quarterly":
        step = 3
    elif cadence == "yearly":
        step = 12
    elif cadence == "monthly":
        step = 1
    else:
        raise ValueError(
            f"Unknown cadence: {cadence!r}. "
            "Expected 'monthly', 'quarterly', or 'yearly'."
        )

    # Every step-th calendar month from the first available one, so a month
    # with no period ends does not shift the rest of the schedule.
    first_year, first_month = (int(part) for part in all_months[0].split("-"))
    last_year, last_month = (int(part) for part in all_months[-1].split("-"))
    sampled: list[str] = []
    offset = 0
    while True:
        year, month = divmod(first_month - 1 + offset, 12)
        year, month = first_year + year, month + 1
        if (year, month) > (last_year, last_month):
            break
        sampled.append(f"{year:04d}-{month:02d}-01")
        offset += step
    return sampled


def _build_portfolios(
    tickers: list[str],
    weighting_modes: list[str],
    screen_df: pd.DataFrame | None = None,
    shares_outstanding_col: str | None = None,
    latest_price_col: str = "LatestPrice",
    ticker_col: str = "Ticker",
) -> tuple[dict[str, dict[str, dict]], list[str]]:
    """Build portfolio dicts for each weighting mode.

    Screens return share counts on the split-adjusted basis of the prices,
    so price × shares is each company's market cap on the screening date.

    Returns:
        A 2-tuple ``(portfolios_by_mode, warnings)`` where
        ``portfolios_by_mode`` maps weighting mode name (e.g. ``"equal"``)
        to a portfolio dict suitable for ``run_backtest_web()``.
    """
    warnings: list[str] = []
    portfolios_by_mode: dict[str, dict[str, dict]] = {}

    for wm in weighting_modes:
        if wm == "equal":
            weight = 1.0 / len(tickers) if tickers else 0.0
            portfolios_by_mode[wm] = {
                t: {"mode": "weight", "value": weight} for t in tickers
            }

        elif wm == "market_cap":
            if screen_df is None or screen_df.empty:
                warnings.append(
                    "Market-cap weighting requested but no screening "
                    "data available; falling back to equal weight."
                )
                weight = 1.0 / len(tickers) if tickers else 0.0
                portfolios_by_mode[wm] = {
                    t: {"mode": "weight", "value": weight} for t in tickers
                }
                continue

            # Resolve shares outstanding column
            shares_col = shares_outstanding_col
            if not shares_col:
                from src.screening.screening import (
                    SHARES_OUTSTANDING_CANDIDATES,
                    _resolve_matching_column,
                )
                shares_col = _resolve_matching_column(
                    list(screen_df.columns), SHARES_OUTSTANDING_CANDIDATES,
                )

            if not shares_col:
                warnings.append(
                    "Market-cap weighting requested but no shares-outstanding "
                    "column found in screening results; falling back to equal weight."
                )
                weight = 1.0 / len(tickers) if tickers else 0.0
                portfolios_by_mode[wm] = {
                    t: {"mode": "weight", "value": weight} for t in tickers
                }
                continue

            # Resolve ticker column
            from src.screening.screening import (
                COMPANYINFO_TICKER_CANDIDATES,
                _resolve_matching_column,
            )
            resolved_ticker_col = _resolve_matching_column(
                list(screen_df.columns), COMPANYINFO_TICKER_CANDIDATES,
            ) or ticker_col

            # Build market caps for the companies selected, not every match.
            df = screen_df.copy()
            if resolved_ticker_col in df.columns:
                df = df[df[resolved_ticker_col].astype(str).isin({str(t) for t in tickers})]
            df = df.dropna(subset=[shares_col, latest_price_col])
            if resolved_ticker_col not in df.columns:
                # Try to use ticker_col directly
                if ticker_col in df.columns:
                    resolved_ticker_col = ticker_col
                else:
                    warnings.append(
                        "Market-cap weighting: no ticker column in screening "
                        "results; falling back to equal weight."
                    )
                    weight = 1.0 / len(tickers) if tickers else 0.0
                    portfolios_by_mode[wm] = {
                        t: {"mode": "weight", "value": weight} for t in tickers
                    }
                    continue

            df["_mcap"] = (
                df[latest_price_col].astype(float)
                * df[shares_col].astype(float)
            )
            df = df[df["_mcap"] > 0]

            if len(df) < 2:
                warnings.append(
                    "Market-cap weighting: fewer than 2 tickers with valid "
                    f"market-cap data after filtering ({len(df)} remaining); "
                    "falling back to equal weight."
                )
                weight = 1.0 / len(tickers) if tickers else 0.0
                portfolios_by_mode[wm] = {
                    t: {"mode": "weight", "value": weight} for t in tickers
                }
                continue

            total_mcap = df["_mcap"].sum()
            portfolio: dict[str, dict] = {}
            for _, row in df.iterrows():
                t = str(row[resolved_ticker_col])
                w = float(row["_mcap"] / total_mcap) if total_mcap > 0 else 0.0
                portfolio[t] = {"mode": "weight", "value": w}

            portfolios_by_mode[wm] = portfolio

        else:
            warnings.append(f"Unknown weighting mode {wm!r}; skipping.")

    return portfolios_by_mode, warnings


def _years_between(start: str | None, end: str | None) -> float | None:
    try:
        days = (datetime.strptime(str(end)[:10], "%Y-%m-%d") - datetime.strptime(str(start)[:10], "%Y-%m-%d")).days
    except (TypeError, ValueError):
        return None
    return days / 365.25 if days > 0 else None


def _annualize(total: float | None, years: float | None, dur: str) -> float | None:
    """CAGR over the years actually held, or over the nominal duration when unknown."""
    if total is None:
        return None
    if years is None:
        return _annualize_return(total, dur)
    if total <= -1.0:
        return -1.0
    return (1.0 + total) ** (1.0 / years) - 1.0 if years > 1 else total


def rolling_run_rows(all_results: list[dict], durations: list[str], weighting_modes: list[str]) -> list[dict]:
    """One flat row per backtest (period × weighting × duration), for tables and charts.

    ``status`` is ``ok``, ``truncated`` (prices end before the holding period
    does), ``no_data`` (no prices at all), or ``failed``. Only ``ok`` rows
    count in the aggregate statistics.
    """
    rows: list[dict] = []
    for result in all_results:
        period = result.get("period", "")
        backtests = result.get("backtests", {}) or {}
        for wm in weighting_modes:
            for dur in durations:
                bt = (backtests.get(wm) or {}).get(dur)
                if bt is None:
                    continue
                m = bt.get("metrics")
                row: dict = {
                    "period": period, "weighting": wm, "duration": dur,
                    "companies": result.get("ticker_count", len(result.get("tickers") or [])),
                    "matches": result.get("matches"),
                    "warnings": list(bt.get("warnings") or [])[:5],
                }
                if m is None:
                    rows.append({**row, "status": "failed"})
                    continue
                if m.get("no_data") or bt.get("no_data"):
                    rows.append({**row, "status": "no_data"})
                    continue
                years = _years_between(m.get("start_date"), m.get("end_date"))
                total = m.get("total_return")
                bench_total = m.get("benchmark_total_return")
                annual = _annualize(total, years, dur) if years is None or "annualized_return" not in m else m["annualized_return"]
                bench_years = _years_between(m.get("comparison_start"), m.get("comparison_end")) or years
                bench_annual = m.get("benchmark_annualized_return")
                if bench_annual is None and bench_total is not None:
                    bench_annual = _annualize(bench_total, bench_years, dur)
                excess = m.get("excess_return")
                if excess is None and total is not None and bench_total is not None:
                    excess = total - bench_total
                excess_annual = m.get("excess_annualized_return")
                if excess_annual is None and annual is not None and bench_annual is not None:
                    excess_annual = annual - bench_annual
                rows.append({
                    **row,
                    "status": "truncated" if bt.get("truncated") else "ok",
                    "start": m.get("start_date"), "end": m.get("end_date"),
                    "requested_end": bt.get("requested_end"), "years": years,
                    "total_return": total, "annualized_return": annual,
                    "price_return": m.get("portfolio_price_return", total),
                    "dividend_return": m.get("portfolio_dividend_return", 0.0),
                    "volatility": m.get("volatility"), "sharpe_ratio": m.get("sharpe_ratio", 0.0),
                    "max_drawdown": m.get("max_drawdown", 0.0),
                    "benchmark_total_return": bench_total, "benchmark_annualized_return": bench_annual,
                    "excess_return": excess, "excess_annualized_return": excess_annual,
                    "beat_benchmark": None if bench_total is None or total is None else total > bench_total,
                })
    return rows


def _monthly_path(chart_data: dict) -> list[list]:
    """Month-end points of a run's cumulative return: ``[date, portfolio, benchmark]``."""
    points = chart_data.get("cumulative") or []
    sampled: list[list] = []
    for index, point in enumerate(points):
        date = str(point.get("date", ""))
        following = str(points[index + 1].get("date", "")) if index + 1 < len(points) else ""
        if index == 0 or following[:7] != date[:7]:
            portfolio, benchmark = point.get("portfolio"), point.get("benchmark")
            sampled.append([date, None if portfolio is None else round(float(portfolio), 4), None if benchmark is None else round(float(benchmark), 4)])
    return sampled


_HOLDING_FIELDS = (
    "weight", "start_price", "end_price", "price_return", "dividend_return",
    "total_return", "weighted_total", "dividends_received", "capital_invested", "market_value",
)


def _compact_holdings(per_company: list[dict]) -> list[dict]:
    rows = []
    for record in per_company or []:
        row: dict = {"ticker": str(record.get("Ticker", "")), "currency": record.get("Currency") or ""}
        for field in _HOLDING_FIELDS:
            value = record.get(field)
            row[field] = None if value is None or (isinstance(value, float) and not np.isfinite(value)) else round(float(value), 6)
        rows.append(row)
    return sorted(rows, key=lambda row: -(row["weighted_total"] or 0.0))


def rolling_run_holdings(all_results: list[dict]) -> dict[str, list[dict]]:
    """Each run's holdings and their contributions, keyed ``period|weighting|duration``."""
    holdings: dict[str, list[dict]] = {}
    for result in all_results:
        for wm, by_duration in (result.get("backtests") or {}).items():
            for dur, bt in (by_duration or {}).items():
                if bt and bt.get("metrics") and not bt.get("no_data"):
                    holdings[f"{result.get('period', '')}|{wm}|{dur}"] = _compact_holdings(bt.get("per_company") or [])
    return holdings


def rolling_paths(all_results: list[dict]) -> dict[str, list[list]]:
    """Each run's monthly equity path, keyed ``period|weighting|duration``."""
    paths: dict[str, list[list]] = {}
    for result in all_results:
        for wm, by_duration in (result.get("backtests") or {}).items():
            for dur, bt in (by_duration or {}).items():
                if bt and bt.get("metrics") and not bt.get("no_data"):
                    paths[f"{result.get('period', '')}|{wm}|{dur}"] = _monthly_path(bt.get("chart_data") or {})
    return paths


def _build_rolling_aggregate(
    all_results: list[dict],
    durations: list[str],
    weighting_modes: list[str],
    periods: list[str],
    benchmark_ticker: str,
    benchmark_mode: str = "ticker",
    base_currency: str = "",
) -> dict:
    """Aggregate statistics over the complete runs of a rolling backtest.

    Runs whose prices end before their holding period does (``truncated``),
    or that had no prices at all, are counted separately and kept out of
    the statistics: a two-year run is not a five-year result, and a run
    with no data is not a 0% return.
    """
    rows = rolling_run_rows(all_results, durations, weighting_modes)
    complete = [row for row in rows if row["status"] == "ok"]
    periods_with_results = {row["period"] for row in rows if row["status"] in ("ok", "truncated")}
    has_benchmark = bool(benchmark_ticker) or benchmark_mode == "portfolio"

    def summary(entries: list[dict]) -> dict:
        if not entries:
            return {"mean_return": 0.0, "median_return": 0.0, "mean_sharpe": 0.0, "count": 0}
        annual = [row["annualized_return"] for row in entries]
        excess = [row["excess_annualized_return"] for row in entries if row["excess_annualized_return"] is not None]
        beats = [row["beat_benchmark"] for row in entries if row["beat_benchmark"] is not None]
        return {
            "mean_return": float(np.mean(annual)),
            "median_return": float(np.median(annual)),
            "best_return": float(np.max(annual)),
            "worst_return": float(np.min(annual)),
            "mean_sharpe": float(np.mean([row["sharpe_ratio"] for row in entries])),
            "mean_drawdown": float(np.mean([row["max_drawdown"] for row in entries])),
            "mean_excess": float(np.mean(excess)) if excess else None,
            "win_rate": float(np.mean(beats)) if beats else None,
            "positive_rate": float(np.mean([value > 0 for value in annual])),
            "count": len(entries),
        }

    by_weighting = {
        wm: {dur: summary([row for row in complete if row["weighting"] == wm and row["duration"] == dur]) for dur in durations}
        for wm in weighting_modes
    }

    benchmark_comparison = None
    compared = [row for row in complete if row["beat_benchmark"] is not None]
    if has_benchmark and compared:
        by_duration = {}
        for dur in durations:
            in_duration = [row for row in compared if row["duration"] == dur]
            if in_duration:
                bench = [row["benchmark_annualized_return"] for row in in_duration if row["benchmark_annualized_return"] is not None]
                out = sum(1 for row in in_duration if row["beat_benchmark"])
                by_duration[dur] = {
                    "out": out, "total": len(in_duration), "win_rate": out / len(in_duration),
                    "bench_mean_return": float(np.mean(bench)) if bench else None,
                }
        outperformed = sum(1 for row in compared if row["beat_benchmark"])
        benchmark_comparison = {
            "outperformed": outperformed,
            "underperformed": len(compared) - outperformed,
            "win_rate": outperformed / len(compared),
            "by_duration": by_duration,
        }

    stats = {
        "total_return": _stat_summary([row["annualized_return"] for row in complete]),
        "sharpe_ratio": _stat_summary([row["sharpe_ratio"] for row in complete]),
        "max_drawdown": _stat_summary([row["max_drawdown"] for row in complete]),
        "excess_return": _stat_summary([row["excess_annualized_return"] for row in complete if row["excess_annualized_return"] is not None]),
    } if complete else None

    heatmap: dict = {wm: {dur: [] for dur in durations} for wm in weighting_modes}
    for row in rows:
        if row["status"] in ("ok", "truncated"):
            heatmap[row["weighting"]][row["duration"]].append({"period": row["period"], "return": row["annualized_return"], "truncated": row["status"] == "truncated"})
    if has_benchmark:
        heatmap["excess"] = {dur: [
            {"period": row["period"], "return": row["excess_annualized_return"], "weighting": row["weighting"], "truncated": row["status"] == "truncated"}
            for row in rows if row["duration"] == dur and row["status"] in ("ok", "truncated") and row["excess_annualized_return"] is not None
        ] for dur in durations}

    successful = len(periods_with_results)
    return {
        "total_runs": successful,
        "successful": successful,
        "failed": len(all_results) - successful,
        "periods": len(periods),
        "backtests": len(rows),
        "complete_backtests": len(complete),
        "truncated": sum(1 for row in rows if row["status"] == "truncated"),
        "no_data": sum(1 for row in rows if row["status"] == "no_data"),
        "failed_backtests": sum(1 for row in rows if row["status"] == "failed"),
        "date_range": {"first": periods[0] if periods else "", "last": periods[-1] if periods else ""},
        "by_weighting": by_weighting,
        "benchmark_comparison": benchmark_comparison,
        "stats": stats,
        "heatmap": heatmap,
    }


def _build_heatmap_data(
    all_results: list[dict],
    durations: list[str],
    weighting_modes: list[str],
) -> dict:
    """Build period × duration × weighting return matrix for heatmap.

    Returns are annualized for comparability across durations.

    Note: ``_build_rolling_aggregate`` now collects heatmap data in a
    single pass; this function remains for standalone use (e.g. tests).
    """
    heatmap: dict[str, dict[str, list[dict]]] = {
        wm: {d: [] for d in durations} for wm in weighting_modes
    }

    for r in all_results:
        period = r.get("period", "")
        backtests = r.get("backtests", {})
        for wm in weighting_modes:
            wm_bt = backtests.get(wm, {})
            for dur in durations:
                bt = wm_bt.get(dur)
                if bt is None:
                    continue
                m = bt.get("metrics")
                ret = m.get("total_return") if m else None
                heatmap[wm][dur].append({
                    "period": period,
                    "return": _annualize_return(ret, dur) if ret is not None else None,
                })

    return heatmap


def run_screening_backtest_rolling(
    db_path: str,
    criteria: list[dict],
    columns: list[str],
    *,
    cadence: str = "monthly",
    durations: list[str] | None = None,
    weighting_modes: list[str] | None = None,
    max_companies: int = 25,
    ranking_algorithm: str = "none",
    ranking_rules: list[dict] | None = None,
    computed_columns: list[dict] | None = None,
    criteria_match: str = "all",
    benchmark_ticker: str = "",
    benchmark_mode: str = "ticker",
    base_currency: str = "",
    db3_path: str = "",
    initial_capital: float = 0.0,
    risk_free_rate: float = 0.0,
    start_period: str | None = None,
    end_period: str | None = None,
    progress_queue: queue.Queue | None = None,
    cancel_event: threading.Event | None = None,
    portfolio_owner: str = "",
    prices_table: str = "Stock_Prices",
    ratios_table: str = "ShareMetrics",
    company_table: str = "CompanyInfo",
    financial_statements_table: str = "FinancialStatements",
) -> dict:
    """Run a screening criteria at regular intervals, backtesting each
    resulting portfolio.

    For each period (month/quarter/year), screens the database at that
    point in time, builds equal-weight and/or market-cap-weighted
    portfolios from the top N matching companies, then runs backtests
    for each requested holding duration.

    Returns a ``RollingBacktestResult`` dict.
    """

    from dateutil.relativedelta import relativedelta

    from src.screening import run_screening as _run_screening
    from src.screening.screening import (
        COMPANYINFO_TICKER_CANDIDATES,
        SHARES_OUTSTANDING_CANDIDATES,
        _resolve_matching_column,
        get_available_metrics,
    )

    if durations is None:
        durations = ["1yr", "2yr", "3yr", "5yr", "10yr"]
    if weighting_modes is None:
        weighting_modes = ["equal"]

    # Validate cadence
    if cadence not in ("monthly", "quarterly", "yearly"):
        raise ValueError(
            f"Invalid cadence: {cadence!r}. "
            "Expected 'monthly', 'quarterly', or 'yearly'."
        )

    if not criteria:
        raise ValueError("At least one screening criterion is required.")

    # ── Discover periods ─────────────────────────────────────────
    periods = _discover_screening_periods(
        db_path, cadence, start_period, end_period,
        financial_statements_table=financial_statements_table,
    )
    if not periods:
        raise ValueError(
            "No screening periods found for the given cadence and bounds."
        )

    # ── Fetch available_metrics once ──────────────────────────────
    available_metrics = get_available_metrics(db_path)

    # ── Auto-inject LatestPrice if absent ─────────────────────────
    if "Stock_Prices.LatestPrice" not in columns:
        resolved_price = _resolve_matching_column(
            list(available_metrics.get("Stock_Prices", [])),
            ["Price"],
        ) or "Price"
        columns_full = list(columns) + [f"Stock_Prices.{resolved_price}"]
    else:
        columns_full = list(columns)

    # ── Run screenings + backtests for each period ────────────────
    all_results: list[dict] = []
    total_periods = len(periods)
    total_backtests = (
        total_periods * len(weighting_modes) * len(durations)
    )
    completed_backtests = 0

    for period_idx, screening_date in enumerate(periods):
        # Check cancellation
        if cancel_event and cancel_event.is_set():
            raise RuntimeError("cancelled")

        # Progress: screening phase
        if progress_queue:
            progress_queue.put({
                "type": "progress",
                "period": screening_date,
                "period_index": period_idx,
                "total_periods": total_periods,
                "status": "screening",
                "completed_backtests": completed_backtests,
                "total_backtests": total_backtests,
                "phase": f"Screening at {screening_date}…",
            })

        # 3a. Run screening at this date
        try:
            screen_df = _run_screening(
                db_path=db_path,
                criteria=criteria,
                columns=columns_full,
                period=None,
                screening_date=screening_date,
                ranking_algorithm=ranking_algorithm,
                ranking_rules=ranking_rules,
                computed_columns=computed_columns,
                available_metrics=available_metrics,
                criteria_match=criteria_match,
            )
        except Exception as e:
            logger.warning(
                "Screening failed for %s: %s", screening_date, e,
            )
            if progress_queue:
                progress_queue.put({
                    "type": "progress",
                    "period": screening_date,
                    "period_index": period_idx,
                    "total_periods": total_periods,
                    "status": "error",
                    "completed_backtests": completed_backtests,
                    "total_backtests": total_backtests,
                    "phase": f"Screening failed at {screening_date}: {e}",
                })
            continue

        if screen_df is None or screen_df.empty:
            logger.info(
                "No matching companies at %s; skipping.", screening_date,
            )
            if progress_queue:
                progress_queue.put({
                    "type": "progress",
                    "period": screening_date,
                    "period_index": period_idx,
                    "total_periods": total_periods,
                    "status": "skipped",
                    "ticker_count": 0,
                    "completed_backtests": completed_backtests,
                    "total_backtests": total_backtests,
                    "phase": f"No matches at {screening_date}.",
                })
            continue

        # 3b. Extract ticker list
        ticker_col = _resolve_matching_column(
            list(screen_df.columns), COMPANYINFO_TICKER_CANDIDATES,
        )
        if not ticker_col:
            logger.warning(
                "No ticker column in screening result for %s; skipping.",
                screening_date,
            )
            continue

        tickers, skipped_matches = _investable_tickers(screen_df, ticker_col, screening_date, max_companies)

        if not tickers:
            logger.info("No tradeable tickers at %s; skipping.", screening_date)
            continue

        # Emit warning if ranking is disabled (arbitrary order)
        period_warnings: list[str] = []
        if ranking_algorithm == "none":
            period_warnings.append(
                "Ranking is disabled; ticker selection order is arbitrary."
            )

        # 3c. Build portfolios
        shares_col = _resolve_matching_column(
            list(screen_df.columns), SHARES_OUTSTANDING_CANDIDATES,
        )
        portfolios_by_mode, pf_warnings = _build_portfolios(
            tickers, weighting_modes,
            screen_df=screen_df,
            shares_outstanding_col=shares_col,
            latest_price_col="LatestPrice",
            ticker_col=ticker_col,
        )
        period_warnings.extend(pf_warnings)

        # Check cancellation
        if cancel_event and cancel_event.is_set():
            raise RuntimeError("cancelled")

        # Progress: backtesting phase
        if progress_queue:
            progress_queue.put({
                "type": "progress",
                "period": screening_date,
                "period_index": period_idx,
                "total_periods": total_periods,
                "status": "backtesting",
                "ticker_count": len(tickers),
                "completed_backtests": completed_backtests,
                "total_backtests": total_backtests,
                "phase": f"Running backtests for {screening_date}…",
            })

        # 3d. Run backtests for each weighting × duration
        period_backtests: dict[str, dict[str, dict]] = {}
        for wm in weighting_modes:
            if wm not in portfolios_by_mode:
                continue
            portfolio = portfolios_by_mode[wm]
            wm_results: dict[str, dict] = {}

            for dur_label in durations:
                dur_years = _BACKTEST_DURATIONS.get(dur_label)
                if dur_years is None:
                    continue

                # Compute end_date
                start_dt = datetime.strptime(screening_date, "%Y-%m-%d")
                end_dt = start_dt + relativedelta(years=dur_years)
                bt_end = end_dt.strftime("%Y-%m-%d")

                try:
                    bt_result = run_backtest_web(
                        db_path=db_path,
                        portfolio=portfolio,
                        start_date=screening_date,
                        end_date=bt_end,
                        benchmark_ticker=benchmark_ticker,
                        benchmark_mode=benchmark_mode,
                        base_currency=base_currency,
                        db3_path=db3_path,
                        initial_capital=initial_capital,
                        risk_free_rate=risk_free_rate,
                        portfolio_owner=portfolio_owner,
                        prices_table=prices_table,
                        ratios_table=ratios_table,
                        company_table=company_table,
                        financial_statements_table=financial_statements_table,
                    )

                    # A run whose prices end well before its holding period
                    # does is not a result for that duration.
                    bt_result["requested_end"] = bt_end
                    last_date = (bt_result.get("metrics") or {}).get("end_date") or ""
                    if last_date and not bt_result.get("no_data") and last_date < (end_dt - relativedelta(days=10)).strftime("%Y-%m-%d"):
                        bt_result["truncated"] = True
                        bt_result.setdefault("warnings", []).append(
                            f"Holding period truncated: requested {bt_end}, prices through {last_date}"
                        )

                    wm_results[dur_label] = bt_result
                    completed_backtests += 1

                except Exception as e:
                    import traceback
                    logger.warning(
                        "Backtest failed for %s / %s / %s: %s\n%s",
                        screening_date, wm, dur_label, e,
                        traceback.format_exc(),
                    )
                    wm_results[dur_label] = {
                        "metrics": None,
                        "chart_data": {},
                        "warnings": [str(e)],
                    }
                    completed_backtests += 1

                # Check cancellation after each backtest
                if cancel_event and cancel_event.is_set():
                    raise RuntimeError("cancelled")

            period_backtests[wm] = wm_results

        all_results.append({
            "period": screening_date,
            "screening_date": screening_date,
            "tickers": tickers,
            "ticker_count": len(tickers),
            "matches": int(len(screen_df)),
            "untradeable_matches": skipped_matches,
            "backtests": period_backtests,
            "warnings": period_warnings,
        })

        # Progress: after each period
        if progress_queue:
            progress_queue.put({
                "type": "progress",
                "period": screening_date,
                "period_index": period_idx,
                "total_periods": total_periods,
                "status": "complete",
                "ticker_count": len(tickers),
                "completed_backtests": completed_backtests,
                "total_backtests": total_backtests,
                "phase": (
                    f"Completed {screening_date} "
                    f"({period_idx + 1}/{total_periods})"
                ),
            })

    # ── Build aggregate ────────────────────────────────────────────
    aggregate = _build_rolling_aggregate(
        all_results, durations, weighting_modes, periods, benchmark_ticker,
        benchmark_mode=benchmark_mode,
        base_currency=base_currency,
    )

    result = {
        "config": {
            "cadence": cadence,
            "durations": durations,
            "weighting_modes": weighting_modes,
            "max_companies": max_companies,
            "criteria": criteria,
            "criteria_match": criteria_match,
            "ranking_algorithm": ranking_algorithm,
            "ranking_rules": ranking_rules or [],
            "benchmark_ticker": benchmark_ticker,
            "benchmark_mode": benchmark_mode,
            "base_currency": base_currency,
            "start_period": start_period or (periods[0] if periods else None),
            "end_period": end_period or (periods[-1] if periods else None),
        },
        "aggregate": aggregate,
        "runs": rolling_run_rows(all_results, durations, weighting_modes),
        "paths": rolling_paths(all_results),
        "run_holdings": rolling_run_holdings(all_results),
        "names": company_names(db_path, [ticker for item in all_results for ticker in item.get("tickers", [])], company_table),
        "period_holdings": {item["period"]: item.get("tickers", []) for item in all_results},
        "results": all_results,
    }

    # Emit final result event through the progress queue
    if progress_queue:
        result_msg = {"type": "result"}
        result_msg.update(result)
        progress_queue.put(result_msg)

    return result
