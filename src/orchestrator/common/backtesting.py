"""
Backtesting module for portfolio strategy evaluation.

Receives a date range and a portfolio (tickers + weights), calculates total
returns (price + dividends) over the period, and compares against a benchmark.

Data sources
------------
* ``stock_prices`` table  â€” daily prices (Date, Ticker, Currency, Price).
* ``ShareMetrics`` table â€” per-share dividends (``Dividend paid per share``),
    linked via ``docID`` to
    ``FinancialStatements`` for ``periodEnd`` and ``Company_Code``.
* ``companyInfo`` table â€” maps ``Company_Code`` â†” ``Company_Ticker``.

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
#  1. PORTFOLIO ALLOCATION
# ---------------------------------------------------------------------------

# Each portfolio entry can be:
#   - a plain float  (backward compat, treated as a weight fraction)
#   - a dict with {"mode": "weight"|"shares"|"value", "value": <number>}
ALLOCATION_MODES = ("weight", "shares", "value")


def _sql_ident(name: str) -> str:
    """Return a safely quoted SQLite identifier."""
    return '"' + name.replace('"', '""') + '"'


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

    * ``weight`` â€“ fraction of total portfolio (e.g. 0.5 = 50 %).
    * ``shares`` â€“ fixed number of shares (requires start price).
    * ``value``  â€“ fixed currency amount (e.g. 10 000 JPY).

    When the portfolio contains *only* weight-mode entries **and** no
    ``initial_capital`` is provided, the weights are returned as-is
    (classic behaviour).  Otherwise all allocations are converted to
    capital amounts and then normalised.

    Args:
        portfolio_config: Mapping of ticker â†’ allocation spec.
        start_prices: Mapping of ticker â†’ opening price on the first day.
        initial_capital: User-specified starting capital.  ``0`` means
            "derive automatically".

    Returns:
        A 3-tuple ``(portfolio_weights, effective_capital, warnings)``:

        * **portfolio_weights** â€“ ``dict[str, float]`` normalised weights
          that sum to 1.0.
        * **effective_capital** â€“ the total capital implied by the
          resolved allocations (may differ from *initial_capital*
          depending on the input configuration).
        * **warnings** â€“ list of human-readable warning strings.
    """
    weight_entries: dict[str, float] = {}   # ticker â†’ weight fraction
    fixed_entries: dict[str, float] = {}    # ticker â†’ capital amount
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
                    f"No start-price data for '{ticker}' â€” "
                    f"cannot convert {val:.0f} shares to capital; skipping."
                )
        elif mode == "value":
            fixed_entries[ticker] = val

    total_weight_frac = sum(weight_entries.values())
    total_fixed_capital = sum(fixed_entries.values())

    # â”€â”€ Fast path: pure weight-only portfolio, no initial_capital â”€â”€â”€â”€â”€
    all_weight_only = (not fixed_entries)
    if all_weight_only and initial_capital <= 0:
        # Classic behaviour â€” return weights directly, normalised
        w_sum = total_weight_frac or 1.0
        weights = {t: w / w_sum for t, w in weight_entries.items()}
        return weights, 0.0, warnings

    # â”€â”€ Mixed or fixed allocations: convert everything to capital â”€â”€â”€â”€â”€
    if initial_capital > 0:
        effective_capital = initial_capital
    elif total_weight_frac < 1.0 and total_fixed_capital > 0:
        # Derive: weight_frac * C + fixed = C  â†’  C = fixed / (1 - W)
        effective_capital = total_fixed_capital / (1.0 - total_weight_frac)
    elif total_fixed_capital > 0:
        # Weights already â‰¥ 100 %, treat fixed as additional
        effective_capital = total_fixed_capital
        if total_weight_frac > 0:
            warnings.append(
                f"Weight-mode tickers sum to {total_weight_frac * 100:.1f}% "
                f"(â‰¥ 100 %); fixed allocations are added on top."
            )
    else:
        # Only weight-mode with explicit initial_capital=0 â€” use nominal
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
#  2. DATA RETRIEVAL
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
        if not db_path:
            raise ValueError("db_path is required when no active connection is provided.")
        conn = sqlite3.connect(db_path)
    try:
        # Build ticker variants â€” try bare, with .T, and without .T
        # so that both "1911" and "19110" match "1911.T" in the DB.
        variants: list[str] = []
        variant_to_original: dict[str, str] = {}
        for t in tickers:
            t = str(t).strip()
            if t not in variant_to_original:
                variants.append(t)
                variant_to_original[t] = t
            if not t.endswith(".T"):
                tv = t + ".T"
                if tv not in variant_to_original:
                    variants.append(tv)
                    variant_to_original[tv] = t
            else:
                tv = t[:-2]
                if tv not in variant_to_original:
                    variants.append(tv)
                    variant_to_original[tv] = t

        placeholders = ",".join(["?"] * len(variants))
        # Check if Currency column exists (backward compat with custom tables)
        col_info = conn.execute(
            f"PRAGMA table_info({_sql_ident(prices_table)})"
        ).fetchall()
        col_names = {row[1] for row in col_info}
        has_currency = "Currency" in col_names
        price_cols = "Date, Ticker, Price" + (", Currency" if has_currency else "")
        query = (
            f"SELECT {price_cols} FROM {prices_table} "
            f"WHERE Ticker IN ({placeholders}) "
            f"AND Date >= ? AND Date <= ? "
            f"ORDER BY Date"
        )
        params = [*variants, start_date, end_date]
        df = pd.read_sql_query(query, conn, params=params)
        df["Date"] = pd.to_datetime(df["Date"])
        df["Price"] = pd.to_numeric(df["Price"], errors="coerce")
        if "Currency" not in df.columns:
            df["Currency"] = "EUR"
        else:
            df["Currency"] = df["Currency"].fillna("EUR").astype(str)
        # Map DB tickers back to original portfolio keys
        df["Ticker"] = df["Ticker"].map(variant_to_original).fillna(df["Ticker"])
        return df
    finally:
        if own_conn:
            conn.close()


def get_dividend_data(
    db_path: str,
    per_share_table: str,
    company_table: str,
    tickers: list[str],
    start_date: str,
    end_date: str,
    *,
    financial_statements_table: str = "FinancialStatements",
    dividend_column: str | None = None,
    conn: sqlite3.Connection | None = None,
) -> pd.DataFrame:
    """Fetch per-share dividends mapped to tickers for the given period.

        Expects the ``ShareMetrics``-style schema with:

        * ``docID`` join key.
        * ``Dividend paid per share`` dividend column.
        * ``FinancialStatements`` join for ``periodEnd`` and ``Company_Code``.

    Args:
        db_path: Path to the SQLite database file.
        per_share_table: Name of the dividends table (usually ``ShareMetrics``).
        company_table: Name of the company-info table.
        tickers: List of ticker symbols.
        start_date: Start date (inclusive) ``YYYY-MM-DD``.
        end_date: End date (inclusive) ``YYYY-MM-DD``.
        financial_statements_table: Table providing ``Company_Code`` and
            ``periodEnd`` for the ``docID`` join path.
        dividend_column: Optional explicit dividend column name.
        conn: Optional existing database connection.

    Returns:
        DataFrame with columns ``Ticker``, ``periodEnd``, ``PerShare_Dividends``.
    """
    own_conn = conn is None
    if own_conn:
        if not db_path:
            raise ValueError("db_path is required when no active connection is provided.")
        conn = sqlite3.connect(db_path)
    try:
        if not tickers:
            return pd.DataFrame(
                columns=["Ticker", "periodEnd", "PerShare_Dividends"]
            )

        table_info = conn.execute(
            f"PRAGMA table_info({_sql_ident(per_share_table)})"
        ).fetchall()
        col_names = {row[1] for row in table_info}

        if dividend_column and dividend_column in col_names:
            div_col = dividend_column
        elif "Dividend paid per share" in col_names:
            div_col = "Dividend paid per share"
        elif "PerShare_Dividends" in col_names:
            div_col = "PerShare_Dividends"
        elif "Dividends" in col_names:
            div_col = "Dividends"
        else:
            logger.warning(
                "No dividend column found in %s (expected one of: %s).",
                per_share_table,
                ["Dividend paid per share", "PerShare_Dividends", "Dividends"],
            )
            return pd.DataFrame(
                columns=["Ticker", "periodEnd", "PerShare_Dividends"]
            )

        # Build ticker variants for fuzzy matching (bare, .T, no .T)
        variants: list[str] = []
        variant_to_original: dict[str, str] = {}
        for t in tickers:
            t = str(t).strip()
            if t not in variant_to_original:
                variants.append(t)
                variant_to_original[t] = t
            if not t.endswith(".T"):
                tv = t + ".T"
                if tv not in variant_to_original:
                    variants.append(tv)
                    variant_to_original[tv] = t
            else:
                tv = t[:-2]
                if tv not in variant_to_original:
                    variants.append(tv)
                    variant_to_original[tv] = t

        placeholders = ",".join(["?"] * len(variants))

        # Resolve company-code column names in FinancialStatements and CompanyInfo
        fs_info = conn.execute(
            f"PRAGMA table_info({_sql_ident(financial_statements_table)})"
        ).fetchall()
        fs_col_set = {row[1] for row in fs_info}
        fs_code_col = None
        for candidate in ("Company_Code", "edinetCode", "EdinetCode"):
            if candidate in fs_col_set:
                fs_code_col = candidate
                break
        if not fs_code_col:
            fs_code_col = "Company_Code"

        ci_info = conn.execute(
            f"PRAGMA table_info({_sql_ident(company_table)})"
        ).fetchall()
        ci_col_set = {row[1] for row in ci_info}
        ci_code_col = None
        for candidate in ("Company_Code", "EdinetCode", "edinetCode"):
            if candidate in ci_col_set:
                ci_code_col = candidate
                break
        if not ci_code_col:
            ci_code_col = "Company_Code"

        # ShareMetrics schema path: ShareMetrics(docID, Dividend paid per share) â†’
        # FinancialStatements(docID, Company_Code, periodEnd) â†’ companyInfo.
        if "docID" in col_names:
            query = (
                f"SELECT c.Company_Ticker AS Ticker, fs.periodEnd, "
                f"p.{_sql_ident(div_col)} AS PerShare_Dividends "
                f"FROM {_sql_ident(per_share_table)} p "
                f"JOIN {_sql_ident(financial_statements_table)} fs ON fs.docID = p.docID "
                f"JOIN {_sql_ident(company_table)} c ON c.{_sql_ident(ci_code_col)} = fs.{_sql_ident(fs_code_col)} "
                f"WHERE c.Company_Ticker IN ({placeholders}) "
                f"AND fs.periodEnd >= ? AND fs.periodEnd <= ? "
                f"ORDER BY fs.periodEnd"
            )
        else:
            logger.warning(
                "Table %s does not have supported join keys for dividends "
                "(expected docID).",
                per_share_table,
            )
            return pd.DataFrame(
                columns=["Ticker", "periodEnd", "PerShare_Dividends"]
            )

        params = [*variants, start_date, end_date]
        df = pd.read_sql_query(query, conn, params=params)
        df["periodEnd"] = pd.to_datetime(df["periodEnd"])
        df["PerShare_Dividends"] = pd.to_numeric(
            df["PerShare_Dividends"], errors="coerce"
        ).fillna(0.0)
        # Map DB tickers back to original portfolio keys
        df["Ticker"] = df["Ticker"].map(variant_to_original).fillna(df["Ticker"])
        return df
    finally:
        if own_conn:
            conn.close()


# ---------------------------------------------------------------------------
#  3. CURRENCY CONVERSION
# ---------------------------------------------------------------------------


def get_ticker_currency(
    db_path: str,
    tickers: list[str],
) -> dict[str, str]:
    """Return the native currency for each ticker from Stock_Prices.

    Tickers not found in Stock_Prices default to ``"EUR"``.
    Uses the same ticker-variant matching logic as :func:`get_portfolio_prices`.
    """
    if not tickers:
        return {}
    conn = sqlite3.connect(db_path)
    try:
        variants: list[str] = []
        variant_to_original: dict[str, str] = {}
        for t in tickers:
            t = str(t).strip()
            if t not in variant_to_original:
                variants.append(t)
                variant_to_original[t] = t
            if not t.endswith(".T"):
                tv = t + ".T"
                if tv not in variant_to_original:
                    variants.append(tv)
                    variant_to_original[tv] = t
            else:
                tv = t[:-2]
                if tv not in variant_to_original:
                    variants.append(tv)
                    variant_to_original[tv] = t

        placeholders = ",".join(["?"] * len(variants))
        try:
            rows = conn.execute(
                f"SELECT DISTINCT Ticker, Currency FROM Stock_Prices "
                f"WHERE Ticker IN ({placeholders}) "
                f"AND Currency IS NOT NULL AND Currency != ''",
                variants,
            ).fetchall()
        except Exception as exc:
            logger.warning("Could not query ticker currencies: %s", exc)
            rows = []

        result: dict[str, str] = {}
        for db_ticker, ccy in rows:
            orig = variant_to_original.get(db_ticker, db_ticker)
            if orig not in result:
                result[orig] = str(ccy).upper()

        for t in tickers:
            t_clean = str(t).strip()
            if t_clean not in result:
                result[t_clean] = "EUR"

        return result
    finally:
        conn.close()


def convert_prices_to_base_currency(
    prices_df: pd.DataFrame,
    base_currency: str,
    db_path: str,
) -> pd.DataFrame:
    """Convert all daily prices in *prices_df* to *base_currency*.

    For each ticker whose native currency differs from *base_currency*,
    fetches the historical FX series and applies the daily conversion.
    FX series are cached per currency pair within the call to avoid
    repeated DB queries.
    """
    if not base_currency or prices_df.empty:
        return prices_df

    bc = base_currency.upper()
    if bc == "":
        return prices_df

    df = prices_df.copy()
    cache: dict[str, dict[str, float]] = {}
    ticker_currency = get_ticker_currency(db_path, list(df["Ticker"].unique()))

    for ticker in df["Ticker"].unique():
        native = ticker_currency.get(ticker, "EUR")
        if native == bc:
            continue

        pair_key = f"{native}_{bc}"
        if pair_key not in cache:
            try:
                from src.portfolio.currency import get_fx_series
                cache[pair_key] = get_fx_series(native, bc, db_path)
            except Exception as e:
                logger.warning(
                    "Failed to load FX data for %s->%s: %s. "
                    "Using native currency for '%s'.",
                    native, bc, e, ticker,
                )
                cache[pair_key] = {}

        fx_dict = cache[pair_key]
        if not fx_dict:
            logger.warning(
                "No FX data for %s->%s; using native currency for '%s'.",
                native, bc, ticker,
            )
            continue

        # Build a forward-filled FX Series to handle missing dates (holidays).
        # Reindex to cover the FULL price date range so dates beyond the
        # FX data range use the last known rate (vital for multi-year
        # backtests where price data may extend past available FX data).
        fx_dates = sorted(fx_dict.keys())
        fx_vals = [fx_dict[d] for d in fx_dates]
        fx_s = pd.Series(fx_vals, index=pd.to_datetime(fx_dates)).sort_index()
        price_dates = df.loc[df["Ticker"] == ticker, "Date"]
        full_range = pd.date_range(
            min(fx_s.index[0], price_dates.min()),
            max(fx_s.index[-1], price_dates.max()),
            freq="D",
        )
        fx_s = fx_s.reindex(full_range, method="ffill")

        ticker_mask = df["Ticker"] == ticker
        for idx in df[ticker_mask].index:
            date_val = df.at[idx, "Date"]
            if isinstance(date_val, pd.Timestamp):
                date_key = date_val
            else:
                date_key = pd.Timestamp(str(date_val))
            try:
                rate = fx_s.loc[date_key]
            except KeyError:
                continue
            if rate > 0:
                df.at[idx, "Price"] = df.at[idx, "Price"] * rate

    df["Currency"] = bc
    return df


def convert_dividends_to_base_currency(
    dividends_df: pd.DataFrame,
    base_currency: str,
    db_path: str,
) -> pd.DataFrame:
    """Convert all per-share dividend amounts to *base_currency*.

    For each dividend payment, determines the ticker's native currency
    and converts the per-share amount using the FX rate on the payment
    date (``periodEnd``).
    """
    if not base_currency or dividends_df.empty:
        return dividends_df

    bc = base_currency.upper()
    if bc == "":
        return dividends_df

    df = dividends_df.copy()
    ticker_currency = get_ticker_currency(
        db_path, list(df["Ticker"].unique()),
    )
    cache: dict[str, dict[str, float]] = {}

    for idx, row in df.iterrows():
        ticker = row["Ticker"]
        native = ticker_currency.get(ticker, "EUR")
        if native == bc:
            continue

        pair_key = f"{native}_{bc}"
        if pair_key not in cache:
            try:
                from src.portfolio.currency import get_fx_series
                cache[pair_key] = get_fx_series(native, bc, db_path)
            except Exception as e:
                logger.warning(
                    "Failed to load FX data for %s->%s: %s",
                    native, bc, e,
                )
                cache[pair_key] = {}

        fx_dict = cache[pair_key]
        if not fx_dict:
            continue

        # Build forward-filled FX Series; fall back to closest rate at edges
        fx_dates = sorted(fx_dict.keys())
        fx_vals = [fx_dict[d] for d in fx_dates]
        fx_s = pd.Series(fx_vals, index=pd.to_datetime(fx_dates)).sort_index()

        period_end = row["periodEnd"]
        if isinstance(period_end, pd.Timestamp):
            date_key = period_end
        else:
            date_key = pd.Timestamp(str(period_end)[:10])

        # Find the rate: exact match, or nearest available (ffill/bfill)
        if date_key in fx_s.index:
            rate = fx_s.loc[date_key]
        elif date_key < fx_s.index[0]:
            rate = fx_s.iloc[0]  # use first available rate
        elif date_key > fx_s.index[-1]:
            rate = fx_s.iloc[-1]  # use last available rate
        else:
            # Date within range but on a gap — interpolate
            pos = fx_s.index.searchsorted(date_key)
            rate = fx_s.iloc[pos - 1]  # last known rate before date_key
        if rate > 0:
            df.at[idx, "PerShare_Dividends"] = (
                row["PerShare_Dividends"] * rate
            )

    return df


def get_portfolio_benchmark_returns(
    db3_path: str,
    start_date: str,
    end_date: str,
    base_currency: str,
    db2_path: str,
) -> pd.DataFrame | None:
    """Fetch portfolio daily values from db3 and format as benchmark DataFrame.

    Reads ``Portfolio_Daily`` from *db3_path*. Values are stored in EUR.
    If *base_currency* != ``"EUR"``, converts daily values using historical
    FX rates, then recomputes daily returns from the converted series.
    """
    conn = sqlite3.connect(db3_path)
    try:
        rows = conn.execute(
            "SELECT date, total_value, net_inflow FROM Portfolio_Daily "
            "WHERE date >= ? AND date <= ? "
            "ORDER BY date",
            (start_date, end_date),
        ).fetchall()
    finally:
        conn.close()

    if not rows or len(rows) < 2:
        return None

    dates = [pd.to_datetime(r[0]) for r in rows]
    total_values = np.array([float(r[1]) for r in rows], dtype=float)
    net_inflows = np.array([float(r[2]) if r[2] is not None else 0.0 for r in rows], dtype=float)

    bc = base_currency.upper()

    if bc != "EUR":
        from src.portfolio.currency import get_fx_series
        fx_series = get_fx_series("EUR", bc, db2_path)
        if fx_series:
            fx_values = np.array([
                fx_series.get(r[0], 1.0) for r in rows
            ], dtype=float)
            total_values = total_values * fx_values
            net_inflows = net_inflows * fx_values
        else:
            logger.warning(
                "No FX data for EUR->%s; portfolio benchmark remains in EUR.",
                bc,
            )

    daily_returns = np.zeros(len(rows))
    daily_returns[0] = 0.0
    for i in range(1, len(rows)):
        v_prev = total_values[i - 1]
        v_curr = total_values[i]
        inflow = net_inflows[i]
        denom = v_prev + inflow
        if denom > 0 and v_prev > 0:
            ret = (v_curr - v_prev - inflow) / denom
            daily_returns[i] = max(min(ret, 1.0), -1.0)
        else:
            daily_returns[i] = 0.0

    cum_returns = np.cumprod(1.0 + daily_returns)

    result_df = pd.DataFrame(
        {
            "benchmark_return": daily_returns,
            "cumulative_return": cum_returns,
        },
        index=dates,
    )
    result_df = result_df.iloc[1:]
    return result_df


# ---------------------------------------------------------------------------
#  4. RETURN CALCULATIONS
# ---------------------------------------------------------------------------


def calculate_portfolio_returns(
    prices_df: pd.DataFrame,
    portfolio_weights: dict[str, float],
    dividends_df: pd.DataFrame | None = None,
    *,
    initial_capital: float = 0.0,
) -> pd.DataFrame:
    """Calculate weighted daily portfolio returns including dividends.

    Pivots the long-format *prices_df* into a wide price matrix, computes
    daily percentage returns for each ticker, applies *portfolio_weights*,
    and adds any per-share dividend contributions on the dates they were paid.

    When *initial_capital* > 0 the calculation is **shares-based**:
    concrete share counts are derived from the weights and capital, and
    dividends are tracked as actual cash received.  This guarantees the
    portfolio total return matches the sum of the per-company weighted
    totals produced by :func:`calculate_per_company_returns`.

    Args:
        prices_df: Long DataFrame with ``Date``, ``Ticker``, ``Price``.
        portfolio_weights: Mapping of ticker â†’ weight (should sum to 1.0).
        dividends_df: Optional DataFrame with ``Ticker``, ``periodEnd``,
            ``PerShare_Dividends``.
        initial_capital: Total starting capital.  When > 0 the shares-based
            path is used, producing values directly comparable to
            :func:`calculate_per_company_returns`.

    Returns:
        DataFrame indexed by ``Date`` with columns:

        * ``portfolio_return`` â€” weighted daily return.
        * ``cumulative_return`` â€” cumulative product of (1 + daily return).
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

    initial_prices_series = price_matrix[tickers_in_data].iloc[0]

    # â”€â”€ Shares-based calculation (explicit, transparent) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    if initial_capital > 0:
        # Compute shares purchased for each ticker at the start
        shares: dict[str, float] = {}
        for t in tickers_in_data:
            p0 = float(initial_prices_series[t])
            if p0 > 0:
                shares[t] = (float(weights[t]) * initial_capital) / p0
            else:
                shares[t] = 0.0

        # Portfolio market value each day = sum(shares Ã— price)
        market_value = pd.Series(0.0, index=price_matrix.index)
        for t in tickers_in_data:
            if shares[t] > 0:
                market_value += shares[t] * price_matrix[t].fillna(method="ffill")

        # Cumulative dividends received (held as cash)
        cumul_dividends = pd.Series(0.0, index=price_matrix.index)
        if dividends_df is not None and not dividends_df.empty:
            idx = price_matrix.index
            for _, row in dividends_df.iterrows():
                ticker = row["Ticker"]
                pay_date = row["periodEnd"]
                div_amount = float(row["PerShare_Dividends"])
                if ticker not in shares or shares[ticker] <= 0:
                    continue
                if div_amount <= 0:
                    continue
                # Map payment date to next available trading day
                pos = idx.searchsorted(pay_date, side="left")
                if pos >= len(idx):
                    logger.debug(
                        "Dividend for %s on %s falls after last "
                        "trading day %s; skipped.",
                        ticker, pay_date.date(), idx[-1].date(),
                    )
                    continue
                effective_date = idx[pos]
                cash = shares[ticker] * div_amount
                cumul_dividends.loc[
                    cumul_dividends.index >= effective_date
                ] += cash

        # Total portfolio value = market value + cash from dividends
        portfolio_value = market_value + cumul_dividends
        initial_portfolio_value = float(portfolio_value.iloc[0])

        daily_returns = portfolio_value.pct_change()
        cumulative_return = portfolio_value / initial_portfolio_value

        result = pd.DataFrame(
            {
                "portfolio_return": daily_returns,
                "cumulative_return": cumulative_return,
            }
        )
        result = result.iloc[1:]  # drop first NaN from pct_change
        return result

    # â”€â”€ Normalised-price calculation (legacy, weight-only) â”€â”€â”€â”€â”€â”€â”€â”€â”€
    normalised = price_matrix[tickers_in_data].div(initial_prices_series, axis=1)
    portfolio_value = normalised.mul(weights, axis=1).sum(axis=1)

    initial_portfolio_value = portfolio_value.iloc[0]

    # Add cumulative dividend cash flows (held as cash, not reinvested)
    # Dividends whose pay_date falls on a non-trading day (weekend / holiday)
    # are mapped forward to the next available trading day.
    if dividends_df is not None and not dividends_df.empty:
        div_cash = pd.Series(0.0, index=portfolio_value.index)
        idx = portfolio_value.index
        for _, row in dividends_df.iterrows():
            ticker = row["Ticker"]
            pay_date = row["periodEnd"]
            div_amount = float(row["PerShare_Dividends"])
            if ticker not in tickers_in_data:
                continue
            if div_amount <= 0:
                continue
            # Find the first trading day on or after the payment date.
            pos = idx.searchsorted(pay_date, side="left")
            if pos >= len(idx):
                logger.debug(
                    "Dividend for %s on %s falls after last trading day "
                    "%s; skipped.",
                    ticker, pay_date.date(), idx[-1].date(),
                )
                continue
            effective_date = idx[pos]
            init_price = initial_prices_series[ticker]
            if init_price > 0:
                cash = weights[ticker] * div_amount / init_price
                div_cash.loc[div_cash.index >= effective_date] += cash
        portfolio_value = portfolio_value + div_cash

    daily_returns = portfolio_value.pct_change()
    cumulative_return = portfolio_value / initial_portfolio_value

    result = pd.DataFrame(
        {
            "portfolio_return": daily_returns,
            "cumulative_return": cumulative_return,
        }
    )
    result = result.iloc[1:]  # drop first NaN from pct_change
    return result


def calculate_return_decomposition(
    prices_df: pd.DataFrame,
    portfolio_weights: dict[str, float],
    dividends_df: pd.DataFrame | None = None,
    *,
    initial_capital: float = 0.0,
) -> dict[str, pd.DataFrame]:
    """Decompose portfolio returns into price-only and dividend components.

    Computes three cumulative time series:
    * **total** â€” the full return including dividends.
    * **price_only** â€” return from share-price changes alone.
    * **dividend_only** â€” the cumulative contribution of dividends.

    Args:
        prices_df: Long DataFrame with ``Date``, ``Ticker``, ``Price``.
        portfolio_weights: Mapping of ticker â†’ weight (should sum to 1.0).
        dividends_df: Optional DataFrame with ``Ticker``, ``periodEnd``,
            ``PerShare_Dividends``.
        initial_capital: Forwarded to :func:`calculate_portfolio_returns`.

    Returns:
        Dictionary with keys ``total``, ``price_only``, ``dividend_only``
        each mapping to a DataFrame indexed by Date with
        ``daily_return`` and ``cumulative_return`` columns.
    """
    # Total return (price + dividends)
    total_df = calculate_portfolio_returns(
        prices_df, portfolio_weights, dividends_df,
        initial_capital=initial_capital,
    )

    # Price-only return (no dividends)
    price_only_df = calculate_portfolio_returns(
        prices_df, portfolio_weights, None,
        initial_capital=initial_capital,
    )

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
        portfolio_weights: Mapping of ticker â†’ weight.
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
        total_divs_per_share = 0.0
        if dividends_df is not None and not dividends_df.empty:
            tk_divs = dividends_df[dividends_df["Ticker"] == ticker]
            for _, drow in tk_divs.iterrows():
                div_amount = drow["PerShare_Dividends"]
                if div_amount > 0 and start_price > 0:
                    div_return += div_amount / start_price
                    total_divs_per_share += div_amount

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
            # Direct calculation: shares Ã— sum of per-share dividends
            rec["dividends_received"] = shares * total_divs_per_share
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
    """Pivot dividends into a Year Ã— Ticker table.

    When *shares_purchased* is provided the values represent total cash
    dividends received (per-share dividend Ã— shares held).  Otherwise the
    raw per-share dividend amounts are shown.

    Args:
        dividends_df: DataFrame with ``Ticker``, ``periodEnd``,
            ``PerShare_Dividends``.
        shares_purchased: Optional mapping of ticker â†’ number of shares
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


def calculate_dividends_by_company_year_long(
    dividends_df: pd.DataFrame | None,
    shares_purchased: dict[str, float] | None = None,
) -> pd.DataFrame:
    """Return dividends in long format: Year, Ticker, Amount, Total.

    An alternative to the wide pivot table produced by
    :func:`calculate_dividends_by_company_year`.  Each row is a single
    (Year, Ticker) pair, making it easier to read with many tickers.

    Args:
        dividends_df: DataFrame with ``Ticker``, ``periodEnd``,
            ``PerShare_Dividends``.
        shares_purchased: Optional mapping of ticker â†’ number of shares
            held.  When given, ``Amount`` represents total cash received
            rather than per-share dividends.

    Returns:
        DataFrame with columns ``Year``, ``Ticker``, ``Amount``, ``Total``,
        sorted by Year and Ticker.
    """
    if dividends_df is None or dividends_df.empty:
        return pd.DataFrame(columns=["Year", "Ticker", "Amount", "Total"])

    df = dividends_df.copy()
    df["Year"] = df["periodEnd"].dt.year

    if shares_purchased:
        shares_series = df["Ticker"].map(shares_purchased).fillna(0.0)
        df["Amount"] = df["PerShare_Dividends"] * shares_series
    else:
        df["Amount"] = df["PerShare_Dividends"]

    result = df.groupby(["Year", "Ticker"], as_index=False)["Amount"].sum()

    # Add year totals (same value repeated for all rows in a year)
    year_totals = result.groupby("Year")["Amount"].transform("sum")
    result["Total"] = year_totals

    return result.sort_values(["Year", "Ticker"]).reset_index(drop=True)


# ---------------------------------------------------------------------------
#  5. DAILY PORTFOLIO TRACKER (SINGLE SOURCE OF TRUTH)
# ---------------------------------------------------------------------------


def build_daily_portfolio_tracker(
    prices_df: pd.DataFrame,
    portfolio_weights: dict[str, float],
    dividends_df: pd.DataFrame | None = None,
    *,
    initial_capital: float = 1_000_000.0,
    base_currency: str = "",
    risk_free_rate: float = 0.0,
) -> dict:
    """Build a **daily** portfolio time series - the single source of truth.

    Tracks every ticker's price, shares, and market value each trading day,
    along with the cash balance (accumulated dividends).  **All** other
    outputs - performance metrics, per-company-per-year aggregates, chart
    data - are derived from this daily tracker, guaranteeing they are
    internally consistent.

    Missing price observations are forward-filled (last known price carried
    forward).  Dividends paid on non-trading days are mapped to the next
    available trading day.

    Parameters
    ----------
    prices_df:
        Long DataFrame with ``Date``, ``Ticker``, ``Price``.
    portfolio_weights:
        Mapping of ticker -> weight (normalised to sum to 1.0 internally).
    dividends_df:
        Optional DataFrame with ``Ticker``, ``periodEnd``,
        ``PerShare_Dividends``.
    initial_capital:
        Total starting capital.
    base_currency:
        Currency code for the cash-row label (e.g. ``"JPY"``).

    Returns
    -------
    dict
        ``daily`` (:class:`~pd.DataFrame`) - one row per trading day with
        columns for each ticker's price, shares, market value, plus
        ``cash``, ``portfolio_mktval``, ``portfolio_total``,
        ``daily_return``, ``cumulative_return``, and price-only /
        dividend-decomposition columns.

        ``per_company_per_year`` (:class:`~pd.DataFrame`) - yearly
        aggregate derived from the daily tracker.

        ``metrics`` (:class:`dict`) - portfolio performance metrics
        (total_return, price_return, dividend_return, etc.).
    """
    # --- 1. Filter to tickers with price data --------------------------
    available = set(prices_df["Ticker"].unique())
    active = {t: w for t, w in portfolio_weights.items() if t in available}
    if not active:
        return {"daily": pd.DataFrame(), "per_company_per_year": pd.DataFrame(),
                "metrics": {}}
    w_sum = sum(active.values())
    if w_sum > 0:
        active = {t: w / w_sum for t, w in active.items()}

    tickers = list(active.keys())
    cash_label = base_currency or "CASH"

    # --- 2. Pivot prices -> wide daily matrix (forward-fill gaps) ------
    price_matrix = prices_df.pivot_table(
        index="Date", columns="Ticker", values="Price", aggfunc="last",
    )
    price_matrix = price_matrix.sort_index()
    price_matrix = price_matrix.ffill()  # interpolate missing prices

    # Keep only portfolio tickers
    tickers_in_data = [t for t in tickers if t in price_matrix.columns]
    price_matrix = price_matrix[tickers_in_data]

    # --- 3. Compute shares for each ticker (buy-and-hold) -------------
    initial_prices: dict[str, float] = {}
    shares: dict[str, float] = {}
    for t in tickers_in_data:
        p0 = float(price_matrix[t].iloc[0])
        if pd.isna(p0) or p0 <= 0:
            continue
        initial_prices[t] = p0
        shares[t] = (active[t] * initial_capital) / p0

    # --- 4. Daily market value per ticker -----------------------------
    daily_mktval: dict[str, pd.Series] = {}
    for t, s in shares.items():
        daily_mktval[t] = price_matrix[t] * s

    # --- 5. Cash balance (accumulated dividends) ----------------------
    cash_series = pd.Series(0.0, index=price_matrix.index)
    dividend_events = pd.Series(0.0, index=price_matrix.index)
    # Per-ticker daily dividend cash (for visibility / debugging)
    per_ticker_div_cash: dict[str, pd.Series] = {
        t: pd.Series(0.0, index=price_matrix.index) for t in shares
    }

    if dividends_df is not None and not dividends_df.empty:
        idx = price_matrix.index
        div_tickers_in_df = set(dividends_df["Ticker"].unique())
        div_tickers_matched = div_tickers_in_df & set(shares.keys())
        div_tickers_unmatched = div_tickers_in_df - set(shares.keys())
        if div_tickers_unmatched:
            logger.warning(
                "Dividends found for tickers not in portfolio shares: %s",
                sorted(div_tickers_unmatched),
            )
        logger.debug(
            "Processing dividends: %d rows, matched tickers=%s",
            len(dividends_df), sorted(div_tickers_matched),
        )

        for _, row in dividends_df.iterrows():
            ticker = row["Ticker"]
            pay_date = row["periodEnd"]
            div_amount = float(row["PerShare_Dividends"])
            if ticker not in shares:
                continue
            if shares[ticker] <= 0:
                continue
            if div_amount <= 0:
                continue
            # Map payment date to next available trading day.
            # Dividends that fall after the last trading day are mapped
            # to the last day (they still count toward total return).
            pos = idx.searchsorted(pay_date, side="left")
            if pos >= len(idx):
                effective_date = idx[-1]  # last available trading day
            else:
                effective_date = idx[pos]
            cash = shares[ticker] * div_amount
            cash_series.loc[cash_series.index >= effective_date] += cash
            dividend_events.loc[effective_date] += cash
            # Per-ticker tracking
            per_ticker_div_cash[ticker].loc[
                per_ticker_div_cash[ticker].index >= effective_date
            ] += cash

    # --- 6. Portfolio totals ------------------------------------------
    portfolio_mktval = pd.Series(0.0, index=price_matrix.index)
    for t in shares:
        portfolio_mktval += daily_mktval[t]
    portfolio_total = portfolio_mktval + cash_series

    # Price-only (no dividends) for decomposition
    price_only_total = portfolio_mktval.copy()

    # Daily returns
    daily_return = portfolio_total.pct_change()
    daily_price_only_return = price_only_total.pct_change()

    # Cumulative returns (relative to initial capital)
    cumulative_return = portfolio_total / initial_capital
    price_only_cum = price_only_total / initial_capital
    # Dividend contribution = total - price_only
    dividend_contribution_cum = cumulative_return - price_only_cum

    # --- 7. Assemble daily DataFrame -----------------------------------
    daily_cols: dict[str, pd.Series] = {}
    for t in tickers_in_data:
        daily_cols[f"price_{t}"] = price_matrix[t]
        daily_cols[f"shares_{t}"] = pd.Series(
            shares.get(t, 0.0), index=price_matrix.index,
        )
        daily_cols[f"mktval_{t}"] = daily_mktval.get(
            t, pd.Series(0.0, index=price_matrix.index),
        )
        daily_cols[f"div_cash_{t}"] = per_ticker_div_cash.get(
            t, pd.Series(0.0, index=price_matrix.index),
        )

    daily_cols["cash"] = cash_series
    daily_cols["dividend_event"] = dividend_events
    daily_cols["portfolio_mktval"] = portfolio_mktval
    daily_cols["portfolio_total"] = portfolio_total
    daily_cols["daily_return"] = daily_return
    daily_cols["cumulative_return"] = cumulative_return
    daily_cols["price_only_total"] = price_only_total
    daily_cols["price_only_daily_return"] = daily_price_only_return
    daily_cols["price_only_cum_return"] = price_only_cum
    daily_cols["dividend_cum_contribution"] = dividend_contribution_cum

    daily_df = pd.DataFrame(daily_cols, index=price_matrix.index)
    # Drop first row (NaN from pct_change)
    daily_df = daily_df.iloc[1:]

    # --- 8. Per-company-per-year aggregate (from ORIGINAL price matrix,
    #       NOT the truncated daily_df — the first row was dropped for
    #       pct_change, but price data needs all rows) -------------------
    years = sorted(set(price_matrix.index.year))
    # Build lookup DataFrames keyed by year
    pm_by_year: dict[int, pd.DataFrame] = {}
    cs_by_year: dict[int, pd.Series] = {}
    for y in years:
        mask = price_matrix.index.year == y
        pm_by_year[y] = price_matrix[mask]
        cs_by_year[y] = cash_series[mask]

    pyp_rows: list[dict] = []
    running_cash = 0.0

    for year in years:
        yr_pm = pm_by_year[year]

        # Total portfolio start value (using FIRST trading day of year)
        port_start_val = running_cash
        for t in shares:
            port_start_val += shares[t] * float(yr_pm[t].iloc[0])

        # Yearly dividends per ticker
        year_divs_per_share: dict[str, float] = {t: 0.0 for t in shares}
        if dividends_df is not None and not dividends_df.empty:
            div_yr = dividends_df.copy()
            div_yr["Year"] = div_yr["periodEnd"].dt.year
            for t in shares:
                td = div_yr[(div_yr["Ticker"] == t) & (div_yr["Year"] == year)]
                year_divs_per_share[t] = float(td["PerShare_Dividends"].sum())

        total_year_divs_cash = 0.0

        # Ticker rows
        for t in shares:
            s_price = float(yr_pm[t].iloc[0])
            e_price = float(yr_pm[t].iloc[-1])
            div_ps = year_divs_per_share[t]
            s_shares = shares[t]

            tot_divs = s_shares * div_ps
            total_year_divs_cash += tot_divs
            e_shares = s_shares  # buy-and-hold

            start_mkt = s_shares * s_price
            end_mkt = e_shares * e_price

            price_ret = (e_price - s_price) / s_price if s_price > 0 else 0.0
            div_ret = div_ps / s_price if s_price > 0 else 0.0
            total_ret = price_ret + div_ret

            wtd_start = start_mkt / port_start_val if port_start_val > 0 else 0.0
            # Weighted end value uses actual portfolio end value
            port_end_val_yr = running_cash + total_year_divs_cash
            for t2 in shares:
                port_end_val_yr += shares[t2] * float(yr_pm[t2].iloc[-1])
            wtd_end = end_mkt / port_end_val_yr if port_end_val_yr > 0 else 0.0
            wtd_ret = active[t] * total_ret

            pyp_rows.append({
                "Year": year,
                "Ticker": t,
                "Starting_Shares": s_shares,
                "Dividend_Per_Share": div_ps,
                "Start_Price": s_price,
                "End_Price": e_price,
                "Total_Dividends_Received": tot_divs,
                "Dividend_Currency": "",
                "Ending_Shares": e_shares,
                "Starting_Market_Value": start_mkt,
                "Ending_Market_Value": end_mkt,
                "Price_Return_Pct": price_ret,
                "Dividend_Return_Pct": div_ret,
                "Total_Return_Pct": total_ret,
                "Weighted_Value_Start": wtd_start,
                "Weighted_Value_End": wtd_end,
                "Weighted_Return": wtd_ret,
            })

        # Cash row
        cash_end_yr = running_cash + total_year_divs_cash
        port_end_val_yr2 = cash_end_yr
        for t in shares:
            port_end_val_yr2 += shares[t] * float(yr_pm[t].iloc[-1])

        cash_wtd_start = running_cash / port_start_val if port_start_val > 0 else 0.0
        cash_wtd_end = cash_end_yr / port_end_val_yr2 if port_end_val_yr2 > 0 else 0.0

        pyp_rows.append({
            "Year": year,
            "Ticker": cash_label,
            "Starting_Shares": running_cash,
            "Dividend_Per_Share": 0.0,
            "Start_Price": 1.0,
            "End_Price": 1.0,
            "Total_Dividends_Received": 0.0,
            "Dividend_Currency": cash_label,
            "Ending_Shares": cash_end_yr,
            "Starting_Market_Value": running_cash,
            "Ending_Market_Value": cash_end_yr,
            "Price_Return_Pct": 0.0,
            "Dividend_Return_Pct": 0.0,
            "Total_Return_Pct": 0.0,
            "Weighted_Value_Start": cash_wtd_start,
            "Weighted_Value_End": cash_wtd_end,
            "Weighted_Return": 0.0,
        })

        running_cash = cash_end_yr

    pyp_df = pd.DataFrame(pyp_rows) if pyp_rows else pd.DataFrame()

    # --- 9. Portfolio metrics (from daily tracker) ---------------------
    final_total = float(portfolio_total.iloc[-1]) if len(portfolio_total) > 0 else initial_capital
    total_return = (final_total / initial_capital) - 1.0

    # Total dividends across all time
    total_dividends = float(dividend_events.sum()) if len(dividend_events) > 0 else 0.0
    dividend_return = total_dividends / initial_capital
    price_return = total_return - dividend_return

    # Annualized
    if len(daily_df) > 0:
        dt_start = daily_df.index[0]
        dt_end = daily_df.index[-1]
        yrs = max((dt_end - dt_start).days / 365.25, 1 / 365.25)
        annualized_return = (1.0 + total_return) ** (1.0 / yrs) - 1.0 if total_return > -1.0 else -1.0
    else:
        annualized_return = total_return

    dr_std = float(daily_return.std()) if len(daily_return) > 1 else 0.0
    daily_vol = dr_std * float(np.sqrt(365))
    sharpe = (annualized_return - risk_free_rate) / daily_vol if daily_vol > 0 else 0.0
    cum = cumulative_return
    running_max = cum.cummax()
    drawdowns = (cum - running_max) / running_max
    max_dd = float(drawdowns.min()) if len(drawdowns) > 0 else 0.0

    metrics: dict = {
        "total_return": total_return,
        "annualized_return": annualized_return,
        "portfolio_price_return": price_return,
        "portfolio_dividend_return": dividend_return,
        "volatility": daily_vol,
        "sharpe_ratio": sharpe,
        "max_drawdown": max_dd,
        "start_date": str(daily_df.index[0].date()) if len(daily_df) > 0 else "",
        "end_date": str(daily_df.index[-1].date()) if len(daily_df) > 0 else "",
        "initial_capital": initial_capital,
        "risk_free_rate": risk_free_rate,
    }

    return {
        "daily": daily_df,
        "per_company_per_year": pyp_df,
        "metrics": metrics,
    }


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

        * ``benchmark_return`` â€” daily total return (price + dividends).
        * ``cumulative_return`` â€” cumulative product of (1 + daily return).
        * ``price_return`` â€” daily price-only return.
        * ``cum_price_return`` â€” cumulative price-only return.
        * ``dividend_return`` â€” daily dividend contribution.
        * ``cum_dividend_return`` â€” cumulative dividend contribution.
    """
    bench = prices_df[prices_df["Ticker"] == benchmark_ticker].copy()
    bench = bench.sort_values("Date").set_index("Date")
    bench = bench[~bench.index.duplicated(keep="last")]

    # Price-only returns
    bench["price_return"] = bench["Price"].pct_change()
    bench["dividend_return"] = 0.0

    # Add dividend yield on payment dates (using previous day's price).
    # Dividends whose pay_date falls on a non-trading day are mapped
    # forward to the next available trading day.
    if dividends_df is not None and not dividends_df.empty:
        bench_divs = dividends_df[dividends_df["Ticker"] == benchmark_ticker]
        bench_idx = bench.index
        for _, row in bench_divs.iterrows():
            pay_date = row["periodEnd"]
            div_amount = row["PerShare_Dividends"]
            # Find the first trading day on or after the payment date.
            pos = bench_idx.searchsorted(pay_date, side="left")
            if pos >= len(bench_idx):
                continue
            effective_date = bench_idx[pos]
            loc = bench.index.get_loc(effective_date)
            prev_price = bench["Price"].iloc[loc - 1] if loc > 0 else bench["Price"].iloc[0]
            if prev_price and prev_price > 0:
                bench.loc[effective_date, "dividend_return"] = div_amount / prev_price

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
#  5. PERFORMANCE METRICS
# ---------------------------------------------------------------------------


def calculate_metrics(
    portfolio_df: pd.DataFrame,
    benchmark_df: pd.DataFrame | None,
    start_date: str,
    end_date: str,
    risk_free_rate: float = 0.0,
    *,
    effective_start: str | None = None,
    effective_end: str | None = None,
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
        * ``total_return`` â€” cumulative portfolio return as a fraction.
        * ``annualized_return``
        * ``volatility`` â€” annualised standard deviation of daily returns.
        * ``sharpe_ratio`` â€” annualised Sharpe ratio.
        * ``max_drawdown``
        * ``risk_free_rate``
        * ``benchmark_total_return``, ``benchmark_annualized_return``
        * ``excess_return`` â€” portfolio minus benchmark total return.
    """
    dt_start = pd.to_datetime(effective_start or start_date)
    dt_end = pd.to_datetime(effective_end or end_date)
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
#  6. REPORT GENERATION
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
    lines.append(f"Period:              {metrics['start_date']}  â†’  {metrics['end_date']}")
    lines.append(f"Total Return:        {metrics['total_return']:+.2%}")
    lines.append(f"Annualized Return:   {metrics['annualized_return']:+.2%}")
    lines.append(f"Volatility (ann.):   {metrics['volatility']:.2%}")
    risk_free = metrics.get('risk_free_rate', 0.0)
    lines.append(f"Sharpe Ratio:        {metrics['sharpe_ratio']:.4f}  (rf={risk_free:.2%})")
    lines.append(f"Max Drawdown:        {metrics['max_drawdown']:.2%}")

    # â”€â”€ Capital allocation â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
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
                f"{bench_info['capital']:,.0f} capital  â†’  "
                f"{bench_info['shares']:,.2f} shares @ {bench_info['start_price']:,.2f}"
            )

    # â”€â”€ Return decomposition (portfolio) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
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

    # â”€â”€ Benchmark â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
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

    # â”€â”€ Per-company breakdown â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
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

    # â”€â”€ Yearly returns â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
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

    # â”€â”€ Dividends per company per year â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
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
#  7. CHART GENERATION
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

    1. **cumulative_returns.png** â€” Portfolio total return vs benchmark.
    2. **drawdown.png** â€” Portfolio drawdown over time.
    3. **return_decomposition.png** â€” Stacked area of price vs dividend
       contribution over time.
    4. **per_company_breakdown.png** â€” Horizontal bar chart of each
       company's price and dividend return contribution.
    5. **dividends_by_year.png** â€” Stacked bar chart of per-share
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
            "matplotlib is not installed â€” skipping chart generation. "
            "Install it with: pip install matplotlib"
        )
        return []

    os.makedirs(output_dir, exist_ok=True)
    created: list[str] = []
    period_label = f"{start_date} â†’ {end_date}"

    # â”€â”€ 1. Cumulative returns: portfolio vs benchmark â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
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

        ax.set_title(f"Cumulative Returns â€” {period_label}", fontsize=14)
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

    # â”€â”€ 2. Drawdown chart â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
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

        ax.set_title(f"Drawdown â€” {period_label}", fontsize=14)
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

    # â”€â”€ 3. Return decomposition (stacked area) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
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
            f"Portfolio Return Decomposition â€” {period_label}", fontsize=14
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

    # â”€â”€ 4. Per-company breakdown (horizontal bar) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
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
            f"Per-Company Return Contribution â€” {period_label}", fontsize=14
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

    # â”€â”€ 5. Dividends by company by year (stacked bar) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
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
                f"Dividends Per Company Per Year â€” {period_label}",
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
#  8. ORCHESTRATOR ENTRY POINT
# ---------------------------------------------------------------------------


def run_backtest(
    backtesting_config: dict,
    db_path: str,
    prices_table: str = "stock_prices",
    ratios_table: str = "ShareMetrics",
    company_table: str = "companyInfo",
    financial_statements_table: str = "FinancialStatements",
) -> dict:
    """Run a full backtest from a config dictionary.

    This is the function called by the orchestrator step.

    Args:
        backtesting_config: Dictionary with keys ``start_date``, ``end_date``,
            ``portfolio`` (dict of tickerâ†’weight), ``benchmark_ticker``
            (optional), ``output_file`` (optional).
        db_path: Path to the SQLite database.
        prices_table: Name of the stock-prices table.
        ratios_table: Name of the dividends table (typically ``ShareMetrics``).
        company_table: Name of the company-info table.
        financial_statements_table: Name of the financial-statements table
            used to map ``docID`` dividends to ``periodEnd``.

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

    if not db_path:
        raise ValueError(
            "Backtesting requires a Source_Database path. "
            "Set backtesting_config.Source_Database in the UI."
        )

    # â”€â”€ Validate configuration â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
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
        # Legacy / pure-weight portfolio â€” validate sum (warning only)
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
            financial_statements_table=financial_statements_table,
            conn=conn,
        )

        # Also fetch dividends for the benchmark ticker (if any)
        all_dividends_df = dividends_df
        if benchmark_ticker:
            bench_divs = get_dividend_data(
                db_path, ratios_table, company_table,
                [benchmark_ticker], start_date, end_date,
                financial_statements_table=financial_statements_table,
                conn=conn,
            )
            if not bench_divs.empty:
                all_dividends_df = pd.concat(
                    [dividends_df, bench_divs], ignore_index=True
                )

        portfolio_prices = prices_df[prices_df["Ticker"].isin(tickers)]

        # 3. Portfolio returns
        portfolio_df = calculate_portfolio_returns(
            portfolio_prices, portfolio_weights, dividends_df,
            initial_capital=initial_capital,
        )

        # 4. Return decomposition (price vs dividend)
        decomposition = calculate_return_decomposition(
            portfolio_prices, portfolio_weights, dividends_df,
            initial_capital=initial_capital,
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


# ---------------------------------------------------------------------------
#  9. BATCH BACKTEST (CSV SET)
# ---------------------------------------------------------------------------

_BACKTEST_DURATIONS: dict[str, int] = {
    "1yr": 1,
    "2yr": 2,
    "3yr": 3,
    "5yr": 5,
    "10yr": 10,
}


def _generate_set_summary(
    all_results: list[dict],
    output_file: str,
) -> None:
    """Write an aggregate summary report for the entire backtest set.

    Args:
        all_results: List of result dicts, one per individual backtest,
            each containing ``year``, ``duration``, ``metrics`` (the dict
            returned by :func:`run_backtest`) and ``tickers``.
        output_file: Path to write the summary text file.
    """
    output_dir = os.path.dirname(output_file)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    lines: list[str] = []
    lines.append("=" * 80)
    lines.append("  BACKTEST SET â€” AGGREGATE SUMMARY")
    lines.append("=" * 80)
    lines.append("")

    total_runs = len(all_results)
    successful = [r for r in all_results if r.get("metrics")]
    failed = total_runs - len(successful)

    lines.append(f"  Total backtests scheduled : {total_runs}")
    lines.append(f"  Successful               : {len(successful)}")
    if failed:
        lines.append(f"  Failed / no data         : {failed}")
    lines.append("")

    # â”€â”€ Benchmark comparison â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    has_bench = [
        r for r in successful
        if r["metrics"].get("benchmark_total_return") is not None
    ]
    outperformed = [
        r for r in has_bench
        if r["metrics"]["total_return"] > r["metrics"]["benchmark_total_return"]
    ]
    underperformed = [
        r for r in has_bench
        if r["metrics"]["total_return"] <= r["metrics"]["benchmark_total_return"]
    ]

    if has_bench:
        lines.append("--- Benchmark Comparison ---")
        lines.append(f"  Backtests with benchmark : {len(has_bench)}")
        lines.append(
            f"  Outperformed benchmark   : {len(outperformed)}  "
            f"({len(outperformed) / len(has_bench) * 100:.0f}%)"
        )
        lines.append(
            f"  Underperformed benchmark : {len(underperformed)}  "
            f"({len(underperformed) / len(has_bench) * 100:.0f}%)"
        )
        lines.append("")

        # Breakdown by duration
        lines.append("  By duration:")
        durations_seen = sorted({r["duration"] for r in has_bench})
        for dur in durations_seen:
            dur_results = [r for r in has_bench if r["duration"] == dur]
            dur_out = [
                r for r in dur_results
                if r["metrics"]["total_return"]
                > r["metrics"]["benchmark_total_return"]
            ]
            lines.append(
                f"    {dur:<6}  {len(dur_out)}/{len(dur_results)} outperformed"
            )
        lines.append("")

    # â”€â”€ Aggregate statistics â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    if successful:
        returns = [r["metrics"]["total_return"] for r in successful]
        ann_returns = [r["metrics"]["annualized_return"] for r in successful]
        sharpes = [r["metrics"]["sharpe_ratio"] for r in successful]
        drawdowns = [r["metrics"]["max_drawdown"] for r in successful]

        lines.append("--- Aggregate Statistics (across all successful backtests) ---")
        lines.append(f"  {'Metric':<28} {'Mean':>10} {'Median':>10} {'Min':>10} {'Max':>10}")
        lines.append("  " + "-" * 70)

        def _row(label: str, vals: list[float], fmt: str = "+.2%"):
            arr = np.array(vals)
            mean_s = format(np.mean(arr), fmt).rjust(10)
            med_s = format(np.median(arr), fmt).rjust(10)
            min_s = format(np.min(arr), fmt).rjust(10)
            max_s = format(np.max(arr), fmt).rjust(10)
            lines.append(f"  {label:<28} {mean_s} {med_s} {min_s} {max_s}")

        _row("Total Return", returns)
        _row("Annualized Return", ann_returns)
        _row("Sharpe Ratio", sharpes, fmt=".4f")
        _row("Max Drawdown", drawdowns)

        if has_bench:
            excess = [
                r["metrics"]["total_return"]
                - r["metrics"]["benchmark_total_return"]
                for r in has_bench
            ]
            lines.append("")
            _row("Excess Return (vs bench)", excess)
        lines.append("")

    # â”€â”€ Per-backtest detail table â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    if successful:
        lines.append("--- Individual Backtest Results ---")
        header = (
            f"  {'Year':<6} {'Duration':<10} {'Period':<25} "
            f"{'Total':>10} {'Ann.':>10} {'Sharpe':>8} {'MaxDD':>10} "
            f"{'Bench':>10} {'Excess':>10} {'Beat?':>6}"
        )
        lines.append(header)
        lines.append("  " + "-" * (len(header) - 2))
        for r in sorted(successful, key=lambda x: (x["year"], x["duration"])):
            m = r["metrics"]
            bench_str = (
                f"{m['benchmark_total_return']:+.2%}"
                if m.get("benchmark_total_return") is not None
                else "N/A"
            )
            excess_str = (
                f"{m['excess_return']:+.2%}"
                if m.get("excess_return") is not None
                else "N/A"
            )
            beat_str = ""
            if m.get("benchmark_total_return") is not None:
                beat_str = (
                    "  âœ“"
                    if m["total_return"] > m["benchmark_total_return"]
                    else "  âœ—"
                )
            period = f"{m['start_date']} â†’ {m['end_date']}"
            lines.append(
                f"  {r['year']:<6} {r['duration']:<10} {period:<25} "
                f"{m['total_return']:>+9.2%} "
                f"{m['annualized_return']:>+9.2%} "
                f"{m['sharpe_ratio']:>7.4f} "
                f"{m['max_drawdown']:>+9.2%} "
                f"{bench_str:>10} "
                f"{excess_str:>10} "
                f"{beat_str:>6}"
            )
        lines.append("")

    lines.append("=" * 80)

    report_text = "\n".join(lines)
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(report_text)
    logger.info("Backtest set summary written to %s", output_file)
    print(f"Backtest set summary written to {output_file}")


def run_backtest_set(
    config: dict,
    db_path: str,
    prices_table: str = "stock_prices",
    ratios_table: str = "ShareMetrics",
    company_table: str = "companyInfo",
    financial_statements_table: str = "FinancialStatements",
) -> list[dict]:
    """Run a batch of backtests from a CSV file of scored companies.

    For each year found in the CSV, five backtests are executed with
    horizons of 1, 2, 3, 5 and 10 years using the tickers and weights
    listed for that year.  Results are written to individual report files
    inside a dedicated output folder, and an aggregate summary is
    generated.

    Args:
        config: Dictionary with the following keys:

            * ``csv_file`` (str) â€” path to the CSV with columns
              ``Year``, ``Tickers``, ``Type``, ``Amount``.
            * ``benchmark_ticker`` (str) â€” ticker symbol of the benchmark.
            * ``output_dir`` (str, optional) â€” base directory for results.
              Defaults to ``data/backtest_set_results``.
            * ``risk_free_rate`` (float, optional) â€” annual risk-free rate
              (default 0.0).
            * ``initial_capital`` (float, optional) â€” starting capital
              (default 0).

        db_path: Path to the SQLite database file.
        prices_table: Name of the stock-prices table.
        ratios_table: Name of the dividends table (typically ``ShareMetrics``).
        company_table: Name of the company-info table.
        financial_statements_table: Name of the financial-statements table
            used to map ``docID`` dividends to ``periodEnd``.

    Returns:
        List of result dicts, one per individual backtest.
    """
    csv_file = config.get("csv_file", "")
    benchmark_ticker = config.get("benchmark_ticker", "")
    output_dir = config.get("output_dir", "data/backtest_set_results")
    risk_free_rate = config.get("risk_free_rate", 0.0)
    initial_capital = config.get("initial_capital", 0.0)

    if not csv_file:
        raise ValueError("backtest_set_config must contain 'csv_file'.")

    # â”€â”€ Read and parse CSV comments for config â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    # Headers with "#" prefix are treated as comments.  Lines starting
    # with "# Benchmark:" and "# Discount Rate:" supply defaults that
    # can be overridden by explicit config keys.
    csv_config: dict[str, str] = {}
    with open(csv_file, "r", encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()
            if not stripped.startswith("#"):
                break
            if stripped.startswith("# Benchmark:"):
                csv_config["benchmark_ticker"] = stripped[len("# Benchmark:"):].strip()
            elif stripped.startswith("# Discount Rate:"):
                csv_config["risk_free_rate"] = stripped[len("# Discount Rate:"):].strip()

    # Use CSV-embedded config as defaults; let explicit config override
    if not benchmark_ticker:
        benchmark_ticker = csv_config.get("benchmark_ticker", "")
    if not risk_free_rate:
        raw_rate = csv_config.get("risk_free_rate", "")
        if raw_rate:
            try:
                risk_free_rate = float(raw_rate)
            except ValueError:
                logger.warning("Invalid risk_free_rate in CSV comments: %r", raw_rate)

    # â”€â”€ Read and validate CSV â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    df = pd.read_csv(csv_file, comment="#")
    required_cols = {"Year", "Tickers", "Type", "Amount"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(
            f"CSV is missing required columns: {missing}. "
            f"Expected: {required_cols}"
        )

    df["Year"] = df["Year"].astype(str).str.strip()
    years = sorted(df["Year"].unique())

    logger.info(
        "Backtest set: %d year(s), %d durations each â†’ %d backtests.",
        len(years), len(_BACKTEST_DURATIONS), len(years) * len(_BACKTEST_DURATIONS),
    )

    os.makedirs(output_dir, exist_ok=True)
    all_results: list[dict] = []

    for year_str in years:
        year_df = df[df["Year"] == year_str]

        # Build portfolio dict from CSV rows
        portfolio: dict[str, dict] = {}
        for _, row in year_df.iterrows():
            ticker = str(row["Tickers"]).strip()
            mode = str(row["Type"]).strip().lower()
            if mode not in ALLOCATION_MODES:
                mode = "weight"
            amount = float(row["Amount"])
            portfolio[ticker] = {"mode": mode, "value": amount}

        if not portfolio:
            logger.warning("Year %s: no portfolio entries found, skipping.", year_str)
            continue

        for dur_label, dur_years in _BACKTEST_DURATIONS.items():
            start_date = f"{year_str}-01-01"
            end_year = int(year_str) + dur_years
            end_date = f"{end_year}-01-01"

            # Per-backtest output folder
            run_dir = os.path.join(output_dir, f"{year_str}_{dur_label}")
            os.makedirs(run_dir, exist_ok=True)
            run_output = os.path.join(run_dir, "backtest_report.txt")

            bt_config: dict = {
                "start_date": start_date,
                "end_date": end_date,
                "portfolio": portfolio,
                "benchmark_ticker": benchmark_ticker,
                "output_file": run_output,
                "risk_free_rate": risk_free_rate,
                "initial_capital": initial_capital,
            }

            result_entry: dict = {
                "year": year_str,
                "duration": dur_label,
                "start_date": start_date,
                "end_date": end_date,
                "tickers": list(portfolio.keys()),
                "metrics": None,
            }

            try:
                metrics = run_backtest(
                    bt_config,
                    db_path=db_path,
                    prices_table=prices_table,
                    ratios_table=ratios_table,
                    company_table=company_table,
                    financial_statements_table=financial_statements_table,
                )
                result_entry["metrics"] = metrics
                logger.info(
                    "  %s %s: total return %.2f%%",
                    year_str, dur_label, metrics["total_return"] * 100,
                )
            except Exception as e:
                logger.error(
                    "  %s %s: backtest failed â€” %s", year_str, dur_label, e,
                )

            all_results.append(result_entry)

    # â”€â”€ Generate aggregate summary â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    summary_file = os.path.join(output_dir, "backtest_set_summary.txt")
    _generate_set_summary(all_results, summary_file)

    return all_results 



