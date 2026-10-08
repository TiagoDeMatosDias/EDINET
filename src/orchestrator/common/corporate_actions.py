"""Put reported dividends on the same share basis as the stored prices.

Stored prices are split-adjusted: providers (JPX, Stooq, Yahoo) restate
history after a split, and raw rows are adjusted through ``Stock_Splits``.
Dividends per share come from annual reports and stay as paid: Toyota paid
¥240 a share for the year to March 2021 and ¥60 for the year to March 2023,
across a 5-for-1 split. A backtest that buys shares at the adjusted price and
credits the reported dividend pays five times too much before the split.

Splits are taken from two sources:

* confirmed rows in ``Stock_Splits`` (exact dates);
* the issued share count at consecutive fiscal year ends in annual reports,
  when it changes by a standard split or consolidation ratio. Share counts
  also move with issuance and buybacks, so large ratios (2, 3, 4, 5, 10 …)
  are accepted within 1.5 %, or within 12 % when book value per share (else
  dividends per share) moved like a split rather than like a merger or an
  issue — Sony's 5-for-1 split came with buyback cancellations, a ratio of
  4.88 — and small ratios (1.1, 1.2, 1.25, 1.5, 2.5) only when exact.

Each annual dividend is also split into its interim and final payments, dated
at the half-year and the fiscal year end (their record dates), so a holder is
credited only with payments whose record date falls while the shares were
held.
"""

from __future__ import annotations

import logging
import math
import sqlite3
from dataclasses import dataclass

import pandas as pd

logger = logging.getLogger(__name__)

_LARGE_RATIOS = (2.0, 3.0, 4.0, 5.0, 10.0, 20.0, 50.0, 100.0)
_SMALL_RATIOS = (1.1, 1.2, 1.25, 1.5, 2.5)
_LARGE_TOLERANCE = 0.015
_CONFIRMED_LARGE_TOLERANCE = 0.12
_SMALL_TOLERANCE = 0.001
# A daily move within this log distance of the split ratio is the split itself,
# showing that the stored series is on the raw basis around that event.
_RAW_JUMP_TOLERANCE = 0.12


@dataclass(frozen=True)
class SplitEvent:
    """Shares were multiplied by ``multiplier`` some time in ``(after, until]``."""

    after: pd.Timestamp
    until: pd.Timestamp
    multiplier: float
    source: str  # "Stock_Splits" or "annual reports"

    @property
    def exact(self) -> bool:
        return self.until - self.after <= pd.Timedelta(days=1)


@dataclass(frozen=True)
class AnnualFacts:
    """Per-share facts from one annual report."""

    period_end: pd.Timestamp
    shares: float | None
    book_value_per_share: float | None = None
    dividend_per_share: float | None = None


def standard_split_multiplier(ratio: float, *, tolerance_large: float = _LARGE_TOLERANCE) -> float | None:
    """Return the standard split ratio *ratio* corresponds to, or ``None``."""
    if not ratio or ratio <= 0 or not math.isfinite(ratio):
        return None
    for candidates, tolerance in ((_LARGE_RATIOS, tolerance_large), (_SMALL_RATIOS, _SMALL_TOLERANCE)):
        for value in candidates:
            for nice in (value, 1.0 / value):
                if abs(math.log(ratio / nice)) <= tolerance:
                    return nice
    return None


def _moved_like_split(before: float | None, after: float | None, multiplier: float) -> bool | None:
    """Did a per-share figure fall by the split ratio? ``None`` when unknown."""
    if not before or not after or before <= 0 or after <= 0:
        return None
    return abs(math.log(after * multiplier / before)) < abs(math.log(after / before))


def infer_share_count_splits(history: list[AnnualFacts] | list[tuple[pd.Timestamp, float | None]]) -> list[SplitEvent]:
    """Split events implied by fiscal-year-end issued share counts, oldest first."""
    facts = [item if isinstance(item, AnnualFacts) else AnnualFacts(item[0], item[1]) for item in history]
    points = sorted((item for item in facts if item.shares and item.shares > 0), key=lambda item: item.period_end)
    events: list[SplitEvent] = []
    for before, after in zip(points, points[1:], strict=False):
        if after.period_end <= before.period_end:
            continue
        ratio = float(after.shares) / float(before.shares)
        multiplier = standard_split_multiplier(ratio)
        if multiplier is None:
            near = standard_split_multiplier(ratio, tolerance_large=_CONFIRMED_LARGE_TOLERANCE)
            if near is not None and (near >= 2 or near <= 0.5):
                evidence = _moved_like_split(before.book_value_per_share, after.book_value_per_share, near)
                if evidence is None:
                    evidence = _moved_like_split(before.dividend_per_share, after.dividend_per_share, near)
                multiplier = near if evidence else None
        if multiplier is not None:
            events.append(SplitEvent(before.period_end, after.period_end, multiplier, "annual reports"))
    return events


def merge_split_events(recorded: list[SplitEvent], inferred: list[SplitEvent]) -> list[SplitEvent]:
    """Recorded events win; an inferred event is dropped when one explains it."""
    merged = list(recorded)
    for event in inferred:
        explained = any(
            event.after < known.until <= event.until
            and abs(math.log(known.multiplier / event.multiplier)) < 0.02
            for known in recorded
        )
        if not explained:
            merged.append(event)
    return sorted(merged, key=lambda item: (item.until, item.after))


def dividend_payments(rows: pd.DataFrame) -> pd.DataFrame:
    """Expand annual dividends into interim and final payments.

    *rows* has ``Ticker``, ``periodEnd`` (fiscal year end), ``PerShare_Dividends``
    (the year's total, which includes the interim) and optionally
    ``Interim_Dividends``. The result keeps ``periodEnd`` as the payment's
    record date (the column name the backtests use), with ``fiscal_period_end``
    and ``payment`` (``interim``/``final``).
    """
    columns = ["Ticker", "periodEnd", "PerShare_Dividends", "fiscal_period_end", "payment"]
    if rows is None or rows.empty:
        return pd.DataFrame(columns=columns)
    records: list[dict] = []
    for row in rows.itertuples(index=False):
        fiscal_end = pd.Timestamp(row.periodEnd)
        annual = float(row.PerShare_Dividends or 0.0)
        interim = float(getattr(row, "Interim_Dividends", 0.0) or 0.0)
        if not math.isfinite(annual):
            annual = 0.0
        if not math.isfinite(interim) or interim < 0:
            interim = 0.0
        final = max(annual - interim, 0.0) if annual > 0 else 0.0
        if interim > 0:
            records.append({
                "Ticker": row.Ticker,
                "periodEnd": fiscal_end - pd.DateOffset(months=6),
                "PerShare_Dividends": interim,
                "fiscal_period_end": fiscal_end,
                "payment": "interim",
            })
        if final > 0:
            records.append({
                "Ticker": row.Ticker,
                "periodEnd": fiscal_end,
                "PerShare_Dividends": final,
                "fiscal_period_end": fiscal_end,
                "payment": "final",
            })
    return pd.DataFrame(records, columns=columns)


def _closest(value: float, references: list[float]) -> float:
    return min(abs(math.log(value / reference)) for reference in references)


def _paid_before(payment: pd.Series, event: SplitEvent, ticker_payments: pd.DataFrame) -> bool:
    """Was *payment* made on the shares as they were before *event*?"""
    record = pd.Timestamp(payment["periodEnd"])
    if record <= event.after:
        return True
    if record >= event.until:
        return False
    # An interim payment inside the window: the split may fall on either side.
    # Compare its size with payments known to be before and after the split.
    amount = float(payment["PerShare_Dividends"])
    others = ticker_payments[ticker_payments.index != payment.name]
    post = [float(value) for value in others.loc[pd.to_datetime(others["periodEnd"]) >= event.until, "PerShare_Dividends"].tail(2) if value > 0]
    pre = [float(value) / event.multiplier for value in others.loc[pd.to_datetime(others["periodEnd"]) <= event.after, "PerShare_Dividends"].tail(2) if value > 0]
    references = post + pre
    if amount <= 0 or not references:
        return False
    return _closest(amount / event.multiplier, references) < _closest(amount, references)


def adjust_payments_for_splits(
    payments: pd.DataFrame,
    events_by_ticker: dict[str, list[SplitEvent]],
    *,
    raw_events: set[tuple[str, pd.Timestamp]] | None = None,
) -> pd.DataFrame:
    """Divide each payment by the splits that came after it.

    ``raw_events`` lists ``(ticker, event.until)`` pairs whose prices were found
    on the raw basis around the event; those events leave dividends alone so
    prices and dividends stay on one basis. Adds ``split_factor`` (1.0 when
    nothing changed) and ``reported_per_share``.
    """
    if payments.empty:
        out = payments.copy()
        out["split_factor"] = pd.Series(dtype=float)
        out["reported_per_share"] = pd.Series(dtype=float)
        return out
    raw_events = raw_events or set()
    out = payments.reset_index(drop=True).copy()
    out["reported_per_share"] = out["PerShare_Dividends"].astype(float)
    factors: list[float] = []
    for _, payment in out.iterrows():
        ticker = str(payment["Ticker"])
        factor = 1.0
        ticker_payments = out[out["Ticker"] == payment["Ticker"]]
        for event in events_by_ticker.get(ticker, []):
            if (ticker, event.until) in raw_events:
                continue
            if _paid_before(payment, event, ticker_payments):
                factor /= event.multiplier
        factors.append(factor)
    out["split_factor"] = factors
    out["PerShare_Dividends"] = out["reported_per_share"] * out["split_factor"]
    return out


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {row[1] for row in conn.execute(f'PRAGMA table_info("{table}")')}
    except sqlite3.Error:
        return set()


def load_split_events(
    conn: sqlite3.Connection,
    tickers: list[str],
    *,
    per_share_table: str = "ShareMetrics",
    financial_statements_table: str = "FinancialStatements",
    company_table: str = "CompanyInfo",
    company_code_column: str = "Company_Code",
    fs_code_column: str = "Company_Code",
) -> dict[str, list[SplitEvent]]:
    """Every known split for *tickers* (stored ticker spelling), oldest first."""
    if not tickers:
        return {}
    placeholders = ",".join("?" for _ in tickers)
    recorded: dict[str, list[SplitEvent]] = {}
    split_columns = _table_columns(conn, "Stock_Splits")
    if {"ticker", "split_date", "ratio_from", "ratio_to"} <= split_columns:
        clauses = []
        if "confirmation" in split_columns:
            clauses.append("confirmation = 'confirmed'")
        if "superseded_by" in split_columns:
            clauses.append("COALESCE(superseded_by, 0) = 0")
        where = " AND ".join([f"ticker IN ({placeholders})", *clauses])
        for ticker, split_date, ratio_from, ratio_to in conn.execute(
            f"SELECT ticker, split_date, ratio_from, ratio_to FROM Stock_Splits WHERE {where} ORDER BY split_date",
            tickers,
        ):
            try:
                multiplier = float(ratio_to) / float(ratio_from)
                date = pd.Timestamp(str(split_date)[:10])
            except (TypeError, ValueError, ZeroDivisionError):
                continue
            if multiplier <= 0 or multiplier == 1:
                continue
            events = recorded.setdefault(str(ticker), [])
            if not any(event.until == date for event in events):
                events.append(SplitEvent(date - pd.Timedelta(days=1), date, multiplier, "Stock_Splits"))

    history: dict[str, list[AnnualFacts]] = {}
    share_columns = _table_columns(conn, per_share_table)
    count_columns = [name for name in ("Number of issued shares as of fiscal year end", "Total number of issued shares") if name in share_columns]
    bps_sql = 'p."Net assets per share"' if "Net assets per share" in share_columns else "NULL"
    dps_sql = 'p."Dividend paid per share"' if "Dividend paid per share" in share_columns else "NULL"
    fs_columns = _table_columns(conn, financial_statements_table)
    if count_columns and "docID" in share_columns and {"docID", "periodEnd"} <= fs_columns:
        count_sql = "COALESCE(" + ", ".join(f'p."{name}"' for name in count_columns) + ")" if len(count_columns) > 1 else f'p."{count_columns[0]}"'
        annual = "AND fs.docTypeCode = '030000'" if "docTypeCode" in fs_columns else ""
        query = (
            f'SELECT c.Company_Ticker, fs.periodEnd, {count_sql}, {bps_sql}, {dps_sql} FROM "{per_share_table}" p '
            f'JOIN "{financial_statements_table}" fs ON fs.docID = p.docID '
            f'JOIN "{company_table}" c ON c."{company_code_column}" = fs."{fs_code_column}" '
            f"WHERE c.Company_Ticker IN ({placeholders}) {annual} ORDER BY fs.periodEnd"
        )
        try:
            for ticker, period_end, shares, bps, dps in conn.execute(query, tickers):
                try:
                    history.setdefault(str(ticker), []).append(AnnualFacts(
                        pd.Timestamp(str(period_end)[:10]),
                        float(shares) if shares else None,
                        float(bps) if bps else None,
                        float(dps) if dps else None,
                    ))
                except (TypeError, ValueError):
                    continue
        except sqlite3.Error:
            logger.debug("Could not read share counts for split inference", exc_info=True)

    events: dict[str, list[SplitEvent]] = {}
    for ticker in {*recorded, *history}:
        merged = merge_split_events(recorded.get(ticker, []), infer_share_count_splits(history.get(ticker, [])))
        if merged:
            events[ticker] = merged
    return events


def raw_basis_events(
    conn: sqlite3.Connection,
    prices_table: str,
    events_by_ticker: dict[str, list[SplitEvent]],
) -> set[tuple[str, pd.Timestamp]]:
    """Events across which the series a backtest reads still jumps by the split ratio."""
    columns = _table_columns(conn, prices_table)
    if not {"Ticker", "Date", "Price"} <= columns:
        return set()
    price_sql = 'COALESCE("Adjusted_Price", "Price")' if "Adjusted_Price" in columns else '"Price"'
    raw: set[tuple[str, pd.Timestamp]] = set()
    for ticker, events in events_by_ticker.items():
        for event in events:
            start = event.after - pd.Timedelta(days=7) if event.exact else event.after
            end = event.until + pd.Timedelta(days=7)
            rows = conn.execute(
                f'SELECT Date, {price_sql} FROM "{prices_table}" WHERE Ticker = ? AND Date > ? AND Date <= ? ORDER BY Date',
                (ticker, start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")),
            ).fetchall()
            prices = [float(price) for _date, price in rows if price and float(price) > 0]
            expected = -math.log(event.multiplier)
            if any(abs(math.log(b / a) - expected) < _RAW_JUMP_TOLERANCE for a, b in zip(prices, prices[1:], strict=False)):
                raw.add((ticker, event.until))
    return raw


def split_factor_at(events: list[SplitEvent], day: pd.Timestamp) -> float:
    """Factor the stored (adjusted) prices carry on *day*: adjusted = as traded × factor."""
    factor = 1.0
    for event in events:
        # Inferred windows start at a fiscal year end, so ``after >= day``
        # means the split came after *day* whenever *day* is a year end.
        if (event.exact and event.until > day) or (not event.exact and event.after >= day):
            factor /= event.multiplier
    return factor


def share_count_basis_factors(
    conn: sqlite3.Connection,
    tickers: list[str],
    as_of: str,
    *,
    per_share_table: str = "ShareMetrics",
    financial_statements_table: str = "FinancialStatements",
    company_table: str = "CompanyInfo",
) -> dict[str, float]:
    """Factors that turn ``adjusted price × reported share count`` into a market cap.

    The share count a screen reports comes from the latest annual report
    filed by *as_of*; the stored price is adjusted for every later split.
    Dividing their product by the price factor at that report's year end
    puts both on the same basis.
    """
    if not tickers:
        return {}
    events = load_split_events(
        conn, tickers,
        per_share_table=per_share_table,
        financial_statements_table=financial_statements_table,
        company_table=company_table,
    )
    if not events:
        return {}
    placeholders = ",".join("?" for _ in tickers)
    fs_columns = _table_columns(conn, financial_statements_table)
    annual = "AND fs.docTypeCode = '030000'" if "docTypeCode" in fs_columns else ""
    filed = "AND substr(fs.submitDateTime, 1, 10) <= ?" if "submitDateTime" in fs_columns else "AND fs.periodEnd <= ?"
    rows = conn.execute(
        f'SELECT c.Company_Ticker, MAX(fs.periodEnd) FROM "{financial_statements_table}" fs '
        f'JOIN "{company_table}" c ON c.Company_Code = fs.Company_Code '
        f"WHERE c.Company_Ticker IN ({placeholders}) {annual} {filed} GROUP BY c.Company_Ticker",
        [*tickers, as_of[:10]],
    ).fetchall()
    factors: dict[str, float] = {}
    for ticker, year_end in rows:
        if not year_end or str(ticker) not in events:
            continue
        factor = split_factor_at(events[str(ticker)], pd.Timestamp(str(year_end)[:10]))
        if factor != 1.0:
            factors[str(ticker)] = factor
    return factors
