"""Put reported per-share figures and dividends on the share basis of the stored prices.

Stored prices are split-adjusted: providers (JPX, Stooq, Yahoo) restate
history after a split, and raw rows are adjusted through ``Stock_Splits``.
Dividends per share come from annual reports and stay as paid: Toyota paid
¥240 a share for the year to March 2021 and ¥60 for the year to March 2023,
across a 5-for-1 split. A backtest that buys shares at the adjusted price and
credits the reported dividend pays five times too much before the split.

Splits are taken from three sources:

* confirmed rows in ``Stock_Splits``, dated at the ex-date (the first day the
  shares trade on the new basis). Holdings change at the split's record date,
  a business day or two later, so a year end or dividend record date up to
  ``_RECORD_DATE_LAG`` after the ex-date is still on the old shares: a split
  traded ex on 30 March leaves the 31 March share count and final dividend
  as they were. Consolidations beyond 100-to-1 are squeeze-outs before a
  delisting, not a change of basis, and are ignored, as is a second record
  of one split (the same ratio within 10 days);
* the issued share counts an annual report gives at the year end and at its
  filing date, when they differ by a standard ratio (a split in the weeks
  between, which the report's EPS and book value already reflect);
* the issued share count at consecutive fiscal year ends in annual reports,
  when it changes by a standard split or consolidation ratio. Share counts
  also move with issuance and buybacks, so large ratios (2, 3, 4, 5, 10 …)
  are accepted within 1.5 %, or within 12 % when book value per share (else
  dividends per share) moved like a split rather than like a merger or an
  issue — Sony's 5-for-1 split came with buyback cancellations, a ratio of
  4.88 — or within 30 % when the stored prices were adjusted by the ratio
  (a consolidation in the year of a merger), and small ratios (1.1, 1.2, 1.25, 1.5, 2.5) only when exact. A
  share-count change with a confirmed split inside is that split's, so no
  split is counted twice.

Each annual dividend is also split into its interim and final payments, dated
at the half-year and the fiscal year end (their record dates), so a holder is
credited only with payments whose record date falls while the shares were
held.
"""

from __future__ import annotations

import logging
import math
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
from typing import NamedTuple

import pandas as pd

from src.utilities.price_provenance import (
    distinct_split_records,
    is_squeeze_out,
    split_jump_in_closes,
)

logger = logging.getLogger(__name__)

_LARGE_RATIOS = (2.0, 3.0, 4.0, 5.0, 10.0, 20.0, 50.0, 100.0)
_SMALL_RATIOS = (1.1, 1.2, 1.25, 1.5, 2.5)
_LARGE_TOLERANCE = 0.015
_CONFIRMED_LARGE_TOLERANCE = 0.12
# A share count can move further from the ratio when the split came with a
# merger or an issue of shares; the price then has to confirm it.
_PRICED_LARGE_TOLERANCE = 0.30
_SMALL_TOLERANCE = 0.001
# A split's record date follows its ex-date by one business day (two before
# July 2019), at most five calendar days across a weekend and holiday.
_RECORD_DATE_LAG = pd.Timedelta(days=5)
# A report can restate its per-share figures for a split its board decided
# before filing that takes effect within weeks after.
_RESTATED_BEFORE_EFFECT = pd.Timedelta(days=92)


@dataclass(frozen=True)
class SplitEvent:
    """Shares were multiplied by ``multiplier`` some time in ``(after, until]``."""

    after: pd.Timestamp
    until: pd.Timestamp
    multiplier: float
    source: str  # "Stock_Splits", "filing date count" or "annual reports"

    @property
    def exact(self) -> bool:
        return self.until - self.after <= pd.Timedelta(days=1)


def _held_before(event: SplitEvent, day: pd.Timestamp) -> bool:
    """Were shares held on *day* (a year end or record date) still on the old basis?"""
    if event.exact:
        return day <= event.until + _RECORD_DATE_LAG
    return day <= event.after


@dataclass(frozen=True)
class AnnualFacts:
    """Per-share facts from one annual report."""

    period_end: pd.Timestamp
    shares: float | None
    book_value_per_share: float | None = None
    dividend_per_share: float | None = None
    # The issued share count at the year end and at the filing date, and when
    # the report was filed: a split in between shows in the one report.
    year_end_shares: float | None = None
    filing_shares: float | None = None
    filed: pd.Timestamp | None = None
    # The stored (split-adjusted) price at the year end over the price as
    # traded, which the report gives as its P/E ratio times its EPS: the
    # factor the price provider applied for later splits.
    price_factor: float | None = None
    net_assets: float | None = None


def standard_split_multiplier(ratio: float, *, tolerance_large: float = _LARGE_TOLERANCE) -> float | None:
    """Return the standard split ratio *ratio* corresponds to, or ``None``."""
    if not ratio or ratio <= 0 or not math.isfinite(ratio):
        return None
    for candidates, tolerance in ((_LARGE_RATIOS, tolerance_large), (_SMALL_RATIOS, _SMALL_TOLERANCE)):
        for value in candidates:
            # A consolidation merges a whole number of shares into one; a
            # count down by a tenth or a sixth is a cancellation of shares.
            for nice in (value, 1.0 / value) if value == int(value) else (value,):
                if abs(math.log(ratio / nice)) <= tolerance:
                    return nice
    # Any other whole-number ratio, held exactly: no issue of shares
    # multiplies a count by exactly 12.
    whole = round(ratio if ratio >= 1 else 1.0 / ratio)
    if 2 <= whole <= 100:
        nice = float(whole) if ratio >= 1 else 1.0 / whole
        if abs(math.log(ratio / nice)) <= _SMALL_TOLERANCE:
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
    for index, (before, after) in enumerate(zip(points, points[1:], strict=False)):
        if after.period_end <= before.period_end:
            continue
        ratio = float(after.shares) / float(before.shares)
        multiplier = standard_split_multiplier(ratio)
        if multiplier is None:
            near = standard_split_multiplier(ratio, tolerance_large=_CONFIRMED_LARGE_TOLERANCE)
            if near is not None and (near >= 2 or near <= 0.5):
                evidence = _moved_like_split(before.book_value_per_share, after.book_value_per_share, near)
                # A split effective just after a year end is already restated in
                # that year's report, a year before the share count shows it.
                if not evidence and index > 0:
                    evidence = _moved_like_split(points[index - 1].book_value_per_share, before.book_value_per_share, near) or evidence
                if evidence is None:
                    evidence = _moved_like_split(before.dividend_per_share, after.dividend_per_share, near)
                multiplier = near if evidence else None
        if multiplier is None:
            multiplier = _priced_split(before, after, ratio)
        if multiplier is not None:
            events.append(SplitEvent(before.period_end, after.period_end, multiplier, "annual reports"))
    return events


def _priced_split(before: AnnualFacts, after: AnnualFacts, ratio: float) -> float | None:
    """A split the stored prices were adjusted for, though issued shares moved too.

    A 10-to-1 consolidation in the year of a merger left a company 8.07
    times fewer shares, not ten; its stored prices before the consolidation
    were multiplied by ten all the same, which the two reports' prices as
    traded (P/E times EPS) show.
    """
    if not before.price_factor or not after.price_factor:
        return None
    priced = before.price_factor / after.price_factor
    # The listed ratios, or any whole number near the count (a 15-for-1
    # split with a few shares issued is 15.08), when the price agrees.
    whole = round(ratio if ratio >= 1 else 1.0 / ratio)
    candidates = [standard_split_multiplier(ratio, tolerance_large=_PRICED_LARGE_TOLERANCE)]
    if whole >= 2 and abs(math.log((ratio if ratio >= 1 else 1.0 / ratio) / whole)) < 0.1:
        candidates.append(float(whole) if ratio >= 1 else 1.0 / whole)
    for near in candidates:
        if near is not None and not 0.5 < near < 2 and abs(math.log(near * priced)) < _CONFIRMED_LARGE_TOLERANCE:
            return near
    return None


def infer_filing_date_splits(history: list[AnnualFacts]) -> list[SplitEvent]:
    """Splits between a year end and the filing of its report, from the two counts it gives.

    The ratio is exact but for the few shares issued or cancelled in those
    weeks, so it is held to the standard-ratio tolerances.
    """
    events: list[SplitEvent] = []
    ordered = sorted(history, key=lambda item: item.period_end)
    for index, facts in enumerate(ordered):
        if not facts.filed or facts.filed <= facts.period_end:
            continue
        if facts.year_end_shares and facts.filing_shares:
            multiplier = standard_split_multiplier(facts.filing_shares / facts.year_end_shares)
            if multiplier is None and index > 0:
                multiplier = _priced_filing_split(ordered[index - 1], facts)
        else:
            following = ordered[index + 1] if index + 1 < len(ordered) else None
            multiplier = _booked_split(facts, following)
        if multiplier is not None:
            events.append(SplitEvent(facts.period_end, facts.filed, multiplier, "filing date count"))
    return events


def _priced_filing_split(previous: AnnualFacts, facts: AnnualFacts) -> float | None:
    """A split before filing that came with an issue of shares, confirmed by the prices.

    A report's P/E times its EPS is the year-end price on the shares its EPS
    is restated for, so the stored price over it steps by a split the report
    restated: by five for a 5-for-1 split, though shares issued with it left
    5.77 times as many shares at filing as at the year end.
    """
    if not previous.price_factor or not facts.price_factor:
        return None
    ratio = facts.filing_shares / facts.year_end_shares
    if ratio < 1.8 and ratio > 1 / 1.8:
        return None
    priced = previous.price_factor / facts.price_factor
    whole = round(ratio if ratio >= 1 else 1.0 / ratio)
    candidates = [standard_split_multiplier(ratio, tolerance_large=_PRICED_LARGE_TOLERANCE)]
    if whole >= 2:
        candidates.append(float(whole) if ratio >= 1 else 1.0 / whole)
    for near in candidates:
        if near is not None and not 0.5 < near < 2 and abs(math.log(near * priced)) < _CONFIRMED_LARGE_TOLERANCE:
            return near
    return None


def _book_basis(facts: AnnualFacts | None) -> float | None:
    """Book value per share × year-end shares ÷ net assets: about 1 on one basis."""
    if not facts or not facts.book_value_per_share or not facts.shares or not facts.net_assets:
        return None
    if facts.book_value_per_share <= 0 or facts.net_assets <= 0:
        return None
    return facts.book_value_per_share * facts.shares / facts.net_assets


def _booked_split(facts: AnnualFacts, following: AnnualFacts | None) -> float | None:
    """A split a report without a filing-date count restated its book value for.

    Book value per share on 1/m of the year-end shares' net assets means the
    report restated it for an m-for-1 split taking effect before filing (a
    report gives the filing-date count only since 2019). Minority interests
    in net assets lower the ratio too, so the next report must be back on
    one basis with at least m times the shares.
    """
    ratio, after = _book_basis(facts), _book_basis(following)
    if not ratio or not after or abs(math.log(after)) > 0.1 or not following.shares:
        return None
    multiplier = standard_split_multiplier(1.0 / ratio, tolerance_large=0.1)
    if multiplier is None or multiplier < 2 or following.shares / facts.shares < multiplier * 0.9:
        return None
    return multiplier


def _without_heuristic_twins(records: list[tuple[SplitEvent, object]], history: list[AnnualFacts]) -> list[SplitEvent]:
    """Recorded splits, less a price-heuristic record the provider's split of the same ratio explains.

    A crash read as a 2-for-1 split in March and confirmed by a share count
    that doubled by the next year end, when the provider records the
    2-for-1 split in October of that year and the count moved by its ratio
    only once, is the provider's split.
    """
    ends = sorted(item.period_end for item in history if item.shares)
    shares = {item.period_end: item.shares for item in history if item.shares}

    def window(event: SplitEvent) -> tuple[pd.Timestamp, pd.Timestamp] | None:
        before = [end for end in ends if _held_before(event, end)]
        after = [end for end in ends if not _held_before(event, end)]
        return (before[-1], after[0]) if before and after else None

    kept = []
    for event, method in records:
        span = window(event) if method == "price_heuristic" else None
        twin = span and any(
            other_method == "provider" and other is not event and abs(math.log(other.multiplier / event.multiplier)) < 0.01 and window(other) == span
            for other, other_method in records
        )
        if twin and abs(math.log(shares[span[1]] / shares[span[0]] / event.multiplier)) < 0.15:
            continue
        kept.append(event)
    return kept


def _company_key_sql(alias: str = "c", code_column: str = "Company_Code") -> str:
    """A company's key: its ticker, or its EDINET code when it has none.

    A delisted company keeps its reports but loses its ticker in the company
    list; its splits still show in its share counts.
    """
    return (
        f"CASE WHEN TRIM(COALESCE({alias}.Company_Ticker, '')) = '' "
        f'THEN {alias}."{code_column}" ELSE {alias}.Company_Ticker END'
    )


def _traded_price(per: object, eps: object) -> float | None:
    """The year-end price a report gives as its P/E times its EPS.

    A loss year's P/E and EPS are both negative; their product is still the
    price.
    """
    try:
        per, eps = float(per), float(eps)
    except (TypeError, ValueError):
        return None
    traded = per * eps
    return traded if traded > 0 and math.isfinite(traded) else None


def _price_shift(before: AnnualFacts | None, after: AnnualFacts | None) -> float | None:
    """The stored price against the price as traded at *after*'s year end relative to *before*'s.

    1/m across an m-for-1 split the price provider adjusted for, 1 across none.
    """
    if not before or not after or not before.price_factor or not after.price_factor:
        return None
    return before.price_factor / after.price_factor


def _near(value: float | None, target: float, multiplier: float) -> bool:
    return value is not None and abs(math.log(value / target)) < abs(math.log(multiplier)) / 3


def _bracketed_price_shift(event: SplitEvent, history: list[AnnualFacts], others: list[SplitEvent] = ()) -> float | None:
    """How the stored price moved against the price as traded across *event*: 1/m for a split, 1 for none.

    An issue of shares, a buyback or a cancellation of treasury shares can
    move the count by a split-like ratio (a company that issued 108 % more
    shares in a loss year, another that cancelled 20 %), but only a split
    moves the price as traded against the split-adjusted one. The two
    reports compared are the nearest with a price that are surely on either
    basis: issuers quote the P/E of a report restated for a split after its
    year end on either. Another split between them (*others*) moves the
    price as well, and then the prices cannot tell.
    """
    filing = event.source == "filing date count"

    def old_basis(item: AnnualFacts) -> bool:
        if item.period_end < event.after:
            return True
        # Its own year end, if its book value per share is still on the year-end shares.
        return not filing and item.period_end == event.after and _near(_book_basis(item), 1.0, event.multiplier)

    def new_basis(item: AnnualFacts) -> bool:
        return item.period_end > event.after if filing else item.period_end >= event.until

    priced = sorted((item for item in history if item.price_factor), key=lambda item: item.period_end)
    before = next((item for item in reversed(priced) if old_basis(item)), None)
    after = next((item for item in priced if new_basis(item)), None)
    if before is None or after is None:
        return None
    for other in others:
        # The same change seen in another count overlaps the event's window.
        if other is event or (other.after < event.until and event.after < other.until):
            continue
        if other.after < after.period_end and before.period_end < other.until:
            return None
    return _price_shift(before, after)


def _price_denies(event: SplitEvent, history: list[AnnualFacts], others: list[SplitEvent] = ()) -> bool:
    """Do the reports' prices show no split where the share counts suggested one?"""
    return _near(_bracketed_price_shift(event, history, others), 1.0, event.multiplier)


def _moved_within(known: SplitEvent, event: SplitEvent) -> bool:
    """Did *known* change holdings between *event*'s two dates?"""
    return _held_before(known, event.after) and not _held_before(known, event.until)


def _another_split(event: SplitEvent, inside: list[SplitEvent], history: list[AnnualFacts]) -> SplitEvent | None:
    """A second split in *event*'s year, beside the dated ones *inside* it.

    The share count left unexplained must itself move by a split ratio of
    two or more (or a consolidation of two or more to one), and book value
    per share must have fallen with it: an issue of shares in the same year
    (a recorded 5-for-1 split and a count up 9.8 times) leaves book value per
    share where the dated splits put it.
    """
    facts = {item.period_end: item for item in history}
    start, end = facts.get(event.after), facts.get(event.until)
    if not (start and end and start.shares and end.shares):
        return None
    multiplier = standard_split_multiplier(end.shares / start.shares / math.prod(known.multiplier for known in inside))
    if multiplier is None or 0.5 < multiplier < 2:
        return None
    before, after = start.book_value_per_share, end.book_value_per_share
    if not before or not after or before <= 0 or after <= 0:
        return None
    # Book value per share in the first report already reflects the splits
    # it was restated for; the rest, with or without another, should show.
    pending = math.prod(known.multiplier for known in inside if not _restated_by(known, start.period_end, start.filed, False))
    if abs(math.log(after * pending * multiplier / before)) >= abs(math.log(after * pending / before)):
        return None
    return SplitEvent(event.after, event.until, multiplier, event.source)


def merge_split_events(
    recorded: list[SplitEvent],
    inferred: list[SplitEvent],
    filing: list[SplitEvent] | None = None,
    history: list[AnnualFacts] | None = None,
) -> list[SplitEvent]:
    """Recorded events win, then splits seen within one report, then year-end counts.

    A share-count change with a better-dated split inside is that split's. A
    recorded split falls between the year ends whose counts it changed: one
    traded ex on 30 March 2022 shows in the count at March 2023, not March
    2022. Two recorded splits in a year explain a fourfold change. What they
    leave unexplained is another split only when the remaining ratio is a
    standard one and book value per share fell with it (*history* has the
    reports); otherwise it was an issue of shares.
    """
    known = list(recorded)
    for event in filing or []:
        if any(_moved_within(other, event) for other in recorded):
            continue
        # The recorded split of the same ratio dated just before the filing
        # (inside the record-date allowance) or after it: the report gave the
        # new count or restated its book value for a split decided before it
        # was filed. One split, and the filing's window says the report is
        # already on the new shares.
        same = [
            other for other in known
            if other.source == "Stock_Splits" and abs(math.log(other.multiplier / event.multiplier)) < 0.01
            and event.after < other.until <= event.until + _RESTATED_BEFORE_EFFECT
        ]
        known = [other for other in known if other not in same]
        known.append(event)
    merged = list(known)
    for event in inferred:
        inside = [other for other in known if _moved_within(other, event)]
        if not inside:
            merged.append(event)
        elif history and (another := _another_split(event, inside, history)) is not None:
            merged.append(another)
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
    if event.exact:
        return _held_before(event, record)
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
    prices_table: str = "Stock_Prices",
) -> dict[str, list[SplitEvent]]:
    """Every known split for *tickers* (stored ticker spelling), oldest first."""
    if not tickers:
        return {}
    placeholders = ",".join("?" for _ in tickers)
    recorded: dict[str, list[SplitEvent]] = {}
    recorded_methods: dict[str, list[tuple[SplitEvent, object]]] = {}
    split_columns = _table_columns(conn, "Stock_Splits")
    if {"ticker", "split_date", "ratio_from", "ratio_to"} <= split_columns:
        clauses = []
        if "confirmation" in split_columns:
            clauses.append("confirmation = 'confirmed'")
        if "superseded_by" in split_columns:
            clauses.append("COALESCE(superseded_by, 0) = 0")
        where = " AND ".join([f"ticker IN ({placeholders})", *clauses])
        method_sql = "detection_method" if "detection_method" in split_columns else "NULL"
        records: dict[str, list[tuple[str, float, object]]] = {}
        for ticker, split_date, ratio_from, ratio_to, method in conn.execute(
            f"SELECT ticker, split_date, ratio_from, ratio_to, {method_sql} FROM Stock_Splits WHERE {where} ORDER BY split_date",
            tickers,
        ):
            try:
                multiplier = float(ratio_to) / float(ratio_from)
                pd.Timestamp(str(split_date)[:10])
            except (TypeError, ValueError, ZeroDivisionError):
                continue
            if multiplier <= 0 or multiplier == 1 or is_squeeze_out(ratio_from, ratio_to):
                continue
            ticker_records = records.setdefault(str(ticker), [])
            # Two splits on one day are one record too many.
            if not any(str(other[0])[:10] == str(split_date)[:10] for other in ticker_records):
                ticker_records.append((str(split_date)[:10], multiplier, method))
        for ticker, ticker_records in records.items():
            # One split recorded twice (a provider's duplicate, or the price
            # heuristic's beside the provider's) is one split.
            recorded_methods[ticker] = [
                (SplitEvent(pd.Timestamp(day) - pd.Timedelta(days=1), pd.Timestamp(day), multiplier, "Stock_Splits"), method)
                for day, multiplier, method in distinct_split_records(ticker_records)
            ]
            recorded[ticker] = [event for event, _method in recorded_methods[ticker]]

    history: dict[str, list[AnnualFacts]] = {}
    share_columns = _table_columns(conn, per_share_table)
    count_columns = [name for name in ("Number of issued shares as of fiscal year end", "Total number of issued shares") if name in share_columns]

    def column_sql(name: str) -> str:
        return f'p."{name}"' if name in share_columns else "NULL"

    fs_columns = _table_columns(conn, financial_statements_table)
    if count_columns and "docID" in share_columns and {"docID", "periodEnd"} <= fs_columns:
        count_sql = "COALESCE(" + ", ".join(f'p."{name}"' for name in count_columns) + ")" if len(count_columns) > 1 else f'p."{count_columns[0]}"'
        annual = "AND fs.docTypeCode = '030000'" if "docTypeCode" in fs_columns else ""
        filed_sql = "fs.submitDateTime" if "submitDateTime" in fs_columns else "NULL"
        balance_columns = _table_columns(conn, "BalanceSheet")
        net_assets_sql = (
            '(SELECT b."Net assets" FROM "BalanceSheet" b WHERE b.docID = fs.docID)'
            if {"docID", "Net assets"} <= balance_columns else "NULL"
        )
        price_columns = _table_columns(conn, prices_table)
        stored_price_sql = "NULL"
        if {"Ticker", "Date", "Price"} <= price_columns:
            price_expr = 'COALESCE(sp."Adjusted_Price", sp."Price")' if "Adjusted_Price" in price_columns else 'sp."Price"'
            stored_price_sql = (
                f'(SELECT {price_expr} FROM "{prices_table}" sp WHERE sp.Ticker = c.Company_Ticker '
                "AND sp.Date <= fs.periodEnd AND sp.Date >= date(fs.periodEnd, '-10 days') ORDER BY sp.Date DESC LIMIT 1)"
            )
        key_sql = _company_key_sql("c", company_code_column)
        query = (
            f'SELECT {key_sql}, fs.periodEnd, {count_sql}, {column_sql("Net assets per share")}, '
            f'{column_sql("Dividend paid per share")}, {column_sql("Number of issued shares as of fiscal year end")}, '
            f'{column_sql("Number of issued shares as of filing date")}, {filed_sql}, '
            f'{column_sql("Price-earnings ratio")}, {column_sql("Basic earnings (loss) per share")}, {stored_price_sql}, {net_assets_sql} FROM "{per_share_table}" p '
            f'JOIN "{financial_statements_table}" fs ON fs.docID = p.docID '
            f'JOIN "{company_table}" c ON c."{company_code_column}" = fs."{fs_code_column}" '
            f"WHERE {key_sql} IN ({placeholders}) {annual} ORDER BY fs.periodEnd"
        )
        try:
            for ticker, period_end, shares, bps, dps, year_end, filing, filed, per, eps, stored, net_assets in conn.execute(query, tickers):
                try:
                    traded = _traded_price(per, eps)
                    history.setdefault(str(ticker), []).append(AnnualFacts(
                        pd.Timestamp(str(period_end)[:10]),
                        float(shares) if shares else None,
                        float(bps) if bps else None,
                        float(dps) if dps else None,
                        float(year_end) if year_end else None,
                        float(filing) if filing else None,
                        pd.Timestamp(str(filed)[:10]) if filed else None,
                        float(stored) / traded if traded and stored and float(stored) > 0 else None,
                        float(net_assets) if net_assets else None,
                    ))
                except (TypeError, ValueError):
                    continue
        except sqlite3.Error:
            logger.debug("Could not read share counts for split inference", exc_info=True)

    price_columns = _table_columns(conn, prices_table)
    price_sql = 'COALESCE("Adjusted_Price", "Price")' if "Adjusted_Price" in price_columns else '"Price"'

    for ticker, methods in recorded_methods.items():
        recorded[ticker] = _without_heuristic_twins(methods, history.get(ticker, []))

    events: dict[str, list[SplitEvent]] = {}
    for ticker in {*recorded, *history}:
        facts = history.get(ticker, [])
        inferred, filing = infer_share_count_splits(facts), infer_filing_date_splits(facts)
        every = [*recorded.get(ticker, []), *inferred, *filing]

        def split(event: SplitEvent, ticker: str = ticker, facts: list[AnnualFacts] = facts, every: list[SplitEvent] = every) -> bool:
            shift = _bracketed_price_shift(event, facts, every)
            if _near(shift, 1.0 / event.multiplier, event.multiplier):
                return True
            small = abs(math.log(event.multiplier)) < math.log(1.5)
            providers = _window_providers(conn, prices_table, ticker, event) if small else set()
            if _near(shift, 1.0, event.multiplier):
                # Unless the stored closes still jump by it: a split the
                # provider never adjusted for keeps the price as traded and
                # stored together. A year of closes holds days that move like
                # a small split, so a small one counts only where no provider
                # adjusted.
                return not (small and providers and "" not in providers) and _steps_in_prices(conn, prices_table, price_sql, ticker, event)
            # No price to tell by. Yahoo records each split it adjusts its
            # closes for, and a recorded split wins over the counts: a small
            # ratio left unrecorded where Yahoo priced every day is an issue
            # of shares that landed near it (5.0 % or 25.0 % more shares).
            return not (small and providers and all(name.startswith("Yahoo") for name in providers))

        denied = [event for event in [*inferred, *filing] if not split(event)]

        def kept(event: SplitEvent, denied: list[SplitEvent] = denied) -> bool:
            # The same change seen in the other count goes with it: a year
            # of closes holds some day that moves like a small split.
            return event not in denied and not any(
                other.source != event.source and abs(math.log(other.multiplier / event.multiplier)) < 0.01
                and other.after < event.until and event.after < other.until
                for other in denied
            )

        merged = merge_split_events(recorded.get(ticker, []), [e for e in inferred if kept(e)], [e for e in filing if kept(e)], facts)
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
    return {
        (ticker, event.until)
        for ticker, events in events_by_ticker.items()
        for event in events
        if _steps_in_prices(conn, prices_table, price_sql, ticker, event)
    }


def _window_providers(conn: sqlite3.Connection, prices_table: str, ticker: str, event: SplitEvent) -> set[str]:
    """The providers that adjusted the stored closes around *event* (empty text for rows none did)."""
    if not {"Provider", "Price_Basis"} <= _table_columns(conn, prices_table):
        return {""}
    start = (event.after - pd.Timedelta(days=7)).strftime("%Y-%m-%d")
    end = (event.until + pd.Timedelta(days=7)).strftime("%Y-%m-%d")
    return {
        str(name)
        for (name,) in conn.execute(
            "SELECT DISTINCT CASE WHEN LOWER(COALESCE(Price_Basis, '')) = 'adjusted' THEN TRIM(COALESCE(Provider, '')) ELSE '' END "
            f'FROM "{prices_table}" WHERE Ticker = ? AND Date > ? AND Date <= ?',
            (ticker, start, end),
        )
    }


def _steps_in_prices(conn: sqlite3.Connection, prices_table: str, price_sql: str, ticker: str, event: SplitEvent) -> bool:
    """Do the stored closes still step by *event*'s ratio somewhere in its window?

    A move is the split's when the price level stays moved after it: a
    year-long window between two reports holds ordinary days that move as
    much as a small split.
    """
    start = event.after - pd.Timedelta(days=7) if event.exact else event.after
    end = event.until + pd.Timedelta(days=7)
    margin = pd.Timedelta(days=30)
    rows = conn.execute(
        f'SELECT Date, {price_sql} FROM "{prices_table}" WHERE Ticker = ? AND Date > ? AND Date <= ? ORDER BY Date',
        (ticker, (start - margin).strftime("%Y-%m-%d"), (end + margin).strftime("%Y-%m-%d")),
    ).fetchall()
    rows = [(pd.Timestamp(str(day)[:10]), float(price)) for day, price in rows if price and float(price) > 0]
    inside = [index for index, (day, _price) in enumerate(rows) if start < day <= end]
    return bool(inside) and split_jump_in_closes([price for _day, price in rows], 1.0 / event.multiplier, inside[0] - 1, inside[-1])


def _book_value_bases(
    conn: sqlite3.Connection,
    tickers: list[str],
    *,
    per_share_table: str,
    financial_statements_table: str,
    company_table: str,
    balance_sheet_table: str = "BalanceSheet",
) -> dict[tuple[str, pd.Timestamp], float]:
    """Book value per share × year-end shares ÷ net assets in each annual report.

    About 1 when book value per share is on the year-end shares; about 1/m
    when the report restated it for an m-for-1 split taking effect after the
    year end, before filing.
    """
    share_columns = _table_columns(conn, per_share_table)
    balance_columns = _table_columns(conn, balance_sheet_table)
    fs_columns = _table_columns(conn, financial_statements_table)
    counts = [name for name in ("Number of issued shares as of fiscal year end", "Total number of issued shares") if name in share_columns]
    if not tickers or not counts or "Net assets per share" not in share_columns or not {"docID", "Net assets"} <= balance_columns:
        return {}
    count_sql = "COALESCE(" + ", ".join(f'p."{name}"' for name in counts) + ")" if len(counts) > 1 else f'p."{counts[0]}"'
    annual = "AND fs.docTypeCode = '030000'" if "docTypeCode" in fs_columns else ""
    placeholders = ",".join("?" for _ in tickers)
    bases: dict[tuple[str, pd.Timestamp], float] = {}
    for ticker, period_end, bps, shares, net_assets in conn.execute(
        f'SELECT {_company_key_sql()}, fs.periodEnd, p."Net assets per share", {count_sql}, b."Net assets" '
        f'FROM "{per_share_table}" p JOIN "{financial_statements_table}" fs ON fs.docID = p.docID '
        f'JOIN "{balance_sheet_table}" b ON b.docID = fs.docID '
        f'JOIN "{company_table}" c ON c.Company_Code = fs.Company_Code WHERE {_company_key_sql()} IN ({placeholders}) {annual}',
        tickers,
    ):
        if _positive(bps) and _positive(shares) and _positive(net_assets):
            bases[(str(ticker), pd.Timestamp(str(period_end)[:10]))] = float(bps) * float(shares) / float(net_assets)
    return bases


def _year_end_price_factors(
    conn: sqlite3.Connection,
    tickers: list[str],
    *,
    per_share_table: str,
    financial_statements_table: str,
    company_table: str,
    prices_table: str,
) -> dict[tuple[str, pd.Timestamp], float]:
    """Stored price over the price as traded (P/E × EPS) at each annual report's year end."""
    share_columns = _table_columns(conn, per_share_table)
    price_columns = _table_columns(conn, prices_table)
    fs_columns = _table_columns(conn, financial_statements_table)
    if not tickers or not {"Price-earnings ratio", "Basic earnings (loss) per share", "docID"} <= share_columns or not {"Ticker", "Date", "Price"} <= price_columns:
        return {}
    price_expr = 'COALESCE(sp."Adjusted_Price", sp."Price")' if "Adjusted_Price" in price_columns else 'sp."Price"'
    annual = "AND fs.docTypeCode = '030000'" if "docTypeCode" in fs_columns else ""
    placeholders = ",".join("?" for _ in tickers)
    factors: dict[tuple[str, pd.Timestamp], float] = {}
    for ticker, period_end, per, eps, stored in conn.execute(
        f'SELECT {_company_key_sql()}, fs.periodEnd, p."Price-earnings ratio", p."Basic earnings (loss) per share", '
        f'(SELECT {price_expr} FROM "{prices_table}" sp WHERE sp.Ticker = c.Company_Ticker AND sp.Date <= fs.periodEnd '
        "AND sp.Date >= date(fs.periodEnd, '-10 days') ORDER BY sp.Date DESC LIMIT 1) "
        f'FROM "{per_share_table}" p JOIN "{financial_statements_table}" fs ON fs.docID = p.docID '
        f'JOIN "{company_table}" c ON c.Company_Code = fs.Company_Code WHERE {_company_key_sql()} IN ({placeholders}) {annual}',
        tickers,
    ):
        traded = _traded_price(per, eps)
        if traded and _positive(stored):
            factors[(str(ticker), pd.Timestamp(str(period_end)[:10]))] = float(stored) / traded
    return factors


def _restated_by(event: SplitEvent, period_end: pd.Timestamp, submitted: pd.Timestamp | None, restated_early: bool, kept_old_shares: bool = False) -> bool:
    """Are a filing's per-share figures (EPS, BPS) already on the post-split basis?

    Issuers restate per-share data for a split that takes effect before the
    report is filed, even after the year end (a split effective 1 April is
    already in the June report of the year to March); *kept_old_shares* says
    the report at the window's start did not.
    """
    if event.exact:
        return submitted is not None and submitted > event.until + _RECORD_DATE_LAG
    if kept_old_shares and period_end <= event.after:
        return False
    if period_end >= event.until or (submitted is not None and submitted >= event.until):
        return True
    return restated_early and period_end >= event.after


class FilingBasis(NamedTuple):
    """Factors that put one filing's per-share figures on the stored price basis.

    See ``share_basis`` for which figure takes which factor.
    """

    doc_id: str
    restated: float
    fiscal: float
    dividend: float
    interim: float


def _interim_paid_before(event: SplitEvent, record: pd.Timestamp, payment: Callable[[], tuple[pd.Series, pd.DataFrame] | None]) -> bool:
    """Was an interim dividend with this record date paid on the shares before *event*?"""
    if event.exact or record <= event.after:
        return _held_before(event, record)
    if record >= event.until:
        return False
    # Inside a window between two year ends: judge by the amount.
    found = payment()
    return found is not None and _paid_before(found[0], event, found[1])


def filing_basis_factors(
    conn: sqlite3.Connection,
    *,
    tickers: list[str] | None = None,
    events: dict[str, list[SplitEvent]] | None = None,
    per_share_table: str = "ShareMetrics",
    financial_statements_table: str = "FinancialStatements",
    company_table: str = "CompanyInfo",
    prices_table: str = "Stock_Prices",
) -> list[FilingBasis]:
    """Per filing, the factors that put its per-share figures on the stored price basis.

    Returns a ``FilingBasis`` for every filing (of *tickers*, default all)
    where any factor is not 1: ``restated`` multiplies figures the issuer
    restates for splits before filing (EPS, book value per share); ``fiscal``
    multiplies figures fixed at the fiscal year end (per-share ratios computed
    from the year-end share count); ``interim`` the interim dividend, paid at
    the half year; ``dividend`` the year's dividend, whose interim may have
    been paid before a split in the year and the final after it. Share counts
    are divided by their factor. *events* skips loading the split events.
    """
    fs_columns = _table_columns(conn, financial_statements_table)
    company_columns = _table_columns(conn, company_table)
    if not {"docID", "periodEnd", "Company_Code"} <= fs_columns or not {"Company_Code", "Company_Ticker"} <= company_columns:
        return []
    if tickers is None:
        tickers = [str(row[0]) for row in conn.execute(
            f'SELECT DISTINCT {_company_key_sql("c")} FROM "{company_table}" c WHERE {_company_key_sql("c")} IS NOT NULL'
        )]
    if events is None:
        events = load_split_events(conn, tickers, per_share_table=per_share_table, financial_statements_table=financial_statements_table, company_table=company_table)
    wanted = set(tickers)
    events = {ticker: found for ticker, found in events.items() if found and ticker in wanted}
    if not events:
        return []
    share_columns = _table_columns(conn, per_share_table)

    def column_sql(name: str) -> str:
        return f'p."{name}"' if name in share_columns and "docID" in share_columns else "NULL"

    submitted_sql = "fs.submitDateTime" if "submitDateTime" in fs_columns else "NULL"
    annual_sql = "fs.docTypeCode = '030000'" if "docTypeCode" in fs_columns else "1"
    placeholders = ",".join("?" for _ in events)
    rows = conn.execute(
        f'SELECT {_company_key_sql()}, fs.docID, fs.periodEnd, {submitted_sql}, {annual_sql}, '
        f'{column_sql("Net assets per share")}, {column_sql("Dividend paid per share")}, {column_sql("Interim dividend paid per share")} '
        f'FROM "{financial_statements_table}" fs JOIN "{company_table}" c ON c.Company_Code = fs.Company_Code '
        + (f'LEFT JOIN "{per_share_table}" p ON p.docID = fs.docID ' if "docID" in share_columns else "")
        + f"WHERE {_company_key_sql()} IN ({placeholders}) ORDER BY 1, fs.periodEnd",
        list(events),
    ).fetchall()
    by_ticker: dict[str, list[tuple[str, pd.Timestamp, pd.Timestamp | None, bool, float | None, float | None, float | None]]] = {}
    for ticker, doc_id, period_end, submitted, annual, bps, dps, interim in rows:
        try:
            end = pd.Timestamp(str(period_end)[:10])
        except (TypeError, ValueError):
            continue
        try:
            filed = pd.Timestamp(str(submitted)[:10]) if submitted else None
        except (TypeError, ValueError):
            filed = None
        by_ticker.setdefault(str(ticker), []).append((
            str(doc_id), end, filed, bool(annual), _positive(bps), _positive(dps), _positive(interim),
        ))

    price_factors = _year_end_price_factors(
        conn, list(by_ticker), per_share_table=per_share_table,
        financial_statements_table=financial_statements_table, company_table=company_table, prices_table=prices_table,
    )
    book_bases = _book_value_bases(
        conn, list(by_ticker), per_share_table=per_share_table,
        financial_statements_table=financial_statements_table, company_table=company_table,
    )
    price_columns = _table_columns(conn, prices_table)
    price_sql = 'COALESCE("Adjusted_Price", "Price")' if "Adjusted_Price" in price_columns else '"Price"'

    def booked_after_year_end(ticker: str, event: SplitEvent) -> bool | None:
        """Is the first report's book value per share on the new shares?

        Book value per share times the year-end share count is the net
        assets on the same shares, and 1/m of them when the report restated
        book value for an m-for-1 split taking effect after the year end.
        ``None`` when the reports do not tell: the next report's book value
        must match its net assets, or the two cover different things (an
        IFRS filer's consolidated book value beside its parent-only net
        assets, large minority interests).
        """
        ratio = book_bases.get((ticker, event.after))
        following = book_bases.get((ticker, event.until))
        if not ratio or not following or abs(math.log(following)) > 0.25:
            return None
        same, restated = abs(math.log(ratio)), abs(math.log(ratio * event.multiplier))
        if restated < 0.25 and same > 0.4:
            return True
        if same < 0.25 and restated > 0.4:
            return False
        return None

    def priced_by_year_end(ticker: str, event: SplitEvent) -> bool | None:
        """Was the first report's EPS already on the new shares, by the prices?

        A report's P/E times its EPS is the year-end price on the shares its
        EPS is on. The stored price over it does not step across the window
        when the first report already restated the split (its year-end count
        still shows the old shares, the split took effect after), and steps
        by the ratio when it did not. ``None`` without prices for both
        reports, or when the stored closes are on the raw basis across it.
        """
        start, end = price_factors.get((ticker, event.after)), price_factors.get((ticker, event.until))
        if not start or not end:
            return None
        step, ratio = math.log(start / end), math.log(event.multiplier)
        if abs(step) < abs(ratio) / 3:
            restated = True
        elif abs(step + ratio) < abs(ratio) / 3:
            restated = False
        else:
            return None
        return None if _steps_in_prices(conn, prices_table, price_sql, ticker, event) else restated

    def kept_old_shares(ticker: str, event: SplitEvent, ends: list[pd.Timestamp]) -> bool:
        """Did the report a split took effect after kept its book value on the old shares?

        A report gives its filing-date count once a split has taken effect,
        yet some issuers leave EPS and book value per share on the year-end
        shares: book value per share times those shares is still the net
        assets (the next report must match its own, or the two figures cover
        different things), and the price as traded (P/E × EPS) steps by the
        ratio against the stored price to the next report. Book value alone
        cannot tell such a report from an issue of shares mistaken for a
        split, which leaves the prices together.
        """
        following_end = next((end for end in ends if end > event.after), None)
        ratio = book_bases.get((ticker, event.after))
        following = book_bases.get((ticker, following_end)) if following_end else None
        if not ratio or not following or abs(math.log(following)) > 0.25:
            return False
        if abs(math.log(ratio)) >= 0.25 or abs(math.log(ratio * event.multiplier)) <= 0.4:
            return False
        start, end = price_factors.get((ticker, event.after)), price_factors.get((ticker, following_end))
        return bool(start and end) and _near(start / end, 1.0 / event.multiplier, event.multiplier)

    def restated_before_effect(ticker: str, event: SplitEvent, ends: list[pd.Timestamp], all_ends: list[pd.Timestamp]) -> bool:
        """Did the last report filed before a split already restate for it?

        Its price as traded (P/E × EPS) then steps by the ratio against the
        stored price from the report before, or its book value per share is
        on 1/m of the year-end shares while the next report's matches its
        net assets.
        """
        end, previous = ends[-1], (ends[-2] if len(ends) > 1 else None)
        start, now = (price_factors.get((ticker, previous)) if previous else None), price_factors.get((ticker, end))
        if start and now and _near(start / now, 1.0 / event.multiplier, event.multiplier):
            return True
        following_end = next((other for other in all_ends if other > end), None)
        ratio = book_bases.get((ticker, end))
        following = book_bases.get((ticker, following_end)) if following_end else None
        if not ratio or not following or abs(math.log(following)) > 0.25:
            return False
        return abs(math.log(ratio * event.multiplier)) < 0.25 and abs(math.log(ratio)) > 0.4

    out: list[FilingBasis] = []
    for ticker, filings in by_ticker.items():
        ticker_events = events.get(ticker, [])
        annual_bps = {end: bps for _doc, end, _filed, annual, bps, _dps, _interim in filings if annual and bps}
        annual_filed = {end: filed for _doc, end, filed, annual, _bps, _dps, _interim in filings if annual and filed}
        restated_early: dict[SplitEvent, bool] = {}
        for event in ticker_events:
            if event.exact:
                continue
            # The year-end report at the window's start already restated:
            # its book value per share fell by the ratio from the year before,
            # and no other split effective by that report's filing explains it.
            before = [end for end in annual_bps if end < event.after]
            if not before or event.after not in annual_bps:
                restated_early[event] = False
                continue
            filed = annual_filed.get(event.after, event.after)
            explained = any(
                other is not event and _held_before(other, before[-1]) and not _held_before(other, filed)
                for other in ticker_events
            )
            restated_early[event] = not explained and bool(
                _moved_like_split(annual_bps[before[-1]], annual_bps[event.after], event.multiplier)
            )
        annual_ends = sorted(annual_bps)
        kept_old = {event for event in ticker_events if event.source == "filing date count" and kept_old_shares(ticker, event, annual_ends)}
        # A recorded split's last report before it: one filed before the split
        # that already restated for it (a board's decision in May, effective
        # in September), or one filed after it that kept the old shares.
        exact_verdict: dict[SplitEvent, tuple[pd.Timestamp, bool]] = {}
        for event in ticker_events:
            if not event.exact:
                continue
            # The last year end still on the old shares (a 31 March record date leaves March's on them).
            ends = [end for end in annual_ends if _held_before(event, end)]
            if not ends:
                continue
            filed = annual_filed.get(ends[-1])
            following = next((end for end in annual_ends if end > ends[-1]), None)
            # Another split between the reports compared, or around the
            # earlier one's year end, moves the same figures: then they cannot
            # tell which split a report is on.
            span_start = (ends[-2] if len(ends) > 1 else ends[-1]) - pd.DateOffset(years=1)
            span_end = following or event.until
            if any(other is not event and span_start < other.until <= span_end + _RECORD_DATE_LAG for other in ticker_events):
                continue
            if filed is not None and filed > event.until + _RECORD_DATE_LAG:
                if kept_old_shares(ticker, SplitEvent(ends[-1], event.until, event.multiplier, event.source), annual_ends):
                    exact_verdict[event] = (ends[-1], False)
            elif restated_before_effect(ticker, event, ends, annual_ends):
                exact_verdict[event] = (ends[-1], True)
        for event in ticker_events:
            if event.exact or event.source != "annual reports":
                continue
            # The prices at the two year ends tell first, then the report's
            # own book value against its net assets, then book values the
            # year before.
            for evidence in (priced_by_year_end, booked_after_year_end):
                verdict = evidence(ticker, event)
                if verdict is not None:
                    restated_early[event] = verdict
                    break
        payments: list[pd.DataFrame] = []

        def interim_payment(end: pd.Timestamp, ticker=ticker, filings=filings, payments=payments) -> tuple[pd.Series, pd.DataFrame] | None:
            # Built once per ticker, and only for an interim inside a split's window.
            if not payments:
                annual_rows = pd.DataFrame(
                    [(ticker, end_, dps or 0.0, interim or 0.0) for _doc, end_, _filed, annual, _bps, dps, interim in filings if annual],
                    columns=["Ticker", "periodEnd", "PerShare_Dividends", "Interim_Dividends"],
                )
                payments.append(dividend_payments(annual_rows))
            frame = payments[0]
            match = frame[(frame["fiscal_period_end"] == end) & (frame["payment"] == "interim")]
            return (match.iloc[0], frame) if not match.empty else None

        for doc_id, end, filed, _annual, _bps, dps, interim in filings:
            restated = fiscal = interim_factor = 1.0
            interim_record = end - pd.DateOffset(months=6)
            for event in ticker_events:
                verdict = exact_verdict.get(event)
                if verdict and end == verdict[0]:
                    on_new_shares = verdict[1]
                else:
                    on_new_shares = _restated_by(event, end, filed, restated_early.get(event, False), event in kept_old)
                if not on_new_shares:
                    restated /= event.multiplier
                if _held_before(event, end):
                    fiscal /= event.multiplier
                if _interim_paid_before(event, interim_record, lambda end=end: interim_payment(end)):
                    interim_factor /= event.multiplier
            dividend = fiscal
            if dps and interim and interim <= dps and interim_factor != fiscal:
                dividend = (interim * interim_factor + (dps - interim) * fiscal) / dps
            if restated != 1.0 or fiscal != 1.0 or dividend != 1.0 or interim_factor != 1.0:
                out.append(FilingBasis(doc_id, restated, fiscal, dividend, interim_factor))
    return out


def _positive(value: object) -> float | None:
    try:
        number = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) and number > 0 else None
