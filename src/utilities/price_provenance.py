"""Schema and helpers for price-source provenance.

``Stock_Prices.Price`` is intentionally kept as the value received from the
source.  The columns maintained here make its interpretation explicit so that
callers never have to infer whether a provider silently adjusted a quote for a
split or dividend.  ``adjusted`` means the provider's quote is already on a
split-adjusted basis; it does not imply dividend/total-return adjustment.
"""

from __future__ import annotations

import math
import re
import sqlite3
from datetime import date, datetime, timedelta, timezone
from typing import NamedTuple

PRICE_PROVENANCE_COLUMNS: dict[str, str] = {
    # ``unknown`` is the safe value for legacy rows whose source convention is
    # not recoverable.  New pipeline rows must use raw or adjusted explicitly.
    "Price_Basis": "TEXT NOT NULL DEFAULT 'raw'",
    "Provider": "TEXT",
    "Source_Id": "TEXT",
    "Source_Revision": "TEXT",
    "Adjustment_Factor": "REAL",
    # Factor derived from confirmed Stock_Splits (1.0 for an already current
    # row, NULL when the row basis is unknown).  ``Adjusted_Price`` is a
    # materialized read-model for SQL consumers such as screening.
    "Split_Adjustment_Factor": "REAL",
    "Adjusted_Price": "REAL",
    "Retrieved_At": "TEXT",
}

_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _quote_identifier(identifier: str) -> str:
    if not _SAFE_IDENTIFIER.match(identifier):
        raise ValueError(f"Unsafe SQLite identifier: {identifier!r}")
    return f'"{identifier}"'


def table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    """Return columns in *table_name*, or an empty set when it is absent."""
    return {
        str(row[1])
        for row in conn.execute(
            f"PRAGMA table_info({_quote_identifier(table_name)})"
        )
    }


def ensure_price_provenance_columns(
    conn: sqlite3.Connection,
    table_name: str = "Stock_Prices",
    *,
    ticker: str | None = None,
) -> set[str]:
    """Create/migrate row-level provenance columns and return all columns.

    Existing rows are deliberately marked ``unknown`` rather than guessed to
    be raw.  A migration/reconciliation job can promote them after inspecting
    the source; this prevents a split adjustment being applied twice.

    With *ticker*, blank bases are normalised for that ticker's rows only.
    Per-ticker writers only read and refresh their own rows, and the
    unindexed table-wide check costs ~0.5 s on a 15M-row table, which made it
    the slowest step of a bulk price update when run for every ticker.
    """
    quoted = _quote_identifier(table_name)
    columns = table_columns(conn, table_name)
    if not columns:
        return columns
    existing_row_count = 0
    if "Price_Basis" not in columns:
        try:
            existing_row_count = int(
                conn.execute(f"SELECT COUNT(*) FROM {quoted}").fetchone()[0]
            )
        except sqlite3.Error:
            existing_row_count = 0
    for name, definition in PRICE_PROVENANCE_COLUMNS.items():
        if name not in columns:
            conn.execute(
                f"ALTER TABLE {quoted} ADD COLUMN {_quote_identifier(name)} {definition}"
            )
    if "Price_Basis" not in columns and existing_row_count:
        # Rows that predate provenance cannot safely be assumed raw.  Mark
        # those rows unknown while retaining a raw default for future rows in
        # newly-created legacy-compatible tables.
        conn.execute(f"UPDATE {quoted} SET \"Price_Basis\" = 'unknown'")
    elif "Price_Basis" in columns:
        # Null/blank values from hand-created imports are just as ambiguous as
        # migrated rows.  Do not let downstream COALESCE expressions silently
        # treat them as raw.
        blank = "(\"Price_Basis\" IS NULL OR TRIM(\"Price_Basis\") = '')"
        if ticker is None:
            conn.execute(f"UPDATE {quoted} SET \"Price_Basis\" = 'unknown' WHERE {blank}")
        else:
            conn.execute(
                f"UPDATE {quoted} SET \"Price_Basis\" = 'unknown' WHERE Ticker = ? AND {blank}",
                (ticker,),
            )
    return table_columns(conn, table_name)


# A consolidation of more than a hundred shares into one is a squeeze-out
# before a delisting (20,000,000 to 1, say): holders are paid out and no later
# price is on the new basis, so it never adjusts prices or per-share figures.
SQUEEZE_OUT_MULTIPLIER = 0.01


def is_squeeze_out(ratio_from: object, ratio_to: object) -> bool:
    """Whether a recorded ``ratio_from:ratio_to`` event is a squeeze-out, not a split."""
    try:
        return float(ratio_to) / float(ratio_from) < SQUEEZE_OUT_MULTIPLIER  # type: ignore[arg-type]
    except (TypeError, ValueError, ZeroDivisionError):
        return False


def utc_now() -> str:
    """Return an ISO-8601 UTC timestamp suitable for provenance columns."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


# A provider and the price heuristic can each record one split, days apart;
# a heuristic dated off a price move can miss the provider's date by weeks.
SPLIT_DUPLICATE_DAYS = 10
HEURISTIC_DUPLICATE_DAYS = 92


def distinct_split_records(records: list[tuple]) -> list[tuple]:
    """One record per split, from records ``(split_date, ratio, detection_method, ...)`` in date order.

    No issuer splits again at the same ratio within days, so records ten days
    apart are one split, dated at the earlier (a heuristic dates the split at
    the price's step, a provider sometimes at the effective date). Within a
    quarter, a provider's record and a heuristic's of the same ratio are one
    split too, at the provider's date.
    """
    kept: list[tuple] = []
    for record in records:
        for index, other in enumerate(kept):
            try:
                days = abs((date.fromisoformat(str(record[0])[:10]) - date.fromisoformat(str(other[0])[:10])).days)
                same_ratio = record[1] > 0 and other[1] > 0 and abs(math.log(record[1] / other[1])) < 0.01
            except (TypeError, ValueError):
                continue
            if not same_ratio:
                continue
            if days <= SPLIT_DUPLICATE_DAYS:
                break
            if days <= HEURISTIC_DUPLICATE_DAYS and {record[2], other[2]} == {"provider", "price_heuristic"}:
                if record[2] == "provider":
                    kept[index] = record
                break
        else:
            kept.append(record)
    return kept


def split_jump_in_closes(closes: list[float], price_factor: float, start: int = 0, stop: int | None = None) -> bool:
    """Do *closes* step by a split's price factor between positions *start* and *stop*?

    A day-to-day move within a quarter of the split's log ratio (at most
    0.12) of it, after which the median of the next ten closes stays shifted
    by the ratio, within a quarter of it, from the median of the ten before:
    a split moves the level for good, where an ordinary 10 % down day on a
    1.1-for-1 split does not, and a fast-moving stock that split 10-for-1
    still shows a tenfold shift.
    """
    return split_jump_index(closes, price_factor, start, stop) is not None


def split_jump_index(closes: list[float], price_factor: float, start: int = 0, stop: int | None = None) -> int | None:
    """The position of the first close after the step :func:`split_jump_in_closes` finds, or ``None``."""
    expected = math.log(price_factor) if price_factor > 0 else 0.0
    if not expected:
        return None
    tolerance = min(0.12, abs(expected) / 4)
    level_tolerance = abs(expected) / 4
    stop = len(closes) - 1 if stop is None else min(stop, len(closes) - 1)
    for index in range(max(start, 0), stop):
        before, after = closes[index], closes[index + 1]
        if before <= 0 or after <= 0 or abs(math.log(after / before) - expected) >= tolerance:
            continue
        earlier = sorted(closes[max(0, index - 9): index + 1])
        later = sorted(closes[index + 1: index + 11])
        if abs(math.log(later[len(later) // 2] / earlier[len(earlier) // 2]) - expected) < level_tolerance:
            return index + 1
    return None


class UnadjustedStep(NamedTuple):
    """Where adjusted closes still step by a split, and whether the provider served them so."""

    day: str  # the first close on the new shares
    served_unadjusted: bool  # both closes either side were fetched after the split


def unadjusted_split_step(conn: sqlite3.Connection, prices_table: str, ticker: str, split_date: str, price_factor: float) -> UnadjustedStep | None:
    """Adjusted closes that still step by a split within two weeks of it.

    Daily quotes fetched before a split stay on the old shares while the
    provider adjusts the history it serves afterwards, so the step can come
    days before the split's listed date. A provider can also list a split it
    has not adjusted its history for: the closes either side of the step
    were then both fetched after the split.
    """
    try:
        day = date.fromisoformat(str(split_date)[:10])
    except ValueError:
        return None
    columns = table_columns(conn, prices_table)
    if "Price_Basis" not in columns:
        return None
    retrieved_sql = "Retrieved_At" if "Retrieved_At" in columns else "NULL"
    rows = conn.execute(
        f"SELECT Date, Price, {retrieved_sql} FROM {_quote_identifier(prices_table)} WHERE Ticker = ? AND Date > ? AND Date <= ? AND Price > 0 "
        "AND LOWER(COALESCE(Price_Basis, '')) = 'adjusted' ORDER BY Date",
        (ticker, (day - timedelta(days=90)).isoformat(), (day + timedelta(days=30)).isoformat()),
    ).fetchall()
    days = [date.fromisoformat(str(row_day)[:10]) for row_day, _price, _retrieved in rows]
    # A provider's split date can trail the step by days (a 10-for-1 split
    # listed for 30 January whose closes fell on the 21st).
    near = [index for index, row_day in enumerate(days) if abs((row_day - day).days) <= 14]
    if not near:
        return None
    index = split_jump_index([float(price) for _day, price, _retrieved in rows], price_factor, near[0] - 1, near[-1])
    if index is None:
        return None
    served = all(rows[at][2] and str(rows[at][2])[:10] >= day.isoformat() for at in (index - 1, index))
    return UnadjustedStep(days[index].isoformat(), served)


def split_shows_in_series(conn: sqlite3.Connection, prices_table: str, ticker: str, split_date: str, price_factor: float) -> bool:
    """Do the stored closes still step by a split's ratio?

    The step is within a week of the split, or where rows of unknown basis
    hand over to a provider's adjusted rows in the two months before it:
    a provider that took over on 15 July already adjusted its closes for a
    split on 30 July, while the older rows stop at the price as traded.
    """
    try:
        day = date.fromisoformat(str(split_date)[:10])
    except ValueError:
        return False
    has_basis = "Price_Basis" in table_columns(conn, prices_table)
    basis_sql = "LOWER(COALESCE(Price_Basis, 'raw'))" if has_basis else "'raw'"
    rows = conn.execute(
        f"SELECT Date, Price, {basis_sql} FROM {_quote_identifier(prices_table)} WHERE Ticker = ? AND Date > ? AND Date <= ? AND Price > 0 ORDER BY Date",
        (ticker, (day - timedelta(days=90)).isoformat(), (day + timedelta(days=30)).isoformat()),
    ).fetchall()
    closes = [float(price) for _day, price, _basis in rows]
    days = [date.fromisoformat(str(row_day)[:10]) for row_day, _price, _basis in rows]
    near = [index for index, row_day in enumerate(days) if abs((row_day - day).days) <= 7]
    if near and split_jump_in_closes(closes, price_factor, near[0] - 1, near[-1]):
        return True
    for index in range(1, len(rows)):
        handover = rows[index - 1][2] == "unknown" and rows[index][2] != "unknown"
        if handover and timedelta(0) <= day - days[index] <= timedelta(days=60) and split_jump_in_closes(closes, price_factor, index - 1, index):
            return True
    return False


def refresh_split_adjusted_prices(
    conn: sqlite3.Connection,
    ticker: str | None = None,
    prices_table: str = "Stock_Prices",
    only_missing: bool = False,
) -> int:
    """Refresh the SQL-friendly split-adjusted read model.

    The source ``Price`` column is never rewritten.  Rows marked raw receive a
    derived factor. Adjusted rows take a confirmed split only when the
    adjusted closes themselves still jump by its ratio within a week of it:
    daily quotes fetched before a split stay on the old shares, and a
    provider can list a split it has not yet adjusted its history for.
    Unknown rows take a confirmed split only
    when the stored closes still jump by its ratio at its date (an import
    made on the raw basis); otherwise they remain NULL so consumers can
    distinguish “not safe to adjust” from a real zero.

    ``only_missing`` fills rows that have never been derived and leaves the
    rest alone. Every writer of prices or splits refreshes what it changed, so
    readers (such as a screening run) only need this cheap catch-up instead of
    rewriting every price row.
    """
    columns = ensure_price_provenance_columns(conn, prices_table, ticker=ticker)
    if "Split_Adjustment_Factor" not in columns or "Adjusted_Price" not in columns:
        return 0
    split_columns = table_columns(conn, "Stock_Splits")
    if not split_columns:
        # A price-only database is still a valid migration target.  Clear any
        # stale derived values and leave raw rows at factor 1.0 until a split
        # table is created and reviewed.
        split_rows = []
    else:
        basis_clause = "AND COALESCE(price_basis, 'raw') = 'raw'" \
            if "price_basis" in split_columns else ""
        superseded_clause = "AND COALESCE(superseded_by, 0) = 0" \
            if "superseded_by" in split_columns else ""
        method_select = "detection_method" if "detection_method" in split_columns else "NULL"
        id_select = "id" if "id" in split_columns else "NULL"
        method_order = (
            "CASE WHEN detection_method = 'provider' THEN 0 "
            "WHEN detection_method = 'manual' THEN 1 ELSE 2 END"
            if "detection_method" in split_columns else "0"
        )
        id_order = "id DESC" if "id" in split_columns else "rowid DESC"
        ticker_clause = "AND ticker = ?" if ticker is not None else ""
        try:
            split_rows = conn.execute(
                "SELECT ticker, split_date, ratio_from, ratio_to, "
                f"{method_select}, {id_select} FROM Stock_Splits "
                "WHERE confirmation = 'confirmed' "
                f"{basis_clause} {superseded_clause} {ticker_clause} "
                f"ORDER BY ticker, split_date, {method_order}, {id_order}",
                (ticker,) if ticker is not None else (),
            ).fetchall()
        except sqlite3.Error:
            split_rows = []
    by_ticker: dict[str, list[tuple[str, float]]] = {}
    records: dict[str, list[tuple[str, float, object]]] = {}
    seen_events: set[tuple[str, str]] = set()
    for split_ticker, split_date, ratio_from, ratio_to, _method, _event_id in split_rows:
        event_key = (str(split_ticker), str(split_date)[:10])
        if event_key in seen_events:
            continue
        seen_events.add(event_key)
        if is_squeeze_out(ratio_from, ratio_to):
            continue
        try:
            ratio = float(ratio_from) / float(ratio_to)
        except (TypeError, ValueError, ZeroDivisionError):
            continue
        records.setdefault(str(split_ticker), []).append((str(split_date), ratio, _method))
    for split_ticker, ticker_records in records.items():
        by_ticker[split_ticker] = [(split_date, ratio) for split_date, ratio, _method in distinct_split_records(ticker_records)]

    conditions: list[str] = []
    params: tuple[str, ...] = ()
    if ticker is not None:
        conditions.append("Ticker = ?")
        params = (ticker,)
    if only_missing:
        conditions.append(
            "Adjusted_Price IS NULL AND Price IS NOT NULL "
            "AND LOWER(COALESCE(Price_Basis, 'raw')) IN ('raw', 'adjusted')"
        )
    where = f" WHERE {' AND '.join(conditions)}" if conditions else ""
    retrieved_sql = "Retrieved_At" if "Retrieved_At" in columns else "NULL"
    rows = conn.execute(
        "SELECT rowid, Ticker, Date, Price, Price_Basis, Split_Adjustment_Factor, "
        f"Adjusted_Price, {retrieved_sql} FROM {_quote_identifier(prices_table)}{where}",
        params,
    ).fetchall()
    shown: dict[str, list[tuple[str, float]]] = {}

    def splits_shown(row_ticker: str) -> list[tuple[str, float]]:
        if row_ticker not in shown:
            shown[row_ticker] = [
                (split_date, split_factor)
                for split_date, split_factor in by_ticker.get(row_ticker, [])
                if split_shows_in_series(conn, prices_table, row_ticker, split_date, split_factor)
            ]
        return shown[row_ticker]

    unadjusted: dict[str, list[tuple[UnadjustedStep, str, float]]] = {}

    def splits_unadjusted(row_ticker: str) -> list[tuple[UnadjustedStep, str, float]]:
        if row_ticker not in unadjusted:
            steps = (
                (unadjusted_split_step(conn, prices_table, row_ticker, split_date, split_factor), split_date[:10], split_factor)
                for split_date, split_factor in by_ticker.get(row_ticker, [])
            )
            unadjusted[row_ticker] = [(step, split_day, split_factor) for step, split_day, split_factor in steps if step]
        return unadjusted[row_ticker]

    changes: list[tuple[float | None, float | None, int]] = []
    for rowid, row_ticker, date_value, price, basis, old_factor, old_adjusted, retrieved in rows:
        basis_text = str(basis or "raw").strip().lower()
        factor: float | None
        adjusted: float | None
        if basis_text == "adjusted":
            # Before the step, if fetched before the split (or the provider
            # served its history unadjusted): a provider's history fetched
            # after a split it adjusted for is already on the new shares.
            fetched = str(retrieved)[:10] if retrieved else None
            factor = math.prod(
                split_factor for step, split_day, split_factor in splits_unadjusted(str(row_ticker))
                if step.day > str(date_value)[:10] and (step.served_unadjusted or (fetched is not None and fetched < split_day))
            ) if str(row_ticker) in by_ticker else 1.0
            adjusted = price * factor if price is not None else None
        elif basis_text != "raw":
            factor, adjusted = None, None
            if str(row_ticker) in by_ticker:
                unknown_factor = math.prod(
                    split_factor for split_date, split_factor in splits_shown(str(row_ticker))
                    if split_date > str(date_value)[:10]
                )
                if unknown_factor != 1.0 and price is not None:
                    factor, adjusted = unknown_factor, price * unknown_factor
        else:
            factor = 1.0
            for split_date, split_factor in by_ticker.get(str(row_ticker), []):
                if split_date > str(date_value)[:10]:
                    factor *= split_factor
            adjusted = price * factor if price is not None else None
        # Write only rows whose derived values change, so refreshing one ticker
        # after appending a day of prices does not rewrite its whole history.
        if (factor, adjusted) != (old_factor, old_adjusted):
            changes.append((factor, adjusted, rowid))
    if changes:
        conn.executemany(
            f"UPDATE {_quote_identifier(prices_table)} SET Split_Adjustment_Factor = ?, "
            "Adjusted_Price = ? WHERE rowid = ?",
            changes,
        )
    return len(changes)


def source_id(provider: str, provider_symbol: str, date: str) -> str:
    """Build a stable source identifier for one provider/date quote."""
    return f"{provider}:{provider_symbol}:{str(date)[:10]}"


def row_basis(row: object, columns: set[str] | None = None) -> str:
    """Read a row's basis while remaining compatible with legacy test tables."""
    if isinstance(row, sqlite3.Row):
        try:
            value = row["Price_Basis"]
        except (IndexError, KeyError):
            value = None
    elif isinstance(row, dict):
        value = row.get("Price_Basis")
    else:
        value = None
    value = str(value or "").strip().lower()
    # Tables created before the migration have no basis column.  Their
    # historical behaviour was raw, so retain that compatibility explicitly;
    # rows in migrated tables use ``unknown`` and are not adjusted implicitly.
    if not value and columns is not None and "Price_Basis" not in columns:
        return "raw"
    return value or "unknown"


__all__ = [
    "PRICE_PROVENANCE_COLUMNS",
    "ensure_price_provenance_columns",
    "row_basis",
    "refresh_split_adjusted_prices",
    "source_id",
    "table_columns",
    "utc_now",
]
