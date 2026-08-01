"""Schema and helpers for price-source provenance.

``Stock_Prices.Price`` is intentionally kept as the value received from the
source.  The columns maintained here make its interpretation explicit so that
callers never have to infer whether a provider silently adjusted a quote for a
split or dividend.  ``adjusted`` means the provider's quote is already on a
split-adjusted basis; it does not imply dividend/total-return adjustment.
"""

from __future__ import annotations

import re
import sqlite3
from datetime import datetime, timezone

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
) -> set[str]:
    """Create/migrate row-level provenance columns and return all columns.

    Existing rows are deliberately marked ``unknown`` rather than guessed to
    be raw.  A migration/reconciliation job can promote them after inspecting
    the source; this prevents a split adjustment being applied twice.
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
        conn.execute(
            f"UPDATE {quoted} SET \"Price_Basis\" = 'unknown' "
            "WHERE \"Price_Basis\" IS NULL OR TRIM(\"Price_Basis\") = ''"
        )
    return table_columns(conn, table_name)


def utc_now() -> str:
    """Return an ISO-8601 UTC timestamp suitable for provenance columns."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def refresh_split_adjusted_prices(
    conn: sqlite3.Connection,
    ticker: str | None = None,
    prices_table: str = "Stock_Prices",
) -> int:
    """Refresh the SQL-friendly split-adjusted read model.

    The source ``Price`` column is never rewritten.  Only rows explicitly
    marked raw receive a derived factor; adjusted rows are copied at factor
    1.0, and unknown rows remain NULL so consumers can distinguish “not safe to
    adjust” from a real zero.
    """
    columns = ensure_price_provenance_columns(conn, prices_table)
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
        try:
            split_rows = conn.execute(
                "SELECT ticker, split_date, ratio_from, ratio_to, "
                f"{method_select}, {id_select} FROM Stock_Splits "
                "WHERE confirmation = 'confirmed' "
                f"{basis_clause} {superseded_clause} "
                f"ORDER BY ticker, split_date, {method_order}, {id_order}",
            ).fetchall()
        except sqlite3.Error:
            split_rows = []
    by_ticker: dict[str, list[tuple[str, float]]] = {}
    seen_events: set[tuple[str, str]] = set()
    for split_ticker, split_date, ratio_from, ratio_to, _method, _event_id in split_rows:
        event_key = (str(split_ticker), str(split_date)[:10])
        if event_key in seen_events:
            continue
        seen_events.add(event_key)
        try:
            ratio = float(ratio_from) / float(ratio_to)
        except (TypeError, ValueError, ZeroDivisionError):
            continue
        by_ticker.setdefault(str(split_ticker), []).append((str(split_date), ratio))

    where = "" if ticker is None else " WHERE Ticker = ?"
    params = () if ticker is None else (ticker,)
    rows = conn.execute(
        f"SELECT rowid, Ticker, Date, Price, Price_Basis FROM { _quote_identifier(prices_table) }{where}",
        params,
    ).fetchall()
    updated = 0
    for rowid, row_ticker, date_value, price, basis in rows:
        basis_text = str(basis or "raw").strip().lower()
        factor: float | None
        adjusted: float | None
        if basis_text == "adjusted":
            factor, adjusted = 1.0, price
        elif basis_text != "raw":
            factor, adjusted = None, None
        else:
            factor = 1.0
            for split_date, split_factor in by_ticker.get(str(row_ticker), []):
                if split_date > str(date_value)[:10]:
                    factor *= split_factor
            adjusted = price * factor if price is not None else None
        conn.execute(
            f"UPDATE {_quote_identifier(prices_table)} SET Split_Adjustment_Factor = ?, "
            "Adjusted_Price = ? WHERE rowid = ?",
            (factor, adjusted, rowid),
        )
        updated += 1
    return updated


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
