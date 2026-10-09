"""Correct power-of-ten slips in filed per-share figures and share counts.

An issuer occasionally tags a figure a power of ten off: a P/E of 5,140.7 for
51.4, or an issued share count of 7,094 meaning 7,094 thousand. Each annual
report is checked against its own other figures, which do not share the
slip:

* P/E times EPS is the year-end price on the shares the EPS is on, so the
  stored price over it is 1 (on today's shares, with the filing's split
  factor);
* EPS times year-end shares over profit, and book value per share times
  year-end shares over net assets, hold about the same from one report of a
  company to the next (both on today's shares).

A figure is corrected only when two measures agree: a slipped share count
moves both the earnings and book measures by the same power of ten; a slipped
P/E moves the price measure, and the stored price is far from P/E times EPS
even before the split factor (a price series a provider left unadjusted for
a split matches it there), while EPS is not off; a slipped EPS moves the earnings and price measures by opposite powers.
Book value per share has only the book measure, which large minority
interests or an equity that nearly vanished move as much, so it is left as
filed. The filed value is kept in ``ShareMetrics_Corrections``, which the
as-filed view shows.
"""

from __future__ import annotations

import logging
import math
import sqlite3

import pandas as pd

logger = logging.getLogger(__name__)

CORRECTIONS_TABLE = "ShareMetrics_Corrections"
EPS = "Basic earnings (loss) per share"
PER = "Price-earnings ratio"
BPS = "Net assets per share"
SHARE_COUNTS = (
    "Number of issued shares as of fiscal year end",
    "Total number of issued shares",
    "Number of issued shares as of filing date",
)
# How close to a power of ten a measure must be (15 % either way).
_DECADE_TOLERANCE = 0.06
# A P/E against the price it was computed from: within 5 %.
_EXACT_DECADE = 0.02
# Two measures of one count agree within 25 %.
_NEAR = math.log(1.25)
# The same count given twice in a report, a digit dropped from one: within 2 %.
_SAME_COUNT = math.log(1.02)
_MIN_REPORTS = 3


def _decade(value: float | None, tolerance: float | None = None) -> int:
    """The power of ten *value* is off by, or 0."""
    if value is None or pd.isna(value) or not math.isfinite(value) or value <= 0:
        return 0
    exponent = math.log10(value)
    nearest = round(exponent)
    return int(nearest) if nearest != 0 and abs(exponent - nearest) < (tolerance or _DECADE_TOLERANCE) else 0


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in conn.execute(f'PRAGMA table_info("{table}")')}


def ensure_corrections_table(conn: sqlite3.Connection, *, clear: bool = False) -> None:
    if clear:
        conn.execute(f'DROP TABLE IF EXISTS "{CORRECTIONS_TABLE}"')
    conn.execute(
        f'CREATE TABLE IF NOT EXISTS "{CORRECTIONS_TABLE}" ('
        '"docID" TEXT NOT NULL, "column_name" TEXT NOT NULL, "filed" REAL, "corrected" REAL, "reason" TEXT, '
        'PRIMARY KEY ("docID", "column_name"))'
    )


def _annual_reports(conn: sqlite3.Connection) -> pd.DataFrame:
    share_columns = _columns(conn, "ShareMetrics")
    present = [name for name in (EPS, PER, BPS, *SHARE_COUNTS) if name in share_columns]
    if not {EPS, BPS} <= set(present) or not any(name in present for name in SHARE_COUNTS):
        return pd.DataFrame()
    counts = [name for name in SHARE_COUNTS[:2] if name in present]
    count_sql = "COALESCE(" + ", ".join(f's."{name}"' for name in counts) + ")" if len(counts) > 1 else f's."{counts[0]}"'
    profit_sql = 'i."Profit (loss)"' if "Profit (loss)" in _columns(conn, "IncomeStatement") else "NULL"
    net_assets_sql = 'b."Net assets"' if "Net assets" in _columns(conn, "BalanceSheet") else "NULL"
    price_columns = _columns(conn, "Stock_Prices")
    price_sql = "NULL"
    if {"Ticker", "Date", "Price"} <= price_columns:
        expression = 'COALESCE(p."Adjusted_Price", p."Price")' if "Adjusted_Price" in price_columns else 'p."Price"'
        price_sql = (
            f"(SELECT {expression} FROM Stock_Prices p WHERE p.Ticker = c.Company_Ticker AND p.Date <= fs.periodEnd "
            "AND p.Date >= date(fs.periodEnd, '-10 days') ORDER BY p.Date DESC LIMIT 1)"
        )
    selected = ", ".join(f's."{name}" AS "{name}"' for name in present)
    key = "CASE WHEN TRIM(COALESCE(c.Company_Ticker, '')) = '' THEN c.Company_Code ELSE c.Company_Ticker END"
    return pd.read_sql_query(
        f"SELECT {key} AS company, fs.docID AS doc, fs.periodEnd AS period_end, {selected}, {count_sql} AS shares, {profit_sql} AS profit, "
        f"{net_assets_sql} AS net_assets, {price_sql} AS stored "
        "FROM FinancialStatements fs JOIN ShareMetrics s ON s.docID = fs.docID "
        "JOIN CompanyInfo c ON c.Company_Code = fs.Company_Code "
        "LEFT JOIN IncomeStatement i ON i.docID = fs.docID LEFT JOIN BalanceSheet b ON b.docID = fs.docID "
        "WHERE fs.docTypeCode = '030000'",
        conn,
    )


def _implied_counts(rows: pd.DataFrame) -> pd.Series:
    """The issued share count a report's own figures imply: profit over EPS and net assets over book value per share, where they agree."""
    by_earnings = (rows.profit / rows[EPS]).where((rows[EPS] * rows.profit > 0) & (rows[EPS].abs() >= 1))
    by_book = (rows.net_assets / rows[BPS]).where((rows[BPS] > 0) & (rows.net_assets > 0))
    agree = (by_earnings / by_book).map(lambda ratio: ratio is not None and pd.notna(ratio) and abs(math.log(ratio)) < _NEAR)
    return ((by_earnings * by_book) ** 0.5).where(agree)


def _near(value: float | None, target: float | None) -> bool:
    return value is not None and target is not None and pd.notna(value) and pd.notna(target) and value > 0 and target > 0 and abs(math.log(value / target)) < _NEAR


def _share_count_slips(rows: pd.DataFrame) -> list[tuple[str, str, float, float, str]]:
    """Share counts a power of ten off the count the report's own EPS and book value imply.

    Judged without split factors, column by column: a slip is one column off
    while the report's other counts or the reports either side sit at the
    implied count; a split moves the count for good, so the next report
    carries the new count and the one before the old.
    """
    slips: list[tuple[str, str, float, float, str]] = []
    rows = rows.copy()
    rows["implied"] = _implied_counts(rows)
    counts = [name for name in SHARE_COUNTS if name in rows.columns]
    for _company, group in rows.groupby("company", sort=False):
        group = group.sort_values("period_end").to_dict("records")
        for index, record in enumerate(group):
            implied = record["implied"]
            if pd.isna(implied):
                continue
            neighbours = [group[at]["shares"] for at in (index - 1, index + 1) if 0 <= at < len(group)]
            if any(not _near(value, implied) for value in neighbours):
                continue
            for name in counts:
                value = record.get(name)
                if value is None or pd.isna(value) or value <= 0:
                    continue
                k = _decade(value / implied)
                others = [record.get(other) for other in counts if other != name]
                if k and (abs(k) >= 3 or any(_near(other, implied) for other in others)):
                    factor = 10.0 ** k
                    # The report usually gives the count right elsewhere (a
                    # digit dropped, 9,335,103 beside 93,335,103): take it.
                    exact = [
                        float(other) for other in others
                        if other is not None and pd.notna(other) and other > 0 and abs(math.log(other * factor / value)) < _SAME_COUNT
                    ]
                    corrected, source = (exact[0], "as the report gives it elsewhere") if exact else (float(value) / factor, "scaled")
                    slips.append((record["doc"], name, float(value), corrected, f"share count {factor:g} times the count the report's EPS and book value imply; {source}"))
    return slips


def find_decimal_slips(rows: pd.DataFrame, factors: dict[str, tuple[float, float]]) -> list[tuple[str, str, float, float, str]]:
    """``(docID, column, filed, corrected, reason)`` for each slip *rows* show.

    *factors* maps a docID to its ``(restated, fiscal)`` split factors.
    """
    if rows.empty:
        return []
    slips = _share_count_slips(rows) if "period_end" in rows.columns else []
    rows = rows.copy()
    rows["restated"] = rows.doc.map(lambda doc: factors.get(doc, (1.0, 1.0))[0])
    rows["fiscal"] = rows.doc.map(lambda doc: factors.get(doc, (1.0, 1.0))[1])
    eps, shares = rows[EPS], rows.shares
    earned = (eps * rows.profit > 0) & (shares > 0) & (eps.abs() >= 0.01)
    rows["earnings"] = (eps * rows.restated * shares / rows.fiscal / rows.profit).where(earned)
    counts = rows.groupby("company")["earnings"].transform("count")
    level = rows.groupby("company")["earnings"].transform("median")
    rows["earnings"] = (rows["earnings"] / level).where(counts >= _MIN_REPORTS)
    traded = (rows[PER] * eps).where((rows[PER] * eps) > 0) if PER in rows.columns else pd.Series(float("nan"), index=rows.index)
    rows["price"] = rows.stored / (traded * rows.restated)
    rows["price_raw"] = rows.stored / traded
    for record in rows.to_dict("records"):
        earnings, price, price_raw = (None if pd.isna(record[name]) else float(record[name]) for name in ("earnings", "price", "price_raw"))
        doc = record["doc"]
        # A P/E is the year-end price over EPS, so a slip lands within a few
        # per cent of a power of ten; a P/E off by 8.9 times is another error.
        k_price = _decade(price, tolerance=_EXACT_DECADE)
        if k_price and price_raw is not None and abs(math.log10(price_raw)) > 0.5 and not _decade(earnings):
            filed = float(record[PER])
            slips.append((doc, PER, filed, filed * 10.0 ** k_price, f"P/E {10.0 ** -k_price:g} times the year-end price over EPS"))
        elif _decade(earnings) and k_price == -_decade(earnings) and record[EPS] > 0:
            k = _decade(earnings)
            filed = float(record[EPS])
            slips.append((doc, EPS, filed, filed / 10.0 ** k, f"EPS {10.0 ** k:g} times profit per share and the price over the P/E"))
    return slips


def correct_decimal_slips(conn: sqlite3.Connection) -> int:
    """Correct the slips in ``ShareMetrics`` and record each; returns how many."""
    from src.orchestrator.common.corporate_actions import filing_basis_factors

    ensure_corrections_table(conn)
    try:
        rows = _annual_reports(conn)
        factors = {item.doc_id: (item.restated, item.fiscal) for item in filing_basis_factors(conn)}
    except (sqlite3.Error, pd.errors.DatabaseError):
        logger.warning("Could not check filed figures for decimal slips", exc_info=True)
        return 0
    slips = find_decimal_slips(rows, factors)
    for doc, column, filed, corrected, reason in slips:
        conn.execute(f'UPDATE ShareMetrics SET "{column}" = ? WHERE docID = ?', (corrected, doc))
        conn.execute(
            f'INSERT OR REPLACE INTO "{CORRECTIONS_TABLE}" ("docID", "column_name", "filed", "corrected", "reason") VALUES (?, ?, ?, ?, ?)',
            (doc, column, filed, corrected, reason),
        )
    if slips:
        logger.info("Corrected %d power-of-ten slips in filed per-share figures and share counts.", len(slips))
    return len(slips)
