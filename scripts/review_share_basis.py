#!/usr/bin/env python3
"""Check that per-share figures in Standardized.db read on one share basis.

Five independent checks, read-only:

* **Prices.** A report's P/E times its EPS is the year-end price on the
  shares its EPS is on, so the stored (split-adjusted) price over it should
  equal the filing's ``restated`` factor. Consecutive reports whose two
  ratios disagree by 1.5x or more are listed.
* **Book value.** Adjusted book value per share over net assets per adjusted
  year-end share should hold steady from one report to the next; a lasting
  step by a split ratio means a filing's ``restated`` and ``fiscal`` factors
  disagree.
* **Earnings.** EPS times year-end shares, both on today's basis, over the
  report's profit stays about the same from one report to the next, whatever
  P/E the issuer quoted; a step by a split ratio means a filing's factors
  are wrong. It tells a report's odd P/E from a wrong per-share figure.
* **Rolling figures.** Every stored 5-year average and growth rate, in every
  rolling table, whose metric is one source column and whose window is
  unambiguous (five reports a year apart, a base report five years back)
  equals the one recomputed here from the reports (per-share figures on
  today's shares).
* **History payload.** For every company with a split, the values the
  history API returns equal the stored figure times its filing's factor,
  and ``reported_values`` equal the figure as filed (the stored one, or the
  filed value of a power-of-ten slip the standardization step corrected).

Usage: ``python scripts/review_share_basis.py [--db PATH] [--csv DIR]``
"""

from __future__ import annotations

import argparse
import collections
import logging
import math
import sqlite3
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.orchestrator.common.corporate_actions import (  # noqa: E402
    filing_basis_factors,
    load_split_events,
    standard_split_multiplier,
)
from src.orchestrator.common.db_config import get_market_db  # noqa: E402
from src.orchestrator.common.share_basis import share_basis_rule  # noqa: E402

# A company without a ticker (delisted) is keyed by its EDINET code, as in corporate_actions.
_KEY = "CASE WHEN TRIM(COALESCE(c.Company_Ticker, '')) = '' THEN c.Company_Code ELSE c.Company_Ticker END"
_ANNUAL_REPORTS = f'''SELECT {_KEY} t, fs.Company_Code code, fs.docID doc, fs.periodEnd e,
  s."Price-earnings ratio" per, s."Basic earnings (loss) per share" eps, s."Net assets per share" bps,
  COALESCE(s."Number of issued shares as of fiscal year end", s."Total number of issued shares") shares, b."Net assets" na, i."Profit (loss)" profit,
  (SELECT COALESCE(Adjusted_Price, Price) FROM Stock_Prices p WHERE p.Ticker = c.Company_Ticker AND p.Date <= fs.periodEnd
     AND p.Date >= date(fs.periodEnd, '-10 days') ORDER BY p.Date DESC LIMIT 1) stored
  FROM FinancialStatements fs JOIN ShareMetrics s ON s.docID = fs.docID JOIN CompanyInfo c ON c.Company_Code = fs.Company_Code
  LEFT JOIN BalanceSheet b ON b.docID = fs.docID LEFT JOIN IncomeStatement i ON i.docID = fs.docID
  WHERE fs.docTypeCode = '030000' ORDER BY 1, fs.periodEnd'''


def _pairs(rows: pd.DataFrame, column: str) -> list[tuple]:
    """Consecutive reports of one company whose *column* moves by 1.5x or more."""
    out = []
    for ticker, group in rows.groupby("t"):
        group = group[group[column] > 0].sort_values("e")
        values, ends = group[column].tolist(), group.e.tolist()
        for index in range(1, len(values)):
            step = values[index] / values[index - 1]
            if abs(math.log(step)) > math.log(1.5):
                reverts = index + 1 < len(values) and abs(math.log(values[index + 1] / values[index] * step)) < math.log(1.25)
                out.append((ticker, group.code.iloc[0], ends[index - 1], ends[index], round(step, 3), reverts))
    return out


def price_and_book_checks(conn: sqlite3.Connection) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    rows = pd.read_sql_query(_ANNUAL_REPORTS, conn)
    basis = {item.doc_id: item for item in filing_basis_factors(conn)}
    rows["restated"] = rows.doc.map(lambda doc: basis[doc].restated if doc in basis else 1.0)
    rows["fiscal"] = rows.doc.map(lambda doc: basis[doc].fiscal if doc in basis else 1.0)
    priced = (rows.per > 0) & (rows.eps > 0) & (rows.stored > 0)
    rows["price_basis"] = (rows.stored / (rows.per * rows.eps * rows.restated)).where(priced)
    rows["book_basis"] = (rows.bps * rows.restated / (rows.na * rows.fiscal / rows.shares)).where((rows.bps > 0) & (rows.na > 0) & (rows.shares > 0))
    # EPS of at least ¥1 (rounding), and only companies whose EPS and profit
    # cover the same group (an IFRS filer's consolidated EPS beside its
    # parent-only profit does not).
    earned = (rows.eps * rows.profit > 0) & (rows.shares > 0) & (rows.eps.abs() >= 1)
    rows["earnings_basis"] = (rows.eps * rows.restated * rows.shares / rows.fiscal / rows.profit).where(earned)
    same_scope = rows.groupby("t").earnings_basis.transform(lambda x: abs(math.log(x.median())) < 0.25 if x.notna().any() else False)
    rows["earnings_basis"] = rows.earnings_basis.where(same_scope.astype(bool))
    columns = ["ticker", "code", "from", "to", "step", "reverts_next_year"]
    earnings = pd.DataFrame(_pairs(rows, "earnings_basis"), columns=columns)
    # A wrong factor steps by a split's ratio; issues and profit swings by anything.
    earnings = earnings[earnings.step.map(lambda step: standard_split_multiplier(step, tolerance_large=0.05) is not None)]
    return (
        pd.DataFrame(_pairs(rows, "price_basis"), columns=columns),
        pd.DataFrame(_pairs(rows, "book_basis"), columns=columns),
        earnings.reset_index(drop=True),
    )


def rolling_check(conn: sqlite3.Connection) -> dict[str, collections.Counter]:
    """Recompute every 5-year average and growth rate from the reports; per rolling table, count matches.

    Covers each rolling metric that is one column of its source table,
    with that table's share-basis rule (per-share figures on today's
    shares, averages on the filing's own basis), over windows that are
    unambiguous: five reports a year apart and a base report five years
    back. Keys are ``(kind, split in the window, matches)``.
    """
    from src.orchestrator.common.rolling_columns import rolling_source_table

    basis = {item.doc_id: item for item in filing_basis_factors(conn)}
    keys = pd.read_sql_query(
        f"SELECT {_KEY} AS company, fs.docID AS doc, fs.periodEnd AS e FROM FinancialStatements fs "
        "JOIN CompanyInfo c ON c.Company_Code = fs.Company_Code WHERE fs.docTypeCode = '030000'",
        conn,
    )
    events = load_split_events(conn, sorted(set(keys.company)))
    keys["month"] = pd.to_datetime(keys.e).dt.year * 12 + pd.to_datetime(keys.e).dt.month
    results: dict[str, collections.Counter] = {}
    tables = [row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE '%_Rolling'")]
    for table in tables:
        source = rolling_source_table(table)
        source_columns = {row[1] for row in conn.execute(f'PRAGMA table_info("{source}")')}
        rolling_columns = {row[1] for row in conn.execute(f'PRAGMA table_info("{table}")')}
        metrics = sorted(column[: -len("_Average_5_Year")] for column in rolling_columns if column.endswith("_Average_5_Year"))
        counts: collections.Counter = collections.Counter()
        recomputable = [metric for metric in metrics if metric in source_columns and f"{metric}_Growth_5_Year" in rolling_columns]
        counts[("not one source column", None, None)] = len(metrics) - len(recomputable)
        for start in range(0, len(recomputable), 60):
            chunk = recomputable[start:start + 60]
            selected = ", ".join(f's."{m}" AS "v{i}", r."{m}_Average_5_Year" AS "a{i}", r."{m}_Growth_5_Year" AS "g{i}"' for i, m in enumerate(chunk))
            values = pd.read_sql_query(f'SELECT s.docID AS doc, {selected} FROM "{source}" s LEFT JOIN "{table}" r ON r.docID = s.docID', conn)
            frame = keys.merge(values, on="doc", how="inner")
            for company, group in frame.groupby("company"):
                group = group.sort_values("month").reset_index(drop=True)
                if group.month.duplicated().any():
                    continue
                for i, metric in enumerate(chunk):
                    rule = share_basis_rule(source, metric)
                    average_rule = share_basis_rule(table, f"{metric}_Average_5_Year")
                    raw = pd.to_numeric(group[f"v{i}"], errors="coerce")
                    if rule:
                        factor = group.doc.map(lambda doc, kind=rule[0]: getattr(basis[doc], kind) if doc in basis else 1.0)
                        adjusted = raw * factor if rule[1] == "*" else raw / factor
                    else:
                        adjusted = raw
                    for index in range(len(group)):
                        window = group[(group.month <= group.month[index]) & (group.month >= group.month[index] - 50)]
                        if len(window) != 5 or (abs(window.month.diff().dropna() - 12) > 2).any() or adjusted[window.index].isna().any():
                            continue
                        split = bool(rule) and any(window.e.iloc[0] < str(event.until.date()) <= window.e.iloc[-1] for event in events.get(company, []))
                        stored = pd.to_numeric(pd.Series([group[f"a{i}"][index]]), errors="coerce")[0]
                        if not pd.isna(stored):
                            own = getattr(basis[group.doc[index]], average_rule[0]) if average_rule and group.doc[index] in basis else 1.0
                            shown = (stored * own if average_rule[1] == "*" else stored / own) if average_rule else stored
                            counts[("averages", split, math.isclose(shown, adjusted[window.index].mean(), rel_tol=1e-6, abs_tol=1e-6))] += 1
                        base = group[(group.month >= group.month[index] - 62) & (group.month <= group.month[index] - 58)]
                        stored = pd.to_numeric(pd.Series([group[f"g{i}"][index]]), errors="coerce")[0]
                        if len(base) == 1 and not pd.isna(stored) and adjusted[base.index[0]] > 0 and adjusted[index] >= 0:
                            years = (group.month[index] - base.month.iloc[0]) / 12
                            expected = (adjusted[index] / adjusted[base.index[0]]) ** (1 / years) - 1
                            counts[("growth rates", split, math.isclose(stored, expected, rel_tol=1e-6, abs_tol=1e-9))] += 1
        results[table] = counts
    return results


def history_check(db_path: str, conn: sqlite3.Connection) -> collections.Counter:
    from src.security_analysis import get_security_statements

    tickers = [row[0] for row in conn.execute(f"SELECT DISTINCT {_KEY} FROM CompanyInfo c")]
    basis = {item.doc_id: item for item in filing_basis_factors(conn)}
    tables = ["ShareMetrics", "PerShare_Metrics", "ShareMetrics_Rolling", "PerShare_Metrics_Rolling"]
    try:  # a figure corrected for a power-of-ten slip reads as filed in the as-filed view
        slips = {(doc, column): value for doc, column, value in conn.execute('SELECT "docID", "column_name", "filed" FROM ShareMetrics_Corrections')}
    except sqlite3.Error:
        slips = {}
    counts: collections.Counter = collections.Counter()
    for ticker in load_split_events(conn, tickers):
        for (code,) in conn.execute(f"SELECT Company_Code FROM CompanyInfo c WHERE {_KEY} = ?", (ticker,)):
            history = get_security_statements(db_path, code, periods=20, statement_sources={table: table for table in tables})
            docs = [record["docID"] for record in history["records"]]
            for table in tables:
                lines = history.get(table) or []
                if not lines:
                    continue
                fields = [line["field"] for line in lines]
                quoted = ", ".join(f'"{field}"' for field in fields)
                stored = {
                    row[0]: dict(zip(fields, row[1:], strict=True))
                    for row in conn.execute(f'SELECT docID, {quoted} FROM "{table}" WHERE docID IN ({",".join("?" * len(docs))})', docs)
                }
                for line in lines:
                    rule = share_basis_rule(table, line["field"])
                    filed = line.get("reported_values", line["values"])
                    for index, doc in enumerate(docs):
                        value = stored.get(doc, {}).get(line["field"])
                        if value is None:
                            continue
                        factor = getattr(basis[doc], rule[0]) if rule and doc in basis else 1.0
                        expected = value * factor if rule and rule[1] == "*" else (value / factor if rule else value)
                        counts["values"] += 1
                        as_filed = slips.get((doc, line["field"]), value) if table == "ShareMetrics" else value
                        if abs(filed[index] - as_filed) > 1e-9 * max(1.0, abs(as_filed)) or abs(line["values"][index] - expected) > 1e-6 * max(1.0, abs(expected)):
                            counts["wrong"] += 1
    return counts


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--db", default=None, help="Standardized database (default: the configured one)")
    parser.add_argument("--csv", default=None, help="Directory to write the listed report pairs to")
    parser.add_argument("--skip-history", action="store_true", help="Skip the history payload check (about a minute)")
    args = parser.parse_args()
    logging.disable(logging.WARNING)
    db_path = args.db or get_market_db()
    conn = sqlite3.connect(f"file:{Path(db_path).resolve()}?mode=ro", uri=True)
    price, book, earnings = price_and_book_checks(conn)
    both = price.merge(earnings, on=["ticker", "from", "to"]) if len(price) and len(earnings) else price.iloc[0:0]
    print(f"prices: {len(price)} report pairs disagree with the stored prices by 1.5x or more "
          f"({int(price.reverts_next_year.sum())} undone the next year: one report's P/E or EPS)")
    print(f"book value: {len(book)} report pairs step by 1.5x or more "
          f"({int(book.reverts_next_year.sum())} undone the next year)")
    print(f"earnings: {len(earnings)} report pairs step by a split's ratio "
          f"({int(earnings.reverts_next_year.sum())} undone the next year); "
          f"{len(both)} of the price disagreements step in earnings too")
    for table, rolling in rolling_check(conn).items():
        parts = []
        for kind in ("averages", "growth rates"):
            checked = sum(value for key, value in rolling.items() if key[0] == kind)
            wrong = sum(value for key, value in rolling.items() if key[0] == kind and not key[2])
            across = sum(value for key, value in rolling.items() if key[0] == kind and key[1])
            parts.append(f"{checked} {kind} ({across} with a split in the window), {wrong} wrong")
        skipped = rolling[("not one source column", None, None)]
        print(f"rolling {table}: " + "; ".join(parts) + (f"; {skipped} metrics of several columns not recomputed" if skipped else ""))
    if not args.skip_history:
        counts = history_check(db_path, conn)
        print(f"history payload: {counts['values']} values checked, {counts['wrong']} wrong")
    if args.csv:
        Path(args.csv).mkdir(parents=True, exist_ok=True)
        price.to_csv(Path(args.csv) / "price_disagreements.csv", index=False)
        book.to_csv(Path(args.csv) / "book_value_steps.csv", index=False)
        earnings.to_csv(Path(args.csv) / "earnings_steps.csv", index=False)
        both.to_csv(Path(args.csv) / "price_and_earnings_steps.csv", index=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
