#!/usr/bin/env python3
"""Check that per-share figures in Standardized.db read on one share basis.

Three independent checks, read-only:

* **Prices.** A report's P/E times its EPS is the year-end price on the
  shares its EPS is on, so the stored (split-adjusted) price over it should
  equal the filing's ``restated`` factor. Consecutive reports whose two
  ratios disagree by 1.5x or more are listed.
* **Book value.** Adjusted book value per share over net assets per adjusted
  year-end share should hold steady from one report to the next; a lasting
  step by a split ratio means a filing's ``restated`` and ``fiscal`` factors
  disagree.
* **History payload.** For every company with a split, the values the
  history API returns equal the stored figure times its filing's factor,
  and ``reported_values`` equal the stored figure.

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
)
from src.orchestrator.common.db_config import get_db2  # noqa: E402
from src.orchestrator.common.share_basis import share_basis_rule  # noqa: E402

_ANNUAL_REPORTS = '''SELECT c.Company_Ticker t, fs.Company_Code code, fs.docID doc, fs.periodEnd e,
  s."Price-earnings ratio" per, s."Basic earnings (loss) per share" eps, s."Net assets per share" bps,
  COALESCE(s."Number of issued shares as of fiscal year end", s."Total number of issued shares") shares, b."Net assets" na,
  (SELECT COALESCE(Adjusted_Price, Price) FROM Stock_Prices p WHERE p.Ticker = c.Company_Ticker AND p.Date <= fs.periodEnd
     AND p.Date >= date(fs.periodEnd, '-10 days') ORDER BY p.Date DESC LIMIT 1) stored
  FROM FinancialStatements fs JOIN ShareMetrics s ON s.docID = fs.docID JOIN CompanyInfo c ON c.Company_Code = fs.Company_Code
  LEFT JOIN BalanceSheet b ON b.docID = fs.docID
  WHERE fs.docTypeCode = '030000' AND TRIM(COALESCE(c.Company_Ticker, '')) <> '' ORDER BY c.Company_Ticker, fs.periodEnd'''


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


def price_and_book_checks(conn: sqlite3.Connection) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows = pd.read_sql_query(_ANNUAL_REPORTS, conn)
    basis = {item.doc_id: item for item in filing_basis_factors(conn)}
    rows["restated"] = rows.doc.map(lambda doc: basis[doc].restated if doc in basis else 1.0)
    rows["fiscal"] = rows.doc.map(lambda doc: basis[doc].fiscal if doc in basis else 1.0)
    priced = (rows.per > 0) & (rows.eps > 0) & (rows.stored > 0)
    rows["price_basis"] = (rows.stored / (rows.per * rows.eps * rows.restated)).where(priced)
    rows["book_basis"] = (rows.bps * rows.restated / (rows.na * rows.fiscal / rows.shares)).where((rows.bps > 0) & (rows.na > 0) & (rows.shares > 0))
    columns = ["ticker", "code", "from", "to", "step", "reverts_next_year"]
    return pd.DataFrame(_pairs(rows, "price_basis"), columns=columns), pd.DataFrame(_pairs(rows, "book_basis"), columns=columns)


def history_check(db_path: str, conn: sqlite3.Connection) -> collections.Counter:
    from src.security_analysis import get_security_statements

    tickers = [row[0] for row in conn.execute("SELECT DISTINCT Company_Ticker FROM CompanyInfo WHERE TRIM(COALESCE(Company_Ticker, '')) <> ''")]
    basis = {item.doc_id: item for item in filing_basis_factors(conn)}
    tables = ["ShareMetrics", "PerShare_Metrics", "ShareMetrics_Rolling", "PerShare_Metrics_Rolling"]
    counts: collections.Counter = collections.Counter()
    for ticker in load_split_events(conn, tickers):
        for (code,) in conn.execute("SELECT Company_Code FROM CompanyInfo WHERE Company_Ticker = ?", (ticker,)):
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
                        if abs(filed[index] - value) > 1e-9 * max(1.0, abs(value)) or abs(line["values"][index] - expected) > 1e-6 * max(1.0, abs(expected)):
                            counts["wrong"] += 1
    return counts


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--db", default=None, help="Standardized database (default: the configured one)")
    parser.add_argument("--csv", default=None, help="Directory to write the listed report pairs to")
    parser.add_argument("--skip-history", action="store_true", help="Skip the history payload check (about a minute)")
    args = parser.parse_args()
    logging.disable(logging.WARNING)
    db_path = args.db or get_db2()
    conn = sqlite3.connect(f"file:{Path(db_path).resolve()}?mode=ro", uri=True)
    price, book = price_and_book_checks(conn)
    print(f"prices: {len(price)} report pairs disagree with the stored prices by 1.5x or more "
          f"({int(price.reverts_next_year.sum())} undone the next year: one report's P/E or EPS)")
    print(f"book value: {len(book)} report pairs step by 1.5x or more "
          f"({int(book.reverts_next_year.sum())} undone the next year)")
    if not args.skip_history:
        counts = history_check(db_path, conn)
        print(f"history payload: {counts['values']} values checked, {counts['wrong']} wrong")
    if args.csv:
        Path(args.csv).mkdir(parents=True, exist_ok=True)
        price.to_csv(Path(args.csv) / "price_disagreements.csv", index=False)
        book.to_csv(Path(args.csv) / "book_value_steps.csv", index=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
