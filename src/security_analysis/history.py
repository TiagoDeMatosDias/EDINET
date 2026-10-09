"""Memory-safe historical statement loading.

Each source table is queried independently.  This avoids SQLite's result-column
limit when a database contains several wide taxonomy-backed statement tables.

Per-share figures and share counts come back on the split-adjusted basis of
the stored prices (``share_basis``), so a series reads continuously across a
split; each adjusted row keeps the figures as filed in ``reported_values``.
"""

from __future__ import annotations

import sqlite3
from typing import Any

from src.orchestrator.common.corporate_actions import (
    FilingBasis,
    SplitEvent,
    filing_basis_factors,
    load_split_events,
)
from src.orchestrator.common.own_filings import own_filings_sql
from src.orchestrator.common.share_basis import (
    SHARE_BASIS_COLUMNS,
    share_basis_rule,
    to_adjusted_basis,
)


def _own_filings_clause(core, conn, schema) -> str:
    """``AND`` clause keeping a company's own annual reports, when filings say which they are."""
    if "docTypeCode" not in core._get_columns(conn, schema.financial_statements_table):
        return ""
    return " AND " + own_filings_sql(
        "fs", core._quote_ident(schema.financial_statements_table), core._quote_ident(schema.fs_code_col)
    )


def _period_records(core, conn, schema, company_code: str, periods: int, own: str = "") -> list[dict[str, Any]]:
    sql = (
        f"SELECT fs.{core._quote_ident(schema.fs_docid_col)} AS docID, "
        f"fs.{core._quote_ident(schema.fs_period_end_col)} AS period_end "
        f"FROM {core._quote_ident(schema.financial_statements_table)} fs "
        f"WHERE fs.{core._quote_ident(schema.fs_code_col)} = ?{own} "
        f"ORDER BY fs.{core._quote_ident(schema.fs_period_end_col)} DESC, "
        f"fs.{core._quote_ident(schema.fs_docid_col)} DESC LIMIT ?"
    )
    frame = core.pd.read_sql_query(sql, conn, params=[company_code, periods])
    if frame.empty:
        return []
    frame["period_end"] = frame["period_end"].astype(str).str[:10]
    return frame.iloc[::-1].reset_index(drop=True).to_dict(orient="records")


def _source_records(core, conn, schema, spec, company_code: str, periods: int, own: str = ""):
    select_parts = [
        f"fs.{core._quote_ident(schema.fs_docid_col)} AS docID",
        f"fs.{core._quote_ident(schema.fs_period_end_col)} AS period_end",
    ]
    select_parts.extend(
        f"{spec.alias}.{core._quote_ident(metric.source_field)} "
        f"AS {core._quote_ident(metric.record_field)}"
        for metric in spec.metrics
    )
    sql = (
        f"SELECT {', '.join(select_parts)} "
        f"FROM {core._quote_ident(schema.financial_statements_table)} fs "
        f"{spec.join_clause} "
        f"WHERE fs.{core._quote_ident(schema.fs_code_col)} = ?{own} "
        f"ORDER BY fs.{core._quote_ident(schema.fs_period_end_col)} DESC, "
        f"fs.{core._quote_ident(schema.fs_docid_col)} DESC LIMIT ?"
    )
    frame = core.pd.read_sql_query(sql, conn, params=[company_code, periods])
    return frame.iloc[::-1].reset_index(drop=True).to_dict(orient="records")


def _company_share_basis(core, conn, schema, company_code: str) -> tuple[list[SplitEvent], dict[str, FilingBasis]]:
    """The company's known splits and each affected filing's basis factors."""
    try:
        row = conn.execute(
            f"SELECT {core._quote_ident(schema.company_ticker_col)} FROM {core._quote_ident(schema.company_table)} "
            f"WHERE {core._quote_ident(schema.company_code_col)} = ? LIMIT 1",
            (company_code,),
        ).fetchone()
        # A company without a ticker (delisted) is keyed by its EDINET code.
        ticker = (str(row[0]).strip() if row and row[0] is not None else "") or company_code
        events = load_split_events(conn, [ticker]).get(ticker, [])
        if not events:
            return [], {}
        basis = filing_basis_factors(conn, tickers=[ticker], events={ticker: events})
    except sqlite3.Error:
        core.logger.warning("Could not load split history for %s; per-share figures stay as filed.", company_code, exc_info=True)
        return [], {}
    return events, {item.doc_id: item for item in basis}


def _adjust_share_basis(rows: list[dict[str, Any]], table_name: str, records: list[dict[str, Any]], basis: dict[str, FilingBasis]) -> None:
    """Put a source's per-share rows on the adjusted basis, keeping the figures as filed."""
    for row in rows:
        rule = share_basis_rule(table_name, str(row.get("field") or ""))
        if rule is None:
            continue
        kind, operator = rule
        factors = [getattr(basis[record["docID"]], kind) if record.get("docID") in basis else 1.0 for record in records]
        reported = row.get("values") or []
        adjusted = [to_adjusted_basis(value, factor, operator) for value, factor in zip(reported, factors, strict=False)]
        if adjusted != reported:
            row["reported_values"] = reported
            row["values"] = adjusted


_SPLIT_SOURCES = {"Stock_Splits": "split record", "filing date count": "filing", "annual reports": "share counts"}


def _split_payload(events: list[SplitEvent]) -> list[dict[str, Any]]:
    """Each split as the UI names it: an ex-date, or the dates it fell between."""
    return [
        {
            "date": event.until.strftime("%Y-%m-%d"),
            "after": None if event.exact else event.after.strftime("%Y-%m-%d"),
            "multiplier": event.multiplier,
            "source": _SPLIT_SOURCES.get(event.source, event.source),
        }
        for event in events
    ]


def get_security_statements_by_source(
    db_path: str,
    company_code: str,
    periods: int,
    statement_sources: dict[str, str] | None,
) -> dict[str, Any]:
    """Load statement history without joining every wide table at once."""
    from . import security_analysis as core

    core.ensure_security_analysis_indexes(db_path)
    schema = core.resolve_schema(db_path)
    requested = core._statement_requested_sources(statement_sources)
    limit = max(1, int(periods))
    conn = core._connect(db_path)
    try:
        specs = core._build_statement_source_specs(conn, schema, requested)
        own = _own_filings_clause(core, conn, schema)
        records = _period_records(core, conn, schema, company_code, limit, own)
        result: dict[str, Any] = {
            "periods": [record["period_end"] for record in records],
            "records": records,
        }
        if not records:
            result.update({source_key: [] for source_key in requested})
            return result
        events: list[SplitEvent] = []
        basis: dict[str, FilingBasis] = {}
        if any(spec.table_name in SHARE_BASIS_COLUMNS for spec in specs.values()):
            events, basis = _company_share_basis(core, conn, schema, company_code)
        result["share_basis"] = {"splits": _split_payload(events)}
        for source_key in requested:
            spec = specs.get(source_key)
            if not spec or not spec.table_name:
                result[source_key] = []
            elif core._is_taxonomy_statement_table(conn, spec.table_name):
                result[source_key] = core._taxonomy_statement_rows(
                    conn, spec.table_name, records, source_key
                )
            elif spec.join_clause and spec.metrics:
                source_records = _source_records(
                    core, conn, schema, spec, company_code, limit, own
                )
                result[source_key] = core._statement_metric_rows(
                    source_records, spec.metrics, source_key
                )
                if basis:
                    _adjust_share_basis(result[source_key], spec.table_name, source_records, basis)
            else:
                result[source_key] = []
        return result
    finally:
        conn.close()

