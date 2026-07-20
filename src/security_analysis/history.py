"""Memory-safe historical statement loading.

Each source table is queried independently.  This avoids SQLite's result-column
limit when a database contains several wide taxonomy-backed statement tables.
"""

from __future__ import annotations

from typing import Any


def _period_records(core, conn, schema, company_code: str, periods: int) -> list[dict[str, Any]]:
    sql = (
        f"SELECT fs.{core._quote_ident(schema.fs_docid_col)} AS docID, "
        f"fs.{core._quote_ident(schema.fs_period_end_col)} AS period_end "
        f"FROM {core._quote_ident(schema.financial_statements_table)} fs "
        f"WHERE fs.{core._quote_ident(schema.fs_code_col)} = ? "
        f"ORDER BY fs.{core._quote_ident(schema.fs_period_end_col)} DESC, "
        f"fs.{core._quote_ident(schema.fs_docid_col)} DESC LIMIT ?"
    )
    frame = core.pd.read_sql_query(sql, conn, params=[company_code, periods])
    if frame.empty:
        return []
    frame["period_end"] = frame["period_end"].astype(str).str[:10]
    return frame.iloc[::-1].reset_index(drop=True).to_dict(orient="records")


def _source_records(core, conn, schema, spec, company_code: str, periods: int):
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
        f"WHERE fs.{core._quote_ident(schema.fs_code_col)} = ? "
        f"ORDER BY fs.{core._quote_ident(schema.fs_period_end_col)} DESC, "
        f"fs.{core._quote_ident(schema.fs_docid_col)} DESC LIMIT ?"
    )
    frame = core.pd.read_sql_query(sql, conn, params=[company_code, periods])
    return frame.iloc[::-1].reset_index(drop=True).to_dict(orient="records")


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
        records = _period_records(core, conn, schema, company_code, limit)
        result: dict[str, Any] = {
            "periods": [record["period_end"] for record in records],
            "records": records,
        }
        if not records:
            result.update({source_key: [] for source_key in requested})
            return result
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
                    core, conn, schema, spec, company_code, limit
                )
                result[source_key] = core._statement_metric_rows(
                    source_records, spec.metrics, source_key
                )
            else:
                result[source_key] = []
        return result
    finally:
        conn.close()

