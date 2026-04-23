import logging
import os
import sqlite3

from src.orchestrator.common.sqlite import OrchestratorProcessorBase

logger = logging.getLogger("src.data_processing")

_DB_HELPER = OrchestratorProcessorBase()


_FINANCIAL_STATEMENTS_COLUMNS = [
    ("edinetCode", "TEXT"),
    ("docID", "TEXT PRIMARY KEY"),
    ("docTypeCode", "TEXT"),
    ("periodStart", "TEXT"),
    ("periodEnd", "TEXT"),
    ("DescriptionOfBusiness", "TEXT"),
    ("DescriptionOfBusiness_EN", "TEXT"),
    ("SharePrice", "REAL"),
]
_PLACEHOLDER_STATEMENT_TABLES = ("IncomeStatement", "BalanceSheet", "CashflowStatement")


def _ensure_placeholder_financial_statement_tables(conn, overwrite=False):
    if overwrite:
        conn.executescript(
            """
            DROP TABLE IF EXISTS FinancialStatements;
            DROP TABLE IF EXISTS IncomeStatement;
            DROP TABLE IF EXISTS BalanceSheet;
            DROP TABLE IF EXISTS CashflowStatement;
            DROP TABLE IF EXISTS statement_line_items;
            """
        )

    fs_cols_sql = ",\n          ".join(f'"{name}" {col_type}' for name, col_type in _FINANCIAL_STATEMENTS_COLUMNS)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS FinancialStatements (\n"
        f"          {fs_cols_sql}\n"
        "        )"
    )

    for table_name in _PLACEHOLDER_STATEMENT_TABLES:
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS \"{table_name}\" (\"docID\" TEXT PRIMARY KEY)"
        )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS statement_line_items (
            statement_family TEXT NOT NULL,
            concept_qname TEXT NOT NULL,
            column_name TEXT,
            display_label TEXT,
            concept_name TEXT,
            taxonomy_release_id INTEGER,
            role_uri TEXT,
            presentation_parent_qname TEXT,
            parent_column_name TEXT,
            line_order REAL,
            line_depth INTEGER,
            period_key TEXT,
            value_type TEXT,
            is_abstract INTEGER,
            is_required_metric INTEGER,
            PRIMARY KEY (statement_family, concept_qname)
        )
        """
    )


def _seed_placeholder_financial_statement_rows(helper, conn, source_schema, source_table):
    source_actual = helper._resolve_table_name_in_schema(conn, source_schema, source_table)
    if not source_actual:
        return 0

    source_ref = f"{helper._sql_ident(source_schema)}.{helper._sql_ident(source_actual)}"
    col_names = helper._resolve_source_col_names(conn, source_schema, source_actual)
    doc_col = col_names.get("docID") or "docID"

    conn.execute(
        f"""
        INSERT INTO {helper._sql_ident('FinancialStatements')} (
            {helper._sql_ident('edinetCode')},
            {helper._sql_ident('docID')},
            {helper._sql_ident('docTypeCode')},
            {helper._sql_ident('periodStart')},
            {helper._sql_ident('periodEnd')}
        )
        SELECT
            MAX(CAST({helper._source_column_expr('s', col_names.get('edinetCode'))} AS TEXT)) AS edinetCode,
            CAST(s.{helper._sql_ident(doc_col)} AS TEXT) AS docID,
            MAX(CAST({helper._source_column_expr('s', col_names.get('docTypeCode'))} AS TEXT)) AS docTypeCode,
            MIN(CAST({helper._source_column_expr('s', col_names.get('periodStart'))} AS TEXT)) AS periodStart,
            MAX(CAST({helper._source_column_expr('s', col_names.get('periodEnd'))} AS TEXT)) AS periodEnd
        FROM {source_ref} s
        WHERE s.{helper._sql_ident(doc_col)} IS NOT NULL
        GROUP BY s.{helper._sql_ident(doc_col)}
        ON CONFLICT(docID) DO UPDATE SET
            edinetCode = excluded.edinetCode,
            docTypeCode = excluded.docTypeCode,
            periodStart = excluded.periodStart,
            periodEnd = excluded.periodEnd
        """
    )
    return conn.execute("SELECT COUNT(*) FROM FinancialStatements").fetchone()[0]


def generate_financial_statements(
    source_database,
    source_table,
    target_database,
    mappings_config,
    company_table=None,
    prices_table=None,
    overwrite=False,
    batch_size=2500,
    max_line_depth=3,
    helper=None,
):
    """Scaffold placeholder financial-statement tables pending a full rework."""
    helper = helper or _DB_HELPER
    source_db = source_database
    target_db = target_database
    if not source_db:
        raise ValueError("source_database is required for generate_financial_statements.")
    if not target_db:
        raise ValueError("target_database is required for generate_financial_statements.")
    source_tbl = source_table or "financialData_full"
    mappings_path = mappings_config
    if not mappings_path:
        raise ValueError("Mappings_Config is required for generate_financial_statements.")

    same_db = os.path.abspath(source_db) == os.path.abspath(target_db)
    conn = sqlite3.connect(target_db)
    try:
        conn.execute("PRAGMA busy_timeout = 30000")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")

        source_schema = "main"
        if not same_db:
            conn.execute("ATTACH DATABASE ? AS src", (source_db,))
            source_schema = "src"

        _ensure_placeholder_financial_statement_tables(conn, overwrite=overwrite)
        seeded_documents = _seed_placeholder_financial_statement_rows(
            helper,
            conn,
            source_schema,
            source_tbl,
        )
        conn.commit()
        logger.warning(
            "generate_financial_statements is currently a placeholder scaffold. Seeded %d document row(s) and created build-out tables only.",
            seeded_documents,
        )
        return {
            "status": "placeholder",
            "documents_seeded": seeded_documents,
            "tables": ["FinancialStatements", *_PLACEHOLDER_STATEMENT_TABLES, "statement_line_items"],
            "mappings_config": mappings_path,
            "batch_size": max(int(batch_size or 2500), 1),
            "max_line_depth": max_line_depth,
        }
    finally:
        conn.close()


def refresh_statement_hierarchy(
    target_database,
    mappings_config,
    max_line_depth=3,
    helper=None,
):
    """Preserve a placeholder metadata table for future financial-statement rework."""
    del helper
    if not target_database:
        raise ValueError("target_database is required for refresh_statement_hierarchy.")
    if not mappings_config:
        raise ValueError("mappings_config is required for refresh_statement_hierarchy.")

    conn = sqlite3.connect(target_database)
    try:
        conn.execute("PRAGMA busy_timeout = 30000")
        _ensure_placeholder_financial_statement_tables(conn, overwrite=False)
        total_rows = conn.execute("SELECT COUNT(*) FROM statement_line_items").fetchone()[0]
        conn.commit()
        logger.warning(
            "refresh_statement_hierarchy is currently a placeholder scaffold. Existing metadata rows were left untouched (%d row(s)).",
            total_rows,
        )
        return {
            "status": "placeholder",
            "rows_retained": total_rows,
            "mappings_config": mappings_config,
            "max_line_depth": max_line_depth,
        }
    finally:
        conn.close()