import logging
import os
import sqlite3

from src.orchestrator.common.sqlite import OrchestratorProcessorBase

logger = logging.getLogger("src.data_processing")

_DB_HELPER = OrchestratorProcessorBase()


_PLACEHOLDER_RATIO_TABLES = ("PerShare", "Valuation", "Quality")


def _ensure_placeholder_ratio_tables(conn, overwrite=False):
    if overwrite:
        conn.executescript(
            """
            DROP TABLE IF EXISTS PerShare;
            DROP TABLE IF EXISTS Valuation;
            DROP TABLE IF EXISTS Quality;
            """
        )

    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS PerShare (
          docID TEXT PRIMARY KEY
        );

        CREATE TABLE IF NOT EXISTS Valuation (
          docID TEXT PRIMARY KEY
        );

        CREATE TABLE IF NOT EXISTS Quality (
          docID TEXT PRIMARY KEY
        );
        """
    )


def _seed_placeholder_ratio_docids(helper, conn, source_schema):
    fs_actual = helper._resolve_table_name_in_schema(conn, source_schema, "FinancialStatements")
    if not fs_actual:
        return 0

    fs_ref = f"{helper._sql_ident(source_schema)}.{helper._sql_ident(fs_actual)}"
    for table_name in _PLACEHOLDER_RATIO_TABLES:
        conn.execute(
            f"INSERT OR IGNORE INTO {helper._sql_ident(table_name)}({helper._sql_ident('docID')}) "
            f"SELECT DISTINCT {helper._sql_ident('docID')} FROM {fs_ref} "
            f"WHERE {helper._sql_ident('docID')} IS NOT NULL"
        )
    return conn.execute(f"SELECT COUNT(*) FROM {fs_ref}").fetchone()[0]

