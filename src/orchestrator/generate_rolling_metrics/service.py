import logging
import json
import os
import random
import sqlite3

import numpy as np
import pandas as pd

from src.orchestrator.common.sqlite import OrchestratorProcessorBase

logger = logging.getLogger("src.data_processing")

_DB_HELPER = OrchestratorProcessorBase()

_ROLLING_WINDOWS = (3, 5, 10)
_PROGRESS_LOG_EVERY_ROWS = 5000
ROLLING_METRICS_CONFIG_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "rolling_metrics.json")
)


def _find_docid_column(conn, schema_name, table_name, helper=None):
    helper = helper or _DB_HELPER
    info = conn.execute(
        f"PRAGMA {helper._sql_ident(schema_name)}.table_info({helper._sql_ident(table_name)})"
    ).fetchall()
    for row in info:
        col_name = str(row[1])
        if col_name.lower() == "docid":
            return col_name
    return None


def _has_docid_primary_key(conn, schema_name, table_name, helper=None):
    helper = helper or _DB_HELPER
    info = conn.execute(
        f"PRAGMA {helper._sql_ident(schema_name)}.table_info({helper._sql_ident(table_name)})"
    ).fetchall()
    docid_pk_rows = [row for row in info if str(row[1]).lower() == "docid" and int(row[5] or 0) > 0]
    return bool(docid_pk_rows)


def _resolve_column_name_in_schema(conn, schema_name, table_name, column_name, helper=None):
    helper = helper or _DB_HELPER
    info = conn.execute(
        f"PRAGMA {helper._sql_ident(schema_name)}.table_info({helper._sql_ident(table_name)})"
    ).fetchall()
    by_lower = {str(row[1]).lower(): str(row[1]) for row in info}
    return by_lower.get(str(column_name or "").lower())


def _load_rolling_metrics_table_spec(config_path):
    with open(config_path, "r", encoding="utf-8") as handle:
        raw = json.load(handle)

    if not isinstance(raw, dict) or not raw:
        raise RuntimeError("rolling_metrics.json must be a non-empty object mapping tables to column lists.")

    normalized: dict[str, list[str]] = {}
    for table_name, columns in raw.items():
        if not isinstance(table_name, str) or not table_name.strip():
            raise RuntimeError("rolling_metrics.json contains an invalid table name.")
        if not isinstance(columns, list) or not columns:
            raise RuntimeError(
                f"rolling_metrics.json table '{table_name}' must provide a non-empty column list."
            )
        normalized_columns = [str(column).strip() for column in columns if str(column).strip()]
        if not normalized_columns:
            raise RuntimeError(
                f"rolling_metrics.json table '{table_name}' does not contain valid column names."
            )
        normalized[table_name] = normalized_columns

    return normalized


def list_docid_primary_key_tables(
    conn,
    schema_name="main",
    excluded_tables=None,
    helper=None,
):
    helper = helper or _DB_HELPER
    excluded_lookup = {
        str(name).lower()
        for name in (excluded_tables or [])
    }

    rows = conn.execute(
        f"SELECT name FROM {helper._sql_ident(schema_name)}.sqlite_master "
        "WHERE type='table'"
    ).fetchall()

    discovered = []
    for (table_name,) in rows:
        if not table_name:
            continue

        lower_name = str(table_name).lower()
        if lower_name.startswith("sqlite_"):
            continue
        if lower_name in excluded_lookup:
            continue
        if lower_name.endswith("_rolling"):
            continue
        if not _has_docid_primary_key(conn, schema_name, table_name, helper=helper):
            continue

        discovered.append(str(table_name))

    return sorted(discovered)


def _resolve_metric_columns(conn, schema_name, table_name, configured_columns, helper=None):
    helper = helper or _DB_HELPER
    resolved_columns = []
    for column_name in configured_columns:
        actual_column = _resolve_column_name_in_schema(
            conn,
            schema_name,
            table_name,
            column_name,
            helper=helper,
        )
        if not actual_column:
            logger.warning(
                "Generate Rolling Metrics: column '%s' not found in table '%s'; skipping column.",
                column_name,
                table_name,
            )
            continue
        resolved_columns.append(actual_column)
    return resolved_columns


def _is_numeric_declared_type(declared_type):
    text = str(declared_type or "").strip().upper()
    if not text:
        return False
    numeric_tokens = ("INT", "REAL", "FLOA", "DOUB", "NUM", "DEC", "BOOL")
    return any(token in text for token in numeric_tokens)


def _collect_numeric_metric_columns(conn, schema_name, table_name, docid_column, metric_columns=None, helper=None):
    helper = helper or _DB_HELPER
    info = conn.execute(
        f"PRAGMA {helper._sql_ident(schema_name)}.table_info({helper._sql_ident(table_name)})"
    ).fetchall()
    metric_lookup = None
    if metric_columns is not None:
        metric_lookup = {str(col).lower() for col in metric_columns}
    numeric_columns = []
    for row in info:
        col_name = str(row[1])
        col_type = row[2]
        if col_name.lower() == str(docid_column).lower():
            continue
        if metric_lookup is not None and col_name.lower() not in metric_lookup:
            continue
        if _is_numeric_declared_type(col_type):
            numeric_columns.append(col_name)
    return numeric_columns


def _compute_rolling_dataframe(df, metric_columns):
    if df.empty:
        return pd.DataFrame(columns=["docID"])  # normalized output shape

    df = df.copy()
    df["periodEnd"] = pd.to_datetime(df["periodEnd"], errors="coerce")
    df.sort_values(["edinetCode", "periodEnd", "docID"], inplace=True)

    computed_columns = {
        "docID": df["docID"],
    }

    for metric_column in metric_columns:
        series = pd.to_numeric(df[metric_column], errors="coerce")
        grouped = series.groupby(df["edinetCode"])

        for window in _ROLLING_WINDOWS:
            avg_col = f"{metric_column}_Average_{window}_Year"
            growth_col = f"{metric_column}_Growth_{window}_Year"

            computed_columns[avg_col] = grouped.transform(
                lambda s, w=window: s.rolling(window=w, min_periods=1).mean()
            )

            prev = grouped.transform(lambda s, shift=window: s.shift(shift))
            computed_columns[growth_col] = np.where(
                (prev > 0) & (series >= 0),
                np.power(series / prev, 1.0 / window) - 1.0,
                np.nan,
            )

    return pd.DataFrame(computed_columns, index=df.index)


def _ensure_rolling_table_schema(conn, table_name, metric_columns, helper=None, overwrite=False):
    helper = helper or _DB_HELPER
    if overwrite:
        conn.execute(f"DROP TABLE IF EXISTS {helper._sql_ident(table_name)}")

    conn.execute(
        f"CREATE TABLE IF NOT EXISTS {helper._sql_ident(table_name)} ("
        f"{helper._sql_ident('docID')} TEXT PRIMARY KEY"
        f")"
    )

    table_info = conn.execute(
        f"PRAGMA table_info({helper._sql_ident(table_name)})"
    ).fetchall()
    existing_columns = {str(row[1]) for row in table_info}

    rolling_columns = []
    for metric_column in metric_columns:
        for window in _ROLLING_WINDOWS:
            rolling_columns.append(f"{metric_column}_Average_{window}_Year")
            rolling_columns.append(f"{metric_column}_Growth_{window}_Year")

    for column_name in rolling_columns:
        if column_name in existing_columns:
            continue
        conn.execute(
            f"ALTER TABLE {helper._sql_ident(table_name)} "
            f"ADD COLUMN {helper._sql_ident(column_name)} REAL"
        )


def _upsert_rolling_rows(conn, table_name, rolling_df, helper=None):
    helper = helper or _DB_HELPER
    if rolling_df.empty:
        return

    rolling_df = rolling_df.where(pd.notna(rolling_df), None)

    temp_name = f"_tmp_{table_name}_{random.randint(1000, 9999)}"
    rolling_df.to_sql(temp_name, conn, if_exists="replace", index=False)

    ordered_columns = [str(col) for col in rolling_df.columns]
    columns_sql = ", ".join(helper._sql_ident(col) for col in ordered_columns)
    conn.execute(
        f"INSERT OR REPLACE INTO {helper._sql_ident(table_name)} ({columns_sql}) "
        f"SELECT {columns_sql} FROM {helper._sql_ident(temp_name)}"
    )
    conn.execute(f"DROP TABLE IF EXISTS {helper._sql_ident(temp_name)}")


def generate_rolling_metrics(
    source_database,
    target_database,
    overwrite=False,
    helper=None,
):
    helper = helper or _DB_HELPER
    source_db = source_database
    target_db = target_database

    if not source_db:
        raise ValueError("source_database is required for generate_rolling_metrics.")
    if not target_db:
        raise ValueError("target_database is required for generate_rolling_metrics.")

    table_spec = _load_rolling_metrics_table_spec(ROLLING_METRICS_CONFIG_PATH)
    total_configured_columns = sum(len(columns) for columns in table_spec.values())

    logger.info(
        "Generate Rolling Metrics: loaded configuration from '%s' with %d table(s) and %d column(s).",
        ROLLING_METRICS_CONFIG_PATH,
        len(table_spec),
        total_configured_columns,
    )

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

        fs_actual = helper._resolve_table_name_in_schema(conn, source_schema, "FinancialStatements")
        if not fs_actual:
            raise RuntimeError(
                "Source table 'FinancialStatements' not found; required for Generate Rolling Metrics."
            )

        fs_docid_column = _find_docid_column(conn, source_schema, fs_actual, helper=helper)
        if not fs_docid_column:
            raise RuntimeError(
                "Source table 'FinancialStatements' is missing a docID column; required for Generate Rolling Metrics."
            )

        fs_edinet_column = _resolve_column_name_in_schema(
            conn,
            source_schema,
            fs_actual,
            "edinetCode",
            helper=helper,
        )
        fs_period_column = _resolve_column_name_in_schema(
            conn,
            source_schema,
            fs_actual,
            "periodEnd",
            helper=helper,
        )
        if not fs_edinet_column or not fs_period_column:
            raise RuntimeError(
                "Source table 'FinancialStatements' must include edinetCode and periodEnd columns for Generate Rolling Metrics."
            )

        helper._create_index_if_not_exists(conn, source_schema, fs_actual, [fs_docid_column])
        helper._create_index_if_not_exists(conn, source_schema, fs_actual, [fs_edinet_column, fs_period_column])

        processed_tables = []
        skipped_tables = []

        fs_ref = f"{helper._sql_ident(source_schema)}.{helper._sql_ident(fs_actual)}"

        for configured_table_name, configured_columns in table_spec.items():
            source_table = helper._resolve_table_name_in_schema(conn, source_schema, configured_table_name)
            if not source_table:
                logger.warning(
                    "Generate Rolling Metrics: table '%s' not found in source schema; skipping table.",
                    configured_table_name,
                )
                skipped_tables.append(configured_table_name)
                continue

            logger.info(
                "Generate Rolling Metrics: starting table '%s' (configured columns: %d).",
                source_table,
                len(configured_columns),
            )

            source_docid_column = _find_docid_column(conn, source_schema, source_table, helper=helper)
            if not source_docid_column:
                skipped_tables.append(source_table)
                continue

            metric_columns = _resolve_metric_columns(
                conn,
                source_schema,
                source_table,
                configured_columns,
                helper=helper,
            )
            if not metric_columns:
                skipped_tables.append(source_table)
                continue

            source_ref = f"{helper._sql_ident(source_schema)}.{helper._sql_ident(source_table)}"
            helper._create_index_if_not_exists(conn, source_schema, source_table, [source_docid_column])

            rolling_table_name = f"{source_table}_Rolling"
            _ensure_rolling_table_schema(
                conn,
                rolling_table_name,
                metric_columns,
                helper=helper,
                overwrite=overwrite,
            )

            numeric_metric_columns = _collect_numeric_metric_columns(
                conn,
                source_schema,
                source_table,
                source_docid_column,
                metric_columns=metric_columns,
                helper=helper,
            )

            company_sql = (
                f"SELECT DISTINCT fs.{helper._sql_ident(fs_edinet_column)} "
                f"FROM {source_ref} s "
                f"INNER JOIN {fs_ref} fs "
                f"ON fs.{helper._sql_ident(fs_docid_column)} = s.{helper._sql_ident(source_docid_column)} "
                f"WHERE fs.{helper._sql_ident(fs_edinet_column)} IS NOT NULL "
                f"ORDER BY fs.{helper._sql_ident(fs_edinet_column)}"
            )
            company_codes = [row[0] for row in conn.execute(company_sql).fetchall()]
            if not company_codes:
                skipped_tables.append(source_table)
                continue

            metric_select_sql = ", ".join(
                f"s.{helper._sql_ident(col)} AS {helper._sql_ident(col)}"
                for col in metric_columns
            )
            numeric_not_null_predicate = ""
            if numeric_metric_columns:
                numeric_expr = " OR ".join(
                    f"s.{helper._sql_ident(col)} IS NOT NULL"
                    for col in numeric_metric_columns
                )
                numeric_not_null_predicate = f" AND ({numeric_expr})"

            processed_any_rows = False
            rows_processed_for_table = 0
            next_progress_log_at = _PROGRESS_LOG_EVERY_ROWS
            for company_code in company_codes:
                select_sql = (
                    f"SELECT "
                    f"s.{helper._sql_ident(source_docid_column)} AS {helper._sql_ident('docID')}, "
                    f"fs.{helper._sql_ident(fs_edinet_column)} AS {helper._sql_ident('edinetCode')}, "
                    f"fs.{helper._sql_ident(fs_period_column)} AS {helper._sql_ident('periodEnd')}, "
                    f"{metric_select_sql} "
                    f"FROM {source_ref} s "
                    f"INNER JOIN {fs_ref} fs "
                    f"ON fs.{helper._sql_ident(fs_docid_column)} = s.{helper._sql_ident(source_docid_column)} "
                    f"WHERE s.{helper._sql_ident(source_docid_column)} IS NOT NULL "
                    f"AND fs.{helper._sql_ident(fs_edinet_column)} = ?"
                    f"{numeric_not_null_predicate}"
                )

                df = pd.read_sql_query(select_sql, conn, params=(company_code,))
                if df.empty:
                    continue

                rolling_df = _compute_rolling_dataframe(df, metric_columns)
                _upsert_rolling_rows(conn, rolling_table_name, rolling_df, helper=helper)
                processed_any_rows = True
                rows_processed_for_table += len(df)
                while rows_processed_for_table >= next_progress_log_at:
                    logger.info(
                        "Generate Rolling Metrics: table '%s' progress %d rows processed.",
                        source_table,
                        next_progress_log_at,
                    )
                    next_progress_log_at += _PROGRESS_LOG_EVERY_ROWS
                conn.commit()

            if not processed_any_rows:
                skipped_tables.append(source_table)
                continue

            logger.info(
                "Generate Rolling Metrics: finished table '%s' (%d rows processed).",
                source_table,
                rows_processed_for_table,
            )
            processed_tables.append(rolling_table_name)

        logger.info("Generate Rolling Metrics completed. Processed %d table(s).", len(processed_tables))
        return {
            "status": "completed",
            "tables_processed": processed_tables,
            "tables_skipped": skipped_tables,
        }
    finally:
        conn.close()
