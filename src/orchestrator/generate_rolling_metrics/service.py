"""Multi-year averages and growth rates of the annual statement figures.

Windows count fiscal years, not rows: a ``window``-year figure needs a filing
for each of the last ``window`` fiscal years (an average skips a line a
filing left out), and growth compounds over the time between the two year
ends. Per-share figures and share counts are put on one share basis first
(``share_basis``), so a split inside the window does not read as a collapse
in earnings per share; averages are then stored on their filing's own basis,
like the figures they average, and growth rates need no basis. A trust bank's
windows read its own annual reports, not those it files for its trusts.
"""

import json
import logging
import os
import random
import sqlite3
from dataclasses import dataclass

import numpy as np
import pandas as pd

from src.orchestrator.common.corporate_actions import FilingBasis, filing_basis_factors
from src.orchestrator.common.own_filings import own_filings_sql
from src.orchestrator.common.rolling_columns import (  # noqa: F401 - re-exported
    ROLLING_WINDOWS,
    parse_rolling_column,
    rolling_average_column,
    rolling_growth_column,
    rolling_source_table,
    rolling_table_name,
)
from src.orchestrator.common.share_basis import SHARE_BASIS_COLUMNS, share_basis_rule
from src.orchestrator.common.sqlite import OrchestratorProcessorBase, connect_read

logger = logging.getLogger("src.data_processing")

_DB_HELPER = OrchestratorProcessorBase()

_PROGRESS_LOG_EVERY_ROWS = 5000
# Year ends this many months off a whole number of years still count as one
# fiscal year apart (a year end moved from 31 March to 30 April, say).
_YEAR_END_SLACK_MONTHS = 2
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


_AGGREGATIONS = ("firstnonnull", "max")


@dataclass(frozen=True)
class RollingMetric:
    """One rolling metric: its output name and the source columns it reads.

    A line EDINET files under several names takes the first that is present
    (``firstnonnull``); ``max`` takes the largest, for revenue reported as
    net sales by some filers and as a larger operating revenue by others.
    ``when`` maps a column to columns one of which must be reported beside
    it (a bank's ordinary revenue counts where its ordinary expenses do).
    """

    name: str
    columns: tuple[str, ...]
    aggregation: str = "firstnonnull"
    when: tuple[tuple[str, tuple[str, ...]], ...] = ()


def _parse_metric(entry, table_name):
    if isinstance(entry, str):
        name = entry.strip()
        return RollingMetric(name, (name,)) if name else None
    if isinstance(entry, dict):
        name = str(entry.get("name") or "").strip()
        columns: list[str] = []
        when: list[tuple[str, tuple[str, ...]]] = []
        for column in entry.get("columns") or ():
            if isinstance(column, dict):
                column_name = str(column.get("column") or "").strip()
                conditions = tuple(str(other).strip() for other in column.get("when") or () if str(other).strip())
                if column_name and conditions:
                    columns.append(column_name)
                    when.append((column_name, conditions))
            elif str(column).strip():
                columns.append(str(column).strip())
        aggregation = str(entry.get("aggregation") or "firstnonnull").strip().lower()
        if name and columns and aggregation in _AGGREGATIONS:
            return RollingMetric(name, tuple(columns), aggregation, tuple(when))
    raise RuntimeError(
        f"rolling_metrics.json table '{table_name}' has an invalid metric {entry!r}: use a column name "
        f"or {{\"name\", \"columns\", \"aggregation\" ({' or '.join(_AGGREGATIONS)})}}."
    )


def _load_rolling_metrics_table_spec(config_path):
    with open(config_path, "r", encoding="utf-8") as handle:
        raw = json.load(handle)

    if not isinstance(raw, dict) or not raw:
        raise RuntimeError("rolling_metrics.json must be a non-empty object mapping tables to column lists.")

    normalized: dict[str, list[RollingMetric]] = {}
    for table_name, columns in raw.items():
        if not isinstance(table_name, str) or not table_name.strip():
            raise RuntimeError("rolling_metrics.json contains an invalid table name.")
        if not isinstance(columns, list):
            raise RuntimeError(
                f"rolling_metrics.json table '{table_name}' must provide a column list."
            )
        normalized[table_name] = [metric for metric in (_parse_metric(entry, table_name) for entry in columns) if metric]

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


def _resolve_metric_columns(conn, schema_name, table_name, configured_metrics, helper=None):
    """The configured metrics with the source columns that exist, by their actual names."""
    helper = helper or _DB_HELPER

    def resolve(column):
        return _resolve_column_name_in_schema(conn, schema_name, table_name, column, helper=helper)

    resolved = []
    for metric in configured_metrics:
        conditions = dict(metric.when)
        columns: list[str] = []
        when: list[tuple[str, tuple[str, ...]]] = []
        for column in metric.columns:
            actual = resolve(column)
            if not actual:
                continue
            if column in conditions:
                present = tuple(found for found in (resolve(other) for other in conditions[column]) if found)
                if not present:
                    continue
                when.append((actual, present))
            columns.append(actual)
        if not columns:
            logger.warning(
                "Generate Rolling Metrics: no column of metric '%s' (%s) found in table '%s'; skipping metric.",
                metric.name,
                ", ".join(metric.columns),
                table_name,
            )
            continue
        # A single-column metric keeps the column's own spelling as its name.
        name = columns[0] if metric.columns == (metric.name,) else metric.name
        resolved.append(RollingMetric(name, tuple(columns), metric.aggregation, tuple(when)))
    return resolved


def _metric_sql(metric, alias, helper=None):
    helper = helper or _DB_HELPER
    conditions = dict(metric.when)

    def column_sql(column):
        reference = f"{alias}.{helper._sql_ident(column)}"
        if column not in conditions:
            return reference
        present = " OR ".join(f"{alias}.{helper._sql_ident(other)} IS NOT NULL" for other in conditions[column])
        return f"(CASE WHEN {present} THEN {reference} END)"

    columns = [column_sql(column) for column in metric.columns]
    if len(columns) == 1:
        return columns[0]
    if metric.aggregation == "max":
        # SQLite's max() is NULL when any argument is: compare each column
        # with the others standing in for it when it is missing.
        return "MAX(" + ", ".join(f"COALESCE({', '.join([column, *(other for other in columns if other != column)])})" for column in columns) + ")"
    return f"COALESCE({', '.join(columns)})"


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


def _basis_factors(df, kind):
    """Each row's share-basis factor of *kind* (1.0 when its filing has none)."""
    if kind is None:
        return None
    return np.array([getattr(basis, kind) if basis is not None else 1.0 for basis in df["_basis"]], dtype=float)


def _on_basis(values, factors, operator, inverse=False):
    if factors is None:
        return values
    multiply = (operator == "*") != inverse
    return values * factors if multiply else values / factors


def _window_masks(months, window):
    """Rows each row's ``window``-year figures use: ``(members, base)``.

    ``members[t, i]`` marks the filings inside row *t*'s window, which must
    reach back a full ``window`` fiscal years with a filing for each;
    ``base[t]`` is the filing ``window`` years before, or -1.
    """
    age = months[:, None] - months[None, :]
    span = 12 * (window - 1)
    members = (age >= 0) & (age <= span + _YEAR_END_SLACK_MONTHS)
    oldest = np.where(members, age, -1).max(axis=1)
    covered = (members.sum(axis=1) >= window) & (oldest >= span - _YEAR_END_SLACK_MONTHS)
    members &= covered[:, None]
    members = members.astype(float)
    distance = np.abs(age - 12 * window).astype(float)
    distance[distance > _YEAR_END_SLACK_MONTHS] = np.inf
    base = np.where(np.isfinite(distance.min(axis=1)), distance.argmin(axis=1), -1)
    return members, base


def _compute_rolling_dataframe(df, metric_columns, source_table=None, filing_basis=None):
    """Rolling averages and growth rates for every row of *df* (any companies).

    *filing_basis* maps a docID to its ``FilingBasis``; columns of
    *source_table* with a share-basis rule are adjusted with it first.
    """
    if df.empty:
        return pd.DataFrame(columns=["docID"])  # normalized output shape

    df = df.copy()
    df["periodEnd"] = pd.to_datetime(df["periodEnd"], errors="coerce")
    df = df[df["periodEnd"].notna()]
    df.sort_values(["company_code", "periodEnd", "docID"], inplace=True)
    df["_basis"] = df["docID"].map(filing_basis or {})
    df["_basis"] = df["_basis"].astype(object).where(df["_basis"].notna(), None)

    output_columns = ["docID"]
    for metric_column in metric_columns:
        for window in ROLLING_WINDOWS:
            output_columns.extend([rolling_average_column(metric_column, window), rolling_growth_column(metric_column, window)])

    frames = []
    for _, group in df.groupby("company_code", sort=False):
        months = (group["periodEnd"].dt.year * 12 + group["periodEnd"].dt.month).to_numpy()
        masks = {window: _window_masks(months, window) for window in ROLLING_WINDOWS}
        computed = {"docID": group["docID"].to_numpy()}
        for metric_column in metric_columns:
            values = pd.to_numeric(group[metric_column], errors="coerce").to_numpy(dtype=float)
            rule = share_basis_rule(source_table, metric_column) if source_table else None
            if rule is not None:
                values = _on_basis(values, _basis_factors(group, rule[0]), rule[1])
            present = (~np.isnan(values)).astype(float)
            filled = np.where(present > 0, values, 0.0)
            for window in ROLLING_WINDOWS:
                members, base = masks[window]
                counts = members @ present
                with np.errstate(invalid="ignore", divide="ignore"):
                    average = np.where(counts > 0, (members @ filled) / counts, np.nan)
                average_rule = share_basis_rule(rolling_table_name(source_table), rolling_average_column(metric_column, window)) if rule else None
                if average_rule is not None:
                    average = _on_basis(average, _basis_factors(group, average_rule[0]), average_rule[1], inverse=True)
                computed[rolling_average_column(metric_column, window)] = average

                previous = np.where(base >= 0, values[np.maximum(base, 0)], np.nan)
                years = np.where(base >= 0, (months - months[np.maximum(base, 0)]) / 12.0, np.nan)
                with np.errstate(invalid="ignore", divide="ignore"):
                    growth = np.where(
                        (previous > 0) & (values >= 0),
                        np.power(values / previous, 1.0 / years) - 1.0,
                        np.nan,
                    )
                computed[rolling_growth_column(metric_column, window)] = growth
        frames.append(pd.DataFrame(computed, index=group.index))

    if not frames:
        return pd.DataFrame(columns=["docID"])
    return pd.concat(frames)[output_columns]


def _load_filing_basis(source_db, table_spec) -> dict[str, FilingBasis]:
    """Share-basis factors by docID, when a configured table holds per-share figures."""
    if not any(table in SHARE_BASIS_COLUMNS for table in table_spec):
        return {}
    try:
        conn = connect_read(source_db)
    except (OSError, sqlite3.Error):
        return {}
    try:
        rows = filing_basis_factors(conn)
    except sqlite3.Error:
        logger.warning("Generate Rolling Metrics: could not load split history; per-share figures stay as reported.", exc_info=True)
        return {}
    finally:
        conn.close()
    logger.info("Generate Rolling Metrics: %d filing(s) put on the split-adjusted share basis.", len(rows))
    return {row.doc_id: row for row in rows}


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
        for window in ROLLING_WINDOWS:
            rolling_columns.append(rolling_average_column(metric_column, window))
            rolling_columns.append(rolling_growth_column(metric_column, window))

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
    context=None,
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

        fs_code_column = None
        for candidate in ("Company_Code", "edinetCode", "EdinetCode"):
            fs_code_column = _resolve_column_name_in_schema(
                conn, source_schema, fs_actual, candidate, helper=helper
            )
            if fs_code_column:
                break
        fs_period_column = _resolve_column_name_in_schema(
            conn,
            source_schema,
            fs_actual,
            "periodEnd",
            helper=helper,
        )
        if not fs_code_column or not fs_period_column:
            raise RuntimeError(
                "Source table 'FinancialStatements' must include a company code column (Company_Code / edinetCode) and periodEnd column for Generate Rolling Metrics."
            )

        helper._create_index_if_not_exists(conn, source_schema, fs_actual, [fs_docid_column])
        helper._create_index_if_not_exists(conn, source_schema, fs_actual, [fs_code_column, fs_period_column])

        processed_tables = []
        skipped_tables = []

        fs_ref = f"{helper._sql_ident(source_schema)}.{helper._sql_ident(fs_actual)}"
        filing_basis = _load_filing_basis(source_db, table_spec)
        own_filings = ""
        if _resolve_column_name_in_schema(conn, source_schema, fs_actual, "docTypeCode", helper=helper):
            own_filings = " AND " + own_filings_sql("fs", fs_ref, helper._sql_ident(fs_code_column))

        table_count = len(table_spec)
        for table_index, (
            configured_table_name,
            configured_columns,
        ) in enumerate(table_spec.items()):
            if context is not None and table_count:
                context.report_progress(
                    table_index,
                    table_count,
                    f"Preparing rolling table {table_index + 1} of {table_count}",
                )
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

            if configured_columns:
                metrics = _resolve_metric_columns(
                    conn,
                    source_schema,
                    source_table,
                    configured_columns,
                    helper=helper,
                )
            else:
                logger.info(
                    "Generate Rolling Metrics: table '%s' has no configured columns; discovering all numeric columns.",
                    source_table,
                )
                metrics = [
                    RollingMetric(column, (column,))
                    for column in _collect_numeric_metric_columns(
                        conn,
                        source_schema,
                        source_table,
                        source_docid_column,
                        metric_columns=None,
                        helper=helper,
                    )
                ]
            metric_columns = [metric.name for metric in metrics]
            if not metric_columns:
                skipped_tables.append(source_table)
                continue

            source_ref = f"{helper._sql_ident(source_schema)}.{helper._sql_ident(source_table)}"
            helper._create_index_if_not_exists(conn, source_schema, source_table, [source_docid_column])

            target_table = rolling_table_name(source_table)
            _ensure_rolling_table_schema(
                conn,
                target_table,
                metric_columns,
                helper=helper,
                overwrite=overwrite,
            )

            numeric_metric_columns = _collect_numeric_metric_columns(
                conn,
                source_schema,
                source_table,
                source_docid_column,
                metric_columns=[column for metric in metrics for column in metric.columns],
                helper=helper,
            )

            company_sql = (
                f"SELECT DISTINCT fs.{helper._sql_ident(fs_code_column)} "
                f"FROM {source_ref} s "
                f"INNER JOIN {fs_ref} fs "
                f"ON fs.{helper._sql_ident(fs_docid_column)} = s.{helper._sql_ident(source_docid_column)} "
                f"WHERE fs.{helper._sql_ident(fs_code_column)} IS NOT NULL "
                f"ORDER BY fs.{helper._sql_ident(fs_code_column)}"
            )
            company_codes = [row[0] for row in conn.execute(company_sql).fetchall()]
            if not company_codes:
                skipped_tables.append(source_table)
                continue

            metric_select_sql = ", ".join(
                f"{_metric_sql(metric, 's', helper=helper)} AS {helper._sql_ident(metric.name)}"
                for metric in metrics
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
            for company_index, company_code in enumerate(company_codes):
                if context is not None and table_count:
                    table_fraction = company_index / len(company_codes)
                    context.report_progress(
                        table_index + table_fraction,
                        table_count,
                        f"Processing company {company_index + 1} of {len(company_codes)}",
                    )
                select_sql = (
                    f"SELECT "
                    f"s.{helper._sql_ident(source_docid_column)} AS {helper._sql_ident('docID')}, "
                    f"fs.{helper._sql_ident(fs_code_column)} AS {helper._sql_ident('company_code')}, "
                    f"fs.{helper._sql_ident(fs_period_column)} AS {helper._sql_ident('periodEnd')}, "
                    f"{metric_select_sql} "
                    f"FROM {source_ref} s "
                    f"INNER JOIN {fs_ref} fs "
                    f"ON fs.{helper._sql_ident(fs_docid_column)} = s.{helper._sql_ident(source_docid_column)} "
                    f"WHERE s.{helper._sql_ident(source_docid_column)} IS NOT NULL "
                    f"AND fs.{helper._sql_ident(fs_code_column)} = ?"
                    f"{own_filings}{numeric_not_null_predicate}"
                )

                df = pd.read_sql_query(select_sql, conn, params=(company_code,))
                if df.empty:
                    continue

                rolling_df = _compute_rolling_dataframe(
                    df, metric_columns, source_table=source_table, filing_basis=filing_basis,
                )
                _upsert_rolling_rows(conn, target_table, rolling_df, helper=helper)
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
            processed_tables.append(target_table)

        if context is not None and table_count:
            context.report_progress(
                table_count,
                table_count,
                "Rolling metrics complete",
            )
        logger.info("Generate Rolling Metrics completed. Processed %d table(s).", len(processed_tables))
        return {
            "status": "completed",
            "tables_processed": processed_tables,
            "tables_skipped": skipped_tables,
        }
    finally:
        conn.close()
