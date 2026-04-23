"""Screening module: query, filter, and export corporate financial data.

Provides functions to introspect a SQLite database produced by the EDINET
pipeline, build parameterised screening queries, execute them, and
persist/load screening criteria and history.
"""

import json
import logging
import math
import os
import re
import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------

SCREENING_TABLES: list[str] = [
    "PerShare",
    "Valuation",
    "Quality",
    "Pershare_Historical",
    "Valuation_Historical",
    "Quality_Historical",
]

COMPANYINFO_EDINET_CANDIDATES: list[str] = [
    "edinetCode",
    "EdinetCode",
]

COMPANYINFO_TICKER_CANDIDATES: list[str] = [
    "Company_Ticker",
    "Ticker",
    "ticker",
]

COMPANYINFO_NAME_CANDIDATES: list[str] = [
    "Company_Name",
    "CompanyName",
    "company_name",
    "Submitter Name",
    "Submitter_Name",
    "SubmitterName",
    "FilerName",
    "Name",
]

COMPANYINFO_INDUSTRY_CANDIDATES: list[str] = [
    "Company_Industry",
    "Industry",
    "industry",
    "Sector",
    "Business_Industry",
]

OPERATOR_MAP: dict[str, str] = {
    ">": ">",
    ">=": ">=",
    "<": "<",
    "<=": "<=",
    "=": "=",
    "!=": "!=",
    "BETWEEN": "BETWEEN",
}

DEFAULT_COLUMNS: list[str] = [
    "CompanyInfo.edinetCode",
    "CompanyInfo.Company_Ticker",
    "FinancialStatements.periodEnd",
]

RANKING_ALGORITHMS: dict[str, str] = {
    "none": "None",
    "weighted_minmax": "Weighted Min-Max",
    "weighted_percentile": "Weighted Percentile",
}

RANKING_DIRECTIONS: dict[str, str] = {
    "higher": "Higher is Better",
    "lower": "Lower is Better",
}

# --- Formatting rules ---
# Maps column-name substrings/patterns to format types.

FORMAT_RULES: dict[str, str] = {
    "Margin": "percent",
    "Yield": "percent",
    "Payout": "percent",
    "Return": "percent",
    "Ratio": "ratio",
    "Turnover": "ratio",
    "Growth": "percent",
    "ZScore": "ratio",
    "MarketCap": "currency",
    "EnterpriseValue": "currency",
    "Price": "currency",
    "SharePrice": "currency",
}

# --- Table alias mapping ---

_TABLE_ALIAS: dict[str, str] = {
    "FinancialStatements": "f",
    "CompanyInfo": "c",
    "Stock_Prices": "s_p",
    "PerShare": "ps",
    "Valuation": "v",
    "Quality": "q",
    "IncomeStatement": "i",
    "BalanceSheet": "b",
    "CashflowStatement": "cf",
    "Pershare_Historical": "psh",
    "Valuation_Historical": "vh",
    "Quality_Historical": "qh",
}


# ---------------------------------------------------------------------------
# SQL display helper
# ---------------------------------------------------------------------------


def _interpolate_sql(sql: str, params: list) -> str:
    """Replace ``?`` placeholders with literal values for display.

    The result is a valid, copy-pasteable SQL statement.  String values
    are single-quoted; numeric values are inserted bare.
    """
    result_parts: list[str] = []
    param_idx = 0
    for char in sql:
        if char == "?" and param_idx < len(params):
            val = params[param_idx]
            param_idx += 1
            if isinstance(val, str):
                # Escape single quotes within the value
                safe = val.replace("'", "''")
                result_parts.append(f"'{safe}'")
            elif isinstance(val, (int, float)):
                result_parts.append(str(val))
            else:
                safe = str(val).replace("'", "''")
                result_parts.append(f"'{safe}'")
        else:
            result_parts.append(char)
    return "".join(result_parts)


def _resolve_matching_column(
    columns: list[str],
    candidates: list[str],
) -> str | None:
    """Return the first candidate that exists in *columns* (case-insensitive)."""
    column_map = {column.lower(): column for column in columns}
    for candidate in candidates:
        match = column_map.get(candidate.lower())
        if match:
            return match
    return None


def get_default_columns(
    available_metrics: dict[str, list[str]] | None = None,
) -> list[str]:
    """Return the default screening result columns for the current schema."""
    company_cols = (available_metrics or {}).get("CompanyInfo", [])
    edinet_col = _resolve_matching_column(
        company_cols, COMPANYINFO_EDINET_CANDIDATES
    ) or "edinetCode"
    ticker_col = _resolve_matching_column(
        company_cols, COMPANYINFO_TICKER_CANDIDATES
    ) or "Company_Ticker"

    columns = [
        f"CompanyInfo.{edinet_col}",
        f"CompanyInfo.{ticker_col}",
        "FinancialStatements.periodEnd",
    ]

    company_name_col = _resolve_matching_column(
        company_cols, COMPANYINFO_NAME_CANDIDATES
    )
    if company_name_col:
        columns.append(f"CompanyInfo.{company_name_col}")

    company_industry_col = _resolve_matching_column(
        company_cols, COMPANYINFO_INDUSTRY_CANDIDATES
    )
    if company_industry_col:
        columns.append(f"CompanyInfo.{company_industry_col}")

    return columns


# ---------------------------------------------------------------------------
# Database introspection
# ---------------------------------------------------------------------------

def get_available_metrics(db_path: str) -> dict[str, list[str]]:
    """Return a dict of ``{table_name: [column_names]}`` for screening.

    Includes the screening metric tables plus ``CompanyInfo``. The ``docID``
    column is excluded from derived metric tables.

    Args:
        db_path: Path to the SQLite database.

    Returns:
        Dict mapping table names to their column lists.
    """
    result: dict[str, list[str]] = {}
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.cursor()

        try:
            cursor.execute("PRAGMA table_info([CompanyInfo])")
            company_cols = [row[1] for row in cursor.fetchall()]
            if company_cols:
                result["CompanyInfo"] = company_cols
        except sqlite3.OperationalError:
            pass

        for table in SCREENING_TABLES:
            try:
                cursor.execute(f"PRAGMA table_info([{table}])")
                cols = [
                    row[1] for row in cursor.fetchall()
                    if row[1] not in ("docID", "edinetCode", "periodEnd")
                ]
                if cols:
                    result[table] = cols
            except sqlite3.OperationalError:
                # Table does not exist in this database
                continue
    finally:
        conn.close()
    return result


def get_available_periods(db_path: str) -> list[str]:
    """Return sorted list of distinct ``periodEnd`` years in the database.

    Args:
        db_path: Path to the SQLite database.

    Returns:
        Sorted list of year strings, e.g. ``["2015", "2016", ..., "2024"]``.
    """
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT DISTINCT SUBSTR(periodEnd, 1, 4) AS yr "
            "FROM FinancialStatements "
            "WHERE periodEnd IS NOT NULL "
            "ORDER BY yr"
        )
        return [row[0] for row in cursor.fetchall() if row[0]]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Query building
# ---------------------------------------------------------------------------

def _validate_column_ref(col: str, available: dict[str, list[str]]) -> bool:
    """Check that a ``Table.Column`` reference is in the available metrics.

    CompanyInfo is validated against the live schema; FinancialStatements
    and Stock_Prices are always joined.
    """
    parts = col.split(".", 1)
    if len(parts) != 2:
        return False
    table, column = parts
    if table == "CompanyInfo":
        if table not in available:
            return True
        available_cols = available[table]
        if any(column.lower() == actual.lower() for actual in available_cols):
            return True
        company_alias_groups = (
            COMPANYINFO_EDINET_CANDIDATES,
            COMPANYINFO_TICKER_CANDIDATES,
            COMPANYINFO_NAME_CANDIDATES,
            COMPANYINFO_INDUSTRY_CANDIDATES,
        )
        for candidates in company_alias_groups:
            if column.lower() not in {candidate.lower() for candidate in candidates}:
                continue
            if _resolve_matching_column(available_cols, candidates):
                return True
        return False
    if table in ("FinancialStatements", "Stock_Prices"):
        return True
    return table in available and column in available[table]


_SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_ ]*$")


def _safe_identifier(name: str) -> str:
    """Validate and return a safe SQL identifier.

    Raises:
        ValueError: If the name contains unsafe characters.
    """
    if not _SAFE_IDENTIFIER_RE.match(name):
        raise ValueError(f"Invalid SQL identifier: {name!r}")
    return name


def _quote_identifier(name: str) -> str:
    """Return *name* quoted as a SQLite identifier."""
    return f"[{name.replace(']', ']]')}]"


def _build_result_column_aliases(columns: list[str]) -> dict[str, str]:
    """Return deterministic result aliases for requested screening columns."""
    base_name_counts: dict[str, int] = {}
    parsed_columns: list[tuple[str, str, str]] = []

    for col in columns:
        table, column = col.split(".", 1)
        parsed_columns.append((col, table, column))
        key = column.lower()
        base_name_counts[key] = base_name_counts.get(key, 0) + 1

    aliases: dict[str, str] = {}
    for col, table, column in parsed_columns:
        if base_name_counts[column.lower()] > 1:
            aliases[col] = f"{table}.{column}"
        else:
            aliases[col] = column
    return aliases


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    """Return *values* without duplicates while preserving input order."""
    return list(dict.fromkeys(values))


def _build_query_column_plan(
    columns: list[str],
    ranking_rules: list[dict] | None = None,
) -> tuple[list[str], dict[str, str], list[str]]:
    """Build query/output column plans for screening results.

    Ranking-rule columns are fetched when needed but kept out of the visible
    result set unless the caller explicitly requested them.
    """
    requested_columns = _dedupe_preserve_order(columns)
    column_aliases = _build_result_column_aliases(requested_columns)
    query_columns = list(requested_columns)

    for rule in ranking_rules or []:
        table = str(rule.get("table", "")).strip()
        column = str(rule.get("column", "")).strip()
        if not table or not column:
            continue
        col_ref = f"{table}.{column}"
        if col_ref in column_aliases:
            continue
        query_columns.append(col_ref)
        column_aliases[col_ref] = col_ref

    visible_columns = [column_aliases[col] for col in requested_columns]
    for auto_column in ("LatestPrice", "PriceDate"):
        if auto_column not in visible_columns:
            visible_columns.append(auto_column)

    return query_columns, column_aliases, visible_columns


def build_screening_query(
    criteria: list[dict],
    columns: list[str],
    period: str | None = None,
    available_metrics: dict[str, list[str]] | None = None,
    column_aliases: dict[str, str] | None = None,
) -> tuple[str, list]:
    """Build a parameterised SQL query for screening.

    Args:
        criteria: List of filter dicts, each with keys ``table``, ``column``,
            ``operator``, ``value`` (and optionally ``value2`` for BETWEEN).
        columns: List of ``"Table.Column"`` strings to SELECT.
        period: Optional year string to filter ``periodEnd``.
        available_metrics: Output of ``get_available_metrics`` for validation.
            If ``None``, validation of screening-table columns is skipped.
        column_aliases: Optional ``{Table.Column: Alias}`` overrides for the
            SELECT projection.

    Returns:
        ``(sql_string, params_list)`` tuple for parameterised execution.

    Raises:
        ValueError: If an invalid table, column, or operator is specified.
    """
    params: list = []

    # --- Determine which tables to join ---
    needed_tables: set[str] = set()
    for col in columns:
        parts = col.split(".", 1)
        if len(parts) == 2:
            needed_tables.add(parts[0])
    for crit in criteria:
        needed_tables.add(crit["table"])
        if crit.get("comparison_mode") == "column":
            compare_table = crit.get("compare_table")
            if compare_table:
                needed_tables.add(compare_table)

    # Validate columns against available metrics
    if available_metrics is not None:
        for col in columns:
            if not _validate_column_ref(col, available_metrics):
                raise ValueError(f"Invalid column reference: {col!r}")
        for crit in criteria:
            table = crit["table"]
            column = crit["column"]
            if not _validate_column_ref(
                f"{table}.{column}", available_metrics
            ):
                raise ValueError(
                    f"Column {column!r} not in table {table!r}"
                )
            if crit.get("comparison_mode") == "column":
                compare_table = crit.get("compare_table")
                compare_column = crit.get("compare_column")
                if not compare_table or not compare_column:
                    raise ValueError(
                        "Dynamic criteria require compare_table and compare_column"
                    )
                if not _validate_column_ref(
                    f"{compare_table}.{compare_column}", available_metrics
                ):
                    raise ValueError(
                        f"Column {compare_column!r} not in table {compare_table!r}"
                    )

    # Validate operators
    for crit in criteria:
        op = crit["operator"]
        if op not in OPERATOR_MAP:
            raise ValueError(f"Invalid operator: {op!r}")

    # --- Build SELECT ---
    select_parts: list[str] = []
    result_aliases = _build_result_column_aliases(columns)
    if column_aliases:
        result_aliases.update(column_aliases)
    for col in columns:
        table, column = col.split(".", 1)
        alias = _TABLE_ALIAS.get(table, table)
        safe_col = _safe_identifier(column)
        result_alias = result_aliases[col]
        select_parts.append(
            f"{alias}.[{safe_col}] AS {_quote_identifier(result_alias)}"
        )

    # Always include latest stock price
    if "Stock_Prices" in needed_tables or True:
        needed_tables.add("Stock_Prices")
        if "s_p.[Price]" not in select_parts:
            select_parts.append("s_p.[Price] AS LatestPrice")
            select_parts.append("s_p.[Date] AS PriceDate")

    select_clause = ", ".join(select_parts) if select_parts else "*"

    # --- Build FROM / JOIN ---
    join_clauses: list[str] = ["FROM FinancialStatements f"]
    join_clauses.append(
        "LEFT JOIN CompanyInfo c ON c.edinetCode = f.edinetCode"
    )

    # Stock prices — latest price per company (via pre-aggregated subquery)
    join_clauses.append(
        "LEFT JOIN ("
        "SELECT Ticker, MAX([Date]) AS MaxDate "
        "FROM Stock_Prices GROUP BY Ticker"
        ") sp_max ON sp_max.Ticker = c.Company_Ticker"
    )
    join_clauses.append(
        "LEFT JOIN Stock_Prices s_p "
        "ON s_p.Ticker = sp_max.Ticker AND s_p.[Date] = sp_max.MaxDate"
    )

    # Screening & statement tables
    for table in sorted(needed_tables):
        if table in ("FinancialStatements", "CompanyInfo", "Stock_Prices"):
            continue
        alias = _TABLE_ALIAS.get(table)
        if alias is None:
            raise ValueError(f"Unknown table: {table!r}")
        safe_table = _safe_identifier(table)
        join_clauses.append(
            f"LEFT JOIN [{safe_table}] {alias} ON f.docID = {alias}.docID"
        )

    # --- Build WHERE ---
    where_parts: list[str] = []

    if period:
        where_parts.append("SUBSTR(f.periodEnd, 1, 4) = ?")
        params.append(period)

    for crit in criteria:
        table = crit["table"]
        column = crit["column"]
        op = OPERATOR_MAP[crit["operator"]]
        alias = _TABLE_ALIAS.get(table, table)
        safe_col = _safe_identifier(column)
        col_ref = f"{alias}.[{safe_col}]"
        comparison_mode = crit.get("comparison_mode", "fixed")

        if comparison_mode == "column":
            if op == "BETWEEN":
                raise ValueError("Dynamic criteria do not support BETWEEN")
            compare_table = crit.get("compare_table")
            compare_column = crit.get("compare_column")
            if not compare_table or not compare_column:
                raise ValueError(
                    "Dynamic criteria require compare_table and compare_column"
                )
            compare_alias = _TABLE_ALIAS.get(compare_table, compare_table)
            safe_compare_col = _safe_identifier(compare_column)
            compare_ref = f"{compare_alias}.[{safe_compare_col}]"
            where_parts.append(f"{col_ref} {op} {compare_ref}")
        elif op == "BETWEEN":
            where_parts.append(f"{col_ref} BETWEEN ? AND ?")
            params.append(crit["value"])
            params.append(crit["value2"])
        else:
            where_parts.append(f"{col_ref} {op} ?")
            params.append(crit["value"])

    where_clause = ""
    if where_parts:
        where_clause = "WHERE " + " AND ".join(where_parts)

    # --- Assemble ---
    sql = f"SELECT {select_clause}\n" + "\n".join(join_clauses)
    if where_clause:
        sql += f"\n{where_clause}"

    return sql, params


# ---------------------------------------------------------------------------
# Screening execution
# ---------------------------------------------------------------------------

def run_screening(
    db_path: str,
    criteria: list[dict],
    columns: list[str],
    period: str | None = None,
    sort_by: str | None = None,
    sort_order: str = "ASC",
    ranking_algorithm: str = "none",
    ranking_rules: list[dict] | None = None,
) -> pd.DataFrame:
    """Execute a screening query and return formatted results.

    Args:
        db_path: Path to the SQLite database.
        criteria: List of filter criteria dicts.
        columns: List of ``"Table.Column"`` strings to include.
        period: Optional year string to filter by.
        sort_by: Optional column name to sort results by.
        sort_order: ``"ASC"`` or ``"DESC"``.
        ranking_algorithm: Ranking method to apply after filtering.
        ranking_rules: Ranking rule dicts with table, column, weight,
            and direction.

    Returns:
        DataFrame with screening results.
    """
    available = get_available_metrics(db_path)
    ranking_columns = ranking_rules if ranking_algorithm != "none" else None
    query_columns, column_aliases, visible_columns = _build_query_column_plan(
        columns,
        ranking_columns,
    )
    sql, params = build_screening_query(
        criteria,
        query_columns,
        period,
        available_metrics=available,
        column_aliases=column_aliases,
    )

    logger.info("Running screening query with %d criteria", len(criteria))
    display_sql = _interpolate_sql(sql, params)
    logger.info("SQL query:\n%s", display_sql)

    conn = sqlite3.connect(db_path)
    try:
        df = pd.read_sql_query(sql, conn, params=params)
    finally:
        conn.close()
    logger.info("Query returned %d rows", len(df))

    df = apply_screening_ranking(df, ranking_algorithm, ranking_rules)

    # --- Sort ---
    if ranking_algorithm != "none" and ranking_rules and "ScreeningRank" in df.columns:
        effective_sort_by = "ScreeningRank"
        effective_sort_order = "ASC"
    else:
        effective_sort_by = sort_by
        effective_sort_order = sort_order

    if effective_sort_by and effective_sort_by in df.columns:
        ascending = effective_sort_order.upper() != "DESC"
        df = df.sort_values(
            by=effective_sort_by,
            ascending=ascending,
            na_position="last",
        )
        df = df.reset_index(drop=True)

    visible_result_columns = [col for col in visible_columns if col in df.columns]
    if "ScreeningRank" in df.columns and "ScreeningRank" not in visible_result_columns:
        visible_result_columns.append("ScreeningRank")
    if visible_result_columns:
        df = df.loc[:, visible_result_columns]

    logger.info("Screening returned %d rows", len(df))
    return df


def _resolve_result_column(df: pd.DataFrame, rule: dict) -> str | None:
    """Resolve the DataFrame column referenced by a ranking rule."""
    target = str(rule.get("column", ""))
    if target in df.columns:
        return target

    table = str(rule.get("table", ""))
    qualified = f"{table}.{target}" if table and target else ""
    if qualified and qualified in df.columns:
        return qualified

    target_lower = target.lower()
    matches = [col for col in df.columns if str(col).lower() == target_lower]
    if len(matches) == 1:
        return matches[0]
    return None


def _build_ranking_component(
    series: pd.Series,
    algorithm: str,
    direction: str,
) -> pd.Series:
    """Convert a numeric series into a 0..1 ranking component."""
    numeric = pd.to_numeric(series, errors="coerce")
    valid = numeric.dropna()
    result = pd.Series(0.0, index=series.index, dtype=float)
    if valid.empty:
        return result

    if algorithm == "weighted_percentile":
        ascending = direction != "lower"
        ranked = valid.rank(method="average", pct=True, ascending=ascending)
        result.loc[ranked.index] = ranked.astype(float)
        return result

    min_val = float(valid.min())
    max_val = float(valid.max())
    if math.isclose(min_val, max_val):
        result.loc[valid.index] = 1.0
        return result

    if direction == "lower":
        scaled = (max_val - valid) / (max_val - min_val)
    else:
        scaled = (valid - min_val) / (max_val - min_val)
    result.loc[scaled.index] = scaled.astype(float)
    return result


def apply_screening_ranking(
    df: pd.DataFrame,
    ranking_algorithm: str = "none",
    ranking_rules: list[dict] | None = None,
) -> pd.DataFrame:
    """Apply weighted ranking to screening results and add score columns."""
    if df is None or df.empty:
        return df
    if ranking_algorithm == "none" or not ranking_rules:
        return df
    if ranking_algorithm not in RANKING_ALGORITHMS:
        raise ValueError(f"Unknown ranking algorithm: {ranking_algorithm!r}")

    score = pd.Series(0.0, index=df.index, dtype=float)
    total_weight = 0.0
    applied_rules = 0

    for rule in ranking_rules:
        resolved_col = _resolve_result_column(df, rule)
        if not resolved_col:
            continue
        try:
            weight = float(rule.get("weight", 1.0))
        except (TypeError, ValueError):
            weight = 1.0
        if weight <= 0:
            continue
        direction = str(rule.get("direction", "higher")).lower()
        if direction not in RANKING_DIRECTIONS:
            direction = "higher"

        component = _build_ranking_component(
            df[resolved_col], ranking_algorithm, direction
        )
        score = score + component * weight
        total_weight += weight
        applied_rules += 1

    if applied_rules == 0 or total_weight <= 0:
        return df

    ranked = df.copy()
    ranked["ScreeningScore"] = score / total_weight
    ranked["ScreeningRank"] = (
        ranked["ScreeningScore"]
        .rank(method="dense", ascending=False)
        .astype(int)
    )
    return ranked


def _resolve_backtest_export_frame(
    df: pd.DataFrame,
    year: str,
    max_companies: int,
) -> pd.DataFrame:
    """Convert screening results into the CSV format used by backtest sets."""
    if df.empty:
        return pd.DataFrame()

    ticker_col = _resolve_matching_column(list(df.columns), ["Company_Ticker", "Ticker"])
    if not ticker_col:
        raise ValueError(
            "Backtest export requires CompanyInfo.Company_Ticker in the screening results."
        )

    company_name_col = _resolve_matching_column(
        list(df.columns), COMPANYINFO_NAME_CANDIDATES
    )
    industry_col = _resolve_matching_column(
        list(df.columns), COMPANYINFO_INDUSTRY_CANDIDATES
    )
    edinet_col = _resolve_matching_column(
        list(df.columns), ["edinetCode", "EdinetCode"]
    )
    period_end_col = _resolve_matching_column(list(df.columns), ["periodEnd"])

    selected = df.copy()
    if "ScreeningRank" in selected.columns:
        selected = selected.sort_values(by=["ScreeningRank", ticker_col])
    elif "ScreeningScore" in selected.columns:
        selected = selected.sort_values(
            by=["ScreeningScore", ticker_col],
            ascending=[False, True],
            na_position="last",
        )
    else:
        selected = selected.sort_values(by=ticker_col)

    selected = selected.head(max_companies).reset_index(drop=True)
    if selected.empty:
        return pd.DataFrame()

    weight = 1.0 / len(selected)
    export_df = pd.DataFrame(
        {
            "Year": [year] * len(selected),
            "Tickers": selected[ticker_col].astype(str),
            "Type": ["weight"] * len(selected),
            "Amount": [weight] * len(selected),
        }
    )

    if edinet_col:
        export_df["EdinetCode"] = selected[edinet_col].astype(str)
    if company_name_col:
        export_df["CompanyName"] = selected[company_name_col].astype(str)
    if industry_col:
        export_df["Industry"] = selected[industry_col].astype(str)
    if period_end_col:
        export_df["PeriodEnd"] = selected[period_end_col].astype(str)
    if "ScreeningRank" in selected.columns:
        export_df["ScreeningRank"] = selected["ScreeningRank"].astype(int)
    if "ScreeningScore" in selected.columns:
        export_df["ScreeningScore"] = selected["ScreeningScore"].astype(float)

    return export_df


def export_screening_to_backtest_csv(
    db_path: str,
    criteria: list[dict],
    columns: list[str],
    output_path: str,
    period: str | None = None,
    max_companies: int = 25,
    ranking_algorithm: str = "none",
    ranking_rules: list[dict] | None = None,
    historical: bool = False,
) -> str:
    """Export screening results in the CSV format used by run_backtest_set."""
    if max_companies <= 0:
        raise ValueError("max_companies must be greater than 0")

    available = get_available_metrics(db_path)
    export_columns = list(columns)
    required_columns = get_default_columns(available)
    company_cols = available.get("CompanyInfo", [])
    ticker_col = _resolve_matching_column(
        company_cols, COMPANYINFO_TICKER_CANDIDATES
    ) or "Company_Ticker"
    required_columns.extend([
        f"CompanyInfo.{ticker_col}",
        "FinancialStatements.periodEnd",
    ])
    for rule in ranking_rules or []:
        ref = f"{rule.get('table', '')}.{rule.get('column', '')}".strip(".")
        if ref:
            required_columns.append(ref)
    export_columns = list(dict.fromkeys([*required_columns, *export_columns]))

    if historical:
        years = get_available_periods(db_path)
    else:
        if not period:
            raise ValueError("A period must be selected for non-historical export")
        years = [period]

    frames: list[pd.DataFrame] = []
    for year in years:
        df = run_screening(
            db_path,
            criteria,
            export_columns,
            period=year,
            ranking_algorithm=ranking_algorithm,
            ranking_rules=ranking_rules,
        )
        export_df = _resolve_backtest_export_frame(df, year, max_companies)
        if not export_df.empty:
            frames.append(export_df)

    if not frames:
        raise ValueError("No companies matched the screening criteria for export")

    combined = pd.concat(frames, ignore_index=True)
    resolved = os.path.abspath(output_path)
    os.makedirs(os.path.dirname(resolved), exist_ok=True)
    combined.to_csv(resolved, index=False)
    logger.info(
        "Exported backtest company list with %d rows to %s",
        len(combined),
        resolved,
    )
    return resolved


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------

def export_screening_to_csv(df: pd.DataFrame, output_path: str) -> str:
    """Export screening results to a CSV file.

    Args:
        df: Screening results DataFrame.
        output_path: Destination file path.

    Returns:
        The resolved output path.
    """
    resolved = os.path.abspath(output_path)
    os.makedirs(os.path.dirname(resolved), exist_ok=True)
    df.to_csv(resolved, index=False)
    logger.info("Exported %d rows to %s", len(df), resolved)
    return resolved


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------

def _format_grouped_number(value: float, decimals: int | None = None) -> str:
    """Format a numeric value with thousands separators and trimmed decimals."""
    if decimals is None:
        decimals = 0 if math.isclose(value, round(value), abs_tol=1e-9) else 3
    formatted = f"{value:,.{decimals}f}"
    if decimals > 0:
        formatted = formatted.rstrip("0").rstrip(".")
    return formatted


def _infer_column_format(column_name: str) -> str | None:
    """Infer display formatting from a screening column name."""
    lowered = str(column_name).lower()
    for pattern, rule in FORMAT_RULES.items():
        if pattern.lower() in lowered:
            return rule
    return None


def format_financial_value(value, column_name: str, formatted: bool = False) -> str:
    """Format a numeric value for display based on column semantics.

    Args:
        value: Raw numeric value (may be ``None`` or ``NaN``).
        column_name: Column name used to infer formatting rules.
        formatted: When ``True``, apply display formatting.

    Returns:
        Formatted string.
    """
    if value is None or pd.isna(value):
        return "—"

    if not formatted:
        return str(value)

    if isinstance(value, bool):
        return str(value)

    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        return str(value)

    if column_name == "ScreeningRank":
        return str(int(round(numeric_value)))
    if column_name == "ScreeningScore":
        return _format_grouped_number(numeric_value, 3)

    column_format = _infer_column_format(column_name)
    if column_format == "percent":
        return f"{numeric_value * 100:,.2f}%"
    if column_format == "currency":
        decimals = 0 if math.isclose(numeric_value, round(numeric_value), abs_tol=1e-9) else 2
        return _format_grouped_number(numeric_value, decimals)
    if column_format == "ratio":
        return _format_grouped_number(numeric_value, 2)

    return _format_grouped_number(numeric_value)


def _sanitize_screening_name(name: str) -> str:
    """Return a filesystem-safe screening name stem."""
    safe_name = re.sub(r'[^\w\s-]', '', name).strip()
    if not safe_name:
        raise ValueError("Screening name must not be empty")
    return safe_name


def _saved_screening_display_name(file_path: Path) -> str:
    """Return the user-facing display name for a saved screening file."""
    try:
        with open(file_path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        display_name = str(data.get("name", "")).strip()
        if display_name:
            return display_name
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        pass
    return file_path.stem


def _find_saved_screening_path(name: str, save_path: Path) -> Path | None:
    """Return the saved-screen file path for a display name."""
    target_name = str(name).strip()
    if not target_name or not save_path.exists():
        return None

    for file_path in sorted(save_path.glob("*.json")):
        if _saved_screening_display_name(file_path) == target_name:
            return file_path
    return None


def _next_saved_screening_path(save_path: Path, safe_name: str) -> Path:
    """Return the next available file path for a sanitized screening name."""
    candidate = save_path / f"{safe_name}.json"
    suffix = 2
    while candidate.exists():
        candidate = save_path / f"{safe_name}-{suffix}.json"
        suffix += 1
    return candidate


# ---------------------------------------------------------------------------
# Persistence — saved screening criteria
# ---------------------------------------------------------------------------

def save_screening_criteria(
    name: str,
    criteria: list[dict],
    columns: list[str],
    period: str | None,
    save_dir: str,
    ranking_algorithm: str = "none",
    ranking_rules: list[dict] | None = None,
) -> Path:
    """Persist a named screening configuration as JSON.

    Args:
        name: Screening name (used as filename stem).
        criteria: List of criteria dicts.
        columns: List of selected column references.
        period: Optional period filter.
        save_dir: Directory to save into.

    Returns:
        Path to the saved JSON file.
    """
    save_path = Path(save_dir)
    save_path.mkdir(parents=True, exist_ok=True)

    display_name = str(name).strip()
    safe_name = _sanitize_screening_name(display_name)

    data = {
        "name": display_name,
        "criteria": criteria,
        "columns": columns,
        "period": period,
        "ranking_algorithm": ranking_algorithm,
        "ranking_rules": ranking_rules or [],
    }

    existing_path = _find_saved_screening_path(display_name, save_path)
    file_path = existing_path or _next_saved_screening_path(save_path, safe_name)
    with open(file_path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2, ensure_ascii=False)

    logger.info("Saved screening criteria '%s' to %s", display_name, file_path)
    return file_path


def load_screening_criteria(name: str, save_dir: str) -> dict:
    """Load a previously saved screening configuration.

    Args:
        name: Screening display name.
        save_dir: Directory to load from.

    Returns:
        Dict with keys ``criteria``, ``columns``, ``period``.

    Raises:
        FileNotFoundError: If the named screening does not exist.
    """
    save_path = Path(save_dir)
    file_path = _find_saved_screening_path(name, save_path)
    if file_path is None:
        raise FileNotFoundError(f"Screening '{name}' not found in {save_path}")

    with open(file_path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def list_saved_screenings(save_dir: str) -> list[str]:
    """Return sorted list of saved screening names.

    Args:
        save_dir: Directory containing saved screening JSON files.

    Returns:
        Sorted list of screening display names.
    """
    save_path = Path(save_dir)
    if not save_path.exists():
        return []
    names = [_saved_screening_display_name(f) for f in save_path.glob("*.json")]
    return sorted(names, key=str.casefold)


def delete_screening_criteria(name: str, save_dir: str) -> None:
    """Delete a saved screening configuration.

    Args:
        name: Screening display name.
        save_dir: Directory containing saved screening JSON files.

    Raises:
        FileNotFoundError: If the named screening does not exist.
    """
    save_path = Path(save_dir)
    file_path = _find_saved_screening_path(name, save_path)
    if file_path is None:
        raise FileNotFoundError(f"Screening '{name}' not found in {save_path}")
    file_path.unlink()
    logger.info("Deleted screening criteria '%s'", name)


# ---------------------------------------------------------------------------
# Persistence — screening history
# ---------------------------------------------------------------------------

def save_screening_history(entry: dict, history_path: str) -> None:
    """Append a screening run record to the history file.

    Each entry is stored as one JSON object per line (JSON-lines format).

    Args:
        entry: Dict with screening run details (timestamp, criteria, etc.).
        history_path: Path to the history file.
    """
    path = Path(history_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    # Add timestamp if not present
    if "timestamp" not in entry:
        entry["timestamp"] = datetime.now().isoformat()

    with open(path, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry, ensure_ascii=False) + "\n")

    logger.info("Saved screening history entry")


def load_screening_history(history_path: str) -> list[dict]:
    """Load screening history from a JSON-lines file.

    Args:
        history_path: Path to the history file.

    Returns:
        List of history entry dicts, most recent first.
    """
    path = Path(history_path)
    if not path.exists():
        return []

    entries: list[dict] = []
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                try:
                    entries.append(json.loads(line))
                except json.JSONDecodeError:
                    logger.warning("Skipping malformed history line")
                    continue

    # Most recent first
    entries.reverse()
    return entries
