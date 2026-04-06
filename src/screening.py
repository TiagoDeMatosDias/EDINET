"""Screening module: query, filter, and export corporate financial data.

Provides functions to introspect a SQLite database produced by the EDINET
pipeline, build parameterised screening queries, execute them, and
persist/load screening criteria and history.
"""

import json
import logging
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


# ---------------------------------------------------------------------------
# Database introspection
# ---------------------------------------------------------------------------

def get_available_metrics(db_path: str) -> dict[str, list[str]]:
    """Return a dict of ``{table_name: [column_names]}`` for screening tables.

    Only tables that exist in the database and are in the allow-list
    ``SCREENING_TABLES`` are included.  The ``docID`` column is excluded
    from each table's column list.

    Args:
        db_path: Path to the SQLite database.

    Returns:
        Dict mapping table names to their column lists.
    """
    result: dict[str, list[str]] = {}
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.cursor()
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

    Also allows columns from core tables (CompanyInfo, FinancialStatements,
    Stock_Prices) which are always joined.
    """
    parts = col.split(".", 1)
    if len(parts) != 2:
        return False
    table, column = parts
    # Core tables are always available
    if table in ("CompanyInfo", "FinancialStatements", "Stock_Prices"):
        return True
    return table in available and column in available[table]


_SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _safe_identifier(name: str) -> str:
    """Validate and return a safe SQL identifier.

    Raises:
        ValueError: If the name contains unsafe characters.
    """
    if not _SAFE_IDENTIFIER_RE.match(name):
        raise ValueError(f"Invalid SQL identifier: {name!r}")
    return name


def build_screening_query(
    criteria: list[dict],
    columns: list[str],
    period: str | None = None,
    available_metrics: dict[str, list[str]] | None = None,
) -> tuple[str, list]:
    """Build a parameterised SQL query for screening.

    Args:
        criteria: List of filter dicts, each with keys ``table``, ``column``,
            ``operator``, ``value`` (and optionally ``value2`` for BETWEEN).
        columns: List of ``"Table.Column"`` strings to SELECT.
        period: Optional year string to filter ``periodEnd``.
        available_metrics: Output of ``get_available_metrics`` for validation.
            If ``None``, validation of screening-table columns is skipped.

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

    # Validate columns against available metrics
    if available_metrics is not None:
        for col in columns:
            if not _validate_column_ref(col, available_metrics):
                raise ValueError(f"Invalid column reference: {col!r}")
        for crit in criteria:
            table = crit["table"]
            column = crit["column"]
            if table in SCREENING_TABLES:
                if table not in available_metrics:
                    raise ValueError(f"Table not available: {table!r}")
                if column not in available_metrics[table]:
                    raise ValueError(
                        f"Column {column!r} not in table {table!r}"
                    )

    # Validate operators
    for crit in criteria:
        op = crit["operator"]
        if op not in OPERATOR_MAP:
            raise ValueError(f"Invalid operator: {op!r}")

    # --- Build SELECT ---
    select_parts: list[str] = []
    for col in columns:
        table, column = col.split(".", 1)
        alias = _TABLE_ALIAS.get(table, table)
        safe_col = _safe_identifier(column)
        select_parts.append(f"{alias}.[{safe_col}]")

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

        if op == "BETWEEN":
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
) -> pd.DataFrame:
    """Execute a screening query and return formatted results.

    Args:
        db_path: Path to the SQLite database.
        criteria: List of filter criteria dicts.
        columns: List of ``"Table.Column"`` strings to include.
        period: Optional year string to filter by.
        sort_by: Optional column name to sort results by.
        sort_order: ``"ASC"`` or ``"DESC"``.

    Returns:
        DataFrame with screening results.
    """
    available = get_available_metrics(db_path)
    sql, params = build_screening_query(
        criteria, columns, period, available_metrics=available
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

    # --- Sort ---
    if sort_by and sort_by in df.columns:
        ascending = sort_order.upper() != "DESC"
        df = df.sort_values(by=sort_by, ascending=ascending, na_position="last")
        df = df.reset_index(drop=True)

    logger.info("Screening returned %d rows", len(df))
    return df


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

def format_financial_value(value, column_name: str) -> str:
    """Format a numeric value for display based on column semantics.

    Args:
        value: Raw numeric value (may be ``None`` or ``NaN``).
        column_name: Column name used to infer formatting rules.

    Returns:
        Formatted string.
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "—"

    return str(value)


# ---------------------------------------------------------------------------
# Persistence — saved screening criteria
# ---------------------------------------------------------------------------

def save_screening_criteria(
    name: str,
    criteria: list[dict],
    columns: list[str],
    period: str | None,
    save_dir: str,
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

    # Sanitise name for filesystem safety
    safe_name = re.sub(r'[^\w\s-]', '', name).strip()
    if not safe_name:
        raise ValueError("Screening name must not be empty")

    data = {
        "name": name,
        "criteria": criteria,
        "columns": columns,
        "period": period,
    }

    file_path = save_path / f"{safe_name}.json"
    with open(file_path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2, ensure_ascii=False)

    logger.info("Saved screening criteria '%s' to %s", name, file_path)
    return file_path


def load_screening_criteria(name: str, save_dir: str) -> dict:
    """Load a previously saved screening configuration.

    Args:
        name: Screening name (filename stem).
        save_dir: Directory to load from.

    Returns:
        Dict with keys ``criteria``, ``columns``, ``period``.

    Raises:
        FileNotFoundError: If the named screening does not exist.
    """
    file_path = Path(save_dir) / f"{name}.json"
    if not file_path.exists():
        raise FileNotFoundError(f"Screening '{name}' not found at {file_path}")

    with open(file_path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def list_saved_screenings(save_dir: str) -> list[str]:
    """Return sorted list of saved screening names.

    Args:
        save_dir: Directory containing saved screening JSON files.

    Returns:
        Sorted list of screening names (file stems).
    """
    save_path = Path(save_dir)
    if not save_path.exists():
        return []
    return sorted(f.stem for f in save_path.glob("*.json"))


def delete_screening_criteria(name: str, save_dir: str) -> None:
    """Delete a saved screening configuration.

    Args:
        name: Screening name (filename stem).
        save_dir: Directory containing saved screening JSON files.

    Raises:
        FileNotFoundError: If the named screening does not exist.
    """
    file_path = Path(save_dir) / f"{name}.json"
    if not file_path.exists():
        raise FileNotFoundError(f"Screening '{name}' not found at {file_path}")
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
