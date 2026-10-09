"""Names of the rolling tables and columns ``generate_rolling_metrics`` writes."""

from __future__ import annotations

import re

ROLLING_WINDOWS = (2, 3, 5, 10)
_ROLLING_TABLE_SUFFIX = "_Rolling"
_ROLLING_COLUMN = re.compile(r"^(?P<metric>.+)_(?P<kind>Average|Growth)_(?P<window>\d+)_Year$")


def rolling_average_column(metric_column: str, window: int) -> str:
    return f"{metric_column}_Average_{window}_Year"


def rolling_growth_column(metric_column: str, window: int) -> str:
    """Compound annual growth over ``window`` years, stored as a fraction."""
    return f"{metric_column}_Growth_{window}_Year"


def rolling_table_name(source_table: str) -> str:
    return f"{source_table}{_ROLLING_TABLE_SUFFIX}"


def rolling_source_table(table: str) -> str | None:
    """The source table a rolling table was generated from, if it is one."""
    if table.endswith(_ROLLING_TABLE_SUFFIX) and len(table) > len(_ROLLING_TABLE_SUFFIX):
        return table[: -len(_ROLLING_TABLE_SUFFIX)]
    return None


def parse_rolling_column(column: str) -> tuple[str, str] | None:
    """``(source metric column, "average" | "growth")`` for a rolling column name."""
    match = _ROLLING_COLUMN.match(column)
    if match is None:
        return None
    return match["metric"], match["kind"].lower()
