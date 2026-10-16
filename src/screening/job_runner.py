"""Run one screening request from plain dicts.

Shared by the web layer (which converts request models to these dicts) and the
screening job handler on a worker, which has no access to app.db and so cannot
import the API module. Nothing here writes the market database.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import pandas as pd

from src import screening as _screening
from src.screening.display_formats import result_column_formats


def criteria_to_dicts(criteria: Iterable[dict[str, Any]]) -> list[dict]:
    """Screening-layer criteria from request criteria dicts, dropping unset values."""
    result = []
    for c in criteria:
        d: dict[str, Any] = {
            "table": c.get("table", ""),
            "column": c.get("column", ""),
            "operator": c.get("operator"),
            "value": c.get("value"),
            "field_type": c.get("field_type", "num"),
            "comparison_mode": c.get("comparison_mode", "fixed"),
        }
        if d["comparison_mode"] == "recent_split":
            d.update(
                split_action=c.get("split_action", "exclude"),
                split_status=c.get("split_status", "confirmed"),
                split_date_operator=c.get("split_date_operator", "on_or_after"),
            )
            if c.get("split_window_days") is not None:
                d["split_window_days"] = c["split_window_days"]
        for key in (
            "value2", "values", "compare_table", "compare_column", "offset",
            "right_side", "left_side", "left_expression",
        ):
            if c.get(key) is not None:
                d[key] = c[key]
        if c.get("group"):
            d["group"] = c["group"]
            d["group_match"] = c.get("group_match", "any")
        if c.get("enabled", True) is False:
            d["enabled"] = False
        result.append(d)
    return result


def ranking_rules_to_dicts(rules: Iterable[dict[str, Any]]) -> list[dict]:
    return [
        {
            "table": r.get("table"),
            "column": r.get("column"),
            "weight": r.get("weight", 1.0),
            "direction": r.get("direction", "higher"),
        }
        for r in rules
    ]


def computed_column_specs(columns: Iterable[dict[str, Any]]) -> list[dict]:
    """Screening-layer values of computed-column request dicts."""
    return [
        {
            "name": column.get("name"),
            "formula_type": column.get("formula_type", "price_ratio"),
            "expression_tokens": column.get("expression_tokens"),
            "numerator_table": column.get("numerator_table", "Stock_Prices"),
            "numerator_column": column.get("numerator_column", "Price"),
            "denominator_table": column.get("denominator_table", ""),
            "denominator_column": column.get("denominator_column", ""),
            "formula": column.get("formula"),
            "format": column.get("format"),
        }
        for column in columns
    ]


def computed_column_formats(columns: Iterable[dict[str, Any]]) -> dict[str, str]:
    """Declared formats of derived result columns, keyed by their names."""
    return {
        str(column["name"]): str(column["format"])
        for column in columns
        if column.get("format") and column["format"] != "number"
    }


def dataframe_to_json(df: pd.DataFrame | None) -> dict:
    """A JSON-safe ``columns``/``rows``/``row_count`` dict (NaN becomes null)."""
    if df is None or df.empty:
        return {"columns": [], "rows": [], "row_count": 0}
    clean = df.where(pd.notna(df), None)
    return {
        "columns": [str(c) for c in clean.columns],
        "rows": clean.values.tolist(),
        "row_count": len(clean),
    }


def validate_screening_request(db_path: str, request: dict[str, Any]) -> None:
    """Build the screen's SQL without running it; ``ValueError`` for an invalid screen.

    Lets the web server answer 400 at once instead of queueing a job that cannot run.
    """
    ranking = ranking_rules_to_dicts(request.get("ranking_rules") or [])
    algorithm = request.get("ranking_algorithm") or "none"
    query_columns, column_aliases, _visible = _screening._build_query_column_plan(
        list(request.get("columns") or []), ranking if algorithm != "none" else None
    )
    _screening.build_screening_query(
        criteria_to_dicts(request.get("criteria") or []),
        query_columns,
        request.get("period"),
        screening_date=request.get("screening_date"),
        available_metrics=_screening.get_available_metrics(db_path),
        column_aliases=column_aliases,
        computed_columns=computed_column_specs(request.get("computed_columns") or []),
        use_adjusted_price=True,
        criteria_match=request.get("criteria_match") or "all",
    )


def run_screening_request(
    db_path: str,
    request: dict[str, Any],
    company_tags: Iterable[tuple[str, str]] | None = None,
) -> dict[str, Any]:
    """Run a ``ScreeningRunRequest`` dict and return the body ``POST /api/screening/run`` returns."""
    computed = list(request.get("computed_columns") or [])
    available = _screening.get_available_metrics(db_path)
    frame = _screening.run_screening(
        db_path=db_path,
        criteria=criteria_to_dicts(request.get("criteria") or []),
        columns=list(request.get("columns") or []),
        period=request.get("period"),
        screening_date=request.get("screening_date"),
        sort_by=request.get("sort_by"),
        sort_order=request.get("sort_order") or "DESC",
        ranking_algorithm=request.get("ranking_algorithm") or "none",
        ranking_rules=ranking_rules_to_dicts(request.get("ranking_rules") or []),
        computed_columns=computed_column_specs(computed),
        available_metrics=available,
        criteria_match=request.get("criteria_match") or "all",
        company_tags=company_tags,
    )
    result = dataframe_to_json(frame)
    result["error"] = None
    result["column_formats"] = {
        **result_column_formats(list(request.get("columns") or []), db_path),
        **computed_column_formats(computed),
    }
    return result
