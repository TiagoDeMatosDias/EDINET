"""Screening API routes.

All database interactions go through ``src.screening`` functions.
The frontend never touches the database directly.
"""

from __future__ import annotations

import io
import logging
from pathlib import Path
from typing import Any

import pandas as pd
from fastapi import APIRouter, Body, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from src import screening as _screening
from src.orchestrator.common.db_config import get_db2

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/screening", tags=["screening"])

# ---------------------------------------------------------------------------
# Persistence paths (same as Tk UI controllers)
# ---------------------------------------------------------------------------

_STATE_DIR = (
    Path(__file__).resolve().parents[3] / "config" / "state"
)
_SAVED_SCREENINGS_DIR = _STATE_DIR / "saved_screenings"
_SCREENING_HISTORY_PATH = _STATE_DIR / "screening_history.jsonl"


def _screening_save_dir() -> str:
    _SAVED_SCREENINGS_DIR.mkdir(parents=True, exist_ok=True)
    return str(_SAVED_SCREENINGS_DIR)


def _screening_history_path() -> str:
    _SCREENING_HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
    return str(_SCREENING_HISTORY_PATH)


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class ScreeningCriterion(BaseModel):
    table: str = Field(default="", description="Table name (e.g. CompanyInfo, PerShare). Not used for full_expression mode.")
    column: str = Field(default="", description="Column name within the table. Not used for full_expression mode.")
    operator: str = Field(..., description="Operator: >, >=, <, <=, =, !=, BETWEEN, IN, LIKE")
    value: Any = Field(default=None, description="Comparison value (required for fixed/LIKE mode)")
    value2: Any = Field(default=None, description="Second value for BETWEEN operator")
    values: list[Any] | None = Field(default=None, description="Value list for IN operator")
    field_type: str = Field(default="num", description="Value type hint: num, text, percent")
    comparison_mode: str = Field(default="fixed", description="fixed, column, expression, in, like, stock_price, or full_expression")
    compare_table: str | None = Field(default=None, description="Comparison table (column mode)")
    compare_column: str | None = Field(default=None, description="Comparison column (column mode)")
    offset: float | None = Field(default=None, description="Numeric offset for column comparison")
    right_side: list[dict] | None = Field(default=None, description="Expression tokens for expression mode")
    left_side: list[dict] | None = Field(default=None, description="Left-side expression tokens for full_expression mode")
    left_expression: str | None = Field(default=None, description="Left-side arithmetic expression for stock_price mode")


class RankingRule(BaseModel):
    table: str = Field(..., description="Table name")
    column: str = Field(..., description="Column name")
    weight: float = Field(default=1.0, description="Rule weight")
    direction: str = Field(default="higher", description="higher or lower")


class ComputedColumn(BaseModel):
    name: str = Field(..., description="Display name for the computed column")
    formula_type: str = Field(default="price_ratio", description="Formula type")
    numerator_table: str = Field(default="Stock_Prices", description="Numerator table")
    numerator_column: str = Field(default="Price", description="Numerator column")
    denominator_table: str = Field(default="", description="Denominator table")
    denominator_column: str = Field(default="", description="Denominator column")
    formula: str | None = Field(default=None, description="Custom SQL expression using table aliases")


class ScreeningRunRequest(BaseModel):
    db_path: str = Field(..., description="Absolute path to the SQLite database")
    criteria: list[ScreeningCriterion] = Field(default_factory=list)
    columns: list[str] = Field(default_factory=list)
    computed_columns: list[ComputedColumn] = Field(default_factory=list)
    period: str | None = Field(default=None, description="Year filter (e.g. '2020')")
    screening_date: str | None = Field(default=None, description="Point-in-time date (YYYY-MM-DD)")
    sort_by: str | None = Field(default=None)
    sort_order: str = Field(default="DESC")
    ranking_algorithm: str = Field(default="none")
    ranking_rules: list[RankingRule] = Field(default_factory=list)


class ScreeningSaveRequest(BaseModel):
    name: str = Field(..., description="Screening configuration name")
    criteria: list[ScreeningCriterion] = Field(default_factory=list)
    columns: list[str] = Field(default_factory=list)
    computed_columns: list[ComputedColumn] = Field(default_factory=list)
    period: str | None = Field(default=None)
    screening_date: str | None = Field(default=None)
    ranking_algorithm: str = Field(default="none")
    ranking_rules: list[RankingRule] = Field(default_factory=list)


class ScreeningExportRequest(BaseModel):
    db_path: str = Field(...)
    criteria: list[ScreeningCriterion] = Field(default_factory=list)
    columns: list[str] = Field(default_factory=list)
    computed_columns: list[ComputedColumn] = Field(default_factory=list)
    period: str | None = Field(default=None)
    screening_date: str | None = Field(default=None)
    ranking_algorithm: str = Field(default="none")
    ranking_rules: list[RankingRule] = Field(default_factory=list)
    format: str = Field(default="csv", description="csv or backtest")
    max_companies: int = Field(default=25)
    historical: bool = Field(default=False)


class ScreeningHistoryEntry(BaseModel):
    name: str | None = Field(default=None)
    criteria_count: int = Field(default=0)
    result_count: int = Field(default=0)
    period: str | None = Field(default=None)
    screening_date: str | None = Field(default=None)
    db_path: str | None = Field(default=None)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _validate_db_path(db_path: str) -> str:
    """Validate a database path exists on the server."""
    path = Path(db_path)
    if not path.exists():
        raise HTTPException(
            status_code=400,
            detail=f"Database not found: {db_path}",
        )
    return str(path.resolve())


def _criteria_to_dicts(criteria: list[ScreeningCriterion]) -> list[dict]:
    """Convert Pydantic criteria to plain dicts, dropping None values."""
    result = []
    for c in criteria:
        d = {
            "table": c.table,
            "column": c.column,
            "operator": c.operator,
            "value": c.value,
            "field_type": c.field_type,
            "comparison_mode": c.comparison_mode,
        }
        if c.value2 is not None:
            d["value2"] = c.value2
        if c.values is not None:
            d["values"] = c.values
        if c.compare_table is not None:
            d["compare_table"] = c.compare_table
        if c.compare_column is not None:
            d["compare_column"] = c.compare_column
        if c.offset is not None:
            d["offset"] = c.offset
        if c.right_side is not None:
            d["right_side"] = c.right_side
        if c.left_side is not None:
            d["left_side"] = c.left_side
        if c.left_expression is not None:
            d["left_expression"] = c.left_expression
        result.append(d)
    return result


def _ranking_rules_to_dicts(rules: list[RankingRule]) -> list[dict]:
    return [
        {"table": r.table, "column": r.column, "weight": r.weight, "direction": r.direction}
        for r in rules
    ]


def _df_to_json(df: pd.DataFrame) -> dict:
    """Convert a DataFrame to a JSON-safe dict with columns and rows."""
    if df is None or df.empty:
        return {"columns": [], "rows": [], "row_count": 0}
    # Replace NaN with None for JSON serialization
    clean = df.where(pd.notna(df), None)
    return {
        "columns": [str(c) for c in clean.columns],
        "rows": clean.values.tolist(),
        "row_count": len(clean),
    }


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/db-path")
def get_default_db_path() -> dict:
    """Return the default screening database path (DB2)."""
    try:
        return {"db_path": get_db2()}
    except Exception as e:
        logger.error("Failed to get default DB path: %s", str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/metrics")
def get_metrics(db_path: str = Query(..., description="Path to SQLite database")) -> dict:
    """Return available screening tables and their columns."""
    try:
        resolved = _validate_db_path(db_path)
        metrics = _screening.get_available_metrics(resolved)
        return {"tables": metrics}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get metrics: %s", str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/periods")
def get_periods(db_path: str = Query(..., description="Path to SQLite database")) -> dict:
    """Return available period years."""
    try:
        resolved = _validate_db_path(db_path)
        periods = _screening.get_available_periods(resolved)
        return {"periods": periods}
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get periods: %s", str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/formulas")
def get_available_formulas() -> dict:
    """Return available pre-defined valuation/computed formulas."""
    formulas = [
        {
            "name": "P/E Ratio",
            "formula_type": "price_ratio",
            "numerator_table": "Stock_Prices",
            "numerator_column": "Price",
            "denominator_table": "PerShare",
            "denominator_column": "EPS",
            "format": "ratio",
        },
        {
            "name": "P/B Ratio",
            "formula_type": "price_ratio",
            "numerator_table": "Stock_Prices",
            "numerator_column": "Price",
            "denominator_table": "PerShare",
            "denominator_column": "BookValue",
            "format": "ratio",
        },
        {
            "name": "P/S Ratio",
            "formula_type": "price_ratio",
            "numerator_table": "Stock_Prices",
            "numerator_column": "Price",
            "denominator_table": "PerShare",
            "denominator_column": "Sales",
            "format": "ratio",
        },
        {
            "name": "Dividend Yield",
            "formula_type": "price_ratio",
            "numerator_table": "PerShare",
            "numerator_column": "Dividends",
            "denominator_table": "Stock_Prices",
            "denominator_column": "Price",
            "format": "percent",
        },
        {
            "name": "Earnings Yield",
            "formula_type": "price_ratio",
            "numerator_table": "PerShare",
            "numerator_column": "EPS",
            "denominator_table": "Stock_Prices",
            "denominator_column": "Price",
            "format": "percent",
        },
    ]
    return {"formulas": formulas}


@router.post("/run")
def run_screening_endpoint(request: ScreeningRunRequest = Body(...)) -> dict:
    """Run a screening query and return results with the generated SQL."""
    try:
        resolved = _validate_db_path(request.db_path)
        criteria_dicts = _criteria_to_dicts(request.criteria)
        ranking_dicts = _ranking_rules_to_dicts(request.ranking_rules)

        all_columns = list(request.columns)
        computed_specs = []
        for cc in request.computed_columns:
            computed_specs.append({
                "name": cc.name,
                "formula_type": cc.formula_type,
                "numerator_table": cc.numerator_table,
                "numerator_column": cc.numerator_column,
                "denominator_table": cc.denominator_table,
                "denominator_column": cc.denominator_column,
                "formula": cc.formula,
            })

        # Build the SQL for display before executing
        available = _screening.get_available_metrics(resolved)
        ranking_columns = ranking_dicts if request.ranking_algorithm != "none" else None
        query_columns, col_aliases, _ = _screening._build_query_column_plan(
            all_columns, ranking_columns
        )
        display_sql, display_params = _screening.build_screening_query(
            criteria_dicts,
            query_columns,
            request.period,
            screening_date=request.screening_date,
            available_metrics=available,
            column_aliases=col_aliases,
            computed_columns=computed_specs,
        )
        sql_display = _screening._interpolate_sql(display_sql, display_params)

        df = _screening.run_screening(
            db_path=resolved,
            criteria=criteria_dicts,
            columns=all_columns,
            period=request.period,
            screening_date=request.screening_date,
            sort_by=request.sort_by,
            sort_order=request.sort_order,
            ranking_algorithm=request.ranking_algorithm,
            ranking_rules=ranking_dicts,
            computed_columns=computed_specs,
        )

        result = _df_to_json(df)
        result["error"] = None
        result["sql_display"] = sql_display
        logger.info("Screening returned %d rows", result["row_count"])
        return result

    except HTTPException:
        raise
    except ValueError as e:
        logger.warning("Screening validation error: %s", str(e))
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Screening run failed: %s", str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/saved")
def list_saved() -> dict:
    """List saved screening configurations."""
    try:
        names = _screening.list_saved_screenings(_screening_save_dir())
        return {"screenings": names}
    except Exception as e:
        logger.error("Failed to list saved screenings: %s", str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/saved/{name}")
def load_saved(name: str) -> dict:
    """Load a saved screening configuration."""
    try:
        return _screening.load_screening_criteria(name, _screening_save_dir())
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"Screening '{name}' not found")
    except Exception as e:
        logger.error("Failed to load screening '%s': %s", name, str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/save")
def save_screening(request: ScreeningSaveRequest = Body(...)) -> dict:
    """Save a screening configuration."""
    try:
        criteria_dicts = _criteria_to_dicts(request.criteria)
        ranking_dicts = _ranking_rules_to_dicts(request.ranking_rules)
        path = _screening.save_screening_criteria(
            name=request.name,
            criteria=criteria_dicts,
            columns=request.columns,
            period=request.period,
            save_dir=_screening_save_dir(),
            ranking_algorithm=request.ranking_algorithm,
            ranking_rules=ranking_dicts,
        )
        return {"saved": True, "path": str(path)}
    except Exception as e:
        logger.error("Failed to save screening: %s", str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/saved/{name}")
def delete_saved(name: str) -> dict:
    """Delete a saved screening configuration."""
    try:
        _screening.delete_screening_criteria(name, _screening_save_dir())
        return {"deleted": True}
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"Screening '{name}' not found")
    except Exception as e:
        logger.error("Failed to delete screening '%s': %s", name, str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/history")
def get_history() -> dict:
    """Return screening run history (most recent first)."""
    try:
        entries = _screening.load_screening_history(_screening_history_path())
        return {"entries": entries}
    except Exception as e:
        logger.error("Failed to load screening history: %s", str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/history")
def save_history(entry: ScreeningHistoryEntry = Body(...)) -> dict:
    """Append a screening run to history."""
    try:
        _screening.save_screening_history(
            entry.model_dump(exclude_none=True),
            _screening_history_path(),
        )
        return {"saved": True}
    except Exception as e:
        logger.error("Failed to save screening history: %s", str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/export")
def export_results(request: ScreeningExportRequest = Body(...)) -> StreamingResponse:
    """Export screening results to CSV (or backtest CSV format)."""
    try:
        resolved = _validate_db_path(request.db_path)
        criteria_dicts = _criteria_to_dicts(request.criteria)
        ranking_dicts = _ranking_rules_to_dicts(request.ranking_rules)

        if request.format == "backtest":
            output_path = _screening.export_screening_to_backtest_csv(
                db_path=resolved,
                criteria=criteria_dicts,
                columns=request.columns,
                output_path=str(Path(resolved).parent / "screening_backtest_export.csv"),
                period=request.period,
                max_companies=request.max_companies,
                ranking_algorithm=request.ranking_algorithm,
                ranking_rules=ranking_dicts,
                historical=request.historical,
            )
            with open(output_path, "r", encoding="utf-8") as f:
                content = f.read()
            filename = "screening_backtest_export.csv"
        else:
            all_columns = list(request.columns)
            df = _screening.run_screening(
                db_path=resolved,
                criteria=criteria_dicts,
                columns=all_columns,
                period=request.period,
                screening_date=request.screening_date,
                ranking_algorithm=request.ranking_algorithm,
                ranking_rules=ranking_dicts,
            )
            stream = io.StringIO()
            df.to_csv(stream, index=False)
            content = stream.getvalue()
            filename = "screening_export.csv"

        return StreamingResponse(
            iter([content]),
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Export failed: %s", str(e))
        raise HTTPException(status_code=500, detail=str(e))
