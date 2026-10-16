"""Screening API routes.

All database interactions go through ``src.screening`` functions.
The frontend never touches the database directly.
"""

from __future__ import annotations

import io
import json
import logging
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Literal

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ConfigDict, Field, field_validator
from starlette.concurrency import run_in_threadpool

from src import screening as _screening
from src.auth.dependencies import require_operator
from src.auth.models import AuthenticatedUser
from src.jobs.service import get_job_service
from src.orchestrator.common.db_config import get_market_db
from src.paths import exports_dir
from src.research.runtime import store as _research_store
from src.screening.display_formats import catalog_column_formats, result_column_formats
from src.screening.job_runner import (
    computed_column_specs,
    criteria_to_dicts,
    ranking_rules_to_dicts,
    validate_screening_request,
)
from src.screening.persistence import normalize_screening_date
from src.web_app.security import get_settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/screening", tags=["screening"])

# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class ScreeningCriterion(BaseModel):
    table: str = Field(default="", description="Table name (e.g. CompanyInfo, PerShare). Not used for full_expression mode.")
    column: str = Field(default="", description="Column name within the table. Not used for full_expression mode.")
    operator: str | None = Field(default=None, description="Operator: >, >=, <, <=, =, !=, BETWEEN, IN, LIKE. Not used for recent_split mode.")
    value: Any = Field(default=None, description="Comparison value (required for fixed/LIKE mode)")
    value2: Any = Field(default=None, description="Second value for BETWEEN operator")
    values: list[Any] | None = Field(default=None, description="Value list for IN operator")
    field_type: str = Field(default="num", description="Value type hint: num, text, percent, or date")
    comparison_mode: str = Field(
        default="fixed",
        description="fixed, column, expression, in, like, stock_price, recent_split, or full_expression",
    )
    split_action: Literal["exclude", "include"] = Field(
        default="exclude",
        description="For recent_split: exclude or include companies matching the event rule",
    )
    split_status: Literal["confirmed", "rejected", "pending", "any"] = Field(
        default="confirmed",
        description="For recent_split: split confirmation status to match",
    )
    split_date_operator: Literal["on_or_after", "on_or_before"] = Field(
        default="on_or_after",
        description="For recent_split: compare split_date on or after/before the cutoff",
    )
    split_window_days: int | None = Field(
        default=None,
        ge=1,
        le=3650,
        description=(
            "For recent_split: match splits within the last N days, counted "
            "back from the as-of date (or today). Takes precedence over the "
            "absolute cutoff in value/split_date_operator."
        ),
    )
    compare_table: str | None = Field(default=None, description="Comparison table (column mode)")
    compare_column: str | None = Field(default=None, description="Comparison column (column mode)")
    offset: float | None = Field(default=None, description="Numeric offset for column comparison")
    right_side: list[dict] | None = Field(default=None, description="Expression tokens for expression mode")
    left_side: list[dict] | None = Field(default=None, description="Left-side expression tokens for full_expression mode")
    left_expression: str | None = Field(default=None, description="Left-side arithmetic expression for stock_price mode")
    group: str | None = Field(
        default=None,
        max_length=64,
        description="Criteria sharing a group id combine by group_match into one term of the screen",
    )
    group_match: Literal["any", "all"] = Field(
        default="any",
        description="How criteria in this criterion's group combine: any (OR) or all (AND)",
    )
    enabled: bool = Field(default=True, description="Disabled criteria are kept in the screen but not applied")


class RankingRule(BaseModel):
    table: str = Field(..., description="Table name")
    column: str = Field(..., description="Column name")
    weight: float = Field(default=1.0, description="Rule weight")
    direction: str = Field(default="higher", description="higher or lower")


class ComputedColumn(BaseModel):
    name: str = Field(..., description="Display name for the computed column")
    formula_type: str = Field(default="price_ratio", description="Formula type")
    expression_tokens: list[dict] | None = Field(default=None, description="Validated metric/value/operator/parenthesis formula tokens")
    numerator_table: str = Field(default="Stock_Prices", description="Numerator table")
    numerator_column: str = Field(default="Price", description="Numerator column")
    denominator_table: str = Field(default="", description="Denominator table")
    denominator_column: str = Field(default="", description="Denominator column")
    formula: str | None = Field(default=None, description="Custom SQL expression using table aliases")
    format: Literal["percent", "number"] | None = Field(
        default=None,
        description="Display format of the result: percent for fractions such as yields",
    )


class ScreeningDateRequest(BaseModel):
    """Strict base contract for requests with an optional point-in-time date."""

    model_config = ConfigDict(extra="forbid")
    screening_date: str | None = Field(
        default=None,
        description="Point-in-time date (YYYY-MM-DD)",
    )

    @field_validator("screening_date", mode="before")
    @classmethod
    def validate_screening_date(cls, value):
        return normalize_screening_date(value)


CriteriaMatch = Literal["all", "any"]


class ScreeningRunRequest(ScreeningDateRequest):
    criteria: list[ScreeningCriterion] = Field(default_factory=list)
    criteria_match: CriteriaMatch = Field(default="all", description="Whether all or any criteria terms must hold")
    columns: list[str] = Field(default_factory=list)
    computed_columns: list[ComputedColumn] = Field(default_factory=list)
    period: str | None = Field(default=None, description="Year filter (e.g. '2020')")
    sort_by: str | None = Field(default=None)
    sort_order: str = Field(default="DESC")
    ranking_algorithm: str = Field(default="none")
    ranking_rules: list[RankingRule] = Field(default_factory=list)


class ScreeningSaveRequest(ScreeningDateRequest):
    name: str = Field(..., description="Screening configuration name")
    criteria: list[ScreeningCriterion] = Field(default_factory=list)
    criteria_match: CriteriaMatch = Field(default="all")
    overwrite: bool = Field(default=False, description="Replace the saved screen with this name if it exists")
    columns: list[str] = Field(default_factory=list)
    computed_columns: list[ComputedColumn] = Field(default_factory=list)
    period: str | None = Field(default=None)
    ranking_algorithm: str = Field(default="none")
    ranking_rules: list[RankingRule] = Field(default_factory=list)


class ScreeningExportRequest(ScreeningDateRequest):
    criteria: list[ScreeningCriterion] = Field(default_factory=list)
    criteria_match: CriteriaMatch = Field(default="all")
    columns: list[str] = Field(default_factory=list)
    computed_columns: list[ComputedColumn] = Field(default_factory=list)
    period: str | None = Field(default=None)
    ranking_algorithm: str = Field(default="none")
    ranking_rules: list[RankingRule] = Field(default_factory=list)
    format: str = Field(default="csv", description="csv or backtest")
    max_companies: int = Field(default=25)
    historical: bool = Field(default=False)


class ScreeningHistoryEntry(ScreeningDateRequest):
    name: str | None = Field(default=None)
    criteria_count: int = Field(default=0)
    result_count: int = Field(default=0)
    period: str | None = Field(default=None)
    db_path: str | None = Field(default=None)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _resolve_db() -> str:
    """The server's configured Standardized database; requests cannot name another."""
    path = Path(get_market_db())
    if not path.is_file():
        raise HTTPException(status_code=503, detail="The screening database is not available.")
    return str(path)


def _criteria_to_dicts(criteria: list[ScreeningCriterion]) -> list[dict]:
    """Convert Pydantic criteria to plain dicts, dropping None values."""
    return criteria_to_dicts([c.model_dump() for c in criteria])


def _ranking_rules_to_dicts(rules: list[RankingRule]) -> list[dict]:
    return ranking_rules_to_dicts([r.model_dump() for r in rules])


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

MAX_PRICE_UPDATE_TICKERS = 100


class UpdatePricesRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tickers: list[str] = Field(..., min_length=1, max_length=MAX_PRICE_UPDATE_TICKERS)

    @field_validator("tickers")
    @classmethod
    def _clean_tickers(cls, value: list[str]) -> list[str]:
        cleaned = list(dict.fromkeys(t.strip() for t in value if t and t.strip()))
        if not cleaned:
            raise ValueError("at least one ticker is required")
        if any(len(t) > 32 for t in cleaned):
            raise ValueError("tickers must be at most 32 characters")
        return cleaned


@router.post("/update-prices")
def update_prices(
    request: UpdatePricesRequest = Body(...),
    _operator: AuthenticatedUser = Depends(require_operator),  # noqa: B008
) -> dict:
    """Update stock prices for the tickers in a screening result set.

    Each ticker is a provider round trip that writes shared market data, so
    the call is operator-only and bounded to ``MAX_PRICE_UPDATE_TICKERS``.
    """
    _resolve_db()
    record = get_job_service().run_sync(
        "price_refresh",
        {"tickers": list(request.tickers)},
        owner_user_id=_operator.user_id,
        title=f"Update prices · {len(request.tickers)} tickers",
    )
    by_ticker = (record.result or {}).get("results") or {}
    results = []
    for ticker in request.tickers:
        result = by_ticker.get(ticker) or {}
        results.append({
            "ticker": ticker,
            "ok": result.get("ok", False),
            "rows_inserted": result.get("rows_inserted", 0),
            "message": result.get("message", ""),
        })
    return {"results": results}


@router.get("/metrics")
def get_metrics() -> dict:
    """Return available screening tables and their columns."""
    resolved = _resolve_db()
    metrics = _screening.get_available_metrics(resolved)
    return {"tables": metrics, "formats": catalog_column_formats(metrics, resolved)}


@router.get("/periods")
def get_periods() -> dict:
    """Return available period years."""
    periods = _screening.get_available_periods(_resolve_db())
    return {"periods": periods}


@router.get("/formulas")
def get_available_formulas() -> dict:
    """Return available pre-defined valuation/computed formulas."""
    formulas = [
        {
            "name": "P/E Ratio",
            "formula_type": "price_ratio",
            "numerator_table": "Stock_Prices",
            "numerator_column": "Price",
            "denominator_table": "ShareMetrics",
            "denominator_column": "Basic earnings (loss) per share",
            "format": "ratio",
        },
        {
            "name": "P/B Ratio",
            "formula_type": "price_ratio",
            "numerator_table": "Stock_Prices",
            "numerator_column": "Price",
            "denominator_table": "ShareMetrics",
            "denominator_column": "Net assets per share",
            "format": "ratio",
        },
        {
            "name": "P/S Ratio",
            "formula_type": "price_ratio",
            "numerator_table": "Stock_Prices",
            "numerator_column": "Price",
            "denominator_table": "PerShare_Metrics",
            "denominator_column": "Sales Per Share",
            "format": "ratio",
        },
        {
            "name": "Dividend Yield",
            "formula_type": "price_ratio",
            "numerator_table": "ShareMetrics",
            "numerator_column": "Dividend paid per share",
            "denominator_table": "Stock_Prices",
            "denominator_column": "Price",
            "format": "percent",
        },
        {
            "name": "Earnings Yield",
            "formula_type": "price_ratio",
            "numerator_table": "ShareMetrics",
            "numerator_column": "Basic earnings (loss) per share",
            "denominator_table": "Stock_Prices",
            "denominator_column": "Price",
            "format": "percent",
        },
    ]
    return {"formulas": formulas}


@router.post("/run")
async def run_screening_endpoint(
    http_request: Request,
    payload: ScreeningRunRequest = Body(...),
) -> dict:
    """Run a screening query on a worker and return the matching rows.

    Answers 202 ``job_pending`` when no worker finishes it in time; the result
    is then stored as the user's last result when the job completes.
    """
    resolved = _resolve_db()
    user = getattr(http_request.state, "user", None)
    request_dict = payload.model_dump()
    try:
        await run_in_threadpool(validate_screening_request, resolved, request_dict)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    tags: list[list[str]] = []
    owner: str | None = None
    if isinstance(user, AuthenticatedUser):
        owner = user.user_id
        pairs = await run_in_threadpool(_screening.collect_user_company_tags, resolved, owner)
        tags = [[code, tag] for code, tag in pairs]
    record = await get_job_service().run(
        "screening",
        {"request": request_dict, "owner_user_id": owner, "company_tags": tags},
        owner_user_id=owner,
        title=f"Screen · {len(payload.criteria)} criteria",
    )
    return record.result or {}


def _require_user(request: Request) -> AuthenticatedUser:
    user = getattr(request.state, "user", None)
    if not isinstance(user, AuthenticatedUser):
        raise HTTPException(status_code=401, detail="Account authentication is required")
    return user


@router.get("/last-result")
def get_last_result(request: Request) -> dict[str, Any]:
    """Return the authenticated user's cached screening result, if present."""
    user = _require_user(request)
    try:
        from src.portfolio.screening_results import get_last_screening_result

        stored = get_last_screening_result(user.user_id)
        # Results cached before the API stopped exposing the generated SQL
        # may still embed it; it is internal and must not reach clients.
        if isinstance(stored, dict) and isinstance(stored.get("result"), dict):
            stored["result"].pop("sql_display", None)
            # Results cached before column formats were returned get them here.
            definition = stored.get("definition") or {}
            if "column_formats" not in stored["result"]:
                try:
                    db_path = _resolve_db()
                except HTTPException:
                    db_path = None
                stored["result"]["column_formats"] = {
                    **result_column_formats(list(definition.get("columns") or []), db_path),
                    **{
                        str(column.get("name")): str(column.get("format"))
                        for column in definition.get("computed_columns") or []
                        if column.get("name") and column.get("format") not in (None, "number")
                    },
                }
        return {"result": stored}
    except Exception as exc:  # noqa: BLE001 - an empty cache is still a valid state
        logger.warning("Could not load latest screening result for %s: %s", user.user_id, exc)
        return {"result": None}


@router.get("/saved")
def list_saved(request: Request) -> dict:
    """List saved screening configurations for the authenticated user."""
    user = _require_user(request)
    screens = _research_store.list_saved_screens(user.user_id)
    return {
        "screenings": [s["name"] for s in screens],
        "items": [_saved_screen_summary(s) for s in screens],
    }


@router.get("/saved/{identifier}")
def load_saved(request: Request, identifier: str) -> dict:
    """Load a saved screening configuration by name or screen ID."""
    user = _require_user(request)
    screens = _research_store.list_saved_screens(user.user_id)
    match = next(
        (s for s in screens if s["screen_id"] == identifier or s["name"] == identifier),
        None,
    )
    if match is None:
        raise HTTPException(status_code=404, detail="Screening not found")
    import json
    definition = json.loads(match["definition_json"])
    return definition


@router.post("/save")
def save_screening(http_request: Request, payload: ScreeningSaveRequest = Body(...)) -> dict:
    """Save a screening configuration for the authenticated user."""
    user = _require_user(http_request)
    name = payload.name.strip()
    if not name:
        raise HTTPException(status_code=400, detail="A screen name is required")
    try:
        definition = {
            "name": name,
            "criteria": _criteria_to_dicts(payload.criteria),
            "criteria_match": payload.criteria_match,
            "columns": payload.columns,
            "period": payload.period,
            "ranking_algorithm": payload.ranking_algorithm,
            "ranking_rules": _ranking_rules_to_dicts(payload.ranking_rules),
            "computed_columns": _computed_column_specs(payload.computed_columns),
            "screening_date": payload.screening_date,
        }
        definition_json = json.dumps(definition, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        existing = next((s for s in _research_store.list_saved_screens(user.user_id) if s["name"] == name), None)
        if existing is not None:
            if not payload.overwrite:
                raise HTTPException(status_code=409, detail="A screening with this name already exists")
            screen = _research_store.update_saved_screen(user.user_id, existing["screen_id"], name, definition_json)
            return {"saved": True, "screen_id": screen["screen_id"], "updated": True}
        screen = _research_store.create_saved_screen(user.user_id, name, definition_json)
        return {"saved": True, "screen_id": screen["screen_id"], "updated": False}
    except HTTPException:
        raise
    except Exception as e:
        if "UNIQUE" in str(e).upper():
            raise HTTPException(status_code=409, detail="A screening with this name already exists") from e
        logger.error("Failed to save screening: %s", str(e))
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.delete("/saved/{identifier}")
def delete_saved(request: Request, identifier: str) -> dict:
    """Delete a saved screening configuration by name or screen ID."""
    user = _require_user(request)
    screens = _research_store.list_saved_screens(user.user_id)
    match = next(
        (s for s in screens if s["screen_id"] == identifier or s["name"] == identifier),
        None,
    )
    if match is None:
        raise HTTPException(status_code=404, detail="Screening not found")
    if not _research_store.delete_saved_screen(user.user_id, match["screen_id"]):
        raise HTTPException(status_code=404, detail="Screening not found")
    return {"deleted": True}


@router.get("/history")
def get_history(
    request: Request,
    limit: int = Query(50, ge=1, le=500, description="Max entries to return"),
    offset: int = Query(0, ge=0, description="Number of entries to skip"),
) -> dict:
    """Return the user's screening run history with pagination (most recent first)."""
    user = _require_user(request)
    page, total = _research_store.list_screening_runs(user.user_id, limit=limit, offset=offset)
    return {"entries": page, "total": total, "limit": limit, "offset": offset}


@router.post("/history")
def save_history(request: Request, entry: ScreeningHistoryEntry = Body(...)) -> dict:
    """Record a screening run in the user's history."""
    user = _require_user(request)
    _research_store.record_screening_run(user.user_id, entry.model_dump(exclude_none=True))
    return {"saved": True}


def _computed_column_specs(columns: list[ComputedColumn]) -> list[dict]:
    """Convert computed-column request models to screening-layer values."""
    return computed_column_specs([column.model_dump() for column in columns])


def _saved_screen_summary(screen: dict[str, Any]) -> dict[str, Any]:
    """Name, timestamps, and the shape of a saved screen for pickers."""
    try:
        definition = json.loads(screen.get("definition_json") or "{}")
    except (TypeError, ValueError):
        definition = {}
    criteria = definition.get("criteria") or []
    return {
        "screen_id": screen.get("screen_id"),
        "name": screen.get("name"),
        "updated_at": screen.get("updated_at"),
        "created_at": screen.get("created_at"),
        "rule_count": sum(1 for criterion in criteria if criterion.get("enabled", True) is not False),
        "column_count": len(definition.get("columns") or []) + len(definition.get("computed_columns") or []),
        "criteria_match": definition.get("criteria_match") or "all",
        "screening_date": definition.get("screening_date"),
    }


def _enforce_export_limit(content: str) -> str:
    """Reject generated responses that exceed the configured byte limit."""
    if len(content.encode("utf-8")) > get_settings().max_export_bytes:
        raise HTTPException(413, "Generated export exceeds the configured size limit")
    return content


def _export_csv_content(
    request: ScreeningExportRequest,
    resolved: str,
    criteria: list[dict],
    ranking_rules: list[dict],
    computed_columns: list[dict],
    company_tags: list[tuple[str, str]] | None = None,
) -> str:
    dataframe = _screening.run_screening(
        db_path=resolved,
        criteria=criteria,
        columns=list(request.columns),
        period=request.period,
        screening_date=request.screening_date,
        ranking_algorithm=request.ranking_algorithm,
        ranking_rules=ranking_rules,
        computed_columns=computed_columns,
        criteria_match=request.criteria_match,
        company_tags=company_tags,
    )
    stream = io.StringIO()
    dataframe.to_csv(stream, index=False)
    return _enforce_export_limit(stream.getvalue())


def _export_backtest_content(
    request: ScreeningExportRequest,
    resolved: str,
    criteria: list[dict],
    ranking_rules: list[dict],
    computed_columns: list[dict],
    company_tags: list[tuple[str, str]] | None = None,
) -> str:
    """Generate a backtest file in an owned, request-unique workspace."""
    export_root = exports_dir()
    export_root.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix="screening-", dir=export_root) as temp_dir:
        output_path = Path(temp_dir) / "screening_backtest_export.csv"
        _screening.export_screening_to_backtest_csv(
            db_path=resolved,
            criteria=criteria,
            columns=request.columns,
            output_path=str(output_path),
            period=request.period,
            max_companies=request.max_companies,
            ranking_algorithm=request.ranking_algorithm,
            ranking_rules=ranking_rules,
            historical=request.historical,
            computed_columns=computed_columns,
            criteria_match=request.criteria_match,
            company_tags=company_tags,
        )
        if output_path.stat().st_size > get_settings().max_export_bytes:
            raise HTTPException(
                413,
                "Generated export exceeds the configured size limit",
            )
        return _enforce_export_limit(output_path.read_text(encoding="utf-8"))


@router.post("/export")
def export_results(http_request: Request, request: ScreeningExportRequest = Body(...)) -> StreamingResponse:
    """Export screening results to CSV (or backtest CSV format).

    Reads only: the user's tags are given to the query instead of written to
    the shared table, as for ``/run``.
    """
    try:
        resolved = _resolve_db()
        criteria_dicts = _criteria_to_dicts(request.criteria)
        ranking_dicts = _ranking_rules_to_dicts(request.ranking_rules)
        computed_specs = _computed_column_specs(request.computed_columns)
        user = getattr(http_request.state, "user", None)
        tags = (
            _screening.collect_user_company_tags(resolved, user.user_id)
            if isinstance(user, AuthenticatedUser)
            else []
        )
        if request.format == "backtest":
            content = _export_backtest_content(
                request, resolved, criteria_dicts, ranking_dicts, computed_specs, tags
            )
            filename = "screening_backtest_export.csv"
        elif request.format == "csv":
            content = _export_csv_content(
                request, resolved, criteria_dicts, ranking_dicts, computed_specs, tags
            )
            filename = "screening_export.csv"
        else:
            raise HTTPException(status_code=400, detail="Unsupported export format")

        return StreamingResponse(
            iter([content]),
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
