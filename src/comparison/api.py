"""Company comparison endpoints built on the existing analysis contracts."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field

from src.comparison.service import (
    DEFAULT_METRICS,
    METRIC_DEFINITIONS,
    extract_latest_statement_metrics,
    normalize_companies,
)
from src.security_analysis import get_security_overview, get_security_peers, get_security_statements

router = APIRouter(prefix="/api/comparison", tags=["comparison"])
_MAX_COMPANIES = 12
_LOGGER = logging.getLogger(__name__)
_ANALYSIS_STATEMENT_SOURCES = {
    "income": "IncomeStatement",
    "balance": "BalanceSheet",
    "shares": "ShareMetrics",
}


def _resolve_db() -> str:
    # Import lazily because web_app.api registers this router during package import.
    from src.web_app.api.security_analysis import _resolve_db as resolve_db
    return resolve_db()


class ComparisonRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    company_codes: list[str] = Field(min_length=2, max_length=_MAX_COMPANIES)
    metrics: list[str] = Field(default_factory=list, max_length=50)


class HistoryRequest(ComparisonRequest):
    periods: int = Field(default=12, ge=1, le=40)


def _codes(values: list[str]) -> list[str]:
    normalized = list(dict.fromkeys(item.strip() for item in values if item.strip()))
    if not 2 <= len(normalized) <= _MAX_COMPANIES:
        raise HTTPException(status_code=400, detail="Provide between 2 and 12 company codes")
    return normalized


def _selected_metrics(values: list[str]) -> list[str]:
    selected = list(dict.fromkeys(metric for metric in values if metric in METRIC_DEFINITIONS))
    return selected or DEFAULT_METRICS.copy()


def _overview(db: str, code: str) -> dict[str, Any] | None:
    try:
        return get_security_overview(db, company_code=code)
    except ValueError:
        return None


def _analysis_metrics(db: str, code: str, overview: dict[str, Any]) -> dict[str, Any]:
    """Use the exact metric calculation exposed by the Analyze workspace."""
    try:
        from src.web_app.api.security_analysis import _compute_metrics

        return _compute_metrics(
            db,
            code,
            overview.get("market", {}),
            overview.get("company", {}),
        )
    except Exception as exc:  # pragma: no cover - defensive fallback for partial databases
        _LOGGER.warning("Could not load Analyze metrics for %s: %s", code, exc)
        return {}


def _set_if_missing(target: dict[str, Any], key: str, value: Any) -> None:
    if target.get(key) is None and value is not None:
        target[key] = value


def _enrich_overview(db: str, code: str, overview: dict[str, Any]) -> dict[str, Any]:
    """Merge Analyze metrics and latest populated statement values into an overview."""
    enriched = dict(overview)
    enriched["metrics"] = _analysis_metrics(db, code, overview)
    try:
        history = get_security_statements(
            db,
            code,
            periods=8,
            statement_sources=_ANALYSIS_STATEMENT_SOURCES,
        )
        statement_metrics, metric_period = extract_latest_statement_metrics(history)
    except Exception as exc:  # pragma: no cover - preserve ratio-only comparisons
        _LOGGER.warning("Could not load statement metrics for %s: %s", code, exc)
        return enriched

    fundamentals = dict(enriched.get("fundamentals_latest") or {})
    for key in ("Revenue", "OperatingIncome", "NetIncome", "TotalAssets", "SharesOutstanding"):
        _set_if_missing(fundamentals, key, statement_metrics.get(key))
    _set_if_missing(fundamentals, "ShareholdersEquity", statement_metrics.get("TotalEquity"))
    enriched["fundamentals_latest"] = fundamentals

    revenue = statement_metrics.get("Revenue")
    valuation = dict(enriched.get("valuation_latest") or {})
    quality = dict(enriched.get("quality_latest") or {})
    if revenue not in (None, 0):
        operating_income = statement_metrics.get("OperatingIncome")
        net_income = statement_metrics.get("NetIncome")
        if operating_income is not None:
            _set_if_missing(valuation, "OperatingMargin", operating_income / revenue)
        if net_income is not None:
            _set_if_missing(valuation, "NetProfitMargin", net_income / revenue)
        cost_of_sales = statement_metrics.get("CostOfSales")
        if cost_of_sales is not None:
            _set_if_missing(quality, "GrossMargin", (revenue - cost_of_sales) / revenue)
    equity = statement_metrics.get("TotalEquity")
    liabilities = statement_metrics.get("TotalLiabilities")
    if equity not in (None, 0) and liabilities is not None:
        _set_if_missing(quality, "DebtToEquity", liabilities / equity)
    current_assets = statement_metrics.get("CurrentAssets")
    current_liabilities = statement_metrics.get("CurrentLiabilities")
    if current_liabilities not in (None, 0) and current_assets is not None:
        _set_if_missing(quality, "CurrentRatio", current_assets / current_liabilities)
    enriched["valuation_latest"] = valuation
    enriched["quality_latest"] = quality

    if metric_period:
        metadata = dict(enriched.get("metadata") or {})
        metadata["comparison_metric_period_end"] = metric_period
        enriched["metadata"] = metadata
    return enriched


def _snapshot_rows(db: str, codes: list[str], metrics: list[str]) -> tuple[list[dict[str, Any]], list[str]]:
    overviews: dict[str, dict[str, Any]] = {}
    missing: list[str] = []
    for code in codes:
        result = _overview(db, code)
        if result is None:
            missing.append(code)
        else:
            overviews[code] = _enrich_overview(db, code, result)
    normalized = normalize_companies(list(overviews), overviews, metrics)
    rows = []
    for code, result in overviews.items():
        row = normalized[code]
        metadata = result.get("metadata", {})
        row.update({
            "company_code": code,
            "company": result.get("company", {}),
            "market": result.get("market", {}),
            "period_end": metadata.get("comparison_metric_period_end") or metadata.get("last_financial_period_end"),
            "price_date": metadata.get("last_price_date"),
            "data_quality_flags": metadata.get("data_quality_flags", []),
        })
        rows.append(row)
    return rows, missing


@router.get("/peers/{company_code}")
def peers(company_code: str, limit: int = Query(default=10, ge=1, le=50)) -> dict[str, Any]:
    if not company_code.strip():
        raise HTTPException(status_code=400, detail="company_code is required")
    try:
        rows = get_security_peers(_resolve_db(), company_code.strip(), limit=limit)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"company_code": company_code.strip(), "peers": rows}


@router.post("/snapshot")
def snapshot(payload: ComparisonRequest) -> dict[str, Any]:
    codes = _codes(payload.company_codes)
    metrics = _selected_metrics(payload.metrics)
    rows, missing = _snapshot_rows(_resolve_db(), codes, metrics)
    return {
        "companies": rows,
        "requested": codes,
        "missing": missing,
        "metrics": metrics,
        "metric_definitions": {metric: METRIC_DEFINITIONS[metric] for metric in metrics},
    }


@router.post("/history")
def history(payload: HistoryRequest) -> dict[str, Any]:
    codes = _codes(payload.company_codes)
    db = _resolve_db()
    metrics = _selected_metrics(payload.metrics)
    output: list[dict[str, Any]] = []
    missing: list[str] = []
    for code in codes:
        result = _overview(db, code)
        if result is None:
            missing.append(code)
            continue
        result = _enrich_overview(db, code, result)
        metadata = result.get("metadata", {})
        output.append({
            "company_code": code,
            "company": result.get("company", {}),
            "metrics": normalize_companies([code], {code: result}, metrics)[code]["metrics"],
            "history": get_security_statements(db, code, periods=payload.periods),
            "period_end": metadata.get("comparison_metric_period_end") or metadata.get("last_financial_period_end"),
        })
    return {"companies": output, "requested": codes, "missing": missing, "periods": payload.periods, "metrics": metrics}
