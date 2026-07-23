"""Security analysis queries for the Tk UI.

This module centralises company-level lookups used by the Security Analysis
view. It resolves table/column naming differences across databases,
aggregates the latest filing snapshot, loads historical statement data,
retrieves price history, and provides a deterministic peer comparison.
"""

from __future__ import annotations

import importlib
import json
import logging
import os
import re
import sqlite3
import threading
from collections import Counter
from dataclasses import dataclass
from datetime import timedelta
from functools import lru_cache
from typing import Any

import pandas as pd

from src.orchestrator.common.sqlite import connect_read, connect_write, transaction
from src.utilities.stock_prices import _create_prices_table, load_ticker_data

logger = logging.getLogger(__name__)

_OPTIMIZED_DB_PATHS: set[str] = set()
_DB_OPTIMIZE_LOCKS: dict[str, threading.Lock] = {}
_DB_OPTIMIZE_LOCKS_GUARD = threading.Lock()

_DEFAULT_STATEMENT_SOURCES = (
    "income_statement",
    "balance_sheet",
    "cashflow_statement",
    "PerShare",
    "Quality",
    "Valuation",
    "PerShare_Historical",
    "Quality_Historical",
    "Valuation_Historical",
)

_LEGACY_STATEMENT_SOURCE_TABLES = {
    "income_statement": "IncomeStatement",
    "balance_sheet": "BalanceSheet",
    "cashflow_statement": "CashflowStatement",
    "financial_statements": "FinancialStatements",
}

_STATEMENT_METADATA_COLUMNS = {"docid", "edinetcode", "periodend"}

_STATEMENT_LABEL_OVERRIDES = {
    "netsales": "Net Sales",
    "grossprofit": "Gross Profit",
    "operatingincome": "Operating Income",
    "netincome": "Net Income",
    "currentassets": "Current Assets",
    "totalassets": "Total Assets",
    "shareholdersequity": "Shareholders' Equity",
    "currentliabilities": "Current Liabilities",
    "totalliabilities": "Total Liabilities",
    "operatingcashflow": "Operating Cashflow",
    "investmentcashflow": "Investment Cashflow",
    "financingcashflow": "Financing Cashflow",
    "peratio": "PE Ratio",
    "pricetobook": "Price to Book",
    "dividendsyield": "Dividend Yield",
    "returnonequity": "Return on Equity",
    "debttoequity": "Debt to Equity",
    "currentratio": "Current Ratio",
    "grossmargin": "Gross Margin",
    "earningsyield": "Earnings Yield",
    "pricetosales": "Price to Sales",
    "enterprisevalue": "Enterprise Value",
    "enterprisevaluetosales": "Enterprise Value to Sales",
    "operatingmargin": "Operating Margin",
    "netprofitmargin": "Net Profit Margin",
}

_STATEMENT_TOKEN_OVERRIDES = {
    "eps": "EPS",
    "pe": "PE",
    "pb": "PB",
    "roe": "ROE",
    "roa": "ROA",
    "ev": "EV",
    "zscore": "Z-Score",
    "stddev": "Std Dev",
}

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_OVERVIEW_TAXONOMY_METRICS = {
    "IncomeStatement": ("netSales", "operatingIncome", "netIncome"),
    "BalanceSheet": ("totalAssets", "shareholdersEquity"),
}

# Columns needed from each wide (non-taxonomy) statement table for the overview
# endpoint.  Selecting every column from every joined table can exceed SQLite's
# 2000-column result-set limit (IncomeStatement alone often has 120+ columns).
# We match case-insensitively against the actual column names in the table.
_OVERVIEW_INCOME_COLS = frozenset({
    "netsales", "operatingincome", "netincome", "grossprofit",
    "operatingincome(loss)", "income(loss)beforeincometaxes",
    "netincome(loss)", "netsales(revenue)", "totalrevenue",
})
_OVERVIEW_BALANCE_COLS = frozenset({
    "totalassets", "shareholdersequity", "currentassets",
    "currentliabilities", "totalliabilities",
})
_OVERVIEW_CASHFLOW_COLS = frozenset({
    "operatingcashflow", "investmentcashflow", "financingcashflow",
})
_OVERVIEW_PERSHARE_COLS = frozenset({
    "eps", "bookvalue", "dividends", "sharesoutstanding",
    "netsalespershare", "operatingincomepershare",
})
_OVERVIEW_VALUATION_COLS = frozenset({
    "peratio", "pricetobook", "dividendsyield", "marketcap",
    "earningsyield", "pricetosales", "enterprisevalue",
    "enterprisevaluetosales",
})
_OVERVIEW_QUALITY_COLS = frozenset({
    "returnonequity", "debttoequity", "currentratio", "grossmargin",
    "operatingmargin", "netprofitmargin",
})
_STATEMENT_LINE_ITEMS_TABLE = "statement_line_items"

# ---------------------------------------------------------------------------
#  1. SCHEMA DISCOVERY
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SecuritySchema:
    """Resolved table and column names for a specific SQLite database."""

    company_table: str
    financial_statements_table: str
    prices_table: str
    income_table: str | None
    balance_table: str | None
    cashflow_table: str | None
    per_share_table: str | None
    valuation_table: str | None
    quality_table: str | None
    document_list_table: str | None
    company_code_col: str
    company_ticker_col: str
    company_name_col: str
    company_name_fallback_col: str | None
    company_industry_col: str | None
    company_market_col: str | None
    company_description_col: str | None
    fs_code_col: str
    fs_docid_col: str
    fs_period_end_col: str
    fs_description_col: str | None
    fs_description_en_col: str | None
    fs_shares_outstanding_col: str | None
    fs_share_price_col: str | None
    doclist_docid_col: str | None
    doclist_submit_dt_col: str | None


@dataclass(frozen=True)
class StatementMetricSpec:
    """Descriptor for a single metric sourced into statement history."""

    source_field: str
    record_field: str
    display_name: str


@dataclass(frozen=True)
class StatementSourceSpec:
    """Descriptor for a statement source table joined into the history query."""

    source_key: str
    table_name: str | None
    alias: str
    join_clause: str | None
    metrics: tuple[StatementMetricSpec, ...]


def _connect(db_path: str) -> sqlite3.Connection:
    """Open a SQLite connection with a row factory suitable for helpers."""
    return connect_read(db_path)


def _quote_ident(name: str) -> str:
    """Return a safely quoted SQLite identifier."""
    return f"[{str(name).replace(']', ']]')}]"


def _normalise_ident(name: str) -> str:
    """Normalise a column/identifier for fuzzy matching.

    Lowercases and strips all whitespace, underscores, hyphens, parentheses,
    and percent signs so that ``Net sales``, ``net_sales`` and ``NetSales``
    all compare equal.
    """
    import re as _re
    return _re.sub(r"[\s_\-()%]+", "", str(name).lower())


def _normalise_db_path(db_path: str) -> str:
    """Normalise a database path so schema discovery can be cached."""
    return os.path.abspath(db_path)


def _get_db_optimization_lock(normalised_path: str) -> threading.Lock:
    """Return a per-database lock for one-time index creation."""
    with _DB_OPTIMIZE_LOCKS_GUARD:
        lock = _DB_OPTIMIZE_LOCKS.get(normalised_path)
        if lock is None:
            lock = threading.Lock()
            _DB_OPTIMIZE_LOCKS[normalised_path] = lock
        return lock


def _list_table_map(conn: sqlite3.Connection) -> dict[str, str]:
    """Return a case-insensitive table-name map for the database."""
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()
    return {str(row[0]).lower(): str(row[0]) for row in rows}


def _resolve_table_name(
    table_map: dict[str, str],
    candidates: list[str],
    *,
    required: bool = True,
) -> str | None:
    """Resolve a table name from a list of case-insensitive candidates."""
    for candidate in candidates:
        resolved = table_map.get(candidate.lower())
        if resolved:
            return resolved
    if required:
        raise RuntimeError(
            f"Required table not found. Tried: {', '.join(candidates)}"
        )
    return None


def _get_columns(conn: sqlite3.Connection, table_name: str) -> list[str]:
    """Return the actual column names for a table."""
    rows = conn.execute(f"PRAGMA table_info({_quote_ident(table_name)})").fetchall()
    return [str(row[1]) for row in rows]


def _resolve_column(
    columns: list[str],
    candidates: list[str],
    *,
    required: bool = True,
) -> str | None:
    """Resolve a column from a list of case-insensitive candidates."""
    by_lower = {col.lower(): col for col in columns}
    for candidate in candidates:
        resolved = by_lower.get(candidate.lower())
        if resolved:
            return resolved
    if required:
        raise RuntimeError(
            f"Required column not found. Tried: {', '.join(candidates)}"
        )
    return None


def _pick_company_name_cols(columns: list[str]) -> tuple[str, str | None]:
    """Resolve primary and fallback company-name columns."""
    primary_candidates = [
        "Company_Name",
        "CompanyName",
        "company_name",
    ]
    fallback_candidates = [
        "Submitter Name",
        "Submitter_Name",
        "SubmitterName",
        "FilerName",
        "Name",
    ]
    primary = _resolve_column(columns, primary_candidates, required=False)
    fallback = _resolve_column(columns, fallback_candidates, required=False)
    if primary:
        if fallback and fallback.lower() == primary.lower():
            fallback = None
        return primary, fallback
    if fallback:
        return fallback, None
    tried = primary_candidates + fallback_candidates
    raise RuntimeError(f"Required column not found. Tried: {', '.join(tried)}")


def _pick_company_industry_col(columns: list[str]) -> str | None:
    """Resolve the best available industry column if present."""
    candidates = [
        "Company_Industry",
        "Industry",
        "industry",
        "Sector",
        "Business_Industry",
    ]
    return _resolve_column(columns, candidates, required=False)


def _pick_company_market_col(columns: list[str]) -> str | None:
    """Resolve the best available listing/market column if present."""
    candidates = [
        "Listed",
        "Listing",
        "Market",
        "Market_Segment",
        "Exchange",
        "Type of Submitter",
    ]
    return _resolve_column(columns, candidates, required=False)


def _pick_company_description_col(columns: list[str]) -> str | None:
    """Resolve an optional description column if available."""
    candidates = [
        "Description",
        "Company_Description",
        "Business_Description",
        "Overview",
    ]
    return _resolve_column(columns, candidates, required=False)


def _pick_fs_description_col(columns: list[str]) -> str | None:
    """Resolve an optional latest-filing business-description column."""
    candidates = [
        "DescriptionOfBusiness",
        "BusinessDescription",
        "Description",
    ]
    return _resolve_column(columns, candidates, required=False)


def _pick_fs_description_en_col(columns: list[str]) -> str | None:
    """Resolve an optional English business-description column."""
    candidates = [
        "DescriptionOfBusiness_EN",
        "BusinessDescription_EN",
        "Description_EN",
    ]
    return _resolve_column(columns, candidates, required=False)


@lru_cache(maxsize=8)
def resolve_schema(db_path: str) -> SecuritySchema:
    """Resolve the database schema used by the security-analysis helpers.

    Args:
        db_path (str): Path to the SQLite database.

    Returns:
        SecuritySchema: Resolved table and column names.
    """
    normalised_path = _normalise_db_path(db_path)
    conn = _connect(normalised_path)
    try:
        table_map = _list_table_map(conn)

        company_table = _resolve_table_name(table_map, ["CompanyInfo", "companyInfo"])
        financial_table = _resolve_table_name(table_map, ["FinancialStatements"])
        prices_table = _resolve_table_name(table_map, ["Stock_Prices", "stock_prices"])
        income_table = _resolve_table_name(table_map, ["IncomeStatement"], required=False)
        balance_table = _resolve_table_name(table_map, ["BalanceSheet"], required=False)
        cashflow_table = _resolve_table_name(table_map, ["CashflowStatement"], required=False)
        per_share_table = _resolve_table_name(table_map, ["PerShare"], required=False)
        valuation_table = _resolve_table_name(table_map, ["Valuation"], required=False)
        quality_table = _resolve_table_name(table_map, ["Quality"], required=False)
        document_list_table = _resolve_table_name(table_map, ["DocumentList"], required=False)

        company_cols = _get_columns(conn, company_table)
        fs_cols = _get_columns(conn, financial_table)

        document_list_cols: list[str] = []
        if document_list_table:
            document_list_cols = _get_columns(conn, document_list_table)

        company_name_col, company_name_fallback_col = _pick_company_name_cols(company_cols)

        return SecuritySchema(
            company_table=company_table,
            financial_statements_table=financial_table,
            prices_table=prices_table,
            income_table=income_table,
            balance_table=balance_table,
            cashflow_table=cashflow_table,
            per_share_table=per_share_table,
            valuation_table=valuation_table,
            quality_table=quality_table,
            document_list_table=document_list_table,
            company_code_col=_resolve_column(company_cols, ["Company_Code", "EdinetCode", "edinetCode"]),
            company_ticker_col=_resolve_column(company_cols, ["Company_Ticker", "Ticker", "ticker"]),
            company_name_col=company_name_col,
            company_name_fallback_col=company_name_fallback_col,
            company_industry_col=_pick_company_industry_col(company_cols),
            company_market_col=_pick_company_market_col(company_cols),
            company_description_col=_pick_company_description_col(company_cols),
            fs_code_col=_resolve_column(fs_cols, ["Company_Code", "edinetCode", "EdinetCode"]),
            fs_docid_col=_resolve_column(fs_cols, ["docID", "DocID"]),
            fs_period_end_col=_resolve_column(fs_cols, ["periodEnd", "PeriodEnd"]),
            fs_description_col=_pick_fs_description_col(fs_cols),
            fs_description_en_col=_pick_fs_description_en_col(fs_cols),
            fs_shares_outstanding_col=_resolve_column(
                fs_cols, ["SharesOutstanding"], required=False
            ),
            fs_share_price_col=_resolve_column(fs_cols, ["SharePrice"], required=False),
            doclist_docid_col=_resolve_column(document_list_cols, ["docID", "DocID"], required=False),
            doclist_submit_dt_col=_resolve_column(
                document_list_cols, ["submitDateTime", "SubmitDateTime"], required=False
            ),
        )
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
#  2. FORMATTING & SCORING HELPERS
# ---------------------------------------------------------------------------
# Text processing, cleaning, and summarisation utilities are now in
# src.security_analysis.text.  We import only the functions used by the
# public API endpoints (get_security_overview, etc.).
#
# _safe_str and _safe_float remain here because they are used by 60+
# call sites throughout this module.

from .text import clean_text_block as _clean_text_block
from .text import summarize_english_text as _summarize_english_text
from .text import summarize_preferred_description as _summarize_preferred_description
from .text import summarize_business_description as _summarize_business_description


def _safe_str(value: Any) -> str:
    """Return a normalised string for display and matching."""
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    return str(value).strip()


def _safe_float(value: Any) -> float | None:
    """Return a float or ``None`` for missing/invalid input."""
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

def _safe_date_str(value: Any) -> str | None:
    """Return a stable YYYY-MM-DD string when possible."""
    text = _safe_str(value)
    if not text:
        return None
    return text[:10]


def _coalesce(*values: Any) -> Any:
    """Return the first non-empty value."""
    for value in values:
        text = _safe_str(value)
        if text:
            return value
    return None


def _unique_preserve_order(values: list[str]) -> list[str]:
    """Return unique non-empty strings while preserving input order."""
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        text = _safe_str(value)
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def _statement_requested_sources(statement_sources: dict[str, str] | None) -> list[str]:
    """Return the ordered set of statement source identifiers to fetch."""
    if statement_sources:
        raw_sources = [value for value in statement_sources.values()]
    else:
        raw_sources = list(_DEFAULT_STATEMENT_SOURCES)
    return _unique_preserve_order([_safe_str(value) for value in raw_sources])


def _statement_source_table_candidates(schema: SecuritySchema, source_key: str) -> list[str]:
    """Return candidate table names for a statement source identifier."""
    canonical = _LEGACY_STATEMENT_SOURCE_TABLES.get(source_key, source_key)
    direct_table_map = {
        "FinancialStatements": schema.financial_statements_table,
        "IncomeStatement": schema.income_table,
        "BalanceSheet": schema.balance_table,
        "CashflowStatement": schema.cashflow_table,
        "PerShare": schema.per_share_table,
        "Valuation": schema.valuation_table,
        "Quality": schema.quality_table,
    }
    candidates: list[str] = []
    resolved = direct_table_map.get(canonical)
    if resolved:
        candidates.append(resolved)
    if canonical:
        collapsed = re.sub(r"[^0-9A-Za-z]+", "", canonical)
        if collapsed and collapsed != canonical:
            candidates.append(collapsed)
    candidates.append(canonical)
    return _unique_preserve_order(candidates)


def _sanitise_statement_source_key(source_key: str) -> str:
    """Return a safe fragment for generated record-field names."""
    cleaned = re.sub(r"[^0-9A-Za-z]+", "_", _safe_str(source_key)).strip("_")
    return cleaned or "statement_source"


def _statement_metric_display_name(field_name: str) -> str:
    """Convert a database column name into a readable metric label."""
    normalized = _safe_str(field_name)
    if not normalized:
        return "Metric"

    override = _STATEMENT_LABEL_OVERRIDES.get(normalized.lower())
    if override:
        return override

    text = normalized.replace("_", " ")
    text = re.sub(r"(?<=[A-Z])(?=[A-Z][a-z])", " ", text)
    text = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", text)
    parts: list[str] = []
    for token in text.split():
        token_override = _STATEMENT_TOKEN_OVERRIDES.get(token.lower())
        if token_override:
            parts.append(token_override)
        elif token.isupper() and len(token) <= 4:
            parts.append(token)
        elif token.isdigit():
            parts.append(token)
        else:
            parts.append(token.capitalize())
    return " ".join(parts)


def _statement_record_field_name(source_key: str, source_field: str, used_fields: set[str]) -> str:
    """Return a unique record-field name for a statement metric."""
    preferred = _safe_str(source_field) or "metric"
    if preferred.lower() not in used_fields:
        used_fields.add(preferred.lower())
        return preferred

    prefix = _sanitise_statement_source_key(source_key)
    candidate = f"{prefix}__{preferred}"
    suffix = 2
    while candidate.lower() in used_fields:
        candidate = f"{prefix}__{preferred}_{suffix}"
        suffix += 1
    used_fields.add(candidate.lower())
    return candidate


def _build_statement_source_specs(
    conn: sqlite3.Connection,
    schema: SecuritySchema,
    statement_sources: list[str],
) -> dict[str, StatementSourceSpec]:
    """Resolve statement source identifiers into joinable table specs."""
    table_map = _list_table_map(conn)
    used_fields: set[str] = set()
    specs: dict[str, StatementSourceSpec] = {}

    for index, source_key in enumerate(statement_sources):
        alias = f"st{index}"
        table_name = _resolve_table_name(
            table_map,
            _statement_source_table_candidates(schema, source_key),
            required=False,
        )
        if not table_name:
            specs[source_key] = StatementSourceSpec(
                source_key=source_key,
                table_name=None,
                alias=alias,
                join_clause=None,
                metrics=tuple(),
            )
            continue

        if _is_taxonomy_statement_table(conn, table_name):
            specs[source_key] = StatementSourceSpec(
                source_key=source_key,
                table_name=table_name,
                alias=alias,
                join_clause=None,
                metrics=tuple(),
            )
            continue

        columns = _get_columns(conn, table_name)
        docid_col = _resolve_column(columns, ["docID", "DocID"], required=False)
        company_code_col = _resolve_column(columns, ["Company_Code", "edinetCode", "EdinetCode"], required=False)
        period_col = _resolve_column(columns, ["periodEnd", "PeriodEnd"], required=False)

        join_clause: str | None = None
        if docid_col:
            join_clause = (
                f"LEFT JOIN {_quote_ident(table_name)} {alias} "
                f"ON {alias}.{_quote_ident(docid_col)} = fs.{_quote_ident(schema.fs_docid_col)}"
            )
        elif company_code_col and period_col:
            join_clause = (
                f"LEFT JOIN {_quote_ident(table_name)} {alias} "
                f"ON {alias}.{_quote_ident(company_code_col)} = fs.{_quote_ident(schema.fs_code_col)} "
                f"AND {alias}.{_quote_ident(period_col)} = fs.{_quote_ident(schema.fs_period_end_col)}"
            )

        if not join_clause:
            logger.warning(
                "Skipping statement source %s because %s has no docID or Company_Code+periodEnd join columns.",
                source_key,
                table_name,
            )
            specs[source_key] = StatementSourceSpec(
                source_key=source_key,
                table_name=table_name,
                alias=alias,
                join_clause=None,
                metrics=tuple(),
            )
            continue

        metrics: list[StatementMetricSpec] = []
        for column_name in columns:
            if column_name.lower() in _STATEMENT_METADATA_COLUMNS:
                continue
            metrics.append(
                StatementMetricSpec(
                    source_field=column_name,
                    record_field=_statement_record_field_name(source_key, column_name, used_fields),
                    display_name=_statement_metric_display_name(column_name),
                )
            )

        specs[source_key] = StatementSourceSpec(
            source_key=source_key,
            table_name=table_name,
            alias=alias,
            join_clause=join_clause,
            metrics=tuple(metrics),
        )

    return specs


def _score_security_match(record: dict[str, Any], tokens: list[str]) -> int | None:
    """Score a company record for search ranking.

    Returns ``None`` when not all tokens match at least one searchable field.
    """
    searchable = {
        "ticker": _safe_str(record.get("ticker")).lower(),
        "company_code": _safe_str(record.get("company_code")).lower(),
        "company_name": _safe_str(record.get("company_name")).lower(),
        "industry": _safe_str(record.get("industry")).lower(),
        "market": _safe_str(record.get("market")).lower(),
    }
    score = 0
    for token in tokens:
        token_score = 0
        if not token:
            continue
        if searchable["ticker"] == token:
            token_score = max(token_score, 120)
        if searchable["company_code"] == token:
            token_score = max(token_score, 115)
        if searchable["ticker"].startswith(token):
            token_score = max(token_score, 95)
        if searchable["company_code"].startswith(token):
            token_score = max(token_score, 90)
        if searchable["company_name"].startswith(token):
            token_score = max(token_score, 80)
        if searchable["industry"].startswith(token):
            token_score = max(token_score, 55)
        if searchable["market"].startswith(token):
            token_score = max(token_score, 35)
        if token in searchable["company_name"]:
            token_score = max(token_score, 30)
        if token in searchable["industry"]:
            token_score = max(token_score, 20)
        if token in searchable["market"]:
            token_score = max(token_score, 12)
        if token in searchable["ticker"]:
            token_score = max(token_score, 18)
        if token in searchable["company_code"]:
            token_score = max(token_score, 18)
        if token_score == 0:
            return None
        score += token_score
    return score


def _statement_metric_rows(
    records: list[dict[str, Any]],
    metric_specs: tuple[StatementMetricSpec, ...],
    source_key: str,
) -> list[dict[str, Any]]:
    """Convert period records into statement-table rows for the UI."""
    rows: list[dict[str, Any]] = []
    for spec in metric_specs:
        rows.append(
            {
                "metric": spec.display_name,
                "field": spec.source_field,
                "record_field": spec.record_field,
                "source": source_key,
                "values": [
                    None if pd.isna(record.get(spec.record_field)) else record.get(spec.record_field)
                    for record in records
                ],
            }
        )
    return rows


def _sql_literal(value: Any) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _normalise_taxonomy_term(value: Any) -> str | None:
    text = _safe_str(value)
    if not text:
        return None
    if ":" in text:
        return text
    match = re.match(r"^([A-Za-z0-9\-]+_[A-Za-z0-9\-]+)_(.+)$", text)
    if match:
        return f"{match.group(1)}:{match.group(2)}"
    return text


def _statement_family_for_table_name(table_name: str | None) -> str | None:
    normalized = re.sub(r"[^0-9A-Za-z]+", "", _safe_str(table_name)).lower()
    families = {
        "incomestatement": "IncomeStatement",
        "balancesheet": "BalanceSheet",
        "cashflowstatement": "CashflowStatement",
    }
    return families.get(normalized)


def _load_statement_line_items(
    conn: sqlite3.Connection,
    table_name: str | None,
) -> list[dict[str, Any]]:
    family = _statement_family_for_table_name(table_name)
    if not family:
        return []

    table_map = _list_table_map(conn)
    metadata_table = _resolve_table_name(table_map, [_STATEMENT_LINE_ITEMS_TABLE], required=False)
    if not metadata_table:
        return []

    rows = conn.execute(
        f"""
        SELECT
            statement_family,
            concept_qname,
            column_name,
            display_label,
            concept_name,
            role_uri,
            presentation_parent_qname,
            parent_column_name,
            line_order,
            line_depth,
            period_key,
            value_type,
            is_abstract,
            is_required_metric
        FROM {_quote_ident(metadata_table)}
        WHERE statement_family = ?
        ORDER BY
            COALESCE(role_uri, ''),
            COALESCE(line_order, 999999999.0),
            COALESCE(line_depth, 0),
            COALESCE(display_label, concept_name, concept_qname, column_name, '')
        """,
        (family,),
    ).fetchall()
    return [dict(row) for row in rows]


def _is_taxonomy_statement_table(conn: sqlite3.Connection, table_name: str | None) -> bool:
    if not table_name:
        return False
    columns = {column.lower() for column in _get_columns(conn, table_name)}
    if "concept_qname" in columns:
        return True
    return any(_safe_str(row.get("column_name")) for row in _load_statement_line_items(conn, table_name))



def _load_statement_fact_metric_values(
    conn: sqlite3.Connection,
    doc_id: str | None,
    metric_request: dict[str, tuple[str, ...]],
) -> dict[str, Any]:
    if not doc_id or not metric_request:
        return {}

    values: dict[str, Any] = {}
    table_map = _list_table_map(conn)
    fs_table = _resolve_table_name(table_map, ["FinancialStatements"], required=False)
    if fs_table:
        fs_columns = _get_columns(conn, fs_table)
        fs_docid_col = _resolve_column(fs_columns, ["docID", "DocID"], required=False)
        fs_column_map = {column.lower(): column for column in fs_columns}
        wanted = []
        for metric_names in metric_request.values():
            for metric_name in metric_names:
                actual_column = fs_column_map.get(metric_name.lower())
                if actual_column and metric_name not in values:
                    wanted.append((metric_name, actual_column))

        if fs_docid_col and wanted:
            row = conn.execute(
                f"SELECT {', '.join(f'fs.{_quote_ident(actual)} AS {_quote_ident(metric)}' for metric, actual in wanted)} "
                f"FROM {_quote_ident(fs_table)} fs "
                f"WHERE fs.{_quote_ident(fs_docid_col)} = ?",
                (doc_id,),
            ).fetchone()
            if row:
                values.update(dict(row))

    return values


def _taxonomy_statement_context_parts(row: dict[str, Any]) -> list[str]:
    parts = [
        _safe_str(row.get("source_period")),
        _safe_str(row.get("source_relative_year")),
        _safe_str(row.get("source_consolidation")),
    ]
    role_uri = _safe_str(row.get("role_uri"))
    if role_uri:
        parts.append(role_uri.rsplit("/", 1)[-1])
    return [part for part in parts if part]


def _taxonomy_statement_rows(
    conn: sqlite3.Connection,
    table_name: str,
    records: list[dict[str, Any]],
    source_key: str,
) -> list[dict[str, Any]]:
    if not table_name:
        return []

    table_columns = {column.lower() for column in _get_columns(conn, table_name)}
    if "concept_qname" not in table_columns:
        doc_ids = [_safe_str(record.get("docID")) for record in records if _safe_str(record.get("docID"))]
        if not doc_ids:
            return []

        metadata_rows = [
            row for row in _load_statement_line_items(conn, table_name)
            if _safe_str(row.get("column_name"))
        ]
        if not metadata_rows:
            return []

        placeholders = ",".join(["?"] * len(doc_ids))
        statement_rows = [
            dict(row)
            for row in conn.execute(
                f"SELECT * FROM {_quote_ident(table_name)} WHERE docID IN ({placeholders})",
                doc_ids,
            ).fetchall()
        ]
        rows_by_docid = {
            _safe_str(row.get("docID")): dict(row)
            for row in statement_rows
            if _safe_str(row.get("docID"))
        }
        label_counts: Counter[str] = Counter(
            _safe_str(row.get("display_label")) or _safe_str(row.get("concept_name")) or _safe_str(row.get("column_name"))
            for row in metadata_rows
        )

        out_rows: list[dict[str, Any]] = []
        for meta in metadata_rows:
            column_name = _safe_str(meta.get("column_name"))
            if not column_name:
                continue
            metric_name = _safe_str(meta.get("display_label")) or _safe_str(meta.get("concept_name")) or column_name
            concept_qname = _safe_str(meta.get("concept_qname")) or column_name
            if label_counts[metric_name] > 1:
                metric_name = f"{metric_name} [{concept_qname}]"

            values: list[Any] = []
            for record in records:
                doc_id = _safe_str(record.get("docID"))
                row = rows_by_docid.get(doc_id, {})
                value = row.get(column_name)
                if pd.isna(value):
                    value = None
                values.append(value)

            out_rows.append(
                {
                    "metric": metric_name,
                    "field": concept_qname,
                    "record_field": column_name,
                    "source": source_key,
                    "values": values,
                    "line_depth": meta.get("line_depth"),
                    "parent_field": _safe_str(meta.get("presentation_parent_qname")) or None,
                    "parent_record_field": _safe_str(meta.get("parent_column_name")) or None,
                    "role_uri": _safe_str(meta.get("role_uri")) or None,
                }
            )
        return out_rows

    doc_ids = [_safe_str(record.get("docID")) for record in records if _safe_str(record.get("docID"))]
    if not doc_ids:
        return []

    placeholders = ",".join(["?"] * len(doc_ids))
    rows = [
        dict(row)
        for row in conn.execute(
            f"""
            SELECT
                docID,
                concept_qname,
                concept_name,
                display_label,
                role_uri,
                line_order,
                line_depth,
                source_period,
                source_relative_year,
                source_consolidation,
                value_numeric,
                raw_value_text
            FROM {_quote_ident(table_name)}
            WHERE docID IN ({placeholders})
            """,
            doc_ids,
        ).fetchall()
    ]
    if not rows:
        return []

    doc_index = {doc_id: index for index, doc_id in enumerate(doc_ids)}
    latest_priority = {doc_id: len(doc_ids) - index for doc_id, index in doc_index.items()}
    rows.sort(
        key=lambda row: (
            -latest_priority.get(_safe_str(row.get("docID")), 0),
            _safe_str(row.get("role_uri")),
            float(row.get("line_order")) if row.get("line_order") is not None else float("inf"),
            int(row.get("line_depth") or 0),
            _safe_str(row.get("concept_qname")),
        )
    )

    grouped: dict[str, dict[str, Any]] = {}
    ordered_fields: list[str] = []
    label_counts: Counter[str] = Counter()

    for row in rows:
        doc_id = _safe_str(row.get("docID"))
        index = doc_index.get(doc_id)
        if index is None:
            continue
        context_parts = _taxonomy_statement_context_parts(row)
        field_parts = [_safe_str(row.get("concept_qname"))]
        field_parts.extend(context_parts)
        field_name = "|".join(part for part in field_parts if part)
        if not field_name:
            continue

        metric_name = _safe_str(row.get("display_label")) or _safe_str(row.get("concept_name")) or _safe_str(row.get("concept_qname"))
        if field_name not in grouped:
            grouped[field_name] = {
                "metric": metric_name,
                "field": field_name,
                "context_parts": context_parts,
                "values": [None] * len(records),
            }
            ordered_fields.append(field_name)
            label_counts[metric_name] += 1

        value = row.get("value_numeric")
        if value is None:
            value = row.get("raw_value_text")
        if grouped[field_name]["values"][index] is None:
            grouped[field_name]["values"][index] = value

    out_rows: list[dict[str, Any]] = []
    for field_name in ordered_fields:
        entry = grouped[field_name]
        metric_name = entry["metric"]
        if label_counts[metric_name] > 1 and entry["context_parts"]:
            metric_name = f"{metric_name} [{' | '.join(entry['context_parts'])}]"
        out_rows.append(
            {
                "metric": metric_name,
                "field": entry["field"],
                "record_field": entry["field"],
                "source": source_key,
                "values": entry["values"],
            }
        )
    return out_rows


def _as_peer_row(snapshot: dict[str, Any]) -> dict[str, Any]:
    """Project a latest security snapshot into a peer-table row."""
    ratios = _compute_ratio_payload(snapshot)
    return {
        "company_code": _safe_str(snapshot.get("company_code")),
        "ticker": _safe_str(snapshot.get("ticker")),
        "company_name": _safe_str(snapshot.get("company_name")),
        "industry": _safe_str(snapshot.get("industry")),
        "latest_price": snapshot.get("latest_price"),
        "latest_price_date": _safe_date_str(snapshot.get("latest_price_date")),
        "PERatio": ratios.get("PERatio"),
        "PriceToBook": ratios.get("PriceToBook"),
        "DividendsYield": ratios.get("DividendsYield"),
        "ReturnOnEquity": _safe_float(snapshot.get("ReturnOnEquity")),
        "MarketCap": ratios.get("MarketCap"),
        "one_year_return": snapshot.get("one_year_return"),
        "period_end": _safe_date_str(snapshot.get("period_end")),
    }


def _compute_ratio_payload(snapshot: dict[str, Any]) -> dict[str, Any]:
    """Compute ratio values with fallbacks when direct values are missing."""
    share_price = _safe_float(_coalesce(snapshot.get("latest_price"), snapshot.get("SharePrice")))
    eps = _safe_float(snapshot.get("EPS"))
    book_value = _safe_float(snapshot.get("BookValue"))
    dividends = _safe_float(snapshot.get("Dividends"))
    shares_outstanding = _safe_float(snapshot.get("SharesOutstanding"))
    market_cap = _safe_float(snapshot.get("MarketCap"))

    pe_ratio = _safe_float(snapshot.get("PERatio"))
    if pe_ratio is None and share_price not in (None, 0.0) and eps not in (None, 0.0) and eps > 0:
        pe_ratio = share_price / eps

    price_to_book = _safe_float(snapshot.get("PriceToBook"))
    if price_to_book is None and share_price not in (None, 0.0) and book_value not in (None, 0.0) and book_value > 0:
        price_to_book = share_price / book_value

    dividend_yield = _safe_float(snapshot.get("DividendsYield"))
    if dividend_yield is None and share_price not in (None, 0.0) and dividends is not None:
        dividend_yield = dividends / share_price

    if market_cap is None and share_price is not None and shares_outstanding is not None:
        market_cap = share_price * shares_outstanding

    return {
        "PERatio": pe_ratio,
        "PriceToBook": price_to_book,
        "DividendsYield": dividend_yield,
        "MarketCap": market_cap,
        "EarningsYield": _safe_float(snapshot.get("EarningsYield")),
        "PriceToSales": _safe_float(snapshot.get("PriceToSales")),
        "EnterpriseValue": _safe_float(snapshot.get("EnterpriseValue")),
        "EnterpriseValueToSales": _safe_float(snapshot.get("EnterpriseValueToSales")),
        "ReturnOnEquity": _safe_float(snapshot.get("ReturnOnEquity")),
        "DebtToEquity": _safe_float(snapshot.get("DebtToEquity")),
        "CurrentRatio": _safe_float(snapshot.get("CurrentRatio")),
        "GrossMargin": _safe_float(snapshot.get("GrossMargin")),
        "OperatingMargin": _safe_float(snapshot.get("OperatingMargin")),
        "NetProfitMargin": _safe_float(snapshot.get("NetProfitMargin")),
    }


# ---------------------------------------------------------------------------
#  3. SNAPSHOT QUERIES
# ---------------------------------------------------------------------------


def _load_company_frame(conn: sqlite3.Connection, schema: SecuritySchema) -> pd.DataFrame:
    """Load core company information into a normalised DataFrame."""
    company_name_parts = [
        f"NULLIF(TRIM(CAST(c.{_quote_ident(schema.company_name_col)} AS TEXT)), '')"
    ]
    if (
        schema.company_name_fallback_col
        and schema.company_name_fallback_col.lower() != schema.company_name_col.lower()
    ):
        company_name_parts.append(
            f"NULLIF(TRIM(CAST(c.{_quote_ident(schema.company_name_fallback_col)} AS TEXT)), '')"
        )

    select_parts = [
        f"c.{_quote_ident(schema.company_code_col)} AS company_code",
        f"c.{_quote_ident(schema.company_ticker_col)} AS ticker",
        f"COALESCE({', '.join(company_name_parts)}) AS company_name",
    ]
    if schema.company_industry_col:
        select_parts.append(f"c.{_quote_ident(schema.company_industry_col)} AS industry")
    else:
        select_parts.append("NULL AS industry")
    if schema.company_market_col:
        select_parts.append(f"c.{_quote_ident(schema.company_market_col)} AS market")
    else:
        select_parts.append("NULL AS market")
    if schema.company_description_col:
        select_parts.append(f"c.{_quote_ident(schema.company_description_col)} AS description")
    else:
        select_parts.append("NULL AS description")

    sql = (
        f"SELECT {', '.join(select_parts)} "
        f"FROM {_quote_ident(schema.company_table)} c"
    )
    return pd.read_sql_query(sql, conn)


@lru_cache(maxsize=4)
def _get_cached_company_frame(db_path: str) -> pd.DataFrame:
    """Cache the normalised company snapshot per database path."""
    normalised_path = _normalise_db_path(db_path)
    schema = resolve_schema(normalised_path)
    conn = _connect(normalised_path)
    try:
        return _load_company_frame(conn, schema)
    finally:
        conn.close()


def ensure_security_analysis_indexes(db_path: str) -> dict[str, Any]:
    """Create one-time indexes used by the Security Analysis view.

    The standardized database can be very large, especially `Stock_Prices`.
    These indexes target the specific access patterns used by search, overview,
    statement selection, price-history lookups, and peer comparisons.
    """
    normalised_path = _normalise_db_path(db_path)
    if normalised_path in _OPTIMIZED_DB_PATHS:
        return {"ok": True, "created": [], "cached": True}

    lock = _get_db_optimization_lock(normalised_path)
    with lock:
        if normalised_path in _OPTIMIZED_DB_PATHS:
            return {"ok": True, "created": [], "cached": True}

        schema = resolve_schema(normalised_path)
        created: list[str] = []
        with transaction(normalised_path) as conn:
            statements = [
                (
                    "idx_sa_prices_ticker_date",
                    f"CREATE INDEX IF NOT EXISTS [idx_sa_prices_ticker_date] "
                    f"ON {_quote_ident(schema.prices_table)} ([Ticker], [Date])",
                ),
                (
                    "idx_sa_company_edinet",
                    f"CREATE INDEX IF NOT EXISTS [idx_sa_company_edinet] "
                    f"ON {_quote_ident(schema.company_table)} ({_quote_ident(schema.company_code_col)})",
                ),
                (
                    "idx_sa_company_ticker",
                    f"CREATE INDEX IF NOT EXISTS [idx_sa_company_ticker] "
                    f"ON {_quote_ident(schema.company_table)} ({_quote_ident(schema.company_ticker_col)})",
                ),
                (
                    "idx_sa_fs_edinet_period",
                    f"CREATE INDEX IF NOT EXISTS [idx_sa_fs_edinet_period] "
                    f"ON {_quote_ident(schema.financial_statements_table)} "
                    f"({_quote_ident(schema.fs_code_col)}, {_quote_ident(schema.fs_period_end_col)})",
                ),
            ]
            if schema.company_industry_col:
                statements.append(
                    (
                        "idx_sa_company_industry",
                        f"CREATE INDEX IF NOT EXISTS [idx_sa_company_industry] "
                        f"ON {_quote_ident(schema.company_table)} ({_quote_ident(schema.company_industry_col)})",
                    )
                )

            for index_name, sql in statements:
                conn.execute(sql)
                created.append(index_name)

        _OPTIMIZED_DB_PATHS.add(normalised_path)
        logger.info(
            "Security Analysis indexes ensured for %s: %s",
            normalised_path,
            ", ".join(created) if created else "none",
        )
        return {"ok": True, "created": created, "cached": False}


def _load_company_record(db_path: str, company_code: str) -> dict[str, Any] | None:
    """Return a single company record from the cached company snapshot."""
    company_df = _get_cached_company_frame(db_path)
    matches = company_df[company_df["company_code"].astype(str) == str(company_code)]
    if matches.empty:
        return None
    return matches.iloc[0].to_dict()


def _load_company_by_ticker(db_path: str, ticker: str) -> dict[str, Any] | None:
    """Find a company record by ticker (searches CompanyInfo table directly)."""
    conn = connect_read(db_path)
    try:
        row = conn.execute(
            "SELECT * FROM CompanyInfo WHERE Company_Ticker = ? LIMIT 1", (ticker,)
        ).fetchone()
        if row:
            return dict(row)
        return None
    finally:
        conn.close()


def _load_latest_prices_frame(conn: sqlite3.Connection, schema: SecuritySchema) -> pd.DataFrame:
    """Load the latest available price row per ticker."""
    sql = f"""
        SELECT p.Ticker AS ticker, p.[Date] AS latest_price_date, p.Price AS latest_price
        FROM {_quote_ident(schema.prices_table)} p
        INNER JOIN (
            SELECT Ticker, MAX([Date]) AS MaxDate
            FROM {_quote_ident(schema.prices_table)}
            GROUP BY Ticker
        ) px ON px.Ticker = p.Ticker AND px.MaxDate = p.[Date]
    """
    return pd.read_sql_query(sql, conn)


def _load_latest_prices_for_tickers(
    conn: sqlite3.Connection,
    schema: SecuritySchema,
    tickers: list[str],
) -> pd.DataFrame:
    """Load the latest available price row for a small set of tickers."""
    clean_tickers = sorted({_safe_str(ticker) for ticker in tickers if _safe_str(ticker)})
    if not clean_tickers:
        return pd.DataFrame(columns=["ticker", "latest_price_date", "latest_price"])

    placeholders = ",".join(["?"] * len(clean_tickers))
    sql = f"""
        SELECT p.Ticker AS ticker, p.[Date] AS latest_price_date, p.Price AS latest_price
        FROM {_quote_ident(schema.prices_table)} p
        INNER JOIN (
            SELECT Ticker, MAX([Date]) AS MaxDate
            FROM {_quote_ident(schema.prices_table)}
            WHERE Ticker IN ({placeholders})
            GROUP BY Ticker
        ) px ON px.Ticker = p.Ticker AND px.MaxDate = p.[Date]
    """
    return pd.read_sql_query(sql, conn, params=clean_tickers)


def _load_price_range(conn: sqlite3.Connection, schema: SecuritySchema, ticker: str) -> dict[str, Any]:
    """Load latest price, previous price, and trailing 52-week range."""
    latest_df = pd.read_sql_query(
        f"SELECT [Date], Price FROM {_quote_ident(schema.prices_table)} "
        "WHERE Ticker = ? ORDER BY [Date] DESC LIMIT 2",
        conn,
        params=[ticker],
    )
    if latest_df.empty:
        return {
            "latest_price": None,
            "latest_price_date": None,
            "previous_price": None,
            "change_pct_1d": None,
            "range_52w_low": None,
            "range_52w_high": None,
        }

    latest_price = _safe_float(latest_df.iloc[0]["Price"])
    latest_date = pd.to_datetime(latest_df.iloc[0]["Date"], errors="coerce")
    previous_price = None
    if len(latest_df) > 1:
        previous_price = _safe_float(latest_df.iloc[1]["Price"])

    change_pct = None
    if latest_price not in (None, 0.0) and previous_price not in (None, 0.0):
        change_pct = (latest_price - previous_price) / previous_price

    low_52 = None
    high_52 = None
    if latest_date is not pd.NaT:
        start_date = (latest_date - timedelta(days=365)).strftime("%Y-%m-%d")
        range_df = pd.read_sql_query(
            f"SELECT MIN(Price) AS low_price, MAX(Price) AS high_price "
            f"FROM {_quote_ident(schema.prices_table)} WHERE Ticker = ? AND [Date] >= ?",
            conn,
            params=[ticker, start_date],
        )
        if not range_df.empty:
            low_52 = _safe_float(range_df.iloc[0]["low_price"])
            high_52 = _safe_float(range_df.iloc[0]["high_price"])

    return {
        "latest_price": latest_price,
        "latest_price_date": latest_date.strftime("%Y-%m-%d") if latest_date is not pd.NaT else None,
        "previous_price": previous_price,
        "change_pct_1d": change_pct,
        "range_52w_low": low_52,
        "range_52w_high": high_52,
    }


def _load_latest_snapshot(conn: sqlite3.Connection, schema: SecuritySchema, company_code: str) -> dict[str, Any] | None:
    """Load the latest filing snapshot for a company."""
    select_parts = [
        f"fs.{_quote_ident(schema.fs_docid_col)} AS docID",
        f"fs.{_quote_ident(schema.fs_code_col)} AS company_code",
        f"fs.{_quote_ident(schema.fs_period_end_col)} AS period_end",
    ]

    if schema.fs_description_col:
        select_parts.append(
            f"fs.{_quote_ident(schema.fs_description_col)} AS filing_description"
        )
    else:
        select_parts.append("NULL AS filing_description")
    if schema.fs_description_en_col:
        select_parts.append(
            f"fs.{_quote_ident(schema.fs_description_en_col)} AS filing_description_en"
        )
    else:
        select_parts.append("NULL AS filing_description_en")

    if schema.fs_shares_outstanding_col:
        select_parts.append(
            f"fs.{_quote_ident(schema.fs_shares_outstanding_col)} AS SharesOutstanding"
        )
    else:
        select_parts.append("NULL AS SharesOutstanding")
    if schema.fs_share_price_col:
        select_parts.append(
            f"fs.{_quote_ident(schema.fs_share_price_col)} AS SharePrice"
        )
    else:
        select_parts.append("NULL AS SharePrice")

    join_clauses: list[str] = []
    needs_taxonomy_overview_metrics = False

    # Helper: select only the columns we actually need for the overview from a
    # wide table.  Selecting every column can blow past SQLite's 2000-column
    # result-set limit when several wide tables are joined (e.g. IncomeStatement
    # alone often carries 120+ columns).
    def _add_needed_columns(
        alias: str,
        table_name: str,
        needed_lower: frozenset[str],
    ) -> None:
        """Append *only* the columns in *needed_lower* that actually exist in
        the table to ``select_parts``.  Matching is case-insensitive and
        whitespace/punctuation-insensitive against the real column names
        returned by PRAGMA table_info (e.g. database column ``Net sales``
        matches whitelist entry ``netsales``)."""
        real_cols = _get_columns(conn, table_name)
        # Pre-compute normalised versions of needed entries for comparison
        needed_normalised = {_normalise_ident(k) for k in needed_lower}
        found = 0
        for col in real_cols:
            if col.lower() == "docid":
                continue
            if _normalise_ident(col) in needed_normalised:
                select_parts.append(
                    f"{alias}.{_quote_ident(col)} AS {_quote_ident(col)}"
                )
                found += 1
        # If we didn't find any of the needed columns, the table probably uses a
        # different naming convention — fall back to selecting everything (the
        # old behaviour) so we don't silently drop data.
        if found == 0 and real_cols:
            for col in real_cols:
                if col.lower() != "docid":
                    select_parts.append(
                        f"{alias}.{_quote_ident(col)} AS {_quote_ident(col)}"
                    )

    if schema.income_table:
        if _is_taxonomy_statement_table(conn, schema.income_table):
            needs_taxonomy_overview_metrics = True
        else:
            join_clauses.append(
                f"LEFT JOIN {_quote_ident(schema.income_table)} i ON i.docID = fs.{_quote_ident(schema.fs_docid_col)}"
            )
            _add_needed_columns("i", schema.income_table, _OVERVIEW_INCOME_COLS)
    if schema.balance_table:
        if _is_taxonomy_statement_table(conn, schema.balance_table):
            needs_taxonomy_overview_metrics = True
        else:
            join_clauses.append(
                f"LEFT JOIN {_quote_ident(schema.balance_table)} b ON b.docID = fs.{_quote_ident(schema.fs_docid_col)}"
            )
            _add_needed_columns("b", schema.balance_table, _OVERVIEW_BALANCE_COLS)
    if schema.cashflow_table:
        if not _is_taxonomy_statement_table(conn, schema.cashflow_table):
            join_clauses.append(
                f"LEFT JOIN {_quote_ident(schema.cashflow_table)} cf ON cf.docID = fs.{_quote_ident(schema.fs_docid_col)}"
            )
            _add_needed_columns("cf", schema.cashflow_table, _OVERVIEW_CASHFLOW_COLS)
    if schema.per_share_table:
        join_clauses.append(
            f"LEFT JOIN {_quote_ident(schema.per_share_table)} ps ON ps.docID = fs.{_quote_ident(schema.fs_docid_col)}"
        )
        _add_needed_columns("ps", schema.per_share_table, _OVERVIEW_PERSHARE_COLS)
    if schema.valuation_table:
        join_clauses.append(
            f"LEFT JOIN {_quote_ident(schema.valuation_table)} v ON v.docID = fs.{_quote_ident(schema.fs_docid_col)}"
        )
        _add_needed_columns("v", schema.valuation_table, _OVERVIEW_VALUATION_COLS)
    if schema.quality_table:
        join_clauses.append(
            f"LEFT JOIN {_quote_ident(schema.quality_table)} q ON q.docID = fs.{_quote_ident(schema.fs_docid_col)}"
        )
        _add_needed_columns("q", schema.quality_table, _OVERVIEW_QUALITY_COLS)

    if (
        schema.document_list_table
        and schema.doclist_docid_col
        and schema.doclist_submit_dt_col
    ):
        join_clauses.append(
            f"LEFT JOIN {_quote_ident(schema.document_list_table)} dl "
            f"ON dl.{_quote_ident(schema.doclist_docid_col)} = fs.{_quote_ident(schema.fs_docid_col)}"
        )
        select_parts.append(
            f"dl.{_quote_ident(schema.doclist_submit_dt_col)} AS submitDateTime"
        )
        order_clause = (
            f"ORDER BY fs.{_quote_ident(schema.fs_period_end_col)} DESC, "
            "submitDateTime DESC, fs.docID DESC"
        )
    else:
        order_clause = (
            f"ORDER BY fs.{_quote_ident(schema.fs_period_end_col)} DESC, "
            f"fs.{_quote_ident(schema.fs_docid_col)} DESC"
        )

    sql = (
        f"SELECT {', '.join(select_parts)} "
        f"FROM {_quote_ident(schema.financial_statements_table)} fs "
        f"{' '.join(join_clauses)} "
        f"WHERE fs.{_quote_ident(schema.fs_code_col)} = ? "
        f"{order_clause} LIMIT 1"
    )
    df = pd.read_sql_query(sql, conn, params=[company_code])
    if df.empty:
        return None
    row = df.iloc[0].to_dict()
    if needs_taxonomy_overview_metrics:
        taxonomy_metrics = _load_statement_fact_metric_values(
            conn,
            _safe_str(row.get("docID")),
            _OVERVIEW_TAXONOMY_METRICS,
        )
        for metric_name, metric_value in taxonomy_metrics.items():
            if row.get(metric_name) is None:
                row[metric_name] = metric_value
    row["period_end"] = _safe_date_str(row.get("period_end"))
    return row


def _latest_snapshots_for_codes(
    conn: sqlite3.Connection,
    schema: SecuritySchema,
    company_codes: list[str],
) -> pd.DataFrame:
    """Load the latest filing snapshot for multiple companies."""
    if not company_codes:
        return pd.DataFrame()

    placeholders = ",".join(["?"] * len(company_codes))
    join_clauses: list[str] = []
    select_parts = [
        f"fs.{_quote_ident(schema.fs_docid_col)} AS docID",
        f"fs.{_quote_ident(schema.fs_code_col)} AS company_code",
        f"fs.{_quote_ident(schema.fs_period_end_col)} AS period_end",
    ]
    if schema.fs_shares_outstanding_col:
        select_parts.append(
            f"fs.{_quote_ident(schema.fs_shares_outstanding_col)} AS SharesOutstanding"
        )
    else:
        select_parts.append("NULL AS SharesOutstanding")
    if schema.per_share_table:
        join_clauses.append(
            f"LEFT JOIN {_quote_ident(schema.per_share_table)} ps ON ps.docID = fs.{_quote_ident(schema.fs_docid_col)}"
        )
        for col in ("EPS", "BookValue", "Dividends"):
            if col in _get_columns(conn, schema.per_share_table):
                select_parts.append(f"ps.{_quote_ident(col)} AS {_quote_ident(col)}")
    if schema.valuation_table:
        join_clauses.append(
            f"LEFT JOIN {_quote_ident(schema.valuation_table)} v ON v.docID = fs.{_quote_ident(schema.fs_docid_col)}"
        )
        for col in ("PERatio", "PriceToBook", "DividendsYield", "MarketCap"):
            if col in _get_columns(conn, schema.valuation_table):
                select_parts.append(f"v.{_quote_ident(col)} AS {_quote_ident(col)}")
    if schema.quality_table:
        join_clauses.append(
            f"LEFT JOIN {_quote_ident(schema.quality_table)} q ON q.docID = fs.{_quote_ident(schema.fs_docid_col)}"
        )
        for col in ("ReturnOnEquity",):
            if col in _get_columns(conn, schema.quality_table):
                select_parts.append(f"q.{_quote_ident(col)} AS {_quote_ident(col)}")

    sql = (
        f"SELECT {', '.join(select_parts)} "
        f"FROM {_quote_ident(schema.financial_statements_table)} fs "
        f"{' '.join(join_clauses)} "
        f"WHERE fs.{_quote_ident(schema.fs_code_col)} IN ({placeholders}) "
        f"ORDER BY fs.{_quote_ident(schema.fs_code_col)}, "
        f"fs.{_quote_ident(schema.fs_period_end_col)} DESC, "
        f"fs.{_quote_ident(schema.fs_docid_col)} DESC"
    )
    df = pd.read_sql_query(sql, conn, params=company_codes)
    if df.empty:
        return df
    df["period_end"] = df["period_end"].astype(str).str[:10]
    df = df.drop_duplicates(subset=["company_code"], keep="first").reset_index(drop=True)
    return df


def _price_return_1y(conn: sqlite3.Connection, schema: SecuritySchema, tickers: list[str]) -> pd.DataFrame:
    """Return latest and trailing one-year price return per ticker."""
    if not tickers:
        return pd.DataFrame(columns=["ticker", "one_year_return"])  # pragma: no cover - trivial guard

    placeholders = ",".join(["?"] * len(tickers))
    sql = (
        f"SELECT Ticker AS ticker, [Date] AS trade_date, Price "
        f"FROM {_quote_ident(schema.prices_table)} WHERE Ticker IN ({placeholders}) "
        "ORDER BY ticker, trade_date"
    )
    prices_df = pd.read_sql_query(sql, conn, params=tickers)
    if prices_df.empty:
        return pd.DataFrame(columns=["ticker", "one_year_return"])

    prices_df["trade_date"] = pd.to_datetime(prices_df["trade_date"], errors="coerce")
    prices_df["Price"] = pd.to_numeric(prices_df["Price"], errors="coerce")
    out_rows: list[dict[str, Any]] = []
    for ticker, ticker_df in prices_df.groupby("ticker"):
        ticker_df = ticker_df.dropna(subset=["trade_date", "Price"]).sort_values("trade_date")
        if ticker_df.empty:
            out_rows.append({"ticker": ticker, "one_year_return": None})
            continue
        latest = ticker_df.iloc[-1]
        target_date = latest["trade_date"] - timedelta(days=365)
        prior_df = ticker_df[ticker_df["trade_date"] <= target_date]
        if prior_df.empty:
            out_rows.append({"ticker": ticker, "one_year_return": None})
            continue
        prior = prior_df.iloc[-1]
        if prior["Price"] in (None, 0.0):
            out_rows.append({"ticker": ticker, "one_year_return": None})
            continue
        out_rows.append(
            {
                "ticker": ticker,
                "one_year_return": (float(latest["Price"]) - float(prior["Price"])) / float(prior["Price"]),
            }
        )
    return pd.DataFrame(out_rows)


# ---------------------------------------------------------------------------
#  4. PUBLIC API
# ---------------------------------------------------------------------------


def _normalize_ibkr_ticker(ticker: str) -> str:
    """Convert IBKR-format tickers to db2 format.

    IBKR appends exchange suffixes like ``5984.T`` (Tokyo).  The database
    stores these as 5-digit tickers: ``59840``.  This function strips the
    ``.T`` suffix and appends a ``0`` to create a valid db2 ticker.

    Returns the original unchanged if it doesn't match the IBKR pattern.
    """
    t = ticker.strip()
    # Pattern: 4 digits + .T (e.g. 5984.T → 59840)
    if len(t) == 6 and t.endswith(".T") and t[:4].isdigit():
        return t[:4] + "0"
    # Already 5 digits (e.g. 59840) — keep as-is
    if len(t) == 5 and t.isdigit():
        return t
    return t


def _normalize_ticker_for_query(ticker: str) -> list[str]:
    """Return a list of ticker variants to try when querying the database.

    For a ticker like ``5984.T``, returns ``["5984.T", "59840"]`` so the
    search can fall back to the db2 format if the IBKR format is not found.
    """
    t = ticker.strip()
    variants = [t]
    if t.endswith(".T") and len(t) == 6 and t[:4].isdigit():
        variants.append(t[:4] + "0")
    elif len(t) == 5 and t.isdigit():
        variants.append(t[:4] + ".T")
    return variants


def search_securities(db_path: str, query: str, limit: int = 25) -> list[dict[str, Any]]:
    """Search companies by name, ticker, EDINET code, or industry.

    Also searches the Stock_Prices table for tickers not found in CompanyInfo.

    Args:
        db_path (str): Path to the SQLite database.
        query (str): Free-text search query.
        limit (int): Maximum number of results to return.

    Returns:
        list[dict[str, Any]]: Ranked company matches with latest price fields.
    """
    tokens = [token.lower() for token in query.strip().split() if token.strip()]
    if not tokens:
        return []

    company_df = _get_cached_company_frame(db_path)
    scored: list[tuple[int, dict[str, Any]]] = []
    seen_tickers: set[str] = set()
    for record in company_df.to_dict(orient="records"):
        score = _score_security_match(record, tokens)
        if score is None:
            continue
        ticker = _safe_str(record.get("ticker"))
        if ticker:
            seen_tickers.add(ticker.lower())
            # Also mark the IBKR variant as seen (e.g. 59110 → 5911.T)
            for v in _normalize_ticker_for_query(ticker):
                seen_tickers.add(v.lower())
        scored.append((score, {
            "company_code": _safe_str(record.get("company_code")),
            "ticker": ticker,
            "company_name": _safe_str(record.get("company_name")),
            "industry": _safe_str(record.get("industry")),
            "market": _safe_str(record.get("market")),
            "latest_price": None,
            "latest_price_date": None,
        }))

    # Also search Stock_Prices for ticker matches not already covered by CompanyInfo
    schema = resolve_schema(db_path)
    conn = _connect(db_path)
    try:
        # Build ticker search: match any ticker containing the query as substring
        price_tickers_df = pd.read_sql_query(
            f"SELECT DISTINCT Ticker FROM {_quote_ident(schema.prices_table)}",
            conn,
        )
        for _, row in price_tickers_df.iterrows():
            pticker = _safe_str(row.get("Ticker"))
            if not pticker:
                continue
            # Skip if the ticker or its db2-normalized form already has a CompanyInfo record
            pt_lower = pticker.lower()
            variants = _normalize_ticker_for_query(pticker)
            already_seen = any(v.lower() in seen_tickers for v in variants)
            if already_seen:
                continue
            # Check if the ticker matches any token (case-insensitive substring)
            if not any(token in pt_lower for token in tokens):
                continue
            # Get latest price for this ticker
            lpr = pd.read_sql_query(
                f"SELECT [Date], Price FROM {_quote_ident(schema.prices_table)} "
                f"WHERE Ticker = ? ORDER BY [Date] DESC LIMIT 1",
                conn,
                params=[pticker],
            )
            latest_price = None
            latest_price_date = None
            if not lpr.empty:
                latest_price = _safe_float(lpr.iloc[0]["Price"])
                latest_price_date = _safe_date_str(lpr.iloc[0]["Date"])
            scored.append((1, {  # score=1 for direct ticker match
                "company_code": None,
                "ticker": pticker,
                "company_name": pticker,
                "industry": None,
                "market": None,
                "latest_price": latest_price,
                "latest_price_date": latest_price_date,
            }))
    finally:
        conn.close()

    scored.sort(
        key=lambda item: (
            -item[0],
            item[1]["company_name"].lower(),
            item[1]["ticker"],
        )
    )

    return [record for _, record in scored[:limit]]


def get_security_overview(db_path: str, company_code: str = "", ticker: str = "") -> dict[str, Any]:
    """Return a summary payload for a selected security.

    Args:
        db_path (str): Path to the SQLite database.
        company_code (str): Selected company EDINET code (preferred).
        ticker (str): Ticker to look up when company_code is empty.

    Returns:
        dict[str, Any]: Company, market, fundamentals, valuation, and metadata.
    """
    code = _safe_str(company_code)
    tkr = _safe_str(ticker)
    if not code and not tkr:
        raise ValueError("Either company_code or ticker is required")

    ensure_security_analysis_indexes(db_path)
    schema = resolve_schema(db_path)

    # Ticker-only lookup: try to find company record, then price data
    if not code and tkr:
        variants = _normalize_ticker_for_query(tkr)
        company = None
        for variant in variants:
            company = _load_company_by_ticker(db_path, variant)
            if company is not None:
                code = company.get("EdinetCode", company.get("company_code", ""))
                break

        if company is not None:
            # Found a company record — proceed to full overview below
            pass
        else:
            # Pure ticker — no CompanyInfo match at all
            conn = _connect(db_path)
            try:
                # Use original ticker for price lookup
                price_info = _load_price_range(conn, schema, tkr)
            finally:
                conn.close()
            return {
            "company": {
                "company_code": None,
                "ticker": tkr,
                "company_name": tkr,
                "industry": None,
                "market": None,
                "description": "",
                "company_info_description": "",
                "filing_description": "",
                "filing_description_en": "",
                "description_summary": "",
            },
            "market": {
                "latest_price": price_info.get("latest_price"),
                "latest_price_date": price_info.get("latest_price_date"),
                "previous_price": price_info.get("previous_price"),
                "change_pct_1d": price_info.get("change_pct_1d"),
                "range_52w_low": price_info.get("range_52w_low"),
                "range_52w_high": price_info.get("range_52w_high"),
            },
            "fundamentals_latest": {},
            "valuation_latest": {},
            "quality_latest": {},
            "metadata": {
                "last_financial_period_end": None,
                "last_price_date": price_info.get("latest_price_date"),
                "doc_id": None,
                "data_quality_flags": ["ticker_only_no_company_record"],
            },
        }

    company = _load_company_record(db_path, code)
    if company is None:
        raise ValueError(f"Security not found for EDINET code: {code}")

    conn = _connect(db_path)
    try:
        try:
            snapshot = _load_latest_snapshot(conn, schema, code)
        except Exception as exc:
            logger.warning(
                "Failed to load latest snapshot for %s (will return partial overview): %s",
                code, exc,
            )
            snapshot = None
        if snapshot is None:
            snapshot = {"company_code": code, "period_end": None}

        ticker = _safe_str(company.get("ticker"))
        price_info = _load_price_range(conn, schema, ticker) if ticker else {
            "latest_price": None,
            "latest_price_date": None,
            "previous_price": None,
            "change_pct_1d": None,
            "range_52w_low": None,
            "range_52w_high": None,
        }
    finally:
        conn.close()

    company_info_description = _clean_text_block(company.get("description"))
    filing_description = _clean_text_block(snapshot.get("filing_description"))
    filing_description_en = _clean_text_block(snapshot.get("filing_description_en"))
    preferred_description = filing_description_en or filing_description or company_info_description
    description_summary = _summarize_preferred_description(preferred_description)

    combined = {
        **snapshot,
        **price_info,
        "ticker": ticker,
        "company_name": _safe_str(company.get("company_name")),
        "industry": _safe_str(company.get("industry")),
        "market": _safe_str(company.get("market")),
        "description": preferred_description,
        "company_info_description": company_info_description,
        "filing_description": filing_description,
        "filing_description_en": filing_description_en,
        "description_summary": description_summary,
    }
    ratios = _compute_ratio_payload(combined)
    data_quality_flags: list[str] = []
    if combined.get("latest_price") is None:
        data_quality_flags.append("missing_latest_price")
    if combined.get("period_end") is None:
        data_quality_flags.append("missing_financial_statements")

    return {
        "company": {
            "company_code": _safe_str(code),
            "ticker": ticker,
            "company_name": _safe_str(company.get("company_name")),
            "industry": _safe_str(company.get("industry")),
            "market": _safe_str(company.get("market")),
            "description": preferred_description,
            "company_info_description": company_info_description,
            "filing_description": filing_description,
            "filing_description_en": filing_description_en,
            "description_summary": description_summary,
        },
        "market": {
            "latest_price": combined.get("latest_price"),
            "latest_price_date": combined.get("latest_price_date"),
            "previous_price": combined.get("previous_price"),
            "change_pct_1d": combined.get("change_pct_1d"),
            "range_52w_low": combined.get("range_52w_low"),
            "range_52w_high": combined.get("range_52w_high"),
        },
        "fundamentals_latest": {
            "Revenue": _safe_float(snapshot.get("netSales")),
            "OperatingIncome": _safe_float(snapshot.get("operatingIncome")),
            "NetIncome": _safe_float(snapshot.get("netIncome")),
            "TotalAssets": _safe_float(snapshot.get("totalAssets")),
            "ShareholdersEquity": _safe_float(snapshot.get("shareholdersEquity")),
            "SharesOutstanding": _safe_float(snapshot.get("SharesOutstanding")),
        },
        "valuation_latest": ratios,
        "quality_latest": {
            "ReturnOnEquity": _safe_float(snapshot.get("ReturnOnEquity")),
            "DebtToEquity": _safe_float(snapshot.get("DebtToEquity")),
            "CurrentRatio": _safe_float(snapshot.get("CurrentRatio")),
            "GrossMargin": _safe_float(snapshot.get("GrossMargin")),
        },
        "metadata": {
            "last_financial_period_end": snapshot.get("period_end"),
            "last_price_date": combined.get("latest_price_date"),
            "doc_id": _safe_str(snapshot.get("docID")),
            "data_quality_flags": data_quality_flags,
        },
    }


def get_security_ratios(db_path: str, company_code: str) -> dict[str, Any]:
    """Return the latest valuation and quality ratios for a security.

    Args:
        db_path (str): Path to the SQLite database.
        company_code (str): Selected company EDINET code.

    Returns:
        dict[str, Any]: Latest ratio values and source metadata.
    """
    overview = get_security_overview(db_path, company_code)
    ratios = dict(overview.get("valuation_latest", {}))
    ratios.update(overview.get("quality_latest", {}))
    ratios["period_end"] = overview.get("metadata", {}).get("last_financial_period_end")
    ratios["latest_price_date"] = overview.get("metadata", {}).get("last_price_date")
    return ratios


def get_security_statements(
    db_path: str,
    company_code: str,
    periods: int = 8,
    statement_sources: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Return historical statements without one oversized cross-table query."""
    from .history import get_security_statements_by_source

    return get_security_statements_by_source(
        db_path,
        company_code,
        periods,
        statement_sources,
    )


def get_security_price_history(
    db_path: str,
    ticker: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> list[dict[str, Any]]:
    """Return historical daily stock prices for a ticker.

    Args:
        db_path (str): Path to the SQLite database.
        ticker (str): Company ticker.
        start_date (str | None): Optional inclusive lower date bound.
        end_date (str | None): Optional inclusive upper date bound.

    Returns:
        list[dict[str, Any]]: Ordered list of price rows.
    """
    if not ticker:
        return []

    ensure_security_analysis_indexes(db_path)
    schema = resolve_schema(db_path)
    conn = _connect(db_path)
    try:
        where_parts = ["Ticker = ?"]
        params: list[Any] = [ticker]
        if start_date:
            where_parts.append("[Date] >= ?")
            params.append(start_date)
        if end_date:
            where_parts.append("[Date] <= ?")
            params.append(end_date)
        sql = (
            f"SELECT [Date] AS trade_date, Price FROM {_quote_ident(schema.prices_table)} "
            f"WHERE {' AND '.join(where_parts)} ORDER BY [Date]"
        )
        df = pd.read_sql_query(sql, conn, params=params)
    finally:
        conn.close()

    if df.empty:
        return []
    df["trade_date"] = df["trade_date"].astype(str).str[:10]
    df["Price"] = pd.to_numeric(df["Price"], errors="coerce")
    return df.rename(columns={"Price": "price"}).to_dict(orient="records")


def get_security_peers(
    db_path: str,
    company_code: str,
    industry: str | None = None,
    limit: int = 10,
) -> list[dict[str, Any]]:
    """Return default peer rows for a security.

    Args:
        db_path (str): Path to the SQLite database.
        company_code (str): Selected company EDINET code.
        industry (str | None): Optional industry override.
        limit (int): Maximum number of peer rows.

    Returns:
        list[dict[str, Any]]: Deterministically ranked peer rows.
    """
    ensure_security_analysis_indexes(db_path)
    schema = resolve_schema(db_path)
    companies = _get_cached_company_frame(db_path)
    selected_df = companies[companies["company_code"].astype(str) == str(company_code)]
    if selected_df.empty:
        return []
    selected = selected_df.iloc[0].to_dict()
    industry_value = _safe_str(industry) or _safe_str(selected.get("industry"))
    if not industry_value:
        return []

    peer_companies = companies[
        (companies["company_code"].astype(str) != str(company_code))
        & (companies["industry"].fillna("").astype(str) == industry_value)
        & (companies["ticker"].fillna("").astype(str) != "")
    ].copy()
    if peer_companies.empty:
        return []

    conn = _connect(db_path)
    try:
        selected_snapshot = _load_latest_snapshot(conn, schema, company_code)
        selected_price_info = _load_price_range(conn, schema, _safe_str(selected.get("ticker")))
        selected_market_cap = _safe_float(
            _compute_ratio_payload({
                **(selected_snapshot or {}),
                **selected_price_info,
            }).get("MarketCap")
        )

        company_codes = peer_companies["company_code"].astype(str).tolist()
        snapshots_df = _latest_snapshots_for_codes(conn, schema, company_codes)
        if snapshots_df.empty:
            return []

        tickers = peer_companies["ticker"].astype(str).tolist()
        latest_prices_df = _load_latest_prices_for_tickers(conn, schema, tickers)
        returns_df = _price_return_1y(conn, schema, tickers)
    finally:
        conn.close()

    merged = peer_companies.merge(snapshots_df, on="company_code", how="inner")
    merged = merged.merge(latest_prices_df, on="ticker", how="left")
    merged = merged.merge(returns_df, on="ticker", how="left")

    rows: list[dict[str, Any]] = []
    for record in merged.to_dict(orient="records"):
        record["latest_price"] = _safe_float(record.get("latest_price"))
        record["latest_price_date"] = _safe_date_str(record.get("latest_price_date"))
        peer_row = _as_peer_row(record)
        peer_row["_rank_distance"] = None
        market_cap = _safe_float(peer_row.get("MarketCap"))
        if selected_market_cap is not None and market_cap is not None:
            peer_row["_rank_distance"] = abs(market_cap - selected_market_cap)
        rows.append(peer_row)

    rows.sort(
        key=lambda row: (
            row.get("_rank_distance") is None,
            row.get("_rank_distance") if row.get("_rank_distance") is not None else 0,
            -(row.get("MarketCap") or 0),
            _safe_str(row.get("company_name")).lower(),
        )
    )

    for row in rows:
        row.pop("_rank_distance", None)
    return rows[:limit]


def update_security_price(db_path: str, ticker: str) -> dict[str, Any]:
    """Update the stock-price history for a single ticker.

    Args:
        db_path (str): Path to the SQLite database.
        ticker (str): Ticker to refresh.

    Returns:
        dict[str, Any]: Structured result payload for the UI.
    """
    if not ticker:
        raise ValueError("ticker is required")

    ensure_security_analysis_indexes(db_path)
    schema = resolve_schema(db_path)
    package_module = importlib.import_module(__package__)
    conn = connect_write(db_path)
    try:
        package_module._create_prices_table(conn, schema.prices_table)

        before_count = conn.execute(
            f"SELECT COUNT(*) FROM {_quote_ident(schema.prices_table)} WHERE Ticker = ?",
            [ticker],
        ).fetchone()[0]

        ok = package_module.load_ticker_data(ticker, schema.prices_table, conn)
        conn.commit()

        after_count = conn.execute(
            f"SELECT COUNT(*) FROM {_quote_ident(schema.prices_table)} WHERE Ticker = ?",
            [ticker],
        ).fetchone()[0]
        range_row = conn.execute(
            f"SELECT MIN([Date]), MAX([Date]) FROM {_quote_ident(schema.prices_table)} WHERE Ticker = ?",
            [ticker],
        ).fetchone()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    rows_inserted = int(after_count) - int(before_count)
    if not ok:
        return {
            "ok": False,
            "rows_inserted": rows_inserted,
            "min_date": range_row[0] if range_row else None,
            "max_date": range_row[1] if range_row else None,
            "message": "Price provider rate limit reached or provider returned no data.",
        }

    return {
        "ok": True,
        "rows_inserted": rows_inserted,
        "min_date": range_row[0] if range_row else None,
        "max_date": range_row[1] if range_row else None,
        "message": f"Updated {ticker} with {rows_inserted} new price rows.",
    }
