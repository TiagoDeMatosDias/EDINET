"""Security analysis queries for the Tk UI.

This module centralises company-level lookups used by the Security Analysis
view. It resolves table/column naming differences across databases,
aggregates the latest filing snapshot, loads historical statement data,
retrieves price history, and provides a deterministic peer comparison.
"""

from __future__ import annotations

import html
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

try:
    from sklearn.feature_extraction.text import TfidfVectorizer
except ImportError:  # pragma: no cover - optional dependency
    TfidfVectorizer = None

from src.stockprice_api import _create_prices_table, load_ticker_data

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

# ---------------------------------------------------------------------------
# Schema discovery
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
    company_edinet_col: str
    company_ticker_col: str
    company_name_col: str
    company_name_fallback_col: str | None
    company_industry_col: str | None
    company_market_col: str | None
    company_description_col: str | None
    fs_edinet_col: str
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
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA busy_timeout = 30000")
    conn.row_factory = sqlite3.Row
    return conn


def _quote_ident(name: str) -> str:
    """Return a safely quoted SQLite identifier."""
    return f"[{str(name).replace(']', ']]')}]"


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
            company_edinet_col=_resolve_column(company_cols, ["EdinetCode", "edinetCode"]),
            company_ticker_col=_resolve_column(company_cols, ["Company_Ticker", "Ticker", "ticker"]),
            company_name_col=company_name_col,
            company_name_fallback_col=company_name_fallback_col,
            company_industry_col=_pick_company_industry_col(company_cols),
            company_market_col=_pick_company_market_col(company_cols),
            company_description_col=_pick_company_description_col(company_cols),
            fs_edinet_col=_resolve_column(fs_cols, ["edinetCode", "EdinetCode"]),
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
# Formatting and scoring helpers
# ---------------------------------------------------------------------------


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


def _clean_text_block(value: Any) -> str:
    """Return plain text from long-form text blocks such as filing descriptions."""
    text = _safe_str(value)
    if not text:
        return ""
    text = html.unescape(text)
    text = re.sub(r"(?is)<br\s*/?>", "\n", text)
    text = re.sub(r"(?is)</(?:p|div|li|tr|td|th|section|article|h[1-6])\s*>", "\n", text)
    text = re.sub(r"(?is)<[^>]+>", " ", text)
    text = text.replace("\xa0", " ").replace("\u3000", " ")
    text = re.sub(r"[ \t\r\f\v]+", " ", text)
    text = re.sub(r" *\n *", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _split_description_units(text: Any) -> list[str]:
    """Split a long business description into sentence-like units."""
    cleaned = _clean_text_block(text)
    if not cleaned:
        return []

    units: list[str] = []
    seen: set[str] = set()
    for block in cleaned.split("\n\n"):
        for line in block.splitlines():
            candidate = line.strip(" \t-・●■◆")
            if not candidate:
                continue
            fragments = [
                fragment.strip()
                for fragment in re.split(r"(?<=[。！？!?])", candidate)
                if fragment.strip()
            ]
            for fragment in fragments or [candidate]:
                normalized = re.sub(r"\s+", "", fragment)
                if not normalized or normalized in seen:
                    continue
                units.append(fragment)
                seen.add(normalized)

    longer_units = [unit for unit in units if len(re.sub(r"\s+", "", unit)) >= 12]
    return longer_units or units


def _description_ngrams(text: str) -> set[str]:
    """Return compact character n-grams suitable for Japanese/English ranking."""
    compact = re.sub(r"\s+", "", _clean_text_block(text))
    compact = re.sub(r"[、。・「」『』（）()【】［］\[\]〈〉《》…:：;；,，]", "", compact)
    if not compact:
        return set()

    grams: set[str] = set()
    for size in (2, 3):
        if len(compact) < size:
            continue
        grams.update(compact[index:index + size] for index in range(len(compact) - size + 1))
    if not grams:
        grams.add(compact)
    return grams


def _looks_like_english_text(text: Any) -> bool:
    """Return ``True`` when text appears to be primarily English."""
    cleaned = _clean_text_block(text)
    if not cleaned:
        return False
    latin_chars = len(re.findall(r"[A-Za-z]", cleaned))
    cjk_chars = len(re.findall(r"[\u3040-\u30ff\u3400-\u9fff]", cleaned))
    return latin_chars >= 20 and latin_chars >= cjk_chars


def _split_english_sentences(text: Any) -> list[str]:
    """Split translated English text into sentence units for summarisation."""
    cleaned = _clean_text_block(text)
    if not cleaned:
        return []

    sentences: list[str] = []
    seen: set[str] = set()
    for block in [paragraph.strip() for paragraph in cleaned.split("\n\n") if paragraph.strip()]:
        fragments = [
            fragment.strip()
            for fragment in re.split(r"(?<=[.!?])\s+", block)
            if fragment.strip()
        ]
        for fragment in fragments or [block]:
            normalized = re.sub(r"\s+", " ", fragment).strip()
            if len(normalized) < 20 or normalized.lower() in seen:
                continue
            sentences.append(normalized)
            seen.add(normalized.lower())
    return sentences


def _english_sentence_tokens(sentence: str) -> set[str]:
    """Return a token set for English sentence redundancy checks."""
    return set(re.findall(r"[A-Za-z][A-Za-z'-]{1,}", sentence.lower()))


def _summarize_english_text(text: Any, paragraph_count: int = 2) -> str:
    """Build a deterministic short summary from translated English text."""
    cleaned = _clean_text_block(text)
    if not cleaned:
        return ""

    sentences = _split_english_sentences(cleaned)
    if len(sentences) < 2 or len(cleaned) <= 180 or TfidfVectorizer is None:
        return cleaned

    try:
        matrix = TfidfVectorizer(stop_words="english").fit_transform(sentences)
    except ValueError:
        return cleaned
    similarity = (matrix * matrix.T).toarray()
    token_sets = [_english_sentence_tokens(sentence) for sentence in sentences]

    base_scores: list[float] = []
    sentence_count = max(len(sentences), 1)
    for index, row in enumerate(similarity):
        centrality = float(row.sum())
        position_bonus = 0.15 * (1.0 - (index / sentence_count))
        base_scores.append(centrality + position_bonus)

    target_sentences = 4 if len(sentences) >= 6 else min(len(sentences), 3)
    selected_indexes: list[int] = []
    while len(selected_indexes) < target_sentences:
        best_index: int | None = None
        best_score: float | None = None
        for index, token_set in enumerate(token_sets):
            if index in selected_indexes:
                continue
            redundancy = 0.0
            if selected_indexes:
                redundancy = max(
                    len(token_set & token_sets[selected]) / max(len(token_set | token_sets[selected]), 1)
                    for selected in selected_indexes
                )
            candidate_score = base_scores[index] - (redundancy * 0.55)
            if best_index is None or candidate_score > best_score:
                best_index = index
                best_score = candidate_score
        if best_index is None:
            break
        selected_indexes.append(best_index)

    if not selected_indexes:
        return cleaned

    selected_indexes.sort()
    selected_sentences = [sentences[index] for index in selected_indexes]
    paragraph_total = min(paragraph_count, len(selected_sentences))
    if paragraph_total <= 1:
        summary = " ".join(selected_sentences).strip()
        return summary if summary and len(summary) < len(cleaned) * 0.85 else cleaned

    base_size, remainder = divmod(len(selected_sentences), paragraph_total)
    cursor = 0
    paragraphs: list[str] = []
    for paragraph_index in range(paragraph_total):
        chunk_size = base_size + (1 if paragraph_index < remainder else 0)
        chunk = selected_sentences[cursor:cursor + chunk_size]
        cursor += chunk_size
        if chunk:
            paragraphs.append(" ".join(chunk))

    summary = "\n\n".join(paragraphs).strip()
    return summary if summary and len(summary) < len(cleaned) * 0.85 else cleaned


def _summarize_preferred_description(text: Any) -> str:
    """Summarize English descriptions with the English summarizer, else Japanese."""
    cleaned = _clean_text_block(text)
    if not cleaned:
        return ""
    if _looks_like_english_text(cleaned):
        return _summarize_english_text(cleaned)
    return _summarize_business_description(cleaned)


def _join_summary_units(units: list[str]) -> str:
    """Join summary units without forcing spaces into Japanese text."""
    combined = ""
    for unit in units:
        clean_unit = unit.strip()
        if not clean_unit:
            continue
        if not combined:
            combined = clean_unit
            continue
        if re.match(r"[A-Za-z0-9]", clean_unit):
            combined += " "
        combined += clean_unit
    return combined.strip()


def _summarize_business_description(text: Any, paragraph_count: int = 2) -> str:
    """Build a deterministic short summary for a long business description."""
    cleaned = _clean_text_block(text)
    if not cleaned:
        return ""

    units = _split_description_units(cleaned)
    if len(units) < 2 or len(cleaned) <= 140:
        return cleaned

    ngrams_by_unit = [_description_ngrams(unit) for unit in units]
    corpus_counts: Counter[str] = Counter()
    for ngrams in ngrams_by_unit:
        corpus_counts.update(ngrams)

    base_scores: list[float] = []
    unit_count = max(len(units), 1)
    for index, ngrams in enumerate(ngrams_by_unit):
        if not ngrams:
            base_scores.append(0.0)
            continue
        centrality = sum(corpus_counts[ngram] for ngram in ngrams) / len(ngrams)
        position_bonus = 0.15 * (1.0 - (index / unit_count))
        base_scores.append(centrality + position_bonus)

    target_units = 4 if len(units) >= 6 else min(len(units), 3)
    selected_indexes: list[int] = []
    while len(selected_indexes) < target_units:
        best_index: int | None = None
        best_score: float | None = None
        for index, ngrams in enumerate(ngrams_by_unit):
            if index in selected_indexes:
                continue
            redundancy = 0.0
            if selected_indexes:
                redundancy = max(
                    len(ngrams & ngrams_by_unit[selected]) / max(len(ngrams | ngrams_by_unit[selected]), 1)
                    for selected in selected_indexes
                )
            candidate_score = base_scores[index] - (redundancy * 0.55)
            if best_index is None or candidate_score > best_score:
                best_index = index
                best_score = candidate_score
        if best_index is None:
            break
        selected_indexes.append(best_index)

    if not selected_indexes:
        return cleaned

    selected_indexes.sort()
    selected_units = [units[index] for index in selected_indexes]
    paragraph_total = min(paragraph_count, len(selected_units))
    if paragraph_total <= 1:
        summary = _join_summary_units(selected_units)
        return summary if len(summary) < len(cleaned) * 0.85 else cleaned

    base_size, remainder = divmod(len(selected_units), paragraph_total)
    cursor = 0
    paragraphs: list[str] = []
    for paragraph_index in range(paragraph_total):
        chunk_size = base_size + (1 if paragraph_index < remainder else 0)
        chunk = selected_units[cursor:cursor + chunk_size]
        cursor += chunk_size
        if chunk:
            paragraphs.append(_join_summary_units(chunk))

    summary = "\n\n".join(paragraphs).strip()
    return summary if summary and len(summary) < len(cleaned) * 0.85 else cleaned


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

        columns = _get_columns(conn, table_name)
        docid_col = _resolve_column(columns, ["docID", "DocID"], required=False)
        edinet_col = _resolve_column(columns, ["edinetCode", "EdinetCode"], required=False)
        period_col = _resolve_column(columns, ["periodEnd", "PeriodEnd"], required=False)

        join_clause: str | None = None
        if docid_col:
            join_clause = (
                f"LEFT JOIN {_quote_ident(table_name)} {alias} "
                f"ON {alias}.{_quote_ident(docid_col)} = fs.{_quote_ident(schema.fs_docid_col)}"
            )
        elif edinet_col and period_col:
            join_clause = (
                f"LEFT JOIN {_quote_ident(table_name)} {alias} "
                f"ON {alias}.{_quote_ident(edinet_col)} = fs.{_quote_ident(schema.fs_edinet_col)} "
                f"AND {alias}.{_quote_ident(period_col)} = fs.{_quote_ident(schema.fs_period_end_col)}"
            )

        if not join_clause:
            logger.warning(
                "Skipping statement source %s because %s has no docID or edinetCode+periodEnd join columns.",
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
        "edinet_code": _safe_str(record.get("edinet_code")).lower(),
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
        if searchable["edinet_code"] == token:
            token_score = max(token_score, 115)
        if searchable["ticker"].startswith(token):
            token_score = max(token_score, 95)
        if searchable["edinet_code"].startswith(token):
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
        if token in searchable["edinet_code"]:
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


def _as_peer_row(snapshot: dict[str, Any]) -> dict[str, Any]:
    """Project a latest security snapshot into a peer-table row."""
    ratios = _compute_ratio_payload(snapshot)
    return {
        "edinet_code": _safe_str(snapshot.get("edinet_code")),
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
# Snapshot queries
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
        f"c.{_quote_ident(schema.company_edinet_col)} AS edinet_code",
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
        conn = _connect(normalised_path)
        created: list[str] = []
        try:
            statements = [
                (
                    "idx_sa_prices_ticker_date",
                    f"CREATE INDEX IF NOT EXISTS [idx_sa_prices_ticker_date] "
                    f"ON {_quote_ident(schema.prices_table)} ([Ticker], [Date])",
                ),
                (
                    "idx_sa_company_edinet",
                    f"CREATE INDEX IF NOT EXISTS [idx_sa_company_edinet] "
                    f"ON {_quote_ident(schema.company_table)} ({_quote_ident(schema.company_edinet_col)})",
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
                    f"({_quote_ident(schema.fs_edinet_col)}, {_quote_ident(schema.fs_period_end_col)})",
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
            conn.commit()
        finally:
            conn.close()

        _OPTIMIZED_DB_PATHS.add(normalised_path)
        logger.info(
            "Security Analysis indexes ensured for %s: %s",
            normalised_path,
            ", ".join(created) if created else "none",
        )
        return {"ok": True, "created": created, "cached": False}


def _load_company_record(db_path: str, edinet_code: str) -> dict[str, Any] | None:
    """Return a single company record from the cached company snapshot."""
    company_df = _get_cached_company_frame(db_path)
    matches = company_df[company_df["edinet_code"].astype(str) == str(edinet_code)]
    if matches.empty:
        return None
    return matches.iloc[0].to_dict()


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


def _load_latest_snapshot(conn: sqlite3.Connection, schema: SecuritySchema, edinet_code: str) -> dict[str, Any] | None:
    """Load the latest filing snapshot for a company."""
    select_parts = [
        f"fs.{_quote_ident(schema.fs_docid_col)} AS docID",
        f"fs.{_quote_ident(schema.fs_edinet_col)} AS edinet_code",
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
    if schema.income_table:
        join_clauses.append(
            f"LEFT JOIN {_quote_ident(schema.income_table)} i ON i.docID = fs.{_quote_ident(schema.fs_docid_col)}"
        )
        for col in _get_columns(conn, schema.income_table):
            if col.lower() != "docid":
                select_parts.append(f"i.{_quote_ident(col)} AS {_quote_ident(col)}")
    if schema.balance_table:
        join_clauses.append(
            f"LEFT JOIN {_quote_ident(schema.balance_table)} b ON b.docID = fs.{_quote_ident(schema.fs_docid_col)}"
        )
        for col in _get_columns(conn, schema.balance_table):
            if col.lower() != "docid":
                select_parts.append(f"b.{_quote_ident(col)} AS {_quote_ident(col)}")
    if schema.cashflow_table:
        join_clauses.append(
            f"LEFT JOIN {_quote_ident(schema.cashflow_table)} cf ON cf.docID = fs.{_quote_ident(schema.fs_docid_col)}"
        )
        for col in _get_columns(conn, schema.cashflow_table):
            if col.lower() != "docid":
                select_parts.append(f"cf.{_quote_ident(col)} AS {_quote_ident(col)}")
    if schema.per_share_table:
        join_clauses.append(
            f"LEFT JOIN {_quote_ident(schema.per_share_table)} ps ON ps.docID = fs.{_quote_ident(schema.fs_docid_col)}"
        )
        for col in _get_columns(conn, schema.per_share_table):
            if col.lower() != "docid":
                select_parts.append(f"ps.{_quote_ident(col)} AS {_quote_ident(col)}")
    if schema.valuation_table:
        join_clauses.append(
            f"LEFT JOIN {_quote_ident(schema.valuation_table)} v ON v.docID = fs.{_quote_ident(schema.fs_docid_col)}"
        )
        for col in _get_columns(conn, schema.valuation_table):
            if col.lower() != "docid":
                select_parts.append(f"v.{_quote_ident(col)} AS {_quote_ident(col)}")
    if schema.quality_table:
        join_clauses.append(
            f"LEFT JOIN {_quote_ident(schema.quality_table)} q ON q.docID = fs.{_quote_ident(schema.fs_docid_col)}"
        )
        for col in _get_columns(conn, schema.quality_table):
            if col.lower() != "docid":
                select_parts.append(f"q.{_quote_ident(col)} AS {_quote_ident(col)}")

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
        f"WHERE fs.{_quote_ident(schema.fs_edinet_col)} = ? "
        f"{order_clause} LIMIT 1"
    )
    df = pd.read_sql_query(sql, conn, params=[edinet_code])
    if df.empty:
        return None
    row = df.iloc[0].to_dict()
    row["period_end"] = _safe_date_str(row.get("period_end"))
    return row


def _latest_snapshots_for_codes(
    conn: sqlite3.Connection,
    schema: SecuritySchema,
    edinet_codes: list[str],
) -> pd.DataFrame:
    """Load the latest filing snapshot for multiple companies."""
    if not edinet_codes:
        return pd.DataFrame()

    placeholders = ",".join(["?"] * len(edinet_codes))
    join_clauses: list[str] = []
    select_parts = [
        f"fs.{_quote_ident(schema.fs_docid_col)} AS docID",
        f"fs.{_quote_ident(schema.fs_edinet_col)} AS edinet_code",
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
        f"WHERE fs.{_quote_ident(schema.fs_edinet_col)} IN ({placeholders}) "
        f"ORDER BY fs.{_quote_ident(schema.fs_edinet_col)}, "
        f"fs.{_quote_ident(schema.fs_period_end_col)} DESC, "
        f"fs.{_quote_ident(schema.fs_docid_col)} DESC"
    )
    df = pd.read_sql_query(sql, conn, params=edinet_codes)
    if df.empty:
        return df
    df["period_end"] = df["period_end"].astype(str).str[:10]
    df = df.drop_duplicates(subset=["edinet_code"], keep="first").reset_index(drop=True)
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
# Public API
# ---------------------------------------------------------------------------


def search_securities(db_path: str, query: str, limit: int = 25) -> list[dict[str, Any]]:
    """Search companies by name, ticker, EDINET code, or industry.

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
    for record in company_df.to_dict(orient="records"):
        score = _score_security_match(record, tokens)
        if score is None:
            continue
        scored.append((score, {
            "edinet_code": _safe_str(record.get("edinet_code")),
            "ticker": _safe_str(record.get("ticker")),
            "company_name": _safe_str(record.get("company_name")),
            "industry": _safe_str(record.get("industry")),
            "market": _safe_str(record.get("market")),
            "latest_price": None,
            "latest_price_date": None,
        }))

    scored.sort(
        key=lambda item: (
            -item[0],
            item[1]["company_name"].lower(),
            item[1]["ticker"],
        )
    )

    return [record for _, record in scored[:limit]]


def get_security_overview(db_path: str, edinet_code: str) -> dict[str, Any]:
    """Return a summary payload for a selected security.

    Args:
        db_path (str): Path to the SQLite database.
        edinet_code (str): Selected company EDINET code.

    Returns:
        dict[str, Any]: Company, market, fundamentals, valuation, and metadata.
    """
    ensure_security_analysis_indexes(db_path)
    schema = resolve_schema(db_path)
    company = _load_company_record(db_path, edinet_code)
    if company is None:
        raise ValueError(f"Security not found for EDINET code: {edinet_code}")

    conn = _connect(db_path)
    try:
        snapshot = _load_latest_snapshot(conn, schema, edinet_code)
        if snapshot is None:
            snapshot = {"edinet_code": edinet_code, "period_end": None}

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
            "edinet_code": _safe_str(edinet_code),
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


def get_security_ratios(db_path: str, edinet_code: str) -> dict[str, Any]:
    """Return the latest valuation and quality ratios for a security.

    Args:
        db_path (str): Path to the SQLite database.
        edinet_code (str): Selected company EDINET code.

    Returns:
        dict[str, Any]: Latest ratio values and source metadata.
    """
    overview = get_security_overview(db_path, edinet_code)
    ratios = dict(overview.get("valuation_latest", {}))
    ratios.update(overview.get("quality_latest", {}))
    ratios["period_end"] = overview.get("metadata", {}).get("last_financial_period_end")
    ratios["latest_price_date"] = overview.get("metadata", {}).get("last_price_date")
    return ratios


def get_security_statements(
    db_path: str,
    edinet_code: str,
    periods: int = 8,
    statement_sources: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Return historical financial statements for a security.

    Args:
        db_path (str): Path to the SQLite database.
        edinet_code (str): Selected company EDINET code.
        periods (int): Maximum number of reporting periods to return.
        statement_sources (dict[str, str] | None): Ordered map of UI labels to
            statement source identifiers or table names.

    Returns:
        dict[str, Any]: Statement tables and ordered period labels.
    """
    ensure_security_analysis_indexes(db_path)
    schema = resolve_schema(db_path)
    requested_sources = _statement_requested_sources(statement_sources)
    conn = _connect(db_path)
    try:
        statement_specs = _build_statement_source_specs(conn, schema, requested_sources)
        select_parts = [
            f"fs.{_quote_ident(schema.fs_docid_col)} AS docID",
            f"fs.{_quote_ident(schema.fs_period_end_col)} AS period_end",
        ]
        join_clauses: list[str] = []

        for source_key in requested_sources:
            spec = statement_specs.get(source_key)
            if spec is None or spec.join_clause is None:
                continue
            join_clauses.append(spec.join_clause)
            for metric in spec.metrics:
                select_parts.append(
                    f"{spec.alias}.{_quote_ident(metric.source_field)} AS {_quote_ident(metric.record_field)}"
                )

        sql = (
            f"SELECT {', '.join(select_parts)} "
            f"FROM {_quote_ident(schema.financial_statements_table)} fs "
            f"{' '.join(join_clauses)} "
            f"WHERE fs.{_quote_ident(schema.fs_edinet_col)} = ? "
            f"ORDER BY fs.{_quote_ident(schema.fs_period_end_col)} DESC, "
            f"fs.{_quote_ident(schema.fs_docid_col)} DESC LIMIT ?"
        )
        df = pd.read_sql_query(sql, conn, params=[edinet_code, max(1, int(periods))])
    finally:
        conn.close()

    result: dict[str, Any] = {
        "periods": [],
        "records": [],
    }
    for source_key in requested_sources:
        result[source_key] = []

    if df.empty:
        return result

    df["period_end"] = df["period_end"].astype(str).str[:10]
    df = df.iloc[::-1].reset_index(drop=True)
    records = df.to_dict(orient="records")

    result["periods"] = [record["period_end"] for record in records]
    result["records"] = records
    for source_key in requested_sources:
        spec = statement_specs.get(source_key)
        if spec is None:
            continue
        result[source_key] = _statement_metric_rows(records, spec.metrics, source_key)
    return result


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
    edinet_code: str,
    industry: str | None = None,
    limit: int = 10,
) -> list[dict[str, Any]]:
    """Return default peer rows for a security.

    Args:
        db_path (str): Path to the SQLite database.
        edinet_code (str): Selected company EDINET code.
        industry (str | None): Optional industry override.
        limit (int): Maximum number of peer rows.

    Returns:
        list[dict[str, Any]]: Deterministically ranked peer rows.
    """
    ensure_security_analysis_indexes(db_path)
    schema = resolve_schema(db_path)
    companies = _get_cached_company_frame(db_path)
    selected_df = companies[companies["edinet_code"].astype(str) == str(edinet_code)]
    if selected_df.empty:
        return []
    selected = selected_df.iloc[0].to_dict()
    industry_value = _safe_str(industry) or _safe_str(selected.get("industry"))
    if not industry_value:
        return []

    peer_companies = companies[
        (companies["edinet_code"].astype(str) != str(edinet_code))
        & (companies["industry"].fillna("").astype(str) == industry_value)
        & (companies["ticker"].fillna("").astype(str) != "")
    ].copy()
    if peer_companies.empty:
        return []

    conn = _connect(db_path)
    try:
        selected_snapshot = _load_latest_snapshot(conn, schema, edinet_code)
        selected_price_info = _load_price_range(conn, schema, _safe_str(selected.get("ticker")))
        selected_market_cap = _safe_float(
            _compute_ratio_payload({
                **(selected_snapshot or {}),
                **selected_price_info,
            }).get("MarketCap")
        )

        edinet_codes = peer_companies["edinet_code"].astype(str).tolist()
        snapshots_df = _latest_snapshots_for_codes(conn, schema, edinet_codes)
        if snapshots_df.empty:
            return []

        tickers = peer_companies["ticker"].astype(str).tolist()
        latest_prices_df = _load_latest_prices_for_tickers(conn, schema, tickers)
        returns_df = _price_return_1y(conn, schema, tickers)
    finally:
        conn.close()

    merged = peer_companies.merge(snapshots_df, on="edinet_code", how="inner")
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
    conn = sqlite3.connect(db_path)
    try:
        _create_prices_table(conn, schema.prices_table)

        before_count = conn.execute(
            f"SELECT COUNT(*) FROM {_quote_ident(schema.prices_table)} WHERE Ticker = ?",
            [ticker],
        ).fetchone()[0]

        ok = load_ticker_data(ticker, schema.prices_table, conn)
        conn.commit()

        after_count = conn.execute(
            f"SELECT COUNT(*) FROM {_quote_ident(schema.prices_table)} WHERE Ticker = ?",
            [ticker],
        ).fetchone()[0]
        range_row = conn.execute(
            f"SELECT MIN([Date]), MAX([Date]) FROM {_quote_ident(schema.prices_table)} WHERE Ticker = ?",
            [ticker],
        ).fetchone()
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