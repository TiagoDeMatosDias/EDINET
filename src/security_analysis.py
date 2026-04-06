"""Security analysis queries for the Tk UI.

This module centralises company-level lookups used by the Security Analysis
view. It resolves table/column naming differences across databases,
aggregates the latest filing snapshot, loads historical statement data,
retrieves price history, and provides a deterministic peer comparison.
"""

from __future__ import annotations

import logging
import os
import sqlite3
from dataclasses import dataclass
from datetime import timedelta
from functools import lru_cache
from typing import Any

import pandas as pd

from src.stockprice_api import _create_prices_table, load_ticker_data

logger = logging.getLogger(__name__)


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
    fs_shares_outstanding_col: str | None
    fs_share_price_col: str | None
    doclist_docid_col: str | None
    doclist_submit_dt_col: str | None


def _connect(db_path: str) -> sqlite3.Connection:
    """Open a SQLite connection with a row factory suitable for helpers."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _quote_ident(name: str) -> str:
    """Return a safely quoted SQLite identifier."""
    return f"[{str(name).replace(']', ']]')}]"


def _normalise_db_path(db_path: str) -> str:
    """Normalise a database path so schema discovery can be cached."""
    return os.path.abspath(db_path)


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


def _statement_metric_rows(records: list[dict[str, Any]], metric_map: list[tuple[str, str]]) -> list[dict[str, Any]]:
    """Convert period records into statement-table rows for the UI."""
    rows: list[dict[str, Any]] = []
    for field_name, display_name in metric_map:
        rows.append(
            {
                "metric": display_name,
                "field": field_name,
                "values": [_safe_float(record.get(field_name)) for record in records],
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

    schema = resolve_schema(db_path)
    conn = _connect(db_path)
    try:
        company_df = _load_company_frame(conn, schema)
        prices_df = _load_latest_prices_frame(conn, schema)
    finally:
        conn.close()

    merged = company_df.merge(prices_df, on="ticker", how="left")
    scored: list[tuple[int, dict[str, Any]]] = []
    for record in merged.to_dict(orient="records"):
        score = _score_security_match(record, tokens)
        if score is None:
            continue
        scored.append((score, {
            "edinet_code": _safe_str(record.get("edinet_code")),
            "ticker": _safe_str(record.get("ticker")),
            "company_name": _safe_str(record.get("company_name")),
            "industry": _safe_str(record.get("industry")),
            "market": _safe_str(record.get("market")),
            "latest_price": _safe_float(record.get("latest_price")),
            "latest_price_date": _safe_date_str(record.get("latest_price_date")),
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
    schema = resolve_schema(db_path)
    conn = _connect(db_path)
    try:
        company_df = _load_company_frame(conn, schema)
        company_df = company_df[company_df["edinet_code"].astype(str) == str(edinet_code)]
        if company_df.empty:
            raise ValueError(f"Security not found for EDINET code: {edinet_code}")

        company = company_df.iloc[0].to_dict()
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

    combined = {
        **snapshot,
        **price_info,
        "ticker": ticker,
        "company_name": _safe_str(company.get("company_name")),
        "industry": _safe_str(company.get("industry")),
        "market": _safe_str(company.get("market")),
        "description": _safe_str(company.get("description")),
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
            "description": _safe_str(company.get("description")),
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


def get_security_statements(db_path: str, edinet_code: str, periods: int = 8) -> dict[str, Any]:
    """Return historical financial statements for a security.

    Args:
        db_path (str): Path to the SQLite database.
        edinet_code (str): Selected company EDINET code.
        periods (int): Maximum number of reporting periods to return.

    Returns:
        dict[str, Any]: Statement tables and ordered period labels.
    """
    schema = resolve_schema(db_path)
    conn = _connect(db_path)
    try:
        select_parts = [
            f"fs.{_quote_ident(schema.fs_docid_col)} AS docID",
            f"fs.{_quote_ident(schema.fs_period_end_col)} AS period_end",
        ]
        join_clauses: list[str] = []

        if schema.income_table:
            join_clauses.append(
                f"LEFT JOIN {_quote_ident(schema.income_table)} i ON i.docID = fs.{_quote_ident(schema.fs_docid_col)}"
            )
            for col in ("netSales", "grossProfit", "operatingIncome", "netIncome"):
                if col in _get_columns(conn, schema.income_table):
                    select_parts.append(f"i.{_quote_ident(col)} AS {_quote_ident(col)}")
        if schema.balance_table:
            join_clauses.append(
                f"LEFT JOIN {_quote_ident(schema.balance_table)} b ON b.docID = fs.{_quote_ident(schema.fs_docid_col)}"
            )
            for col in (
                "cash",
                "currentAssets",
                "totalAssets",
                "shareholdersEquity",
                "currentLiabilities",
                "TotalLiabilities",
            ):
                if col in _get_columns(conn, schema.balance_table):
                    select_parts.append(f"b.{_quote_ident(col)} AS {_quote_ident(col)}")
        if schema.cashflow_table:
            join_clauses.append(
                f"LEFT JOIN {_quote_ident(schema.cashflow_table)} cf ON cf.docID = fs.{_quote_ident(schema.fs_docid_col)}"
            )
            for col in (
                "operatingCashflow",
                "investmentCashflow",
                "financingCashflow",
                "capex",
                "dividends",
            ):
                if col in _get_columns(conn, schema.cashflow_table):
                    select_parts.append(f"cf.{_quote_ident(col)} AS {_quote_ident(col)}")

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

    if df.empty:
        return {
            "periods": [],
            "records": [],
            "income_statement": [],
            "balance_sheet": [],
            "cashflow_statement": [],
        }

    df["period_end"] = df["period_end"].astype(str).str[:10]
    df = df.iloc[::-1].reset_index(drop=True)
    records = df.to_dict(orient="records")

    return {
        "periods": [record["period_end"] for record in records],
        "records": records,
        "income_statement": _statement_metric_rows(
            records,
            [
                ("netSales", "Net Sales"),
                ("grossProfit", "Gross Profit"),
                ("operatingIncome", "Operating Income"),
                ("netIncome", "Net Income"),
            ],
        ),
        "balance_sheet": _statement_metric_rows(
            records,
            [
                ("cash", "Cash"),
                ("currentAssets", "Current Assets"),
                ("totalAssets", "Total Assets"),
                ("shareholdersEquity", "Shareholders' Equity"),
                ("currentLiabilities", "Current Liabilities"),
                ("TotalLiabilities", "Total Liabilities"),
            ],
        ),
        "cashflow_statement": _statement_metric_rows(
            records,
            [
                ("operatingCashflow", "Operating Cashflow"),
                ("investmentCashflow", "Investment Cashflow"),
                ("financingCashflow", "Financing Cashflow"),
                ("capex", "Capex"),
                ("dividends", "Dividends"),
            ],
        ),
    }


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
    schema = resolve_schema(db_path)
    conn = _connect(db_path)
    try:
        companies = _load_company_frame(conn, schema)
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

        edinet_codes = peer_companies["edinet_code"].astype(str).tolist()
        snapshots_df = _latest_snapshots_for_codes(conn, schema, edinet_codes)
        if snapshots_df.empty:
            return []

        latest_prices_df = _load_latest_prices_frame(conn, schema)
        returns_df = _price_return_1y(conn, schema, peer_companies["ticker"].astype(str).tolist())
    finally:
        conn.close()

    selected_overview = get_security_overview(db_path, edinet_code)
    selected_market_cap = _safe_float(
        selected_overview.get("valuation_latest", {}).get("MarketCap")
    )

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