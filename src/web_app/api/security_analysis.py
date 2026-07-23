"""Security Analysis API routes.

All database interactions go through ``src.security_analysis`` functions.
Database resolution is server-side via DB2_PATH — never exposed to clients.
"""

from __future__ import annotations

import logging
import re
import sqlite3
from typing import Any

from fastapi import APIRouter, Body, HTTPException, Query
from pydantic import BaseModel, Field

from src import security_analysis as _security
from src.orchestrator.common.db_config import get_db2
from src.orchestrator.common.sqlite import connect_read
from src.web_app.security import (
    AppSettings,
    PathPolicyError,
    configured_database_policy,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/security", tags=["security_analysis"])
_APP_SETTINGS = AppSettings.from_env()
_DB_PATH_POLICY = configured_database_policy(_APP_SETTINGS.allowed_data_roots)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _resolve_db() -> str:
    db_path = get_db2()
    if not db_path:
        raise HTTPException(status_code=503, detail="No database configured.")
    try:
        return str(_DB_PATH_POLICY.authorize_database(db_path))
    except PathPolicyError as exc:
        raise HTTPException(status_code=503, detail="Database not found.") from exc


def _safe_float(value: Any) -> float | None:
    if value is None: return None
    try: return float(value)
    except (TypeError, ValueError): return None


def _safe_str(value: Any) -> str:
    """Return a normalised string for display and matching."""
    if value is None:
        return ""
    try:
        if isinstance(value, float) and value != value:  # NaN
            return ""
    except Exception:
        pass
    return str(value).strip()


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class UpdatePriceRequest(BaseModel):
    ticker: str = Field(..., description="Ticker to refresh")


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------

@router.get("/search")
def search_securities(
    q: str = Query(default="", description="Search query"),
    limit: int = Query(default=25),
) -> dict:
    query = q.strip()
    if not query: return {"results": []}
    try:
        return {"results": _security.search_securities(_resolve_db(), query, limit=limit)}
    except HTTPException: raise
    except Exception as e:
        logger.error("Search failed: %s", str(e))
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# Overview — company summary with computed metrics
# ---------------------------------------------------------------------------

@router.get("/overview")
def get_overview(
    company_code: str = Query(default="", description="Company code"),
    ticker: str = Query(default="", description="Ticker (when no company_code)"),
) -> dict:
    """Company summary with metrics computed from the actual DB tables."""
    code = company_code.strip()
    tkr = ticker.strip()
    if not code and not tkr:
        raise HTTPException(status_code=400, detail="Either company_code or ticker is required")
    try:
        db = _resolve_db()
        result = _security.get_security_overview(db, company_code=code, ticker=tkr)
        try:
            result["metrics"] = _compute_metrics(db, code or tkr,
                                                  result.get("market", {}),
                                                  result.get("company", {}))
        except Exception as exc:
            logger.warning("Metrics computation failed for %s: %s", code or tkr, exc)
            result["metrics"] = {k: None for k in (
                "LatestPrice", "MarketCap", "PERatio", "PriceToBook",
                "PriceToSales", "DividendsYield", "PayoutRatio",
                "ReturnOnAssets", "ReturnOnEquity", "CurrentRatio",
            )}
        return result
    except HTTPException: raise
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error("Overview failed: %s", str(e))
        raise HTTPException(status_code=500, detail=str(e))


def _compute_metrics(db: str, code: str, market: dict, company: dict) -> dict:
    """Compute key metrics using direct queries against the actual tables."""
    ticker = company.get("ticker", "")
    if not ticker:
        return _empty_metrics()

    conn = connect_read(db)
    try:
        tables = {r[0].lower(): r[0] for r in
                  conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}

        # Get latest stock price
        price = None
        if "stock_prices" in tables:
            r = conn.execute(
                f'SELECT Price FROM "{tables["stock_prices"]}" '
                f'WHERE Ticker=? ORDER BY Date DESC LIMIT 1', (ticker,)
            ).fetchone()
            if r: price = _safe_float(r["Price"])

        # Find the latest docID with actual data in ShareMetrics
        doc_id = _find_doc_with_data(conn, tables, code,
                                      ["ShareMetrics", "PerShare_Metrics",
                                       "Financial_Ratios", "Financial_Ratios_Rolling"])

        if not doc_id:
            return {"LatestPrice": price}

        # Query each table for this docID
        share = _query_row(conn, tables, "ShareMetrics", doc_id)
        ps_metrics = _query_row(conn, tables, "PerShare_Metrics", doc_id)
        fin_ratios = _query_row(conn, tables, "Financial_Ratios", doc_id)
        fin_rolling = _query_row(conn, tables, "Financial_Ratios_Rolling", doc_id)

        eps = _col(share, "Basic earnings (loss) per share")
        bvps = _col(share, "Net assets per share")
        dps = _col(share, "Dividend paid per share")
        shares = _col(share, "Number of issued shares as of filing date")
        sps = _col(ps_metrics, "Sales Per Share")
        cr = _col(fin_ratios, "Current Ratio")
        roa = _col(fin_rolling, "Return on Assets_Average_3_Year")
        roe = _col(fin_rolling, "Return on Equity_Average_3_Year")

        return {
            "LatestPrice": price,
            "MarketCap": (price * shares) if (price and shares) else None,
            "PERatio": (price / eps) if (price and eps and eps != 0) else None,
            "PriceToBook": (price / bvps) if (price and bvps and bvps != 0) else None,
            "PriceToSales": (price / sps) if (price and sps and sps != 0) else None,
            "DividendsYield": (dps / price) if (dps and price and price != 0) else None,
            "PayoutRatio": (dps / eps) if (dps and eps and eps != 0) else None,
            "ReturnOnAssets": roa,
            "ReturnOnEquity": roe,
            "CurrentRatio": cr,
        }
    finally:
        conn.close()


def _find_doc_with_data(conn, tables, code, table_names):
    """Find the latest docID where at least one of the given tables has data."""
    # Resolve the actual edinet/company code column in FinancialStatements
    fs_info = conn.execute("PRAGMA table_info(FinancialStatements)").fetchall()
    fs_cols = {row[1] for row in fs_info}
    fs_code_col = None
    for candidate in ("Company_Code", "edinetCode", "EdinetCode"):
        if candidate in fs_cols:
            fs_code_col = candidate
            break
    if not fs_code_col:
        fs_code_col = "Company_Code"

    for tname in table_names:
        actual = tables.get(tname.lower())
        if not actual: continue
        rows = conn.execute(
            f'SELECT fs.docID FROM FinancialStatements fs '
            f'JOIN "{actual}" m ON m.docID = fs.docID '
            f'WHERE fs."{fs_code_col}"=? ORDER BY fs.periodEnd DESC LIMIT 5',
            (code,)
        ).fetchall()
        for r in rows:
            mrow = conn.execute(
                f'SELECT * FROM "{actual}" WHERE docID=?', (r["docID"],)
            ).fetchone()
            if mrow:
                nn = sum(1 for k in mrow.keys()
                         if k.lower() != "docid" and mrow[k] is not None)
                if nn > 0: return r["docID"]
    # Fallback: latest docID
    r = conn.execute(
        f"SELECT docID FROM FinancialStatements "
        f"WHERE \"{fs_code_col}\"=? ORDER BY periodEnd DESC LIMIT 1", (code,)
    ).fetchone()
    return r["docID"] if r else None


def _query_row(conn, tables, tname, doc_id):
    actual = tables.get(tname.lower())
    if not actual or not doc_id: return {}
    r = conn.execute(f'SELECT * FROM "{actual}" WHERE docID=?', (doc_id,)).fetchone()
    return dict(r) if r else {}


def _col(row, name):
    """Get a column value from a row dict by exact name."""
    if not row: return None
    return _safe_float(row.get(name))


def _empty_metrics():
    return {k: None for k in (
        "LatestPrice", "MarketCap", "PERatio", "PriceToBook",
        "PriceToSales", "DividendsYield", "PayoutRatio",
        "ReturnOnAssets", "ReturnOnEquity", "CurrentRatio",
    )}


# ---------------------------------------------------------------------------
# Formulas — metric definitions for the frontend tile labels
# ---------------------------------------------------------------------------

@router.get("/formulas")
def get_formulas() -> dict:
    return {"formulas": [
        {"name": "Latest Price",    "id": "LatestPrice",    "format": "price"},
        {"name": "Market Cap",      "id": "MarketCap",      "format": "currency"},
        {"name": "P/E Ratio",       "id": "PERatio",        "format": "ratio"},
        {"name": "P/B Ratio",       "id": "PriceToBook",    "format": "ratio"},
        {"name": "P/S Ratio",       "id": "PriceToSales",   "format": "ratio"},
        {"name": "Dividend Yield",  "id": "DividendsYield", "format": "percent"},
        {"name": "Payout Ratio",    "id": "PayoutRatio",    "format": "percent"},
        {"name": "Return on Assets","id": "ReturnOnAssets", "format": "percent"},
        {"name": "Return on Equity","id": "ReturnOnEquity", "format": "percent"},
        {"name": "Current Ratio",   "id": "CurrentRatio",   "format": "ratio"},
    ]}


# ---------------------------------------------------------------------------
# Price history
# ---------------------------------------------------------------------------

@router.get("/price-history")
def get_price_history(
    ticker: str = Query(...),
    start_date: str | None = Query(default=None),
    end_date: str | None = Query(default=None),
) -> dict:
    if not ticker.strip():
        raise HTTPException(status_code=400, detail="ticker is required")
    try:
        return {"prices": _security.get_security_price_history(
            _resolve_db(), ticker.strip(), start_date, end_date)}
    except HTTPException: raise
    except Exception as e:
        logger.error("Price history failed: %s", str(e))
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# Update price
# ---------------------------------------------------------------------------

@router.post("/update-price")
def update_price(request: UpdatePriceRequest = Body(...)) -> dict:
    if not request.ticker.strip():
        raise HTTPException(status_code=400, detail="ticker is required")
    try:
        return _security.update_security_price(_resolve_db(), request.ticker.strip())
    except HTTPException: raise
    except Exception as e:
        logger.error("Update price failed: %s", str(e))
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# History — all table data as metric-rows
# ---------------------------------------------------------------------------

@router.get("/history")
def get_history(
    company_code: str = Query(...),
    periods: int = Query(default=20),
) -> dict:
    """All historical statement data as metric-rows grouped by table."""
    if not company_code.strip():
        raise HTTPException(status_code=400, detail="company_code is required")
    try:
        db = _resolve_db()

        # Discover all joinable tables
        conn = connect_read(db)
        try:
            table_map = {}
            for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'"):
                name = r[0]
                if name.lower() in ("companyinfo", "financialstatements",
                                     "stock_prices", "documentlist", "sqlite_sequence"):
                    continue
                cols = [c[1] for c in conn.execute(f'PRAGMA table_info("{name}")')]
                cl = {c.lower() for c in cols}
                if "docid" in cl or ("company_code" in cl or "edinetcode" in cl) and "periodend" in cl:
                    table_map[name] = name
        finally:
            conn.close()

        sources = {name: name for name in table_map.values()}
        statements = _security.get_security_statements(
            db, company_code.strip(),
            periods=max(1, int(periods)),
            statement_sources=sources if sources else None,
        )

        display_names = {
            "IncomeStatement": "Income Statement",
            "IncomeStatement_Rolling": "Income Statement (Rolling)",
            "BalanceSheet": "Balance Sheet",
            "BalanceSheet_Rolling": "Balance Sheet (Rolling)",
            "CashflowStatement": "Cashflow Statement",
            "CashflowStatement_Rolling": "Cashflow Statement (Rolling)",
            "PerShare": "Share Metrics",
            "ShareMetrics": "Share Metrics",
            "ShareMetrics_Rolling": "Share Metrics (Rolling)",
            "Valuation": "Financial Ratios",
            "Financial_Ratios": "Financial Ratios",
            "Financial_Ratios_Rolling": "Financial Ratios (Rolling)",
            "PerShare_Metrics": "Per Share Metrics",
            "PerShare_Metrics_Rolling": "Per Share Metrics (Rolling)",
        }

        tables_out = {}
        for key, rows in statements.items():
            if key in ("periods", "records"): continue
            if not rows or not isinstance(rows, list) or not len(rows): continue
            metrics = []
            for row in rows:
                f = row.get("field", row.get("record_field", ""))
                if not f: continue
                metrics.append({
                    "field": f,
                    "display_name": row.get("metric", f),
                    "values": row.get("values", []),
                })
            if metrics:
                tables_out[key] = {
                    "display_name": display_names.get(key, key.replace("_", " ").title()),
                    "metrics": metrics,
                }
        return {"periods": statements.get("periods", []), "tables": tables_out}
    except HTTPException: raise
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error("History failed: %s", str(e))
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# Taxonomy tree — hierarchical tree grid with financial data
# ---------------------------------------------------------------------------

_FAMILY_TABLE_MAP = {
    "incomestatement": "IncomeStatement",
    "balancesheet": "BalanceSheet",
    "cashflowstatement": "CashflowStatement",
    "sharemetrics": "ShareMetrics",
}


@router.get("/taxonomy-tree")
def get_taxonomy_tree(
    company_code: str = Query(...),
    statement_family: str = Query(...),
    periods: int = Query(default=20),
) -> dict:
    """Return a hierarchical taxonomy tree with financial values for each node.

    Queries the Taxonomy table to build the parent/child tree, then matches
    each concept's ``primary_label_en`` to actual columns in the corresponding
    financial statement table.  Nodes without a matching column are returned
    as abstract grouping headers (``has_data: false``).
    """
    if not company_code.strip():
        raise HTTPException(status_code=400, detail="company_code is required")
    family = statement_family.strip()
    if not family:
        raise HTTPException(status_code=400, detail="statement_family is required")

    try:
        db = _resolve_db()
        conn = connect_read(db)
        try:
            return _build_taxonomy_tree_response(
                conn, db, company_code.strip(), family, max(1, int(periods))
            )
        finally:
            conn.close()
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error("Taxonomy tree failed: %s", str(e))
        raise HTTPException(status_code=500, detail=str(e))


def _build_taxonomy_tree_response(
    conn: sqlite3.Connection,
    db_path: str,
    company_code: str,
    statement_family: str,
    periods: int,
) -> dict:
    """Core logic for building the taxonomy tree response."""

    # ── 1. Resolve table names ──────────────────────────────────────────
    table_map = {
        r[0].lower(): r[0]
        for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }

    fs_table = _resolve_table_case_insensitive(table_map, "financialstatements")
    if not fs_table:
        raise ValueError("FinancialStatements table not found")

    # Find the financial data table for this statement family
    family_lower = re.sub(r"[^a-z]", "", statement_family.lower())
    data_table_candidate = _FAMILY_TABLE_MAP.get(family_lower, statement_family)
    data_table = _resolve_table_case_insensitive(table_map, data_table_candidate)

    # ── 2. Get code column from FinancialStatements ──────────────────────
    fs_cols = {
        c[1].lower(): c[1]
        for c in conn.execute(f"PRAGMA table_info({_q(fs_table)})")
    }
    code_col = (
        fs_cols.get("edinetcode")
        or fs_cols.get("company_code")
        or fs_cols.get("edinetcode")
    )
    if not code_col:
        # fallback: find any column with 'code' in name
        for c in fs_cols:
            if "code" in c:
                code_col = fs_cols[c]
                break
    if not code_col:
        raise ValueError("Cannot find company code column in FinancialStatements")
    period_col = fs_cols.get("periodend", "periodEnd")
    docid_col = fs_cols.get("docid", "docID")
    release_col = fs_cols.get("release_id")

    # ── 3. Fetch docIDs, periods, and release_ids ────────────────────────
    select_cols = [
        f"{_q(docid_col)} AS docID",
        f"{_q(period_col)} AS period_end",
    ]
    if release_col:
        select_cols.append(f"{_q(release_col)} AS release_id")

    doc_rows = conn.execute(
        f"SELECT {', '.join(select_cols)} FROM {_q(fs_table)} "
        f"WHERE {_q(code_col)} = ? "
        f"ORDER BY {_q(period_col)} DESC, {_q(docid_col)} DESC LIMIT ?",
        (company_code, periods),
    ).fetchall()

    if not doc_rows:
        return {
            "statement_family": statement_family,
            "release_id": None,
            "periods": [],
            "tree": [],
        }

    # Reverse so periods are chronological (oldest first)
    doc_rows_rev = list(reversed(doc_rows))
    doc_ids = [r["docID"] for r in doc_rows_rev]
    period_labels = [_safe_date_label(r["period_end"]) for r in doc_rows_rev]

    # Pick the most recent release_id (first row in original order = most recent)
    release_id = None
    if release_col:
        release_id = _safe_str(doc_rows[0]["release_id"]) or None

    # ── 4. Get available columns in the data table ───────────────────────
    data_columns: dict[str, str] = {}  # lower → actual
    if data_table:
        data_columns = {
            c[1].lower(): c[1]
            for c in conn.execute(f"PRAGMA table_info({_q(data_table)})")
        }
        # Remove metadata columns
        for meta in ("docid", "edinetcode", "company_code", "periodend"):
            data_columns.pop(meta, None)

    # ── 5. Build taxonomy tree ──────────────────────────────────────────
    taxonomy_table = _resolve_table_case_insensitive(table_map, "taxonomy")

    if taxonomy_table and release_id and data_columns:
        tree = _build_tree_from_taxonomy(
            conn, taxonomy_table, release_id, statement_family,
            data_table, data_columns, doc_ids,
        )
    elif data_columns:
        # No taxonomy — build flat list from table columns
        tree = _build_flat_tree_from_columns(
            conn, data_table, data_columns, doc_ids,
        )
    else:
        tree = []

    return {
        "statement_family": statement_family,
        "release_id": release_id,
        "periods": period_labels,
        "tree": tree,
    }


def _build_tree_from_taxonomy(
    conn: sqlite3.Connection,
    taxonomy_table: str,
    release_id: str,
    statement_family: str,
    data_table: str | None,
    data_columns: dict[str, str],
    doc_ids: list[str],
) -> list[dict]:
    """Build a hierarchical tree from the Taxonomy table, attaching values."""

    # Query taxonomy rows for this release + family, ordered by level then label
    tax_rows = conn.execute(
        f"SELECT concept_qname, parent_concept_qname, primary_label_en, level, value_type "
        f"FROM {_q(taxonomy_table)} "
        f"WHERE release_id = ? AND statement_family = ? "
        f"ORDER BY level, primary_label_en",
        (release_id, statement_family),
    ).fetchall()

    if not tax_rows:
        return _build_flat_tree_from_columns(conn, data_table, data_columns, doc_ids)

    # Convert sqlite3.Row objects to plain dicts
    tax_dicts = [dict(r) for r in tax_rows]

    # ── Phase 1: Group rows by (parent_qname, label, level) to merge
    #    industry-variant concepts that share the same display label ──
    groups: dict[tuple[str, str, int], list[dict]] = {}
    for row in tax_dicts:
        parent = _safe_str(row.get("parent_concept_qname") or "")
        label = _safe_str(row.get("primary_label_en", "")) or _safe_str(row.get("concept_qname", ""))
        lvl = row.get("level", 0) or 0
        key = (parent, label.lower(), lvl)
        if key not in groups:
            groups[key] = []
        groups[key].append(row)

    # ── Phase 2: Create merged nodes (one per unique label+parent+level) ──
    # Map from every original concept_qname → merged concept_qname
    qname_to_merged: dict[str, str] = {}
    nodes_by_qname: dict[str, dict] = {}

    # Build concept → column mapping (handles disambiguated column names)
    concept_to_col = _build_concept_column_map(conn, data_columns) if data_columns else {}

    for (parent, _label_lower, lvl), variants in groups.items():
        # Pick the "best" qname: prefer one whose label matches a data column
        best_qname = variants[0].get("concept_qname", "")
        best_has_data = False
        best_is_number = False

        for row in variants:
            qname = _safe_str(row.get("concept_qname", ""))
            label = _safe_str(row.get("primary_label_en", "")) or qname
            # Try concept_qname mapping first, then label-based fallback
            col_name = concept_to_col.get(qname) if concept_to_col else None
            if not col_name and data_columns:
                col_name = data_columns.get(label.lower())
            is_number = _safe_str(row.get("value_type", "")).lower() == "number"
            if col_name and is_number:
                best_qname = qname
                best_has_data = True
                best_is_number = True
                break
            if is_number:
                best_is_number = True

        if not best_has_data and best_is_number:
            for row in variants:
                qname = _safe_str(row.get("concept_qname", ""))
                label = _safe_str(row.get("primary_label_en", "")) or qname
                col_name = concept_to_col.get(qname) if concept_to_col else None
                if not col_name and data_columns:
                    col_name = data_columns.get(label.lower())
                if col_name:
                    best_qname = row.get("concept_qname", "")
                    best_has_data = True
                    break

        display_label = _safe_str(variants[0].get("primary_label_en", "")) or _safe_str(variants[0].get("concept_qname", ""))

        nodes_by_qname[best_qname] = {
            "concept_qname": best_qname,
            "label": display_label,
            "level": lvl,
            "has_data": best_has_data,
            "values": None,
            "children": [],
        }

        # Map all variant qnames → the chosen merged qname
        for row in variants:
            qname_to_merged[_safe_str(row.get("concept_qname", ""))] = best_qname

    # ── Phase 3: Build tree using merged qnames for parent references ──
    roots: list[dict] = []
    seen_in_roots: set[str] = set()

    for (parent_raw, _label_lower, lvl), variants in groups.items():
        merged_qname = qname_to_merged.get(
            _safe_str(variants[0].get("concept_qname", ""))
        )
        if not merged_qname or merged_qname not in nodes_by_qname:
            continue
        node = nodes_by_qname[merged_qname]

        merged_parent = qname_to_merged.get(parent_raw) if parent_raw else None

        if merged_parent and merged_parent in nodes_by_qname:
            parent_node = nodes_by_qname[merged_parent]
            if merged_qname not in {c.get("concept_qname") for c in parent_node["children"]}:
                parent_node["children"].append(node)
        else:
            if merged_qname not in seen_in_roots:
                seen_in_roots.add(merged_qname)
                roots.append(node)

    # ── Phase 4: Sort children within each parent by their natural order ──
    _sort_tree_children(roots, tax_dicts)

    # ── Phase 5: Query values for all data nodes in one batch ──
    if data_table and doc_ids:
        _populate_tree_values(conn, data_table, data_columns, doc_ids, nodes_by_qname)

    # ── Phase 6: Remove empty abstract nodes ──
    roots = _prune_empty_branches(roots)

    # ── Phase 7: Recursively merge sibling duplicates (industry variants
    #    from different merged parents end up as siblings) ──
    return _merge_sibling_duplicates(roots)


def _merge_sibling_duplicates(nodes: list[dict]) -> list[dict]:
    """Recursively merge sibling nodes that share the same display label.

    After merging industry-variant parents, children from different variants
    end up as siblings with the same label.  This pass collapses them.
    """
    # Group children by (label.lower(), level)
    groups: dict[tuple[str, int], list[dict]] = {}
    for node in nodes:
        key = (node["label"].lower(), node["level"])
        if key not in groups:
            groups[key] = []
        groups[key].append(node)

    merged: list[dict] = []
    for (_label_lower, _lvl), variants in groups.items():
        if len(variants) == 1:
            # Single node — just recurse into children
            v = variants[0]
            v["children"] = _merge_sibling_duplicates(v["children"])
            merged.append(v)
        else:
            # Multiple siblings with same label — merge them
            keeper = variants[0]
            keeper["has_data"] = keeper["has_data"] or any(v["has_data"] for v in variants[1:])
            # Combine all children, deduplicating by label
            all_children: dict[str, dict] = {}
            for v in variants:
                for child in v.get("children", []):
                    ckey = child["label"].lower()
                    if ckey not in all_children:
                        all_children[ckey] = child
                    else:
                        # Merge child data flag
                        all_children[ckey]["has_data"] = (
                            all_children[ckey]["has_data"] or child["has_data"]
                        )
                        # Combine their grandchildren
                        for gc in child.get("children", []):
                            gckey = gc["label"].lower()
                            existing = {c["label"].lower() for c in all_children[ckey].get("children", [])}
                            if gckey not in existing:
                                all_children[ckey].setdefault("children", []).append(gc)
                            else:
                                # Merge grandchild
                                for existing_gc in all_children[ckey].get("children", []):
                                    if existing_gc["label"].lower() == gckey:
                                        existing_gc["has_data"] = existing_gc["has_data"] or gc["has_data"]
                                        break
            keeper["children"] = _merge_sibling_duplicates(list(all_children.values()))
            merged.append(keeper)

    return merged


def _sort_tree_children(nodes: list[dict], tax_dicts: list[dict]) -> None:
    """Sort children within each node to match taxonomy presentation order."""
    # Build a qname → position map from the original taxonomy order
    qname_order: dict[str, int] = {}
    for i, row in enumerate(tax_dicts):
        qname = _safe_str(row.get("concept_qname", ""))
        if qname not in qname_order:
            qname_order[qname] = i

    def sort_recursive(children: list[dict]) -> None:
        children.sort(key=lambda n: qname_order.get(n.get("concept_qname", ""), 999999))
        for child in children:
            if child.get("children"):
                sort_recursive(child["children"])

    sort_recursive(nodes)


def _build_flat_tree_from_columns(
    conn: sqlite3.Connection,
    data_table: str | None,
    data_columns: dict[str, str],
    doc_ids: list[str],
) -> list[dict]:
    """Build a flat (level=0) tree from table columns when Taxonomy is unavailable."""
    tree: list[dict] = []
    if not data_table or not doc_ids:
        return tree

    # Get all values for all columns in one query
    placeholders = ",".join(["?"] * len(doc_ids))
    rows = conn.execute(
        f"SELECT * FROM {_q(data_table)} WHERE docID IN ({placeholders})",
        doc_ids,
    ).fetchall()

    # Build values per column
    col_values: dict[str, list] = {col: [None] * len(doc_ids) for col in data_columns.values()}
    for i, row in enumerate(rows):
        row_dict = dict(row)
        for col_lower, col_actual in data_columns.items():
            if col_actual in row_dict:
                val = row_dict[col_actual]
                col_values[col_actual][i] = None if val is None else (
                    float(val) if not isinstance(val, (int, float)) or _is_nan(val) else val
                )

    for col_actual in sorted(data_columns.values()):
        values = col_values.get(col_actual, [None] * len(doc_ids))
        # Skip columns where all values are None
        if all(v is None for v in values):
            continue
        tree.append({
            "concept_qname": col_actual,
            "label": _prettify_column_name(col_actual),
            "level": 0,
            "has_data": True,
            "values": values,
            "children": [],
        })

    return tree


def _build_concept_column_map(
    conn: sqlite3.Connection,
    data_columns: dict[str, str],
) -> dict[str, str]:
    """Build a mapping from concept_qname to the actual wide-table column name.

    Uses Statement_Hierarchy to resolve disambiguated column names
    (e.g., 'Buildings - Accumulated depreciation').
    """
    result: dict[str, str] = {}
    if not data_columns:
        return result

    # Try to use Statement_Hierarchy for parent-aware disambiguation
    sh_exists = False
    try:
        conn.execute("SELECT 1 FROM Statement_Hierarchy LIMIT 1")
        sh_exists = True
    except Exception:
        pass

    if sh_exists:
        # Build a column_concept_qname → disambiguated column name mapping
        col_concept_to_name: dict[str, str] = {}
        col_rows = conn.execute(
            "SELECT concept_qname, parent_concept_qname, primary_label_en "
            "FROM Statement_Hierarchy WHERE is_column = 1"
        ).fetchall()

        # Detect which labels have collisions
        from collections import Counter
        label_counts = Counter(
            str(r["primary_label_en"] or "").lower() for r in col_rows
        )
        for r in col_rows:
            cq = str(r["concept_qname"] or "")
            label = str(r["primary_label_en"] or "")
            name = label
            if label_counts.get(label.lower(), 0) > 1:
                parent_qname = str(r["parent_concept_qname"] or "")
                if parent_qname:
                    parent_row = conn.execute(
                        "SELECT primary_label_en FROM Statement_Hierarchy "
                        "WHERE concept_qname = ?",
                        (parent_qname,),
                    ).fetchone()
                    if parent_row:
                        parent_label = str(parent_row[0] or "")
                        if parent_label:
                            name = f"{parent_label} - {label}"
            # Match against actual column names
            actual_col = data_columns.get(name.lower())
            if actual_col:
                col_concept_to_name[cq] = actual_col

        # Map every taxonomy concept to its column (via column_concept_qname)
        tax_rows = conn.execute(
            "SELECT concept_qname, column_concept_qname FROM Statement_Hierarchy"
        ).fetchall()
        for tr in tax_rows:
            cq = str(tr["concept_qname"] or "")
            ccq = str(tr["column_concept_qname"] or "")
            if ccq in col_concept_to_name:
                result[cq] = col_concept_to_name[ccq]
    else:
        # Fallback: match by label only (no Statement_Hierarchy available)
        tax_rows = conn.execute(
            "SELECT concept_qname, primary_label_en FROM Taxonomy WHERE value_type = 'number'"
        ).fetchall()
        for tr in tax_rows:
            cq = str(tr["concept_qname"] or "")
            label = str(tr["primary_label_en"] or "")
            actual_col = data_columns.get(label.lower())
            if actual_col:
                result[cq] = actual_col

    return result


def _populate_tree_values(
    conn: sqlite3.Connection,
    data_table: str,
    data_columns: dict[str, str],
    doc_ids: list[str],
    nodes_by_qname: dict[str, dict],
) -> None:
    """Query the financial table once and populate values for all data nodes."""
    # Build concept → column mapping (handles disambiguated column names)
    concept_to_col = _build_concept_column_map(conn, data_columns)

    # Collect which columns we need
    needed_cols: list[str] = []
    qname_to_col: dict[str, str] = {}
    for qname, node in nodes_by_qname.items():
        if not node["has_data"]:
            continue
        # Try concept_qname-based mapping first, then fall back to label matching
        col = concept_to_col.get(qname)
        if not col:
            col = data_columns.get(node["label"].lower())
        if col:
            needed_cols.append(col)
            qname_to_col[qname] = col

    if not needed_cols:
        return

    # Deduplicate columns
    unique_cols = list(dict.fromkeys(needed_cols))

    placeholders = ",".join(["?"] * len(doc_ids))
    select = ", ".join(f"{_q(c)}" for c in unique_cols)
    rows = conn.execute(
        f"SELECT {select} FROM {_q(data_table)} WHERE docID IN ({placeholders})",
        doc_ids,
    ).fetchall()

    # Build column → values mapping
    col_values: dict[str, list] = {}
    for col in unique_cols:
        col_values[col] = [None] * len(doc_ids)

    for i, row in enumerate(rows):
        row_dict = dict(row)
        for col in unique_cols:
            val = row_dict.get(col)
            if val is not None and not _is_nan(val):
                try:
                    col_values[col][i] = float(val)
                except (TypeError, ValueError):
                    col_values[col][i] = None

    # Assign values back to nodes
    for qname, node in nodes_by_qname.items():
        if not node["has_data"]:
            continue
        col = qname_to_col.get(qname)
        if col:
            node["values"] = col_values.get(col, [None] * len(doc_ids))


def _prune_empty_branches(nodes: list[dict]) -> list[dict]:
    """Remove nodes that have no data and no children with data."""
    result: list[dict] = []
    for node in nodes:
        node["children"] = _prune_empty_branches(node["children"])
        has_children = len(node["children"]) > 0
        if node["has_data"] or has_children:
            result.append(node)
    return result


def _resolve_table_case_insensitive(
    table_map: dict[str, str], target: str
) -> str | None:
    """Find a table name case-insensitively."""
    key = target.lower()
    return table_map.get(key)


def _q(name: str) -> str:
    """Quote an SQLite identifier."""
    return f'"{name}"'


def _safe_date_label(value: Any) -> str:
    """Return YYYY-MM or YYYY-MM-DD from a date-ish value."""
    text = _safe_str(value)
    if not text:
        return "?"
    # Prefer YYYY-MM if it's a standard period end
    if len(text) >= 7:
        return text[:7]
    return text[:10]


def _is_nan(value: Any) -> bool:
    """Check if a value is NaN."""
    if value is None:
        return False
    try:
        return value != value  # NaN check
    except Exception:
        return False


def _prettify_column_name(name: str) -> str:
    """Convert a database column name to a readable label."""
    text = name.replace("_", " ")
    text = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", text)
    parts = []
    for token in text.split():
        if token.isupper() and len(token) <= 4:
            parts.append(token)
        else:
            parts.append(token[0].upper() + token[1:] if len(token) > 1 else token.upper())
    return " ".join(parts)
