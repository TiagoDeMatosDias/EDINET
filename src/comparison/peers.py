"""Peer suggestions for a comparison: listed companies in the same industry, closest in size."""

from __future__ import annotations

import math
import os
from collections.abc import Callable
from typing import Any

from src.orchestrator.common.sqlite import connect_read

# Values shown beside each suggestion, from the same calculation as the Analyze workspace.
PEER_METRICS = ("MarketCap", "PERatio", "PriceToBook", "ReturnOnEquity", "DividendsYield")

MetricsFn = Callable[[str, str], dict[str, Any]]

_METRICS_CACHE: dict[tuple[str, float, float], dict[str, dict[str, Any]]] = {}


def _cached_metrics(db: str, compute: MetricsFn) -> MetricsFn:
    """Analyze metrics per company, kept until the database (or its write-ahead log) changes."""
    try:
        stamp = os.path.getmtime(db)
    except OSError:
        return compute
    wal = f"{db}-wal"
    key = (db, stamp, os.path.getmtime(wal) if os.path.exists(wal) else 0.0)
    if key not in _METRICS_CACHE:
        _METRICS_CACHE.clear()
        _METRICS_CACHE[key] = {}
    cache = _METRICS_CACHE[key]

    def lookup(code: str, ticker: str) -> dict[str, Any]:
        if code not in cache:
            cache[code] = compute(code, ticker)
        return cache[code]

    return lookup


def _size_distance(value: float | None, reference: float | None) -> float | None:
    """How far apart two market caps are, in orders of magnitude (0 = the same size)."""
    if not value or not reference or value <= 0 or reference <= 0:
        return None
    return abs(math.log(value / reference))


def rank_peers(
    companies: list[dict[str, Any]],
    selected_codes: list[str],
    metrics_for: MetricsFn,
    limit: int = 12,
) -> dict[str, Any]:
    """Rank listed companies that share an industry with any selected company.

    Each candidate is measured against the selected company of its industry
    whose market cap is closest; the nearest in size come first, and companies
    without a market cap follow in name order.
    """
    selected = [company for company in companies if company["company_code"] in selected_codes]
    industries = list(dict.fromkeys(str(company.get("industry") or "") for company in selected))
    industries = [industry for industry in industries if industry]
    chosen = set(selected_codes)
    members: dict[str, list[tuple[str, float | None]]] = {}
    for company in selected:
        industry = str(company.get("industry") or "")
        cap = metrics_for(company["company_code"], str(company.get("ticker") or "")).get("MarketCap") if company.get("ticker") else None
        members.setdefault(industry, []).append((company["company_code"], cap))

    rows: list[dict[str, Any]] = []
    for company in companies:
        industry = str(company.get("industry") or "")
        code = company["company_code"]
        if industry not in members or code in chosen or not company.get("ticker"):
            continue
        values = metrics_for(code, str(company["ticker"]))
        cap = values.get("MarketCap")
        nearest, distance = None, None
        for member_code, member_cap in members[industry]:
            gap = _size_distance(cap, member_cap)
            if gap is not None and (distance is None or gap < distance):
                nearest, distance = member_code, gap
        reference = dict(members[industry]).get(nearest) if nearest else None
        rows.append({
            "company_code": code,
            "ticker": company.get("ticker"),
            "company_name": company.get("company_name") or code,
            "industry": industry,
            "market": company.get("market"),
            **{metric: values.get(metric) for metric in PEER_METRICS},
            "nearest_code": nearest,
            "size_ratio": cap / reference if cap and reference else None,
            "_distance": distance,
        })
    rows.sort(key=lambda row: (
        row["_distance"] is None,
        row["_distance"] or 0.0,
        str(row["company_name"]).casefold(),
    ))
    for row in rows:
        row.pop("_distance")
    counts = {industry: sum(1 for row in rows if row["industry"] == industry) for industry in industries}
    return {
        "company_codes": selected_codes,
        "industries": [{"industry": industry, "candidates": counts[industry]} for industry in industries],
        "total": len(rows),
        "peers": rows[:limit],
    }


def _price_currencies(db: str, tickers: list[str]) -> dict[str, str]:
    if not tickers:
        return {}
    conn = connect_read(db)
    try:
        placeholders = ",".join("?" * len(tickers))
        rows = conn.execute(
            f"SELECT Ticker, Currency, MAX(Date) FROM Stock_Prices WHERE Ticker IN ({placeholders}) GROUP BY Ticker",
            tickers,
        ).fetchall()
    except Exception:  # noqa: BLE001 - currency is a formatting hint only
        return {}
    finally:
        conn.close()
    return {str(row[0]): str(row[1]) for row in rows if row[1]}


def find_peers(db: str, company_codes: list[str], limit: int = 12) -> dict[str, Any]:
    """Peer suggestions for the selected companies, read from the research database."""
    from src.security_analysis.security_analysis import _get_cached_company_frame
    from src.web_app.api.security_analysis import _compute_metrics

    frame = _get_cached_company_frame(db)
    companies = [
        {**record, "company_code": str(record.get("company_code") or ""), "ticker": str(record.get("ticker") or "")}
        for record in frame.fillna("").to_dict(orient="records")
    ]
    metrics_for = _cached_metrics(db, lambda code, ticker: _compute_metrics(db, code, {}, {"ticker": ticker}))
    result = rank_peers(companies, company_codes, metrics_for, limit)
    currencies = _price_currencies(db, [str(row["ticker"]) for row in result["peers"]])
    for row in result["peers"]:
        row["price_currency"] = currencies.get(str(row["ticker"]))
    return result
