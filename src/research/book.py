"""The research book: every company a user follows, with their research and live market values.

A company is in the book once the user has tagged it, written a note on it,
recorded a thesis or target, or set an alert on it. Market values come from the
same calculation as the Analyze workspace, so alerts can say whether their
condition holds today.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from .alerts import evaluate_expression
from .positions import CLOSED_TAG, OPEN_TAG, is_edinet_code
from .storage import ResearchStore

_LOGGER = logging.getLogger(__name__)

# Values the book shows beside each company; alerts can watch any of them.
BOOK_METRICS = (
    "LatestPrice", "MarketCap", "PERatio", "PriceToBook", "PriceToSales",
    "DividendsYield", "PayoutRatio", "ReturnOnEquity", "ReturnOnAssets", "CurrentRatio",
)

MetricsLookup = dict[str, dict[str, Any]]


def _price_tickers(db: str, symbols: dict[str, list[str]]) -> dict[str, str]:
    """The stored price ticker for each security, trying each broker symbol's stored forms in turn."""
    from src.orchestrator.common.sqlite import connect_read
    from src.portfolio.market_data import price_ticker_candidates

    conn = connect_read(db)
    try:
        found: dict[str, str] = {}
        for code, names in symbols.items():
            for candidate in (form for symbol in names for form in price_ticker_candidates(symbol)):
                if conn.execute("SELECT 1 FROM Stock_Prices WHERE Ticker = ? LIMIT 1", (candidate,)).fetchone():
                    found[code] = candidate
                    break
        return found
    finally:
        conn.close()


def _market_data(
    db: str | None,
    codes: list[str],
    securities: dict[str, dict[str, Any]] | None = None,
) -> tuple[dict[str, dict[str, Any]], MetricsLookup, dict[str, str]]:
    """Names, Analyze metrics, and price currencies for ``codes``; empty when the market database is unavailable.

    ``securities`` describes codes that are not EDINET companies (a US share
    held in the portfolio): their name and broker symbols, from which the
    stored price is found.
    """
    if not db or not codes:
        return {}, {}, {}
    try:
        from src.comparison.peers import _cached_metrics, _price_currencies
        from src.security_analysis.security_analysis import _get_cached_company_frame
        from src.web_app.api.security_analysis import _compute_metrics

        frame = _get_cached_company_frame(db)
        wanted = set(codes)
        records = frame[frame["company_code"].astype(str).isin(wanted)].fillna("").to_dict(orient="records")
        info = {str(record["company_code"]): record for record in records}
        others = {code: item for code, item in (securities or {}).items() if code in wanted and code not in info}
        tickers = _price_tickers(db, {code: item.get("symbols") or [code] for code, item in others.items()})
        for code, item in others.items():
            info[code] = {"company_code": code, "company_name": item.get("name") or code, "ticker": tickers.get(code, ""), "industry": None, "kind": "security"}
        lookup = _cached_metrics(db, lambda code, ticker: _compute_metrics(db, code, {}, {"ticker": ticker}))
        metrics = {
            code: lookup(code, str(record.get("ticker")))
            for code, record in info.items() if record.get("ticker")
        }
        currencies = _price_currencies(db, [str(record["ticker"]) for record in info.values() if record.get("ticker")])
        return info, metrics, currencies
    except Exception as exc:  # noqa: BLE001 - the book still lists research without market values
        _LOGGER.warning("Could not load market values for the research book: %s", exc)
        return {}, {}, {}


def _expression(alert: dict[str, Any]) -> dict[str, Any]:
    try:
        expression = json.loads(alert.get("expression_json") or "{}")
    except (TypeError, json.JSONDecodeError):
        return {}
    return expression if isinstance(expression, dict) else {}


def build_book(
    store: ResearchStore,
    user_id: str,
    db: str | None,
    securities: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Companies with research state, newest activity first, plus tags and alerts with their status today."""
    memberships = store.list_all_company_tags(user_id)
    research = {row["edinet_code"]: row for row in store.list_company_research(user_id)}
    notes = store.list_notes(user_id)
    alerts = store.list_alerts(user_id)

    activity: dict[str, str] = {}

    def touch(code: str | None, when: str | None) -> None:
        if code:
            activity[code] = max(activity.get(code, ""), str(when or ""))

    tags_by_code: dict[str, list[str]] = {}
    for row in memberships:
        tags_by_code.setdefault(row["edinet_code"], []).append(row["tag"])
        touch(row["edinet_code"], row.get("created_at"))
    note_stats: dict[str, dict[str, Any]] = {}
    for note in notes:
        code = note.get("edinet_code")
        if not code:
            continue
        stats = note_stats.setdefault(code, {"count": 0, "last": ""})
        stats["count"] += 1
        stats["last"] = max(stats["last"], str(note.get("updated_at") or ""))
        touch(code, note.get("updated_at"))
    for code, row in research.items():
        touch(code, row.get("updated_at"))
    for alert in alerts:
        touch(alert.get("edinet_code"), alert.get("updated_at"))

    codes = sorted(activity, key=lambda code: activity[code], reverse=True)
    info, metrics, currencies = _market_data(db, codes, securities)

    alert_rows = []
    alert_counts: dict[str, dict[str, int]] = {}
    for alert in alerts:
        expression = _expression(alert)
        code = alert.get("edinet_code") or ""
        metric = str(expression.get("metric") or "")
        current = metrics.get(code, {}).get(metric) if code else None
        triggered = bool(alert.get("enabled")) and current is not None and evaluate_expression(expression, {metric: current})
        counts = alert_counts.setdefault(code, {"total": 0, "triggered": 0})
        counts["total"] += 1
        counts["triggered"] += int(triggered)
        alert_rows.append({
            "alert_id": alert["alert_id"],
            "name": alert["name"],
            "edinet_code": code or None,
            "company_name": info.get(code, {}).get("company_name") or code or None,
            "metric": metric,
            "operator": expression.get("operator"),
            "value": expression.get("value"),
            "enabled": bool(alert.get("enabled")),
            "current_value": current,
            "triggered": triggered,
            "price_currency": currencies.get(str(info.get(code, {}).get("ticker") or "")),
            "created_at": alert.get("created_at"),
        })

    companies = []
    for code in codes:
        record = info.get(code, {})
        thesis = research.get(code, {})
        values = metrics.get(code, {})
        ticker = str(record.get("ticker") or "")
        tags = tags_by_code.get(code, [])
        companies.append({
            "company_code": code,
            "company_name": record.get("company_name") or (securities or {}).get(code, {}).get("name") or code,
            "ticker": ticker,
            "industry": record.get("industry") or None,
            # A company has an EDINET record; a security is any other holding, keyed by its symbol.
            "kind": "company" if is_edinet_code(code) else "security",
            "position": "open" if OPEN_TAG in tags else "closed" if CLOSED_TAG in tags else None,
            "tags": tags,
            "thesis_status": thesis.get("thesis_status"),
            "thesis": thesis.get("thesis"),
            "target_value": thesis.get("target_value"),
            "target_currency": thesis.get("target_currency"),
            "review_on": thesis.get("review_on"),
            "note_count": note_stats.get(code, {}).get("count", 0),
            "last_note_at": note_stats.get(code, {}).get("last") or None,
            "alert_count": alert_counts.get(code, {}).get("total", 0),
            "alerts_triggered": alert_counts.get(code, {}).get("triggered", 0),
            "updated_at": activity[code] or None,
            "price_currency": currencies.get(ticker),
            **{metric: values.get(metric) for metric in BOOK_METRICS},
        })

    tag_counts: dict[str, int] = {}
    for row in memberships:
        tag_counts[row["tag"]] = tag_counts.get(row["tag"], 0) + 1
    tag_names = sorted({str(row["tag"]) for row in store.list_all_tags(user_id)} | set(tag_counts), key=str.casefold)
    return {
        "companies": companies,
        "tags": [{"name": name, "member_count": tag_counts.get(name, 0)} for name in tag_names],
        "alerts": alert_rows,
    }
