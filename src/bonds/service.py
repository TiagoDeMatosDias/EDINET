"""Read side of ``Bonds.db`` for the Analysis and Research pages.

Valuation uses the latest JGB curve. A bond's current spread is the one its
JSDA reference price implies when it is quoted, and otherwise the spread it
was priced at when issued ("spread at issue"). Carrying the spread at issue
to today's government yield of the same remaining tenor gives a
constant-spread price: what an unquoted bond is worth if the market still
demands the spread it paid. Peer fair value applies the median current
spread of comparable bonds (same ranking, a rating within a notch, a similar
tenor; spreads at issue only from bonds issued in the last three years), so a
bond whose spread is above its peers' looks cheap and one below looks rich.
"""

from __future__ import annotations

import json
import sqlite3
import statistics
from datetime import date, timedelta
from typing import Any

from .names import CompanyDirectory
from .store import decode
from .valuation import Curve, CurveBook, clean_price, modified_duration, year_fraction

EDINET_VIEWER = "https://disclosure2.edinet-fsa.go.jp/WZEK0040.aspx?{doc_id},,,"
PEER_LOOKBACK_YEARS = 3
SIMILAR_LIMIT = 15
_JUNIOR = {"subordinated", "hybrid"}

UNIVERSE_FIELDS = (
    "bond_id", "edinet_code", "company_name", "company_name_en", "ticker", "industry", "listed", "name", "series", "currency",
    "seniority", "features", "coupon", "coupon_kind", "frequency", "issue_date", "maturity", "perpetual", "call_date",
    "amount_issued", "outstanding", "outstanding_as_of", "private", "rating", "rating_agency", "rating_notch",
    "rating_inferred", "issue_spread", "issue_yield", "issue_tenor",
    "market_date", "market_price", "market_yield", "market_spread", "market_reporters",
)


def edinet_link(doc_id: str | None) -> str | None:
    return EDINET_VIEWER.format(doc_id=doc_id) if doc_id else None


def ranking(seniority: str | None) -> str:
    return "junior" if seniority in _JUNIOR else "convertible" if seniority == "convertible" else "senior"


def _round(value: float | None, digits: int = 6) -> float | None:
    return None if value is None else round(value, digits)


def value_bond(bond: dict[str, Any], curve: Curve | None, today: str) -> dict[str, Any]:
    """Remaining tenor, today's government yield for it, and the constant-spread price and duration."""
    to_call = bool(bond.get("call_date")) and (bond.get("call_date") or "") > today
    end = bond.get("call_date") if to_call else bond.get("maturity")
    horizon = year_fraction(today, end)
    result: dict[str, Any] = {"years_to_maturity": _round(year_fraction(today, bond.get("maturity")), 4), "horizon": _round(horizon, 4), "horizon_to": "call" if to_call else "maturity"}
    # The spread comparisons use: the market's when the bond is quoted, else the one it was issued at.
    if bond.get("market_spread") is not None:
        result.update(spread=bond["market_spread"], spread_basis="market")
    elif bond.get("issue_spread") is not None:
        result.update(spread=bond["issue_spread"], spread_basis="issue")
    if horizon is None or horizon <= 0 or curve is None or (bond.get("currency") or "JPY") != "JPY":
        return result
    government = curve.at(horizon)
    result["jgb_now"] = _round(government, 8)
    coupon = bond.get("coupon")
    spread = bond.get("issue_spread")
    frequency = int(bond.get("frequency") or 2)
    if coupon is not None and spread is not None and government is not None and bond.get("coupon_kind") not in ("floating", "unknown"):
        model_yield = government + spread
        result["model_yield"] = _round(model_yield, 8)
        result["model_price"] = _round(clean_price(coupon, horizon, model_yield, frequency), 4)
        result["duration"] = _round(modified_duration(coupon, horizon, model_yield, frequency), 4)
    return result


def _connect(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _curves(conn: sqlite3.Connection) -> CurveBook:
    return CurveBook.load(conn)


def _updates(conn: sqlite3.Connection) -> dict[str, Any]:
    try:
        rows = conn.execute("SELECT source, updated_at, detail FROM Bond_Updates").fetchall()
    except sqlite3.Error:
        return {}
    result = {}
    for row in rows:
        try:
            detail = json.loads(row["detail"] or "{}")
        except ValueError:
            detail = {}
        result[row["source"]] = {"updated_at": row["updated_at"], **(detail if isinstance(detail, dict) else {})}
    return result


def curve_payload(curves: CurveBook) -> dict[str, Any] | None:
    latest = curves.latest()
    if latest is None:
        return None
    year_ago = curves.on((date.fromisoformat(latest.date) - timedelta(days=365)).isoformat())
    return {
        "date": latest.date,
        "points": latest.points(),
        "year_ago": {"date": year_ago.date, "points": year_ago.points()} if year_ago else None,
    }


def status(path: str) -> dict[str, Any]:
    conn = _connect(path)
    try:
        counts = conn.execute(
            "SELECT COUNT(*) AS bonds, SUM(status = 'outstanding') AS outstanding, COUNT(DISTINCT edinet_code) AS companies FROM Bonds"
        ).fetchone()
        curves = _curves(conn)
        latest = curves.latest()
        return {
            "bonds": counts["bonds"] or 0,
            "outstanding": counts["outstanding"] or 0,
            "companies": counts["companies"] or 0,
            "curve_date": latest.date if latest else None,
            "updates": _updates(conn),
        }
    finally:
        conn.close()


def _documents(conn: sqlite3.Connection, edinet_code: str, catalogued: set[str]) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT doc_id, kind, submitted_at, period_end, bond_count, status, description, archive IS NOT NULL AS stored FROM Bond_Documents "
        "WHERE edinet_code = ? AND status IN ('parsed', 'empty') ORDER BY submitted_at DESC",
        (edinet_code,),
    ).fetchall()
    return [
        {
            "doc_id": row["doc_id"],
            "kind": row["kind"],
            "submitted_at": row["submitted_at"],
            "period_end": row["period_end"],
            "bond_count": row["bond_count"],
            "description": row["description"],
            "edinet_url": edinet_link(row["doc_id"]),
            "stored": bool(row["stored"]),
            # Annual reports are read from the filing catalog, so the Filing Explorer can open them.
            "in_catalog": row["kind"] == "annual" or row["doc_id"] in catalogued,
        }
        for row in rows
        if row["kind"] == "issuance" and row["bond_count"] or row["kind"] == "annual"
    ]


def _subsidiary_labels(conn: sqlite3.Connection, directory: CompanyDirectory, edinet_code: str) -> dict[str, str]:
    """Labels for the issuers in this group's bonds that have a name only in Japanese."""
    rows = conn.execute("SELECT company_name, company_name_en, issuer FROM Bonds WHERE edinet_code = ? AND is_parent = 0", (edinet_code,)).fetchall()
    if not rows:
        return {}
    return directory.subsidiary_labels(edinet_code, rows[0]["company_name"] or "", rows[0]["company_name_en"], [row["issuer"] or "" for row in rows])


def _with_links(bond: dict[str, Any], directory: CompanyDirectory, subsidiaries: dict[str, str] | None = None) -> dict[str, Any]:
    directory.present(bond, subsidiaries)
    bond["issuance_url"] = edinet_link(bond.get("issuance_doc_id"))
    bond["schedule_url"] = edinet_link(bond.get("schedule_doc_id"))
    return bond


def company_bonds(path: str, edinet_code: str, *, today: str | None = None, catalogued: set[str] | None = None) -> dict[str, Any]:
    """Every bond in a company's latest filings, with totals, a maturity ladder, and its documents."""
    today = today or date.today().isoformat()
    conn = _connect(path)
    try:
        bonds = [decode(row) for row in conn.execute("SELECT * FROM Bonds WHERE edinet_code = ? ORDER BY maturity IS NULL, maturity, name", (edinet_code,))]
        curves = _curves(conn)
        curve = curves.latest()
        directory = CompanyDirectory.load(conn)
        subsidiaries = _subsidiary_labels(conn, directory, edinet_code)
        for bond in bonds:
            bond.update(value_bond(bond, curve, today))
            _with_links(bond, directory, subsidiaries)
        documents = _documents(conn, edinet_code, catalogued or set())
        peers = _peer_spreads(conn, today)
    finally:
        conn.close()

    live = [bond for bond in bonds if bond["status"] == "outstanding"]
    yen = [bond for bond in live if (bond.get("currency") or "JPY") == "JPY" and bond.get("outstanding")]
    total = sum(bond["outstanding"] for bond in yen)
    priced = [bond for bond in yen if bond.get("coupon") is not None]
    weight = sum(bond["outstanding"] for bond in priced)
    dated = [bond for bond in yen if bond.get("years_to_maturity") is not None]
    dated_weight = sum(bond["outstanding"] for bond in dated)
    ladder: dict[int, dict[str, float]] = {}
    for bond in yen:
        if not bond.get("maturity"):
            continue
        year = int(bond["maturity"][:4])
        bucket = ladder.setdefault(year, {"parent": 0.0, "group": 0.0})
        bucket["parent" if bond["is_parent"] else "group"] += bond["outstanding"]
    rated = sorted((bond for bond in bonds if bond.get("rating") and not bond.get("rating_inferred") and bond.get("issue_date")), key=lambda bond: bond["issue_date"], reverse=True)
    spreads = [bond["spread"] for bond in live if bond.get("spread") is not None and bond["is_parent"] and ranking(bond.get("seniority")) == "senior"]
    company_spread = statistics.median(spreads) if spreads else None
    benchmark = None
    if rated:
        notch = rated[0].get("rating_notch")
        sample = [spread for spread_notch, spread in peers if notch is not None and abs(spread_notch - notch) <= 1]
        benchmark = statistics.median(sample) if sample else None
    first = bonds[0] if bonds else {}
    return {
        "edinet_code": edinet_code,
        "company_name": first.get("company_name") or directory.by_code.get(edinet_code, {}).get("name"),
        "today": today,
        "curve": curve_payload(curves),
        "summary": {
            "outstanding_count": len(live),
            "total_outstanding": total or None,
            "foreign_currency_count": sum(1 for bond in live if (bond.get("currency") or "JPY") != "JPY"),
            "not_separately_reported": sum(1 for bond in live if bond.get("outstanding") is None),
            "average_coupon": sum(bond["coupon"] * bond["outstanding"] for bond in priced) / weight if weight else None,
            "average_years": sum(bond["years_to_maturity"] * bond["outstanding"] for bond in dated) / dated_weight if dated_weight else None,
            "next_maturity": min((bond["maturity"] for bond in live if bond.get("maturity")), default=None),
            "due_within_year": sum(bond["outstanding"] for bond in yen if bond.get("years_to_maturity") is not None and bond["years_to_maturity"] <= 1) or None,
            "as_of": max((bond.get("schedule_period_end") or "" for bond in bonds), default="") or None,
            "ratings": rated[0].get("ratings") if rated else [],
            "rating": rated[0].get("rating") if rated else None,
            "rated_on": rated[0].get("issue_date") if rated else None,
            "median_spread": company_spread,
            "rating_peer_spread": benchmark,
        },
        "ladder": [{"year": year, **amounts} for year, amounts in sorted(ladder.items())],
        "bonds": bonds,
        "documents": documents,
    }


def _peer_spreads(conn: sqlite3.Connection, today: str) -> list[tuple[float, float]]:
    """Current spreads of senior public bonds: quoted ones, and recent issues without a quote."""
    since = (date.fromisoformat(today) - timedelta(days=365 * PEER_LOOKBACK_YEARS)).isoformat()
    rows = conn.execute(
        "SELECT rating_notch, COALESCE(market_spread, issue_spread) FROM Bonds WHERE status = 'outstanding' AND is_parent = 1 AND private = 0 "
        "AND rating_notch IS NOT NULL AND seniority IN ('senior', 'secured') "
        "AND (market_spread IS NOT NULL OR (issue_spread IS NOT NULL AND issue_date >= ?))",
        (since,),
    ).fetchall()
    return [(row[0], row[1]) for row in rows]


_COMPANY_FIELDS = ("company_name", "company_name_ja", "ticker", "industry", "listed")
_FLAGS = {"perpetual", "private", "rating_inferred"}
_DETAIL_ONLY = {"outstanding_as_of", "issue_tenor", "issue_yield"}


def _compact_bond(bond: dict[str, Any]) -> dict[str, Any]:
    item = {
        key: value for key, value in bond.items()
        if key not in _COMPANY_FIELDS and key not in _DETAIL_ONLY and value is not None and value != [] and value != ""
        and not (key in _FLAGS and not value)
    }
    for key, value in item.items():
        if isinstance(value, float):
            item[key] = round(value, 6)
    return item


def universe(path: str, *, today: str | None = None, include_group: bool = False) -> dict[str, Any]:
    """Every outstanding bond for the market view, each with today's constant-spread valuation.

    Company fields are sent once per company in ``companies`` rather than on every bond.
    """
    today = today or date.today().isoformat()
    conn = _connect(path)
    try:
        # Rows that group several bonds under one maturity range are not single bonds to compare.
        where = "status = 'outstanding' AND (maturity IS NOT NULL OR perpetual = 1)" + ("" if include_group else " AND is_parent = 1")
        bonds = [decode(row) for row in conn.execute(f"SELECT {', '.join(UNIVERSE_FIELDS)} FROM Bonds WHERE {where} ORDER BY maturity")]
        curves = _curves(conn)
        updates = _updates(conn)
        directory = CompanyDirectory.load(conn)
    finally:
        conn.close()
    curve = curves.latest()
    companies: dict[str, dict[str, Any]] = {}
    rows = []
    for bond in bonds:
        bond.update(value_bond(bond, curve, today))
        directory.present(bond)
        bond.pop("name", None)
        companies.setdefault(bond["edinet_code"], {key: bond.get(key) for key in _COMPANY_FIELDS})
        rows.append(_compact_bond(bond))
    industries = sorted({company["industry"] for company in companies.values() if company.get("industry")})
    return {"today": today, "curve": curve_payload(curves), "companies": companies, "bonds": rows, "industries": industries, "updates": updates}


def _distance(target: dict[str, Any], other: dict[str, Any], today: str) -> float:
    horizon, other_horizon = target.get("horizon"), other.get("horizon")
    distance = abs((other_horizon or 0) - (horizon or 0)) / 2 if horizon is not None and other_horizon is not None else 3.0
    notch, other_notch = target.get("rating_notch"), other.get("rating_notch")
    distance += abs(other_notch - notch) if notch is not None and other_notch is not None else 2.0
    distance += 3.0 if ranking(target.get("seniority")) != ranking(other.get("seniority")) else 0.0
    distance += 0.5 if target.get("industry") != other.get("industry") else 0.0
    age = year_fraction(other.get("issue_date"), today)
    distance += 0.3 * max(0.0, (age or 0) - 2)
    distance += 1.0 if other.get("private") else 0.0
    return distance


def _current(bond: dict[str, Any], since: str) -> float | None:
    """A peer's spread for comparison: quoted, or at issue when the issue is recent enough to still be informative."""
    if bond.get("market_spread") is not None:
        return bond["market_spread"]
    if bond.get("issue_spread") is not None and (bond.get("issue_date") or "") >= since:
        return bond["issue_spread"]
    return None


def peer_value(target: dict[str, Any], peers: list[dict[str, Any]], curve: Curve | None, today: str) -> dict[str, Any] | None:
    """Fair value at the median current spread of comparable bonds."""
    horizon = target.get("horizon")
    if horizon is None or horizon <= 0 or curve is None or target.get("coupon") is None or target.get("coupon_kind") in ("floating", "unknown"):
        return None
    since = (date.fromisoformat(today) - timedelta(days=365 * PEER_LOOKBACK_YEARS)).isoformat()
    candidates = [
        {**bond, "_spread": spread} for bond in peers
        if (spread := _current(bond, since)) is not None and not bond.get("private")
        and ranking(bond.get("seniority")) == ranking(target.get("seniority")) and bond.get("horizon") is not None
        and (target.get("rating_notch") is None or (bond.get("rating_notch") is not None and abs(bond["rating_notch"] - target["rating_notch"]) <= 1))
    ]
    for window in (2.0, 4.0, 8.0):
        sample = [bond for bond in candidates if abs(bond["horizon"] - horizon) <= window]
        if len(sample) >= 5:
            break
    if len(sample) < 3:
        return None
    spread = statistics.median(bond["_spread"] for bond in sample)
    government = curve.at(horizon)
    if government is None:
        return None
    fair_yield = government + spread
    frequency = int(target.get("frequency") or 2)
    result = {
        "peer_count": len(sample),
        "tenor_window": window,
        "peer_spread": _round(spread, 8),
        "fair_yield": _round(fair_yield, 8),
        "fair_price": _round(clean_price(target["coupon"], horizon, fair_yield, frequency), 4),
        "spread_quartiles": [_round(value, 8) for value in statistics.quantiles([bond["_spread"] for bond in sample], n=4)] if len(sample) >= 4 else None,
        "quoted_peers": sum(1 for bond in sample if bond.get("market_spread") is not None),
    }
    if target.get("spread") is not None:
        result["relative_spread"] = _round(target["spread"] - spread, 8)
    return result


def bond_detail(path: str, bond_id: str, *, today: str | None = None) -> dict[str, Any] | None:
    """One bond with its terms, sources, valuation, similar bonds of other issuers, and the peer spread curve."""
    today = today or date.today().isoformat()
    conn = _connect(path)
    try:
        row = conn.execute("SELECT * FROM Bonds WHERE bond_id = ?", (bond_id,)).fetchone()
        if row is None:
            return None
        directory = CompanyDirectory.load(conn)
        bond = decode(row)
        _with_links(bond, directory, None if bond.get("is_parent") else _subsidiary_labels(conn, directory, bond["edinet_code"]))
        curves = _curves(conn)
        curve = curves.latest()
        bond.update(value_bond(bond, curve, today))
        issuance = None
        if bond.get("issuance_doc_id"):
            found = conn.execute(
                "SELECT i.*, d.archive IS NOT NULL AS stored FROM Bond_Issuances i JOIN Bond_Documents d ON d.doc_id = i.doc_id "
                "WHERE i.doc_id = ? AND i.name = ? LIMIT 1",
                (bond["issuance_doc_id"], bond.get("name")),
            ).fetchone()
            issuance = decode(found) if found else None
        history = [
            {"date": row[0], "price": row[1], "yield": row[2] / 100 if row[2] is not None else None}
            for row in conn.execute(
                "SELECT price_date, price, yield FROM Bond_Market_Prices WHERE code = ? ORDER BY price_date DESC LIMIT 260",
                (bond.get("jsda_code"),),
            )
        ][::-1] if bond.get("jsda_code") else []
        others = [decode(item) for item in conn.execute(
            f"SELECT {', '.join(UNIVERSE_FIELDS)} FROM Bonds WHERE status = 'outstanding' AND is_parent = 1 AND (currency = 'JPY' OR currency IS NULL) "
            "AND (maturity IS NOT NULL OR perpetual = 1)"
        )]
    finally:
        conn.close()
    for other in others:
        other.update(value_bond(other, curve, today))
        directory.present(other)
    issuer_bonds = [other for other in others if other["edinet_code"] == bond["edinet_code"] and other["bond_id"] != bond_id]
    peers = [other for other in others if other["edinet_code"] != bond["edinet_code"]]
    ranked = sorted(peers, key=lambda other: _distance(bond, other, today))
    similar: list[dict[str, Any]] = []
    for other in ranked:
        if len(similar) >= SIMILAR_LIMIT:
            break
        # Show one bond per company so a frequent issuer does not fill the list.
        if any(item["edinet_code"] == other["edinet_code"] for item in similar):
            continue
        similar.append(other)
    valuation = peer_value(bond, peers, curve, today)
    since = (date.fromisoformat(today) - timedelta(days=365 * PEER_LOOKBACK_YEARS)).isoformat()
    curve_peers = [
        {"bond_id": other["bond_id"], "company_name": other["company_name"], "horizon": other["horizon"], "spread": spread, "rating": other.get("rating"), "quoted": other.get("market_spread") is not None}
        for other in peers
        if (spread := _current(other, since)) is not None and other.get("horizon") is not None and not other.get("private")
        and ranking(other.get("seniority")) == ranking(bond.get("seniority"))
        and (bond.get("rating_notch") is None or (other.get("rating_notch") is not None and abs(other["rating_notch"] - bond["rating_notch"]) <= 1))
    ]
    return {
        "today": today,
        "bond": bond,
        "issuance": issuance,
        "valuation": valuation,
        "market_history": history,
        "issuer_bonds": issuer_bonds,
        "similar": similar,
        "spread_curve": {"peers": curve_peers, "issuer": [{"bond_id": other["bond_id"], "horizon": other["horizon"], "spread": other["spread"]} for other in issuer_bonds if other.get("spread") is not None and other.get("horizon") is not None]},
        "curve": curve_payload(curves),
    }


def document_archive(path: str, doc_id: str) -> bytes | None:
    conn = _connect(path)
    try:
        row = conn.execute("SELECT archive FROM Bond_Documents WHERE doc_id = ?", (doc_id,)).fetchone()
    finally:
        conn.close()
    return bytes(row[0]) if row and row[0] else None
