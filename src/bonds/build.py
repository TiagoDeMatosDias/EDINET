"""Merge what each filing says into one row per bond.

Each company's latest annual-report bond schedule is the list of what was
outstanding at its fiscal year end: every row becomes a bond, including the
bonds of consolidated subsidiaries. Issuance supplements add the full terms
(ratings, issue price, coupon dates, features) to the matching schedule row,
and bonds issued after the latest annual report come from their supplement
alone. A supplement matches a schedule row of the same company when the
series number and maturity agree, or the maturity and coupon do.

Spreads are measured against the JGB par curve: the yield at issue (from the
issue price and coupon, to the first call date for callable bonds) less the
government yield of the same tenor on the issue date.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import sqlite3
from collections import defaultdict
from datetime import date
from typing import Any

from .jsda import latest_quotes, match_quotes
from .parsing import compact, is_floating, notch_label, rating_notch
from .store import decode, now
from .valuation import CurveBook, year_fraction, yield_from_price

logger = logging.getLogger(__name__)

_AGENCY_PRIORITY = ("R&I", "JCR", "S&P", "Moody's", "Fitch")
_COMPANY_WORDS = re.compile(r"株式会社|\(株\)|㈱|有限会社|合同会社|holdings|ホールディングス|グループ|[\s・.,]", re.IGNORECASE)
_PARENT_WORDS = {"当社", "提出会社", "親会社", "当行", "当金庫"}


def _short_name(name: str | None) -> str:
    return _COMPANY_WORDS.sub("", compact(name or "")).casefold()


def is_parent_issuer(issuer: str, company_names: tuple[str, ...]) -> bool:
    key = compact(issuer)
    if not key or key in _PARENT_WORDS:
        return True
    short = _short_name(issuer)
    if not short:
        return True
    return any(name and (short == name or (len(short) >= 2 and (short in name or name in short))) for name in map(_short_name, company_names))


def bond_id(edinet_code: str, issuer_key: str, series: int | None, name: str, maturity: str | None, maturity_text: str) -> str:
    identity = "|".join([edinet_code, issuer_key, str(series) if series is not None else compact(name), maturity or compact(maturity_text)])
    return f"{edinet_code}-{hashlib.sha1(identity.encode('utf-8')).hexdigest()[:10]}"


def composite_rating(ratings: list[dict[str, str]]) -> tuple[str | None, str | None, float | None]:
    """The rating used for comparisons: the first of R&I, JCR, S&P, Moody's, Fitch that rated the bond."""
    by_agency = {item.get("agency"): item.get("rating") for item in ratings or []}
    for agency in _AGENCY_PRIORITY:
        notch = rating_notch(by_agency.get(agency))
        if notch is not None:
            return notch_label(notch), agency, float(notch)
    return None, None, None


def _company_info(db2_path: str | None) -> dict[str, dict[str, Any]]:
    if not db2_path:
        return {}
    try:
        conn = sqlite3.connect(f"file:{db2_path}?mode=ro", uri=True)
    except sqlite3.Error:
        return {}
    try:
        rows = conn.execute(
            'SELECT Company_Code, "Submitter Name", Company_Name, Company_Ticker, Company_Industry, Listed FROM CompanyInfo'
        ).fetchall()
    except sqlite3.Error:
        return {}
    finally:
        conn.close()
    return {
        str(code): {"name": name or "", "name_en": name_en or "", "ticker": ticker or "", "industry": industry or "", "listed": 1 if str(listed or "").lower().startswith("listed") else 0}
        for code, name, name_en, ticker, industry, listed in rows
    }


def _yield_terms(bond: dict[str, Any]) -> tuple[str | None, float | None]:
    """The date the yield runs to (first call for callable bonds) and the coupon it earns; JGBs only price yen bonds."""
    if bond.get("coupon_kind") in ("floating",) or bond.get("coupon") is None or (bond.get("currency") or "JPY") != "JPY":
        return None, None
    features = bond.get("features") or []
    if "convertible" in features:
        return None, None
    if bond.get("call_date"):
        return bond["call_date"], bond.get("coupon")
    # A callable bond's coupon prices it to its first call; without that date its yield is unknown.
    if "callable" in features or "deferrable" in features or bond.get("perpetual"):
        return None, None
    # Subordinated bonds with very long maturities are callable hybrids even when the title does not say so.
    tenor = year_fraction(bond.get("issue_date"), bond.get("maturity"))
    if bond.get("seniority") in ("subordinated", "hybrid") and tenor is not None and tenor > 15:
        return None, None
    return bond.get("maturity"), bond.get("coupon")


def add_issue_spread(bond: dict[str, Any], curves: CurveBook) -> None:
    end, coupon = _yield_terms(bond)
    tenor = year_fraction(bond.get("issue_date"), end)
    if tenor is None or tenor <= 0.05 or coupon is None:
        return
    price = bond.get("issue_price") or 100.0
    issue_yield = yield_from_price(coupon, tenor, price, int(bond.get("frequency") or 2))
    curve = curves.on(bond.get("issue_date"))
    government = curve.at(tenor) if curve else None
    bond["issue_tenor"] = round(tenor, 4)
    bond["issue_yield"] = round(issue_yield, 8) if issue_yield is not None else None
    bond["jgb_at_issue"] = round(government, 8) if government is not None else None
    if issue_yield is not None and government is not None:
        bond["issue_spread"] = round(issue_yield - government, 8)


def _match(row: dict[str, Any], issues: list[dict[str, Any]], used: set[tuple[str, int]]) -> dict[str, Any] | None:
    if not row.get("maturity"):
        return None
    candidates = [issue for issue in issues if (issue["doc_id"], issue["seq"]) not in used and issue.get("maturity") == row["maturity"]]
    for issue in candidates:
        same_series = row.get("series") is not None and row.get("series") == issue.get("series")
        same_coupon = row.get("coupon") is not None and issue.get("coupon") is not None and abs(row["coupon"] - issue["coupon"]) < 5e-6
        if same_series or same_coupon:
            used.add((issue["doc_id"], issue["seq"]))
            return issue
    # The schedule left out the series or wrote the coupon differently: one bond maturing that day is enough.
    if len(candidates) == 1 and (row.get("series") is None or candidates[0].get("series") is None):
        used.add((candidates[0]["doc_id"], candidates[0]["seq"]))
        return candidates[0]
    return None


PUBLIC_MINIMUM = 1e9


def likely_private(bond: dict[str, Any]) -> bool:
    """Privately placed: said so, bank-guaranteed, or known only from a schedule and smaller than any public issue.

    Bank-guaranteed bonds and small placements pay the guarantee fee outside the
    coupon, so their spreads are not comparable with public bonds.
    """
    features = bond.get("features") or []
    if "private" in features or "guaranteed" in features or "私募" in (bond.get("offering") or ""):
        return True
    if bond.get("issuance_doc_id"):
        return False
    size = max(bond.get("amount_issued") or 0, bond.get("outstanding") or 0, bond.get("opening") or 0)
    return 0 < size < PUBLIC_MINIMUM


def _status(bond: dict[str, Any], today: str) -> str:
    maturity = bond.get("maturity")
    if maturity and maturity <= today:
        return "matured"
    if bond.get("outstanding") == 0:
        return "redeemed"
    return "outstanding"


def build_bonds(conn: sqlite3.Connection, db2_path: str | None = None, *, today: str | None = None) -> list[dict[str, Any]]:
    """Every bond the stored filings describe, merged; see the module docstring."""
    today = today or date.today().isoformat()
    companies = _company_info(db2_path)
    curves = CurveBook.load(conn)
    filers = {row[0]: row[1] or "" for row in conn.execute("SELECT edinet_code, filer_name FROM Bond_Documents")}

    issues_by_company: dict[str, list[dict[str, Any]]] = defaultdict(list)
    seen: dict[tuple[str, Any, Any, Any, Any], str] = {}
    issue_rows = conn.execute("SELECT * FROM Bond_Issuances ORDER BY submitted_at DESC, doc_id, seq").fetchall()
    for row in issue_rows:
        issue = decode(row)
        # A re-filed supplement repeats a bond; the latest filing wins.
        identity = (issue["edinet_code"], issue.get("series"), issue.get("maturity"), issue.get("coupon"), compact(issue.get("name")))
        if identity in seen:
            continue
        seen[identity] = issue["doc_id"]
        issues_by_company[issue["edinet_code"]].append(issue)

    latest: dict[str, tuple[str, str]] = {}
    for doc_id, edinet_code, period_end in conn.execute(
        "SELECT doc_id, edinet_code, period_end FROM Bond_Documents WHERE kind = 'annual' AND status IN ('parsed', 'empty') "
        "ORDER BY period_end, submitted_at"
    ):
        latest[edinet_code] = (doc_id, period_end or "")
    rows_by_doc: dict[str, list[dict[str, Any]]] = defaultdict(list)
    wanted = [doc_id for doc_id, _ in latest.values()]
    for start in range(0, len(wanted), 500):
        batch = wanted[start:start + 500]
        for row in conn.execute(f"SELECT * FROM Bond_Schedule_Rows WHERE doc_id IN ({', '.join('?' for _ in batch)}) ORDER BY doc_id, seq", batch):
            rows_by_doc[row["doc_id"]].append(decode(row))

    stamp = now()
    bonds: list[dict[str, Any]] = []
    for edinet_code in sorted(set(issues_by_company) | set(latest)):
        info = companies.get(edinet_code, {})
        names = (info.get("name", ""), info.get("name_en", ""), filers.get(edinet_code, ""))
        base = {
            "edinet_code": edinet_code,
            "company_name": info.get("name") or filers.get(edinet_code) or edinet_code,
            "company_name_en": info.get("name_en", ""),
            "ticker": info.get("ticker", ""),
            "industry": info.get("industry", ""),
            "listed": info.get("listed", 0),
            "updated_at": stamp,
        }
        issues = issues_by_company.get(edinet_code, [])
        used: set[tuple[str, int]] = set()
        schedule_doc, period_end = latest.get(edinet_code, (None, ""))
        schedule = rows_by_doc.get(schedule_doc or "", [])
        aggregated = any(not row.get("maturity") and row.get("maturity_text") for row in schedule)
        for row in schedule:
            parent = is_parent_issuer(row.get("issuer", ""), names)
            issue = _match(row, issues, used) if parent else None
            bond = {
                **base,
                "issuer": base["company_name"] if parent else row.get("issuer", ""),
                "is_parent": int(parent),
                "name": row.get("name", ""),
                "series": row.get("series"),
                "currency": row.get("currency") or "JPY",
                "seniority": row.get("seniority") or "senior",
                "features": row.get("features") or [],
                "coupon": row.get("coupon"),
                "coupon_kind": "zero" if row.get("coupon") == 0 else "fixed" if row.get("coupon") is not None else "floating" if is_floating(row.get("coupon_text")) else "unknown",
                "frequency": None,
                "issue_date": row.get("issue_date"),
                "maturity": row.get("maturity"),
                "maturity_text": row.get("maturity_text", ""),
                "perpetual": 0,
                "call_date": None,
                "amount_issued": None,
                "issue_price": None,
                "outstanding": row.get("closing"),
                "opening": row.get("opening"),
                "outstanding_as_of": period_end,
                "current_portion": row.get("current_portion"),
                "collateral": row.get("collateral", ""),
                "offering": "",
                "ratings": [],
                "schedule_doc_id": schedule_doc,
                "schedule_period_end": period_end,
            }
            if issue:
                _apply_issue(bond, issue)
            bonds.append(bond)
        for issue in issues:
            if (issue["doc_id"], issue["seq"]) in used:
                continue
            issued_after = not schedule or (issue.get("issue_date") or issue.get("submitted_at") or "") > period_end
            if issued_after:
                outstanding = issue.get("amount")
            elif aggregated:
                # The schedule groups bonds into rows with a range of maturities, so this one is not shown alone.
                outstanding = None
            else:
                # Issued before the latest schedule's year end yet not listed in it: already redeemed.
                outstanding = 0.0
            bond = {
                **base,
                "issuer": base["company_name"],
                "is_parent": 1,
                "series": issue.get("series"),
                "outstanding": outstanding,
                "outstanding_as_of": (issue.get("issue_date") or "") if issued_after else (None if aggregated else period_end),
                "schedule_period_end": None if issued_after else period_end,
                "current_portion": None,
                "schedule_doc_id": None,
                "maturity_text": issue.get("maturity_text", ""),
            }
            _apply_issue(bond, issue)
            bonds.append(bond)

    _infer_ratings(bonds)
    for bond in bonds:
        bond["private"] = int(likely_private(bond))
        bond["status"] = _status(bond, today)
        issuer_key = "" if bond["is_parent"] else _short_name(bond.get("issuer"))
        bond["bond_id"] = bond_id(bond["edinet_code"], issuer_key, bond.get("series"), bond.get("name", ""), bond.get("maturity"), bond.get("maturity_text", ""))
        add_issue_spread(bond, curves)
    # Two rows can describe the same bond (a schedule that lists it twice); keep the first.
    unique: dict[str, dict[str, Any]] = {}
    for bond in bonds:
        unique.setdefault(bond["bond_id"], bond)
    merged = list(unique.values())
    add_market_quotes(merged, latest_quotes(conn), curves)
    return merged


def add_market_quotes(bonds: list[dict[str, Any]], quotes: list[dict[str, Any]], curves: CurveBook) -> None:
    """JSDA reference prices for the bonds they match, with the yield and spread they imply.

    The yield is recomputed from the reference price on the same conventions as
    the spread at issue (to the first call for callable bonds), so the two
    spreads compare like for like.
    """
    live = [bond for bond in bonds if bond.get("status") == "outstanding"]
    for bond_id, quote in match_quotes(live, quotes).items():
        bond = next(item for item in live if item["bond_id"] == bond_id)
        day = quote["price_date"]
        bond.update({
            "jsda_code": quote["code"],
            "jsda_name": quote["name"],
            "market_date": day,
            "market_price": quote["price"],
            "market_change": quote.get("change"),
            "market_reporters": quote.get("reporters"),
        })
        end, coupon = _yield_terms(bond)
        if end and end <= day:
            end = bond.get("maturity")
        years = year_fraction(day, end)
        if coupon is None or years is None or years <= 0.05:
            continue
        market_yield = yield_from_price(coupon, years, quote["price"], int(bond.get("frequency") or 2))
        curve = curves.on(day)
        government = curve.at(years) if curve else None
        if market_yield is not None:
            bond["market_yield"] = round(market_yield, 8)
            if government is not None:
                bond["market_spread"] = round(market_yield - government, 8)


def _apply_issue(bond: dict[str, Any], issue: dict[str, Any]) -> None:
    rating, agency, notch = composite_rating(issue.get("ratings") or [])
    features = sorted(set(bond.get("features") or []) | set(issue.get("features") or []))
    bond.update({
        "name": issue.get("name") or bond.get("name", ""),
        "currency": issue.get("currency") or "JPY",
        "seniority": issue.get("seniority") or bond.get("seniority") or "senior",
        "features": features,
        "coupon": issue.get("coupon") if issue.get("coupon") is not None else bond.get("coupon"),
        "coupon_kind": issue.get("coupon_kind") or bond.get("coupon_kind"),
        "frequency": issue.get("frequency"),
        "issue_date": issue.get("issue_date") or bond.get("issue_date"),
        "maturity": issue.get("maturity") or bond.get("maturity"),
        "perpetual": int(bool(issue.get("perpetual"))),
        "call_date": issue.get("call_date"),
        "amount_issued": issue.get("amount"),
        "issue_price": issue.get("issue_price"),
        "collateral": issue.get("collateral") or bond.get("collateral", ""),
        "offering": issue.get("offering", ""),
        "ratings": issue.get("ratings") or [],
        "rating": rating,
        "rating_agency": agency,
        "rating_notch": notch,
        "rating_inferred": 0,
        "issuance_doc_id": issue["doc_id"],
        "issuance_submitted_at": issue.get("submitted_at"),
    })


def _rating_class(seniority: str | None) -> str:
    return "junior" if seniority in ("subordinated", "hybrid") else "senior"


def _infer_ratings(bonds: list[dict[str, Any]]) -> None:
    """Give unrated bonds the issuer's latest rating for the same ranking (senior or subordinated)."""
    latest: dict[tuple[str, str], dict[str, Any]] = {}
    for bond in bonds:
        if bond.get("rating_notch") is None or not bond.get("is_parent"):
            continue
        key = (bond["edinet_code"], _rating_class(bond.get("seniority")))
        if key not in latest or (bond.get("issue_date") or "") > (latest[key].get("issue_date") or ""):
            latest[key] = bond
    for bond in bonds:
        if bond.get("rating_notch") is not None or not bond.get("is_parent") or bond.get("seniority") == "convertible":
            continue
        source = latest.get((bond["edinet_code"], _rating_class(bond.get("seniority"))))
        if source is None:
            continue
        bond.update({
            "ratings": source.get("ratings") or [],
            "rating": source.get("rating"),
            "rating_agency": source.get("rating_agency"),
            "rating_notch": source.get("rating_notch"),
            "rating_inferred": 1,
        })


def summary(bonds: list[dict[str, Any]]) -> dict[str, Any]:
    outstanding = [bond for bond in bonds if bond.get("status") == "outstanding"]
    return {
        "bonds": len(bonds),
        "outstanding": len(outstanding),
        "companies": len({bond["edinet_code"] for bond in bonds}),
        "rated": sum(1 for bond in outstanding if bond.get("rating")),
        "with_spread": sum(1 for bond in outstanding if bond.get("issue_spread") is not None),
        "features": json.dumps(sorted({feature for bond in bonds for feature in bond.get("features") or []})),
    }
