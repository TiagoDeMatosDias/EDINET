"""Read side of the ownership tables for the Analysis and Research pages.

A company's view is its latest annual report: the largest shareholders at the
record date with the change since the report before, and the officers at the
filing date (the slate proposed to the shareholders' meeting, where the report
was filed ahead of it). An entity's view is every company it holds or serves,
across the years stored. Companies are named as Company Analysis names them,
read from ``CompanyInfo`` on every request.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable
from datetime import date
from typing import Any

from .build import CURRENT_YEARS
from .careers import Translate, translate_lines
from .directory import FilerDirectory
from .names import account_label, title_en, title_roles
from .store import career_entries, statistics

EDINET_VIEWER = "https://disclosure2.edinet-fsa.go.jp/WZEK0040.aspx?{doc_id},,,"
KINDS = ("person", "individual", "company", "organization")
# Valuing a company takes Company Analysis's own calculation, tens of milliseconds each: one request values this many.
MARKET_CAP_LIMIT = 200
ENTITY_PAGE = 100
# Ownership by kind of holder, in the order drawn: (key, label, the report's categories it adds up).
_DISTRIBUTION = (
    ("financial_institutions", "Financial institutions", ("financial_institutions",)),
    ("foreign", "Foreign investors", ("foreign_investors", "foreign_individuals")),
    ("other_corporations", "Other corporations", ("other_corporations",)),
    ("individuals", "Individuals and others", ("individuals",)),
    ("securities_firms", "Securities firms", ("securities_firms",)),
    ("government", "National and local governments", ("government",)),
)

MarketCaps = Callable[[list[str]], dict[str, float | None]]


def edinet_link(doc_id: str | None) -> str | None:
    return EDINET_VIEWER.format(doc_id=doc_id) if doc_id else None


def _connect(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _round(value: Any, digits: int = 6) -> float | None:
    return None if value is None else round(float(value), digits)


def age(birth_date: str | None, today: str) -> int | None:
    try:
        born, now = date.fromisoformat(str(birth_date)), date.fromisoformat(today)
    except ValueError:
        return None
    return now.year - born.year - ((now.month, now.day) < (born.month, born.day))


def _updates(conn: sqlite3.Connection) -> dict[str, Any]:
    try:
        rows = conn.execute("SELECT source, updated_at, detail FROM Ownership_Updates").fetchall()
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


def status(path: str) -> dict[str, Any]:
    conn = _connect(path)
    try:
        documents = conn.execute("SELECT COUNT(*), COUNT(DISTINCT edinet_code), MAX(period_end) FROM Ownership_Documents WHERE status = 'parsed'").fetchone()
        entities = conn.execute("SELECT COUNT(*) FROM Ownership_Entities").fetchone()[0]
        return {"reports": documents[0] or 0, "companies": documents[1] or 0, "latest_period": documents[2], "entities": entities or 0, "updates": _updates(conn)}
    finally:
        conn.close()


def _company_fields(directory: FilerDirectory, code: str | None) -> dict[str, Any]:
    company = directory.company(code)
    if company is None:
        return {"edinet_code": code, "company_name": code, "company_name_ja": None, "ticker": "", "industry": "", "listed": False}
    return {
        "edinet_code": code,
        "company_name": company["name"],
        "company_name_ja": company["name_ja"] if company["name_ja"] != company["name"] else None,
        "ticker": company["ticker"],
        "industry": company["industry"],
        "listed": company["listed"],
    }


def _entity_names(row: sqlite3.Row | dict[str, Any], directory: FilerDirectory) -> dict[str, Any]:
    """An entity's names; one that files with EDINET is named as Company Analysis names it today."""
    item = {"entity_id": row["entity_id"], "kind": row["kind"], "name": row["name"], "name_ja": row["name_ja"], "edinet_code": row["edinet_code"]}
    company = directory.company(row["edinet_code"]) if row["kind"] == "company" else None
    if company is not None:
        english = company["name"] != company["name_ja"]
        item.update(
            name=company["name"] if english else row["name"] or company["name"],
            name_ja=company["name_ja"] if english or row["name"] != company["name_ja"] else None,
            ticker=company["ticker"], industry=company["industry"], listed=company["listed"],
        )
    return item


def _reports(conn: sqlite3.Connection, edinet_code: str) -> list[sqlite3.Row]:
    """The company's parsed reports, newest first, one per fiscal period (the last one submitted)."""
    rows = conn.execute(
        "SELECT * FROM Ownership_Documents WHERE edinet_code = ? AND status = 'parsed' ORDER BY period_end DESC, submitted_at DESC",
        (edinet_code,),
    ).fetchall()
    seen: set[str] = set()
    reports = []
    for row in rows:
        if row["period_end"] not in seen:
            seen.add(row["period_end"])
            reports.append(row)
    return reports


def _share_facts(conn: sqlite3.Connection, report: sqlite3.Row) -> dict[str, Any]:
    """Shares issued, the company's own shares, and ownership by kind of holder, from the report's share statistics."""
    values = statistics(report["statistics"])
    slices = {item.get("key"): item for item in values.get("distribution") or [] if isinstance(item, dict)}
    distribution = []
    for key, label, parts in _DISTRIBUTION:
        found = [slices[part] for part in parts if part in slices]
        ratios = [item["ratio"] for item in found if item.get("ratio") is not None]
        holders = [item["holders"] for item in found if item.get("holders") is not None]
        if ratios or holders:
            distribution.append({"key": key, "label": label, "ratio": _round(sum(ratios)) if ratios else None, "holders": sum(holders) if holders else None})
    return {
        "issued_shares": values.get("issued_shares") or _issued(conn, report["doc_id"]),
        "treasury_shares": values.get("treasury_shares"),
        "treasury_ratio": _round(values.get("treasury_ratio")),
        "shareholders": values.get("shareholders"),
        "distribution": distribution,
    }


def _holder_rows(conn: sqlite3.Connection, doc_id: str, basis: str) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT s.*, e.kind, e.name AS entity_name, e.name_ja AS entity_name_ja, e.edinet_code AS holder_code, e.holdings, e.positions "
        "FROM Ownership_Shareholders s LEFT JOIN Ownership_Entities e ON e.entity_id = s.entity_id "
        "WHERE s.doc_id = ? AND s.basis = ? ORDER BY s.rank, s.seq",
        (doc_id, basis),
    ).fetchall()


def _holder(row: sqlite3.Row, directory: FilerDirectory, issuer: str) -> dict[str, Any]:
    names = _entity_names({"entity_id": row["entity_id"], "kind": row["kind"] or "organization", "name": row["entity_name"] or row["name"], "name_ja": row["entity_name_ja"], "edinet_code": row["holder_code"]}, directory)
    account = row["account"] or ""
    return {
        **names,
        "rank": row["rank"],
        "filed_name": row["name"],
        "account": account_label(account) or None,
        "account_ja": account or None,
        "address": row["address"] or None,
        "shares": row["shares"],
        "ratio": _round(row["ratio"]),
        "own_shares": row["holder_code"] == issuer,
        # How many other companies list the holder among their largest shareholders today.
        "other_holdings": max(0, int(row["holdings"] or 0) - 1),
        "positions": int(row["positions"] or 0),
    }


def _shareholders(conn: sqlite3.Connection, directory: FilerDirectory, edinet_code: str, reports: list[sqlite3.Row]) -> dict[str, Any] | None:
    listed = [report for report in reports if report["shareholder_count"]]
    if not listed:
        return None
    report = listed[0]
    rows = [_holder(row, directory, edinet_code) for row in _holder_rows(conn, report["doc_id"], "shares")]
    voting = [_holder(row, directory, edinet_code) for row in _holder_rows(conn, report["doc_id"], "voting")]
    history = conn.execute(
        "SELECT doc_id, period_end, entity_id, rank, ratio, shares FROM Ownership_Shareholders WHERE edinet_code = ? AND basis = 'shares'",
        (edinet_code,),
    ).fetchall()
    documents = {item["doc_id"]: item["period_end"] for item in listed}
    periods = sorted(set(documents.values()))
    by_entity: dict[str, dict[str, tuple[float | None, float | None, int]]] = {}
    for item in history:
        if item["doc_id"] in documents and item["entity_id"]:
            # A holder listed under two accounts in one year is one stake.
            cell = by_entity.setdefault(item["entity_id"], {}).get(item["period_end"])
            ratio = (cell[0] or 0.0) + (item["ratio"] or 0.0) if cell else item["ratio"]
            shares = (cell[1] or 0.0) + (item["shares"] or 0.0) if cell else item["shares"]
            by_entity[item["entity_id"]][item["period_end"]] = (ratio, shares, min(cell[2], item["rank"]) if cell else item["rank"])
    previous = periods[-2] if len(periods) > 1 else None
    seen: set[str] = set()
    for row in rows:
        series = by_entity.get(row["entity_id"] or "", {})
        before = series.get(previous) if previous else None
        first = row["entity_id"] not in seen
        seen.add(row["entity_id"])
        # The change is the holder's whole stake against last year's, shown once for a holder with several rows.
        row["previous_ratio"] = _round(before[0]) if before and first else None
        row["change"] = _round((series[report["period_end"]][0] or 0.0) - (before[0] or 0.0)) if before and first and report["period_end"] in series else None
        row["new"] = bool(previous) and before is None
        row["since"] = _held_since(series, periods)
    names = {row["entity_id"]: row for row in rows}
    past = _past_holders(conn, directory, [entity for entity in by_entity if entity not in names])
    table = [
        {**{key: holder[key] for key in ("entity_id", "kind", "name", "name_ja")}, "ratios": [_round(by_entity[entity].get(period, (None,))[0]) for period in periods]}
        for entity, holder in {**names, **past}.items() if entity in by_entity
    ]
    table.sort(key=lambda item: (item["ratios"][-1] is None, -(item["ratios"][-1] or 0.0), -max(value or 0.0 for value in item["ratios"])))
    facts = _share_facts(conn, report)
    return {
        "doc_id": report["doc_id"],
        "period_end": report["period_end"],
        "as_of": report["shareholders_as_of"] or report["period_end"],
        "edinet_url": edinet_link(report["doc_id"]),
        "top_ratio": _round(report["top_ratio"]) if report["top_ratio"] is not None else _round(sum(row["ratio"] or 0.0 for row in rows)),
        "top_shares": report["top_shares"],
        **facts,
        "rows": rows,
        "voting": voting,
        "history": {"periods": periods, "holders": table},
    }


def _held_since(series: dict[str, Any], periods: list[str]) -> str | None:
    """The first of the unbroken run of years, ending with the latest, in which the holder is listed."""
    since = None
    for period in reversed(periods):
        if period not in series:
            break
        since = period
    return since


def _past_holders(conn: sqlite3.Connection, directory: FilerDirectory, entities: list[str]) -> dict[str, dict[str, Any]]:
    """Names of holders that were listed in earlier years but are not in the latest report."""
    if not entities:
        return {}
    marks = ",".join("?" for _ in entities)
    rows = conn.execute(f"SELECT entity_id, kind, name, name_ja, edinet_code FROM Ownership_Entities WHERE entity_id IN ({marks})", entities).fetchall()
    return {row["entity_id"]: _entity_names(row, directory) for row in rows}


def _officer(row: sqlite3.Row, base: float | None, today: str) -> dict[str, Any]:
    roles = title_roles(row["title"])
    english = title_en(row["title"])
    return {
        "entity_id": row["entity_id"],
        "name": row["entity_name"] or row["name"],
        "name_ja": row["name"] if (row["entity_name"] or row["name"]) != row["name"] else None,
        "title": english or row["title"] or "",
        "title_ja": row["title"] if english and row["title"] else None,
        "representative": roles["representative"],
        "outside": bool(row["outside"]) or roles["outside"],
        "auditor": roles["auditor"] or roles["audit_committee"],
        "birth_date": row["birth_date"],
        "age": age(row["birth_date"], today),
        "shares": row["shares"],
        "other_shares": row["other_shares"],
        "ratio": _round(row["shares"] / base, 8) if row["shares"] is not None and base else None,
        "term_start": row["term_start"],
        "term_end": row["term_end"],
        "term": row["term"] or None,
        "other_positions": max(0, int(row["positions"] or 0) - 1),
        "holdings": int(row["holdings"] or 0),
    }


def _people(conn: sqlite3.Connection, edinet_code: str, reports: list[sqlite3.Row], issued: float | None, today: str) -> dict[str, Any] | None:
    listed = [report for report in reports if report["officer_count"]]
    if not listed:
        return None
    report = listed[0]
    proposed = int(report["proposed"] or 0)
    rows = conn.execute(
        "SELECT o.*, e.name AS entity_name, e.holdings, e.positions FROM Ownership_Officers o LEFT JOIN Ownership_Entities e ON e.entity_id = o.entity_id "
        "WHERE o.doc_id = ? AND o.proposed = ? ORDER BY o.seq",
        (report["doc_id"], proposed),
    ).fetchall()
    # Shares outstanding come from the report's share statistics, or from what the largest holders' total implies.
    base = issued or (report["top_shares"] / report["top_ratio"] if report["top_shares"] and report["top_ratio"] else None)
    officers = [_officer(row, base, today) for row in rows]
    seats = conn.execute(
        "SELECT entity_id, MIN(period_end) FROM Ownership_Officers WHERE edinet_code = ? AND entity_id IS NOT NULL GROUP BY entity_id",
        (edinet_code,),
    ).fetchall()
    first_seen = dict(seats)
    earliest = min((item["period_end"] for item in listed), default=None)
    for officer in officers:
        since = first_seen.get(officer["entity_id"])
        # In the earliest report stored, "since" would only say where the record begins.
        officer["since"] = since if since and since != earliest else None
    departed: list[dict[str, Any]] = []
    if len(listed) > 1:
        before = listed[1]
        current = {officer["entity_id"] for officer in officers}
        gone = conn.execute(
            "SELECT o.*, e.name AS entity_name, e.holdings, e.positions FROM Ownership_Officers o LEFT JOIN Ownership_Entities e ON e.entity_id = o.entity_id "
            "WHERE o.doc_id = ? AND o.proposed = ? ORDER BY o.seq",
            (before["doc_id"], int(before["proposed"] or 0)),
        ).fetchall()
        departed = [
            {key: value for key, value in _officer(row, None, today).items() if key in ("entity_id", "name", "name_ja", "title", "title_ja", "other_positions")}
            for row in gone if row["entity_id"] not in current
        ]
        if proposed:
            # Officers in office when the report was filed who are not on the slate proposed to the meeting.
            leaving = conn.execute(
                "SELECT o.*, e.name AS entity_name, e.holdings, e.positions FROM Ownership_Officers o LEFT JOIN Ownership_Entities e ON e.entity_id = o.entity_id "
                "WHERE o.doc_id = ? AND o.proposed = 0 ORDER BY o.seq",
                (report["doc_id"],),
            ).fetchall()
            known = current | {item["entity_id"] for item in departed}
            departed = [
                *({key: value for key, value in _officer(row, None, today).items() if key in ("entity_id", "name", "name_ja", "title", "title_ja", "other_positions")} for row in leaving if row["entity_id"] not in known),
                *departed,
            ]
    held = [officer["shares"] for officer in officers if officer["shares"] is not None]
    ages = [officer["age"] for officer in officers if officer["age"] is not None]
    total = sum(held) if held else None
    return {
        "doc_id": report["doc_id"],
        "period_end": report["period_end"],
        "as_of": report["officers_as_of"] or str(report["submitted_at"] or "")[:10] or None,
        "edinet_url": edinet_link(report["doc_id"]),
        "proposed": bool(proposed),
        "source": report["officer_source"],
        "men": report["men"],
        "women": report["women"],
        "outside": sum(1 for officer in officers if officer["outside"]),
        "average_age": round(sum(ages) / len(ages), 1) if ages else None,
        "total_shares": total,
        "total_ratio": _round(total / base, 8) if total is not None and base else None,
        "rows": officers,
        "departed": departed,
        "previous_period": listed[1]["period_end"] if len(listed) > 1 else None,
    }


def company_ownership(path: str, edinet_code: str, *, today: str | None = None) -> dict[str, Any]:
    """A company's largest shareholders and officers from its latest annual report."""
    today = today or date.today().isoformat()
    conn = _connect(path)
    try:
        directory = FilerDirectory.load(conn, history=False)
        reports = _reports(conn, edinet_code)
        shareholders = _shareholders(conn, directory, edinet_code, reports)
        people = _people(conn, edinet_code, reports, (shareholders or {}).get("issued_shares"), today)
        newest = conn.execute("SELECT MAX(period_end) FROM Ownership_Documents WHERE status = 'parsed'").fetchone()[0] or ""
        holder = conn.execute("SELECT entity_id, holdings, past_holdings FROM Ownership_Entities WHERE entity_id = ?", (edinet_code,)).fetchone()
    finally:
        conn.close()
    latest = reports[0]["period_end"] if reports else None
    return {
        **_company_fields(directory, edinet_code),
        # The company as a shareholder of others, when another company's report lists it.
        "holder": {"entity_id": holder["entity_id"], "holdings": holder["holdings"], "past_holdings": holder["past_holdings"]} if holder else None,
        "today": today,
        "latest_period": latest,
        # A company that stopped filing keeps its last report; say so rather than show it as current.
        "stale": bool(latest and newest and latest[:4].isdigit() and newest[:4].isdigit() and int(newest[:4]) - int(latest[:4]) > CURRENT_YEARS),
        "reports": len(reports),
        "shareholders": shareholders,
        "people": people,
    }


# --- entities -------------------------------------------------------------------

def _entity_row(row: sqlite3.Row, directory: FilerDirectory, today: str) -> dict[str, Any]:
    item = _entity_names(row, directory)
    item.update(
        birth_date=row["birth_date"], age=age(row["birth_date"], today), address=row["address"], headline=row["headline"],
        holdings=row["holdings"], past_holdings=row["past_holdings"], positions=row["positions"], past_positions=row["past_positions"],
    )
    return item


def entities(path: str, *, query: str = "", kind: str = "", limit: int = ENTITY_PAGE, offset: int = 0, today: str | None = None) -> dict[str, Any]:
    """People and organisations by name, the most connected first."""
    today = today or date.today().isoformat()
    limit = max(1, min(int(limit), 500))
    where, params = ["1 = 1"], []
    for word in query.casefold().split()[:6]:
        where.append("search LIKE ? ESCAPE '\\'")
        params.append("%" + word.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%")
    if kind in KINDS:
        where.append("kind = ?")
        params.append(kind)
    conn = _connect(path)
    try:
        directory = FilerDirectory.load(conn, history=False)
        total = conn.execute(f"SELECT COUNT(*) FROM Ownership_Entities WHERE {' AND '.join(where)}", params).fetchone()[0]
        rows = conn.execute(
            f"SELECT * FROM Ownership_Entities WHERE {' AND '.join(where)} "
            "ORDER BY (holdings + positions) DESC, (past_holdings + past_positions) DESC, name LIMIT ? OFFSET ?",
            (*params, limit, max(0, int(offset))),
        ).fetchall()
        counts = {row[0]: row[1] for row in conn.execute("SELECT kind, COUNT(*) FROM Ownership_Entities GROUP BY kind")}
    finally:
        conn.close()
    return {"total": total, "counts": counts, "entities": [_entity_row(row, directory, today) for row in rows]}


def _holdings(conn: sqlite3.Connection, directory: FilerDirectory, entity: str, *, addresses: bool = False) -> list[dict[str, Any]]:
    """The entity's stake in each company, newest report first within a company; ``addresses`` keeps the address filed."""
    rows = conn.execute(
        "SELECT edinet_code, doc_id, period_end, as_of, rank, account, address, shares, ratio, latest FROM Ownership_Shareholders "
        "WHERE entity_id = ? AND basis = 'shares' ORDER BY edinet_code, period_end, rank",
        (entity,),
    ).fetchall()
    by_company: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        by_company.setdefault(row["edinet_code"], []).append(row)
    holdings = []
    for code, items in by_company.items():
        periods: dict[str, dict[str, Any]] = {}
        for item in items:
            # Several accounts of one holder in a year add up to its stake; the best rank stands for it.
            cell = periods.setdefault(item["period_end"], {"period_end": item["period_end"], "ratio": 0.0, "shares": 0.0, "rank": item["rank"], "rows": []})
            cell["ratio"] += item["ratio"] or 0.0
            cell["shares"] += item["shares"] or 0.0
            cell["rank"] = min(cell["rank"], item["rank"])
            cell["rows"].append(item)
        series = [periods[key] for key in sorted(periods)]
        last = series[-1]
        newest = last["rows"]
        current = any(item["latest"] for item in newest)
        accounts = list(dict.fromkeys(account_label(item["account"]) for item in newest if item["account"]))
        holding = {
            **_company_fields(directory, code),
            "doc_id": newest[0]["doc_id"],
            "period_end": last["period_end"],
            "as_of": newest[0]["as_of"] or last["period_end"],
            "current": current,
            "rank": last["rank"],
            "shares": last["shares"] or None,
            "ratio": _round(last["ratio"]),
            "change": _round(last["ratio"] - series[-2]["ratio"]) if len(series) > 1 else None,
            "first_period": series[0]["period_end"],
            "account": "; ".join(accounts) or None,
            "account_ja": "; ".join(dict.fromkeys(item["account"] for item in newest if item["account"])) or None,
            # The stake in each stored year, oldest first, for a stake still listed.
            "history": [[cell["period_end"], _round(cell["ratio"])] for cell in series] if current else [],
        }
        if addresses:
            holding["address"] = newest[0]["address"] or None
        holdings.append(holding)
    holdings.sort(key=lambda item: (not item["current"], -(item["ratio"] or 0.0)))
    return holdings


def _positions(conn: sqlite3.Connection, directory: FilerDirectory, entity: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows = conn.execute(
        "SELECT o.edinet_code, o.doc_id, o.period_end, o.as_of, o.proposed, o.title, o.shares, o.outside, o.term_start, o.term_end, o.term, o.latest, o.career, "
        "d.top_shares, d.top_ratio, d.statistics FROM Ownership_Officers o LEFT JOIN Ownership_Documents d ON d.doc_id = o.doc_id "
        "WHERE o.entity_id = ? ORDER BY o.edinet_code, o.period_end, o.proposed",
        (entity,),
    ).fetchall()
    by_company: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        by_company.setdefault(row["edinet_code"], []).append(row)
    positions, careers = [], []
    for code, items in by_company.items():
        last = items[-1]
        roles = title_roles(last["title"])
        english = title_en(last["title"])
        base = statistics(last["statistics"]).get("issued_shares") or _issued(conn, last["doc_id"]) or (last["top_shares"] / last["top_ratio"] if last["top_shares"] and last["top_ratio"] else None)
        company = _company_fields(directory, code)
        titles: list[dict[str, Any]] = []
        for item in items:
            label = title_en(item["title"]) or item["title"] or ""
            if not titles or titles[-1]["title"] != label:
                titles.append({"period_end": item["period_end"], "title": label})
        positions.append({
            **company,
            "doc_id": last["doc_id"],
            "period_end": last["period_end"],
            "as_of": last["as_of"],
            "current": any(item["latest"] for item in items),
            "title": english or last["title"] or "",
            "title_ja": last["title"] if english and last["title"] else None,
            "representative": roles["representative"],
            "outside": bool(last["outside"]) or roles["outside"],
            "shares": last["shares"],
            "ratio": _round(last["shares"] / base, 8) if last["shares"] is not None and base else None,
            "term_end": last["term_end"],
            "first_period": items[0]["period_end"],
            "titles": titles,
            "share_history": [{"period_end": item["period_end"], "shares": item["shares"]} for item in items if item["shares"] is not None and not item["proposed"]],
        })
        told = next((item for item in reversed(items) if item["career"]), None)
        if told is not None:
            careers.append({
                **{key: company[key] for key in ("edinet_code", "company_name", "ticker")},
                "doc_id": told["doc_id"], "period_end": told["period_end"], "current": positions[-1]["current"], "entries": career_entries(told["career"]),
            })
    positions.sort(key=lambda item: item["period_end"] or "", reverse=True)
    positions.sort(key=lambda item: not item["current"])
    # The fullest account first: from a company the person still serves, the newest report, the most lines.
    careers.sort(key=lambda item: (item["current"], item["period_end"] or "", len(item["entries"])), reverse=True)
    return positions, careers


def _issued(conn: sqlite3.Connection, doc_id: str) -> float | None:
    try:
        row = conn.execute('SELECT "Number of issued shares as of fiscal year end", "Total number of issued shares" FROM ShareMetrics WHERE docID = ?', (doc_id,)).fetchone()
    except sqlite3.Error:
        return None
    return (row[0] or row[1]) if row else None


def entity_detail(path: str, entity: str, *, today: str | None = None) -> dict[str, Any] | None:
    """One person or organisation: every company it holds or serves, and what the filings say about it."""
    today = today or date.today().isoformat()
    conn = _connect(path)
    try:
        row = conn.execute("SELECT * FROM Ownership_Entities WHERE entity_id = ?", (entity,)).fetchone()
        if row is None:
            return None
        directory = FilerDirectory.load(conn, history=False)
        holdings = _holdings(conn, directory, entity, addresses=row["kind"] == "individual")
        positions, careers = _positions(conn, directory, entity)
        same_name = []
        if row["name_key"]:
            same_name = conn.execute(
                "SELECT * FROM Ownership_Entities WHERE name_key = ? AND entity_id != ? ORDER BY (holdings + positions) DESC LIMIT 20",
                (row["name_key"], entity),
            ).fetchall()
        own = None
        if row["kind"] == "company" and row["edinet_code"]:
            own = conn.execute("SELECT COUNT(*) FROM Ownership_Documents WHERE edinet_code = ? AND status = 'parsed'", (row["edinet_code"],)).fetchone()[0]
    finally:
        conn.close()
    current = [item for item in holdings if item["current"]]
    summary = _entity_row(row, directory, today)
    summary["has_reports"] = bool(own)
    return {
        "today": today,
        "entity": summary,
        "holdings": holdings,
        "positions": positions,
        "careers": careers,
        "same_name": [_entity_row(item, directory, today) for item in same_name],
        "totals": {
            "holdings": len(current),
            "past_holdings": len(holdings) - len(current),
            "positions": sum(1 for item in positions if item["current"]),
            "past_positions": sum(1 for item in positions if not item["current"]),
            # Shareholder lists give an address with each name: several for one name may mean several people.
            "addresses": len({item["address"] for item in holdings if item.get("address")}),
        },
    }


def market_caps(path: str, codes: list[str], lookup: MarketCaps) -> dict[str, float | None]:
    """Market capitalisation of each listed company among ``codes``, for valuing stakes; unlisted ones are left out."""
    wanted = list(dict.fromkeys(code for code in codes if code))[:MARKET_CAP_LIMIT]
    if not wanted:
        return {}
    conn = _connect(path)
    try:
        directory = FilerDirectory.load(conn, history=False)
    finally:
        conn.close()
    listed = [code for code in wanted if (directory.company(code) or {}).get("ticker")]
    return lookup(listed) if listed else {}


def career_english(path: str, lines: list[str], *, company: str = "", on: str = "", translate: Translate | None = None) -> dict[str, str]:
    """English for the lines of a career summary told by ``company``'s report, by the line as filed."""
    conn = _connect(path)
    try:
        directory = FilerDirectory.load(conn)
    finally:
        conn.close()
    own = (directory.company(company) or {}).get("name") or "the Company"
    return translate_lines(lines, directory, own=str(own), on=on, translate=translate)
