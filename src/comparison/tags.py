"""The user's tags as company sets a comparison can load."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from src.research.storage import ResearchStore


def tag_sets(
    tag_names: list[str],
    memberships: list[dict[str, Any]],
    companies: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    """Each tag with its members that have financial data, in the order they were tagged.

    Research also tags holdings without EDINET filings (a US share, an ETF) by
    their portfolio symbol. A comparison cannot load those, so ``member_count``
    counts every member while ``companies`` lists only the ones in ``companies``.
    """
    members: dict[str, list[dict[str, Any]]] = {name: [] for name in tag_names}
    for row in sorted(memberships, key=lambda row: (str(row.get("created_at") or ""), str(row["edinet_code"]))):
        members.setdefault(str(row["tag"]), []).append(row)
    sets: list[dict[str, Any]] = []
    for name in sorted(members, key=lambda name: (name.casefold(), name)):
        codes = list(dict.fromkeys(str(row["edinet_code"]) for row in members[name]))
        sets.append({
            "name": name,
            "member_count": len(codes),
            "companies": [
                {
                    "company_code": code,
                    "company_name": companies[code].get("company_name") or code,
                    "ticker": companies[code].get("ticker") or "",
                    "industry": companies[code].get("industry") or "",
                }
                for code in codes
                if code in companies
            ],
        })
    return sets


def find_tag_sets(db: str, store: ResearchStore, user_id: str) -> list[dict[str, Any]]:
    """The user's tags from the research store, with members named from the market database."""
    from src.security_analysis.security_analysis import _get_cached_company_frame

    memberships = store.list_all_company_tags(user_id)
    tag_names = list(dict.fromkeys(str(row["tag"]) for row in store.list_all_tags(user_id)))
    tagged = {str(row["edinet_code"]) for row in memberships}
    frame = _get_cached_company_frame(db)
    frame = frame[frame["company_code"].astype(str).isin(tagged)]
    companies = {str(record["company_code"]): record for record in frame.fillna("").to_dict(orient="records")}
    return tag_sets(tag_names, memberships, companies)
