"""Report builder: resolve company, filing, and observation data for frozen reports."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from src.filings.catalog import FilingCatalog
from src.research.storage import ResearchStore

from .manifest import build_manifest, canonical_json


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def resolve_report_data(
    recipe: dict[str, Any],
    user_id: str,
    catalog: FilingCatalog | None = None,
    research: ResearchStore | None = None,
) -> dict[str, Any]:
    """Resolve real data referenced by a report recipe into frozen sections.

    The recipe may include:
    - company_codes: list[str]
    - doc_ids: list[str]
    - watchlist_id: str
    - sections: list[str]  (overview, filings, quality, research)

    Returns a frozen report_data dict ready for serialization.
    """
    company_codes: list[str] = list(dict.fromkeys(recipe.get("company_codes", [])))
    doc_ids: list[str] = list(dict.fromkeys(recipe.get("doc_ids", [])))

    # Resolve watchlist members
    watchlist_id = recipe.get("watchlist_id")
    if watchlist_id and research:
        wl = research.get_watchlist(user_id, watchlist_id)
        if wl:
            items = research.list_watchlist_items(user_id, watchlist_id)
            for item in items:
                code = item["edinet_code"]
                if code not in company_codes:
                    company_codes.append(code)

    # Resolve filings
    filing_summaries: list[dict[str, Any]] = []
    if catalog:
        for doc_id in doc_ids:
            filing = catalog.get_filing(doc_id)
            if filing:
                filing_data = dict(filing)
                filing_data.pop("archive_path", None)
                quality = catalog.list_quality_issues(doc_id)
                filing_data["quality_issue_count"] = len(quality)
                filing_summaries.append(filing_data)

    # Resolve observations
    observations_data: list[dict[str, Any]] = []
    if catalog:
        for company_code in company_codes[:50]:
            for doc_id in doc_ids[:100]:
                facts = catalog.list_facts(doc_id)
                for fact in facts:
                    observations_data.append({
                        "company_code": company_code,
                        "doc_id": doc_id,
                        "concept": fact["concept"],
                        "value_text": fact["value_text"],
                        "numeric_value": fact["numeric_value"],
                        "context_id": fact["context_id"],
                        "unit_id": fact["unit_id"],
                    })

    # Resolve quality summary
    quality_summary: list[dict[str, Any]] = []
    if catalog:
        quality_summary = [dict(r) for r in catalog.quality_summary()]

    # Resolve research notes
    research_notes: list[dict[str, Any]] = []
    if research:
        for company_code in company_codes:
            notes = research.list_notes(user_id, company_code)
            research_notes.extend(notes)

    return {
        "as_of": _now(),
        "company_codes": company_codes,
        "doc_ids": doc_ids,
        "filing_summaries": filing_summaries,
        "observations": observations_data[:10000],
        "quality_summary": quality_summary,
        "research_notes": research_notes,
        "sections": recipe.get("sections", []),
    }


def build_report_sections(
    data: dict[str, Any],
    recipe: dict[str, Any],
) -> dict[str, Any]:
    """Produce section content from resolved report data."""
    sections: dict[str, Any] = {}
    requested = recipe.get("sections", ["overview"])
    seen = set(requested)

    if "overview" in seen:
        sections["overview"] = {
            "title": "Report overview",
            "company_count": len(data.get("company_codes", [])),
            "filing_count": len(data.get("filing_summaries", [])),
            "generated_at": data.get("as_of"),
        }
    if "filings" in seen:
        sections["filings"] = data.get("filing_summaries", [])
    if "observations" in seen:
        sections["observations"] = {
            "count": len(data.get("observations", [])),
            "sample": data.get("observations", [])[:500],
        }
    if "quality" in seen:
        sections["quality"] = data.get("quality_summary", [])
    if "research" in seen:
        sections["research"] = data.get("research_notes", [])

    return sections
