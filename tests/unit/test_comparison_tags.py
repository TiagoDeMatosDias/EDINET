from __future__ import annotations

import pandas as pd
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.auth.models import AuthenticatedUser
from src.comparison.tags import tag_sets
from src.research.storage import ResearchStore

COMPANIES = {
    "E1": {"company_code": "E1", "company_name": "Alpha Trading", "ticker": "1000", "industry": "Wholesale Trade"},
    "E2": {"company_code": "E2", "company_name": "Beta Trading", "ticker": "", "industry": ""},
    "E3": {"company_code": "E3", "company_name": "", "ticker": "3000", "industry": "Wholesale Trade"},
}


def _row(code: str, tag: str, created_at: str) -> dict:
    return {"edinet_code": code, "tag": tag, "created_at": created_at}


def test_tag_sets_list_comparable_members_in_the_order_they_were_tagged():
    memberships = [
        _row("E1", "Trading houses", "2026-02-01"),
        _row("E3", "Trading houses", "2026-01-01"),
        _row("E2", "Trading houses", "2026-01-01"),
        _row("E1", "banks", "2026-03-01"),
    ]

    sets = tag_sets(["Trading houses", "banks"], memberships, COMPANIES)

    # Names sort without regard to case.
    assert [item["name"] for item in sets] == ["banks", "Trading houses"]
    trading = sets[1]
    assert [company["company_code"] for company in trading["companies"]] == ["E2", "E3", "E1"]
    assert trading["companies"][0] == {"company_code": "E2", "company_name": "Beta Trading", "ticker": "", "industry": ""}
    # A company without a name is shown by its code.
    assert trading["companies"][1]["company_name"] == "E3"


def test_tag_sets_count_every_member_but_offer_only_companies_with_data():
    memberships = [
        _row("E1", "Open position", "2026-01-01"),
        _row("MO", "Open position", "2026-01-02"),
        _row("VWRA", "Closed position", "2026-01-03"),
    ]

    sets = tag_sets(["Closed position", "Open position", "Empty"], memberships, COMPANIES)

    by_name = {item["name"]: item for item in sets}
    assert by_name["Open position"]["member_count"] == 2
    assert [company["company_code"] for company in by_name["Open position"]["companies"]] == ["E1"]
    assert by_name["Closed position"] == {"name": "Closed position", "member_count": 1, "companies": []}
    # A tag created without members is still listed.
    assert by_name["Empty"] == {"name": "Empty", "member_count": 0, "companies": []}


def test_tag_sets_include_a_membership_whose_tag_has_no_definition():
    sets = tag_sets([], [_row("E1", "Legacy", "2026-01-01")], COMPANIES)

    assert [(item["name"], item["member_count"]) for item in sets] == [("Legacy", 1)]


def test_tags_endpoint_returns_the_signed_in_users_tags(tmp_path, monkeypatch):
    import src.comparison.api as comparison_api
    import src.research.runtime as research_runtime
    import src.security_analysis.security_analysis as security_analysis

    store = ResearchStore(tmp_path / "app.db")
    store.set_company_tags("user-a", "E1", ["Trading houses"])
    store.set_company_tags("user-a", "MO", ["Trading houses"])
    store.set_company_tags("user-b", "E2", ["Someone else's"])
    monkeypatch.setattr(research_runtime, "store", store)
    monkeypatch.setattr(comparison_api, "_resolve_db", lambda: "fixture.db")
    monkeypatch.setattr(security_analysis, "_get_cached_company_frame", lambda _db: pd.DataFrame(COMPANIES.values()))
    app = FastAPI()
    app.include_router(comparison_api.router)
    signed_in = {"user": AuthenticatedUser("user-a", "tester", None, "member", "active")}

    @app.middleware("http")
    async def authenticate(request, call_next):
        request.state.user = signed_in["user"]
        return await call_next(request)

    client = TestClient(app)

    assert client.get("/api/comparison/tags").json() == {"tags": [{
        "name": "Trading houses",
        "member_count": 2,
        "companies": [{"company_code": "E1", "company_name": "Alpha Trading", "ticker": "1000", "industry": "Wholesale Trade"}],
    }]}

    signed_in["user"] = None
    assert client.get("/api/comparison/tags").json() == {"tags": []}
