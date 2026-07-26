"""Transactional API tests for company tags."""

from __future__ import annotations

import sqlite3

from fastapi import FastAPI
from fastapi.testclient import TestClient

import src.web_app.api.tags as tags_api
from src.auth.models import AuthenticatedUser
from src.research.storage import ResearchStore


def test_tag_crud_uses_managed_database(tmp_path, monkeypatch):
    database = tmp_path / "tags.db"
    monkeypatch.setattr(tags_api, "_research_store", ResearchStore(database))
    app = FastAPI()
    app.include_router(tags_api.router)

    @app.middleware("http")
    async def authenticate(request, call_next):
        request.state.user = AuthenticatedUser("user-a", "tester", None, "member", "active")
        return await call_next(request)

    client = TestClient(app)

    created_tag = client.post("/api/tags", json={"name": "Watchlist"})
    assert created_tag.status_code == 200
    created = client.post("/api/tags/E00001/Watchlist")
    assert created.status_code == 200
    assert created.json()["tag"] == "Watchlist"

    assert client.get("/api/tags/E00001").json() == {
        "tags": ["Watchlist"]
    }
    assert client.get("/api/tags").json() == {
        "tags": [{"name": "Watchlist", "member_count": 1}]
    }
    assert client.get("/api/tags/Watchlist/companies").json() == {
        "tag": "Watchlist", "companies": ["E00001"]
    }

    renamed = client.patch("/api/tags/Watchlist", json={"name": "Core"})
    assert renamed.status_code == 200
    assert client.get("/api/tags/E00001").json() == {"tags": ["Core"]}

    removed = client.delete("/api/tags/E00001/Core")
    assert removed.status_code == 200
    assert client.get("/api/tags/E00001").json() == {"tags": []}
    assert client.delete("/api/tags/Core").status_code == 200


def test_tag_members_endpoint_accepts_legacy_membership_without_definition(tmp_path, monkeypatch):
    database = tmp_path / "tags.db"
    store = ResearchStore(database)
    store.set_company_tags("user-a", "E00001", ["Legacy"])
    with sqlite3.connect(database) as conn:
        conn.execute("DELETE FROM tag_definitions WHERE user_id = ? AND tag = ?", ("user-a", "Legacy"))
        conn.commit()
    monkeypatch.setattr(tags_api, "_research_store", store)
    app = FastAPI()
    app.include_router(tags_api.router)

    @app.middleware("http")
    async def authenticate(request, call_next):
        request.state.user = AuthenticatedUser("user-a", "tester", None, "member", "active")
        return await call_next(request)

    client = TestClient(app)

    assert client.get("/api/tags/Legacy/companies").json() == {
        "tag": "Legacy", "companies": ["E00001"]
    }
