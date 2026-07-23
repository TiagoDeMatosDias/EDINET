"""Transactional API tests for company tags."""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

import src.web_app.api.tags as tags_api


def test_tag_crud_uses_managed_database(tmp_path, monkeypatch):
    database = tmp_path / "tags.db"
    monkeypatch.setattr(tags_api, "get_db2", lambda: str(database))
    app = FastAPI()
    app.include_router(tags_api.router)
    client = TestClient(app)

    created = client.post("/api/tags/E00001/Watchlist")
    assert created.status_code == 200
    assert created.json()["tag"] == "Watchlist"

    assert client.get("/api/tags/E00001").json() == {
        "tags": ["Watchlist"]
    }
    assert client.get("/api/tags").json() == {
        "tags": [{"name": "Watchlist", "member_count": 1}]
    }

    removed = client.delete("/api/tags/E00001/Watchlist")
    assert removed.status_code == 200
    assert client.get("/api/tags/E00001").json() == {"tags": []}
