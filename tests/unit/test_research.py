"""Owner isolation tests for research state."""

from __future__ import annotations

from src.research.storage import ResearchStore


def test_watchlists_and_notes_are_owner_scoped(tmp_path):
    store = ResearchStore(tmp_path / "research.db")
    first = store.create_watchlist("user-a", "Core holdings")
    store.add_watchlist_item("user-a", first["watchlist_id"], "E00001", "Example Co")
    store.create_note("user-a", "Thesis", "Watch margins", "E00001")
    store.create_note("user-b", "Private", "Do not leak")

    assert store.list_watchlists("user-a")[0]["item_count"] == 1
    assert store.list_watchlists("user-b") == []
    assert [note["title"] for note in store.list_notes("user-a")] == ["Thesis"]
    assert [note["title"] for note in store.list_notes("user-b")] == ["Private"]


def test_company_research_thesis_and_targets(tmp_path):
    store = ResearchStore(tmp_path / "research.db")
    result = store.upsert_company_research("user-a", "E00001", thesis_status="buy", target_value=5000, target_currency="JPY")
    assert result["thesis_status"] == "buy"
    assert result["target_value"] == 5000
    updated = store.upsert_company_research("user-a", "E00001", thesis_status="hold")
    assert updated["thesis_status"] == "hold"
    assert updated["version"] == 2


def test_note_versioning_and_optimistic_conflict(tmp_path):
    store = ResearchStore(tmp_path / "research.db")
    note = store.create_note("user-a", "Thesis", "Initial body")
    ok, v1 = store.update_note("user-a", note["note_id"], "Updated", "Body v2", None, expected_version=None)
    assert ok
    assert v1 == 2
    ok, conflict_version = store.update_note("user-a", note["note_id"], "Stale", "Body v3", None, expected_version=1)
    assert not ok
    assert conflict_version == 2


def test_company_tags_are_owner_scoped(tmp_path):
    store = ResearchStore(tmp_path / "research.db")
    store.set_company_tags("user-a", "E00001", ["growth", "dividend"])
    assert len(store.list_company_tags("user-a", "E00001")) == 2
    assert len(store.list_company_tags("user-b", "E00001")) == 0


def test_watchlist_member_reorder(tmp_path):
    store = ResearchStore(tmp_path / "research.db")
    wl = store.create_watchlist("user-a", "Test")
    store.add_watchlist_item("user-a", wl["watchlist_id"], "E1", "One")
    store.add_watchlist_item("user-a", wl["watchlist_id"], "E2", "Two")
    store.reorder_watchlist_items("user-a", wl["watchlist_id"], ["E2", "E1"])
    items = store.list_watchlist_items("user-a", wl["watchlist_id"])
    assert [i["edinet_code"] for i in items] == ["E2", "E1"]
