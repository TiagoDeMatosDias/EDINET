from __future__ import annotations

import sqlite3
from contextlib import closing

from fastapi import FastAPI
from fastapi.testclient import TestClient

import src.research.api as research_api
import src.research.positions as positions_module
import src.web_app.api.tags as tags_api
from src.auth.models import AuthenticatedUser
from src.portfolio.schema import create_tables
from src.research.book import build_book
from src.research.positions import (
    CLOSED_TAG,
    OPEN_TAG,
    portfolio_positions,
    sync_from_portfolio,
    sync_position_tags,
)
from src.research.storage import ResearchStore


def _portfolio(path, held_now, held_before, user="u"):
    create_tables(str(path))
    with closing(sqlite3.connect(path)) as conn, conn:
        for symbol, category, quantity in held_now:
            conn.execute(
                "INSERT INTO Portfolio_Holdings(symbol, asset_category, owner_user_id, quantity, currency) VALUES (?, ?, ?, ?, 'JPY')",
                (symbol, category, user, quantity),
            )
        for symbol, category in held_now_and_before(held_now, held_before):
            conn.execute(
                "INSERT INTO Holdings_History(date, symbol, asset_category, owner_user_id, quantity) VALUES ('2025-01-06', ?, ?, ?, 10)",
                (symbol, category, user),
            )
        conn.execute(
            "INSERT INTO Transactions(transaction_id, owner_user_id, activity_type, asset_category, symbol, description, currency, trade_date) "
            "VALUES ('t1', ?, 'TRADE', 'STK', 'MO', 'ALTRIA GROUP INC', 'USD', '2024-01-02')",
            (user,),
        )
        conn.execute(
            "INSERT INTO Transactions(transaction_id, owner_user_id, activity_type, asset_category, symbol, description, currency, trade_date) "
            "VALUES ('t2', ?, 'DIVIDEND', 'STK', 'MO', 'MO(US02209S1033) CASH DIVIDEND USD 1.06 PER SHARE', 'USD', '2025-04-30')",
            (user,),
        )


def held_now_and_before(held_now, held_before):
    return [(symbol, category) for symbol, category, _ in held_now] + list(held_before)


def _market(path):
    with closing(sqlite3.connect(path)) as conn, conn:
        conn.execute("CREATE TABLE CompanyInfo (Company_Code TEXT, Company_Ticker TEXT, Company_Name TEXT)")
        conn.executemany("INSERT INTO CompanyInfo VALUES (?, ?, ?)", [("E02144", "72030", "Toyota"), ("E01777", "67580", "Sony")])


def test_positions_map_tokyo_listings_to_companies_and_key_others_by_symbol(tmp_path):
    db3, db2 = tmp_path / "Portfolio.db", tmp_path / "market.db"
    _portfolio(db3, [("7203.T", "STK", 100), ("MO", "STK", 20), ("SPY 260619C00500000", "OPT", 1), ("CASH JPY", "CASH", 5)], [("6758.T", "STK"), ("INTC", "STK")])
    _market(db2)

    positions = {item["code"]: item for item in portfolio_positions(str(db3), str(db2), "u")}

    assert set(positions) == {"E02144", "MO", "E01777", "INTC"}
    assert positions["E02144"]["is_open"] and positions["E02144"]["symbols"] == ["7203.T"]
    assert positions["MO"]["is_open"] and positions["MO"]["name"] == "ALTRIA GROUP INC"
    assert not positions["E01777"]["is_open"] and positions["E01777"]["listed_in_japan"]
    assert not positions["INTC"]["is_open"] and not positions["INTC"]["listed_in_japan"]
    assert portfolio_positions(str(db3), str(db2), "someone-else") == []


def test_position_tags_move_with_the_position(tmp_path):
    store = ResearchStore(tmp_path / "research.db")
    store.set_company_tags("u", "E02144", ["Autos"])

    first = sync_position_tags(store, "u", [
        {"code": "E02144", "is_open": True},
        {"code": "MO", "is_open": True},
        {"code": "INTC", "is_open": False},
    ])
    assert first["newly_open"] == ["E02144", "MO"] and first["newly_closed"] == ["INTC"]
    assert {row["tag"] for row in store.list_company_tags("u", "E02144")} == {"Autos", OPEN_TAG}

    # Toyota is sold: it moves from open to closed and keeps its own tags.
    second = sync_position_tags(store, "u", [
        {"code": "E02144", "is_open": False},
        {"code": "MO", "is_open": True},
        {"code": "INTC", "is_open": False},
    ])
    assert second["newly_closed"] == ["E02144"] and second["newly_open"] == []
    assert {row["tag"] for row in store.list_company_tags("u", "E02144")} == {"Autos", CLOSED_TAG}

    # With no positions left both tags disappear rather than linger empty.
    sync_position_tags(store, "u", [])
    assert {row["tag"] for row in store.list_all_tags("u")} == {"Autos"}


def test_an_unreadable_portfolio_leaves_the_tags_alone(tmp_path, monkeypatch):
    store = ResearchStore(tmp_path / "research.db")
    store.replace_tag_members("u", OPEN_TAG, {"E02144"})

    def broken(*_args):
        raise sqlite3.OperationalError("unable to open database file")

    monkeypatch.setattr(positions_module, "portfolio_positions", broken)
    assert sync_from_portfolio(store, "u") is None
    assert [row["edinet_code"] for row in store.list_tag_companies("u", OPEN_TAG)] == ["E02144"]


def test_the_book_shows_positions_and_names_holdings_without_filings(tmp_path):
    store = ResearchStore(tmp_path / "research.db")
    sync_position_tags(store, "u", [{"code": "E02144", "is_open": True}, {"code": "INTC", "is_open": False}])
    book = build_book(store, "u", None, {"INTC": {"code": "INTC", "name": "INTEL CORP", "symbols": ["INTC"]}})
    rows = {row["company_code"]: row for row in book["companies"]}
    assert rows["E02144"]["position"] == "open" and rows["E02144"]["kind"] == "company"
    assert rows["INTC"]["position"] == "closed" and rows["INTC"]["kind"] == "security"
    assert rows["INTC"]["company_name"] == "INTEL CORP"


def _client(module, store, monkeypatch):
    monkeypatch.setattr(module, "store" if module is research_api else "_research_store", store)
    app = FastAPI()
    app.include_router(module.router)

    @app.middleware("http")
    async def authenticate(request, call_next):
        request.state.user = AuthenticatedUser("u", "tester", None, "member", "active")
        return await call_next(request)

    return TestClient(app)


def test_position_tags_cannot_be_edited_by_hand(tmp_path, monkeypatch):
    store = ResearchStore(tmp_path / "research.db")
    sync_position_tags(store, "u", [{"code": "E02144", "is_open": True}])

    research = _client(research_api, store, monkeypatch)
    # Replacing a company's tags keeps its position tag and ignores a forged one.
    saved = research.put("/api/research/tags/E02144", json={"tags": ["Autos", CLOSED_TAG]}).json()["tags"]
    assert {row["tag"] for row in saved} == {"Autos", OPEN_TAG}

    tags = _client(tags_api, store, monkeypatch)
    assert tags.patch(f"/api/tags/{OPEN_TAG}", json={"name": "Mine"}).status_code == 409
    assert tags.delete(f"/api/tags/{OPEN_TAG}").status_code == 409
    assert tags.delete(f"/api/tags/E02144/{OPEN_TAG}").status_code == 409
    assert tags.post(f"/api/tags/E01777/{CLOSED_TAG}").status_code == 409
    assert tags.post("/api/tags/E01777/Watch").status_code == 200


def test_loading_the_book_re_tags_from_the_portfolio(tmp_path, monkeypatch):
    store = ResearchStore(tmp_path / "research.db")
    monkeypatch.setattr(research_api, "_market_db", lambda: None)
    monkeypatch.setattr(research_api, "sync_from_portfolio", lambda _store, user: {
        **sync_position_tags(_store, user, [{"code": "MO", "is_open": True, "name": "ALTRIA GROUP INC", "listed_in_japan": False, "symbols": ["MO"]}]),
        "positions": [{"code": "MO", "is_open": True, "name": "ALTRIA GROUP INC", "listed_in_japan": False, "symbols": ["MO"]}],
    })
    client = _client(research_api, store, monkeypatch)

    book = client.get("/api/research/book").json()
    assert book["positions"] == {"open": 1, "closed": 0}
    assert book["companies"][0]["company_name"] == "ALTRIA GROUP INC"
    assert book["companies"][0]["position"] == "open"
    assert book["position_tags"] == {"open": OPEN_TAG, "closed": CLOSED_TAG}


def test_pricing_looks_up_a_holding_without_filings_by_ticker(monkeypatch):
    import src.security_analysis as security_analysis
    from src.research.pricing import pricing_inputs

    lookups = []

    def overview(_db, **kwargs):
        lookups.append(kwargs)
        raise ValueError("stop here")

    monkeypatch.setattr(security_analysis, "get_security_overview", overview)
    assert pricing_inputs("market.db", "MO") is None
    assert pricing_inputs("market.db", "E02144") is None
    assert lookups == [{"ticker": "MO"}, {"company_code": "E02144"}]
