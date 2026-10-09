"""The data folder layout and the one-time migration from the old layout."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from src import paths
from src.orchestrator.common import db_config, migrate_layout
from src.orchestrator.common.migrate_layout import LegacyLayoutError, migrate_legacy_layout
from src.settings import get_setting


@pytest.fixture
def install(tmp_path, monkeypatch):
    """An application folder using the default data folder (no EDINET_DATA_DIR)."""
    monkeypatch.delenv("EDINET_DATA_DIR", raising=False)
    monkeypatch.setattr(paths, "_SOURCE_ROOT", tmp_path)
    db_config.reload()
    yield tmp_path
    db_config.reload()


def _database(path: Path, statements: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.executescript(statements)
    conn.commit()
    conn.close()


def _rows(path: Path, sql: str) -> list[tuple]:
    conn = sqlite3.connect(path)
    try:
        return conn.execute(sql).fetchall()
    finally:
        conn.close()


def _legacy_install(root: Path) -> None:
    state = root / "config" / "state"
    rebuildable = root / "data" / "databases"
    _database(
        state / "databases" / "auth.db",
        "CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL);"
        "CREATE TABLE users (user_id TEXT PRIMARY KEY, username TEXT);"
        "CREATE INDEX idx_users_name ON users(username);"
        "INSERT INTO users VALUES ('u1', 'alice');",
    )
    _database(
        state / "databases" / "research.db",
        "CREATE TABLE watchlists (watchlist_id TEXT PRIMARY KEY, user_id TEXT);"
        "INSERT INTO watchlists VALUES ('w1', 'u1');",
    )
    _database(
        state / "databases" / "pipeline_jobs.db",
        "CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL);"
        "INSERT INTO schema_migrations VALUES (1, 't'), (2, 't'), (3, 't');"
        "CREATE TABLE pipeline_jobs (job_id TEXT PRIMARY KEY);"
        "INSERT INTO pipeline_jobs VALUES ('j1');",
    )
    _database(
        state / "databases" / "Portfolio.db",
        "CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL);"
        "INSERT INTO schema_migrations VALUES (1, 't');"
        "CREATE TABLE Transactions (id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT);"
        "INSERT INTO Transactions(symbol) VALUES ('7203'), ('6758'), ('9984');"
        "DELETE FROM Transactions WHERE symbol = '9984';",
    )
    _database(state / "databases" / "chat.db", "CREATE TABLE messages (id TEXT); INSERT INTO messages VALUES ('m1');")
    _database(
        rebuildable / "Standardized.db",
        "CREATE TABLE CompanyInfo (Company_Code TEXT); INSERT INTO CompanyInfo VALUES ('E02144');",
    )
    _database(
        rebuildable / "Base.db",
        "CREATE TABLE DocumentList (docID TEXT); CREATE INDEX idx_document_list_docid ON DocumentList(docID);"
        "INSERT INTO DocumentList VALUES ('S100A'), ('S100B');",
    )
    _database(rebuildable / "Bonds.db", "CREATE TABLE Bonds (bond_id TEXT); INSERT INTO Bonds VALUES ('b1');")
    _database(rebuildable / "Filings.db", "CREATE TABLE filings (doc_id TEXT); INSERT INTO filings VALUES ('S100A');")
    (state / "secrets").mkdir(parents=True)
    (state / "secrets" / "chat_message_keys.json").write_text(json.dumps({"active": 1, "keys": {"1": "a2V5"}}))
    (state / "screening_history.jsonl").write_text('{"name": "test_run"}\n')
    (state / "jobs" / "job-1" / "uploads").mkdir(parents=True)
    backtest = root / "data" / "Backtests" / "20260101_000000"
    backtest.mkdir(parents=True)
    (backtest / "owner.json").write_text(json.dumps({"owner_user_id": "u1"}))
    (backtest / "meta.json").write_text(json.dumps({"kind": "single", "title": "Toyota", "subtitle": "", "headline": {"cagr": 0.1}}))
    (root / "data" / "certs").mkdir(parents=True)
    (root / "data" / "certs" / "cert.pem").write_text("certificate")
    (root / "config" / "database_paths.json").write_text(
        json.dumps({"db1": "data/databases/Base.db", "db2": "data/databases/Standardized.db"})
    )
    (root / ".env").write_text("# EDINET\nAPI_KEY=your_api_key_here\nEDINET_API_TOKEN='real-key'\n")


def test_every_path_is_in_the_data_folder(install, monkeypatch):
    assert paths.data_dir() == install / "data"
    assert Path(db_config.get_app_db()) == install / "data" / "app.db"
    assert Path(db_config.get_market_db()) == install / "data" / "market.db"
    assert paths.backtests_dir() == install / "data" / "artifacts" / "backtests"

    monkeypatch.setenv("EDINET_DATA_DIR", str(install / "elsewhere"))
    assert Path(db_config.get_chat_db()) == install / "elsewhere" / "chat.db"


def test_storage_settings_move_the_large_databases(install):
    from src.settings import set_setting

    set_setting("storage.filings_db_path", "/disks/big/filings.db")
    set_setting("storage.market_db_path", "market/market.db")
    db_config.reload()

    assert Path(db_config.get_filings_db()) == Path("/disks/big/filings.db")
    assert Path(db_config.get_market_db()) == install / "data" / "market" / "market.db"


def test_lookups_refuse_to_create_databases_beside_an_unmigrated_layout(install):
    _legacy_install(install)

    with pytest.raises(LegacyLayoutError, match="main.py migrate"):
        db_config.get_app_db()
    assert not (install / "data" / "app.db").exists()


def test_dry_run_changes_nothing(install):
    _legacy_install(install)
    reported: list[str] = []

    steps = migrate_legacy_layout(dry_run=True, report=reported.append)

    assert steps and reported[0].startswith("Would migrate")
    assert not (install / "data" / "app.db").exists()
    assert (install / "config" / "state" / "databases" / "auth.db").exists()


def test_migration_reaches_the_new_layout_without_losing_anything(install):
    _legacy_install(install)
    data = install / "data"

    migrate_legacy_layout(report=lambda line: None)

    app_db = data / "app.db"
    assert _rows(app_db, "SELECT user_id, username FROM users") == [("u1", "alice")]
    assert _rows(app_db, "SELECT watchlist_id FROM watchlists") == [("w1",)]
    assert _rows(app_db, "SELECT job_id FROM pipeline_jobs") == [("j1",)]
    assert _rows(app_db, "SELECT symbol FROM Transactions ORDER BY id") == [("7203",), ("6758",)]
    # The AUTOINCREMENT counter still remembers the deleted row.
    assert _rows(app_db, "SELECT seq FROM sqlite_sequence WHERE name = 'Transactions'") == [(3,)]
    assert _rows(app_db, "SELECT name FROM sqlite_master WHERE name = 'idx_users_name'") == [("idx_users_name",)]
    assert set(_rows(app_db, "SELECT component, version FROM schema_migrations")) == {
        ("pipeline_jobs", 1), ("pipeline_jobs", 2), ("pipeline_jobs", 3), ("portfolio", 1),
    }
    assert get_setting("chat.message_keys") == {"active": 1, "keys": {"1": "a2V5"}}
    assert get_setting("edinet.api_key") == "real-key"

    market = data / "market.db"
    assert _rows(market, "SELECT Company_Code FROM CompanyInfo") == [("E02144",)]
    assert _rows(market, "SELECT docID FROM DocumentList ORDER BY docID") == [("S100A",), ("S100B",)]
    assert _rows(market, "SELECT bond_id FROM Bonds") == [("b1",)]
    assert _rows(data / "filings.db", "SELECT doc_id FROM filings") == [("S100A",)]
    assert _rows(data / "chat.db", "SELECT id FROM messages") == [("m1",)]

    assert (data / "artifacts" / "jobs" / "job-1" / "uploads").is_dir()
    backtest = data / "artifacts" / "backtests" / "20260101_000000"
    assert _rows(app_db, "SELECT backtest_id, owner_user_id, kind, title, headline_json FROM saved_backtests") == [
        ("20260101_000000", "u1", "single", "Toyota", '{"cagr": 0.1}'),
    ]
    assert not (backtest / "owner.json").exists() and (backtest / "owner.json.migrated").exists()
    assert (data / "certs" / "cert.pem").read_text() == "certificate"

    # Merged and imported files are kept, renamed; moved ones are gone.
    state = install / "config" / "state"
    for kept in (
        state / "databases" / "auth.db.migrated",
        state / "secrets" / "chat_message_keys.json.migrated",
        state / "screening_history.jsonl.migrated",
        install / "data" / "databases" / "Base.db.migrated",
        install / "config" / "database_paths.json.migrated",
        install / ".env.migrated",
    ):
        assert kept.exists(), kept
    assert not (install / "data" / "databases" / "Standardized.db").exists()
    assert not (install / ".env").exists()

    assert migrate_layout.plan() == []
    assert Path(db_config.get_app_db()) == app_db


def test_an_interrupted_merge_resumes(install, monkeypatch):
    _legacy_install(install)
    retire = migrate_layout._retire
    calls = {"count": 0}

    def crash_after_first_merge(path):
        calls["count"] += 1
        if path.name == "Base.db":
            raise RuntimeError("power cut")
        retire(path)

    monkeypatch.setattr(migrate_layout, "_retire", crash_after_first_merge)
    with pytest.raises(RuntimeError, match="power cut"):
        migrate_legacy_layout(report=lambda line: None)
    monkeypatch.setattr(migrate_layout, "_retire", retire)

    migrate_legacy_layout(report=lambda line: None)

    market = install / "data" / "market.db"
    assert _rows(market, "SELECT docID FROM DocumentList ORDER BY docID") == [("S100A",), ("S100B",)]
    assert _rows(market, "SELECT bond_id FROM Bonds") == [("b1",)]


def test_migration_refuses_a_database_that_is_in_use(install):
    _legacy_install(install)
    source = install / "config" / "state" / "databases" / "research.db"
    reader = sqlite3.connect(source)
    reader.execute("BEGIN")
    reader.execute("SELECT count(*) FROM watchlists").fetchone()
    try:
        with pytest.raises(LegacyLayoutError, match="in use"):
            migrate_legacy_layout(report=lambda line: None)
    finally:
        reader.close()
    assert source.exists()
    assert not (install / "data" / "app.db").exists()


def test_an_overridden_data_folder_never_touches_the_old_layout(install, monkeypatch):
    _legacy_install(install)
    monkeypatch.setenv("EDINET_DATA_DIR", str(install / "scratch"))

    assert migrate_legacy_layout(report=lambda line: None) == []
    assert Path(db_config.get_app_db()) == install / "scratch" / "app.db"
    assert (install / "config" / "state" / "databases" / "auth.db").exists()
