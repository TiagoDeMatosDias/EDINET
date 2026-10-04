"""Irreplaceable databases live in the state directory, apart from rebuildable data."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from src.orchestrator.common import db_config
from src.orchestrator.common.migrate_state_databases import main as migrate
from src.orchestrator.common.migrate_state_databases import pending_moves


@pytest.fixture
def project(tmp_path, monkeypatch):
    """A project root with no database entries in database_paths.json."""
    monkeypatch.setattr(db_config, "_PROJECT_ROOT", str(tmp_path))
    monkeypatch.setattr(db_config, "_cache", {})
    monkeypatch.delenv("EDINET_STATE_DIR", raising=False)
    return tmp_path


def _database_with_rows(path: Path, rows: int = 3) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("CREATE TABLE items (value INTEGER)")
    conn.executemany("INSERT INTO items VALUES (?)", [(n,) for n in range(rows)])
    conn.commit()
    conn.close()


def _rows(path: Path) -> int:
    conn = sqlite3.connect(path)
    try:
        return conn.execute("SELECT count(*) FROM items").fetchone()[0]
    finally:
        conn.close()


def test_rebuildable_and_state_databases_default_to_separate_directories(project):
    assert Path(db_config.get_db2()) == project / "data" / "databases" / "Standardized.db"
    assert Path(db_config.get_filings_db()) == project / "data" / "databases" / "Filings.db"
    for getter, name in (
        (db_config.get_auth_db, "auth.db"),
        (db_config.get_research_db, "research.db"),
        (db_config.get_db3, "Portfolio.db"),
        (db_config.get_pipeline_jobs_db, "pipeline_jobs.db"),
        (db_config.get_chat_db, "chat.db"),
    ):
        assert Path(getter()) == project / "config" / "state" / "databases" / name


def test_state_directory_override_moves_state_databases(project, monkeypatch, tmp_path):
    monkeypatch.setenv("EDINET_STATE_DIR", str(tmp_path / "elsewhere"))

    assert Path(db_config.get_auth_db()) == tmp_path / "elsewhere" / "databases" / "auth.db"


def test_database_left_at_old_location_is_never_replaced_by_an_empty_one(project):
    _database_with_rows(project / "data" / "databases" / "auth.db")

    with pytest.raises(db_config.StateDatabaseMigrationRequired, match="migrate_state_databases"):
        db_config.get_auth_db()
    assert not (project / "config" / "state" / "databases" / "auth.db").exists()


def test_explicit_configuration_keeps_a_database_where_it_is(project, monkeypatch):
    legacy = project / "data" / "databases" / "auth.db"
    _database_with_rows(legacy)
    monkeypatch.setattr(db_config, "_cache", {"auth_db": "data/databases/auth.db"})

    assert Path(db_config.get_auth_db()) == legacy
    assert pending_moves() == []


def test_migration_moves_state_databases_with_their_contents(project, capsys):
    legacy = project / "data" / "databases"
    _database_with_rows(legacy / "auth.db", rows=5)
    _database_with_rows(legacy / "Portfolio.db", rows=2)
    _database_with_rows(legacy / "Standardized.db")

    assert migrate(["--dry-run"]) == 0
    assert (legacy / "auth.db").exists()

    assert migrate([]) == 0

    state = project / "config" / "state" / "databases"
    assert _rows(state / "auth.db") == 5
    assert _rows(state / "Portfolio.db") == 2
    assert not (legacy / "auth.db").exists()
    assert (legacy / "Standardized.db").exists(), "rebuildable databases stay put"
    assert Path(db_config.get_auth_db()) == state / "auth.db"
    assert migrate([]) == 0
    assert "Nothing to move" in capsys.readouterr().out


def test_migration_refuses_a_database_that_is_in_use(project):
    source = project / "data" / "databases" / "research.db"
    _database_with_rows(source)
    reader = sqlite3.connect(source)
    reader.execute("BEGIN")
    reader.execute("SELECT count(*) FROM items").fetchone()
    try:
        assert migrate([]) == 1
    finally:
        reader.close()
    assert source.exists()
    assert not (project / "config" / "state" / "databases" / "research.db").exists()
