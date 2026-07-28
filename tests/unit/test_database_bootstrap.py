"""Tests for clean-startup database initialization."""

from __future__ import annotations

import sqlite3

from src.orchestrator.common.database_bootstrap import ensure_application_databases


def test_ensure_application_databases_creates_all_configured_databases(tmp_path):
    paths = {
        name: tmp_path / "nested" / f"{name}.db"
        for name in (
            "db1",
            "db2",
            "db3",
            "auth",
            "research",
            "pipeline_jobs",
            "filings",
        )
    }

    result = ensure_application_databases(
        db1_path=paths["db1"],
        db2_path=paths["db2"],
        db3_path=paths["db3"],
        auth_db_path=paths["auth"],
        research_db_path=paths["research"],
        jobs_db_path=paths["pipeline_jobs"],
        filings_db_path=paths["filings"],
    )

    assert set(result) == set(paths)
    assert all(path.exists() and path.stat().st_size > 0 for path in paths.values())

    with sqlite3.connect(paths["db1"]) as conn:
        assert conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' LIMIT 1"
        ).fetchone() is None

    with sqlite3.connect(paths["db2"]) as conn:
        assert conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' LIMIT 1"
        ).fetchone() is None

    with sqlite3.connect(paths["db3"]) as conn:
        assert conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'Transactions'"
        ).fetchone() is not None

    expected_tables = {
        "auth": "users",
        "research": "watchlists",
        "pipeline_jobs": "pipeline_jobs",
        "filings": "filings",
    }
    for database_name, table_name in expected_tables.items():
        with sqlite3.connect(paths[database_name]) as conn:
            assert conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
                (table_name,),
            ).fetchone() is not None

    with sqlite3.connect(paths["db1"]) as conn:
        conn.execute("CREATE TABLE sentinel (value TEXT NOT NULL)")
        conn.execute("INSERT INTO sentinel(value) VALUES ('preserve-me')")
        conn.commit()

    ensure_application_databases(
        db1_path=paths["db1"],
        db2_path=paths["db2"],
        db3_path=paths["db3"],
        auth_db_path=paths["auth"],
        research_db_path=paths["research"],
        jobs_db_path=paths["pipeline_jobs"],
        filings_db_path=paths["filings"],
    )

    with sqlite3.connect(paths["db1"]) as conn:
        assert conn.execute("SELECT value FROM sentinel").fetchone() == ("preserve-me",)
