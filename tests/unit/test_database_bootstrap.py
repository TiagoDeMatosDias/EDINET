"""Tests for clean-startup database initialization."""

from __future__ import annotations

import sqlite3

from src.orchestrator.common.database_bootstrap import ensure_application_databases


def _tables(path) -> set[str]:
    with sqlite3.connect(path) as conn:
        return {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}


def test_ensure_application_databases_creates_the_four_databases(tmp_path):
    paths = {name: tmp_path / "nested" / f"{name}.db" for name in ("app", "chat", "market", "filings")}

    result = ensure_application_databases(
        app_db_path=paths["app"],
        chat_db_path=paths["chat"],
        market_db_path=paths["market"],
        filings_db_path=paths["filings"],
    )

    assert result == paths
    # app.db holds settings, accounts, research, pipeline jobs, and portfolio.
    assert {"settings", "users", "watchlists", "pipeline_jobs", "Transactions", "schema_migrations"} <= _tables(paths["app"])
    with sqlite3.connect(paths["app"]) as conn:
        components = {row[0] for row in conn.execute("SELECT DISTINCT component FROM schema_migrations")}
    assert components == {"pipeline_jobs", "portfolio"}
    assert "messages" in _tables(paths["chat"])
    assert "filings" in _tables(paths["filings"])
    # Market tables come from the pipeline; only the bond tables are fixed.
    assert "Bonds" in _tables(paths["market"])

    with sqlite3.connect(paths["market"]) as conn:
        conn.execute("CREATE TABLE sentinel (value TEXT NOT NULL)")
        conn.execute("INSERT INTO sentinel(value) VALUES ('preserve-me')")

    ensure_application_databases(
        app_db_path=paths["app"],
        chat_db_path=paths["chat"],
        market_db_path=paths["market"],
        filings_db_path=paths["filings"],
    )

    with sqlite3.connect(paths["market"]) as conn:
        assert conn.execute("SELECT value FROM sentinel").fetchone() == ("preserve-me",)
