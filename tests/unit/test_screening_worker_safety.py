"""Screens run in workers beside pipelines: no shared-database writes once prepared."""

from __future__ import annotations

import sqlite3

import pytest

from src.orchestrator.common.sqlite import (
    clear_protected_database_paths,
    connect_read,
    protect_database_paths,
)
from src.screening.screening import (
    _screening_database_ready,
    ensure_screening_database,
    install_company_tags,
)


@pytest.fixture
def market_db(tmp_path):
    path = tmp_path / "market.db"
    with sqlite3.connect(path) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("CREATE TABLE FinancialStatements (docID TEXT PRIMARY KEY, edinetCode TEXT, periodEnd TEXT)")
        conn.execute("CREATE TABLE Stock_Prices (Ticker TEXT, Date TEXT, Price REAL)")
        conn.execute("INSERT INTO Stock_Prices VALUES ('7203', '2026-01-05', 100.0)")
    yield str(path)
    clear_protected_database_paths()


def test_a_prepared_database_is_not_written_again(market_db):
    assert not _screening_database_ready(market_db)
    ensure_screening_database(market_db)
    assert _screening_database_ready(market_db)

    # Once prepared, a screen needs no write: it works with writes forbidden.
    protect_database_paths([market_db])
    ensure_screening_database(market_db)


def test_an_unprepared_database_cannot_be_prepared_read_only(market_db):
    protect_database_paths([market_db])
    with pytest.raises(sqlite3.OperationalError):
        ensure_screening_database(market_db)


def test_company_tags_shadow_the_shared_table_without_writing_it(market_db):
    ensure_screening_database(market_db)
    with sqlite3.connect(market_db) as conn:
        conn.execute("INSERT INTO Company_Tags VALUES ('E00001', 'someone else')")

    conn = connect_read(market_db)
    try:
        install_company_tags(conn, [("E02144", "Open position"), ("E01777", "sogo sosha"), ("", "dropped")])
        visible = [tuple(row) for row in conn.execute("SELECT edinetCode, tag FROM Company_Tags ORDER BY edinetCode")]
        assert visible == [("E01777", "sogo sosha"), ("E02144", "Open position")]
        # The connection stays read-only for the shared database.
        with pytest.raises(sqlite3.OperationalError):
            conn.execute("INSERT INTO main.Company_Tags VALUES ('E9', 'x')")
    finally:
        conn.close()

    with sqlite3.connect(market_db) as check:
        assert check.execute("SELECT edinetCode, tag FROM Company_Tags").fetchall() == [("E00001", "someone else")]
