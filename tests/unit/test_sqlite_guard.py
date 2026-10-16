"""The read-only guard for databases the server must not write."""

from __future__ import annotations

import sqlite3

import pytest

from src.orchestrator.common.sqlite import (
    clear_protected_database_paths,
    connect_read,
    connect_write,
    is_protected_database,
    protect_database_paths,
)


@pytest.fixture(autouse=True)
def disarm():
    clear_protected_database_paths()
    yield
    clear_protected_database_paths()


@pytest.fixture
def databases(tmp_path):
    protected, free = tmp_path / "market.db", tmp_path / "app.db"
    for path in (protected, free):
        with sqlite3.connect(path) as conn:
            conn.execute("CREATE TABLE items (value TEXT)")
            conn.execute("INSERT INTO items VALUES ('one')")
        # sqlite3's context manager commits but does not close
    return protected, free


def test_nothing_is_protected_by_default(databases):
    protected, _ = databases
    assert not is_protected_database(protected)
    conn = connect_write(protected)
    conn.execute("INSERT INTO items VALUES ('two')")
    conn.commit()
    conn.close()


def test_writes_fail_on_a_protected_path(databases):
    protected, _ = databases
    protect_database_paths([protected])

    conn = connect_write(protected)
    try:
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            conn.execute("INSERT INTO items VALUES ('two')")
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            conn.execute("CREATE TABLE other (x)")
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            conn.execute("DELETE FROM items")
    finally:
        conn.close()
    with connect_read(protected) as check:
        assert [row[0] for row in check.execute("SELECT value FROM items")] == ["one"]


def test_reads_and_create_if_not_exists_still_work(databases):
    protected, _ = databases
    protect_database_paths([protected])

    conn = connect_write(protected)
    try:
        assert conn.execute("SELECT COUNT(*) FROM items").fetchone()[0] == 1
        conn.execute("CREATE TABLE IF NOT EXISTS items (value TEXT)")
        assert conn.execute("PRAGMA query_only").fetchone()[0] == 1
    finally:
        conn.close()


def test_other_paths_are_unaffected(databases):
    protected, free = databases
    protect_database_paths([protected])

    assert is_protected_database(protected) and not is_protected_database(free)
    conn = connect_write(free)
    conn.execute("INSERT INTO items VALUES ('two')")
    conn.commit()
    conn.close()


def test_paths_are_compared_after_resolving(databases, tmp_path):
    protected, _ = databases
    protect_database_paths([tmp_path / "." / "market.db"])

    assert is_protected_database(protected)
    assert is_protected_database(tmp_path / "sub" / ".." / "market.db")


def test_clear_disarms_the_guard(databases):
    protected, _ = databases
    protect_database_paths([protected])
    clear_protected_database_paths()

    conn = connect_write(protected)
    conn.execute("INSERT INTO items VALUES ('two')")
    conn.commit()
    conn.close()


def test_already_open_connections_keep_their_mode(databases):
    protected, _ = databases
    conn = connect_write(protected)
    protect_database_paths([protected])
    conn.execute("INSERT INTO items VALUES ('two')")
    conn.commit()
    conn.close()


def test_managed_initialization_of_an_existing_wal_database_still_works(tmp_path):
    from src.orchestrator.common.sqlite import initialize_managed_database

    path = tmp_path / "filings.db"
    conn = connect_write(path)
    initialize_managed_database(conn)
    conn.execute("CREATE TABLE t (x)")
    conn.commit()
    conn.close()
    protect_database_paths([path])

    conn = connect_write(path)
    try:
        initialize_managed_database(conn)
        assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    finally:
        conn.close()
