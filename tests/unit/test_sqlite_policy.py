"""Tests for shared SQLite connection and transaction policy."""

from __future__ import annotations

import sqlite3
from time import perf_counter

import pytest

from src.orchestrator.common.sqlite import (
    DatabaseBusyError,
    connect_read,
    connect_write,
    initialize_managed_database,
    transaction,
)


def _create_database(path):
    connection = connect_write(path)
    initialize_managed_database(connection)
    connection.execute("CREATE TABLE values_table (value INTEGER)")
    connection.commit()
    connection.close()


def test_read_connection_rejects_writes(tmp_path):
    path = tmp_path / "readonly.db"
    _create_database(path)
    connection = connect_read(path)
    try:
        with pytest.raises(sqlite3.OperationalError):
            connection.execute("INSERT INTO values_table VALUES (1)")
    finally:
        connection.close()


def test_transaction_rolls_back_and_closes_after_exception(tmp_path):
    path = tmp_path / "rollback.db"
    _create_database(path)
    with pytest.raises(RuntimeError):
        with transaction(path) as connection:
            connection.execute("INSERT INTO values_table VALUES (1)")
            raise RuntimeError("stop")

    connection = connect_read(path)
    try:
        assert connection.execute("SELECT COUNT(*) FROM values_table").fetchone()[0] == 0
    finally:
        connection.close()


def test_locked_database_has_bounded_clear_failure(tmp_path):
    path = tmp_path / "locked.db"
    _create_database(path)
    blocker = connect_write(path)
    blocker.execute("BEGIN IMMEDIATE")
    started = perf_counter()
    try:
        with pytest.raises(DatabaseBusyError, match="100 ms"):
            with transaction(path, busy_timeout_ms=100) as connection:
                connection.execute("INSERT INTO values_table VALUES (1)")
    finally:
        blocker.rollback()
        blocker.close()
    assert perf_counter() - started < 1.5


def test_wal_reader_sees_committed_snapshot_during_writer(tmp_path):
    path = tmp_path / "wal.db"
    _create_database(path)
    writer = connect_write(path)
    writer.execute("BEGIN IMMEDIATE")
    writer.execute("INSERT INTO values_table VALUES (1)")
    reader = connect_read(path)
    try:
        assert reader.execute("SELECT COUNT(*) FROM values_table").fetchone()[0] == 0
    finally:
        reader.close()
        writer.rollback()
        writer.close()
