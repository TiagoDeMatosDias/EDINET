"""Portfolio schema migration and backup tests."""

from __future__ import annotations

import sqlite3

import pytest

from src.portfolio.schema import create_tables


def _legacy_database(path):
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE Portfolio_Daily (
            date TEXT PRIMARY KEY,
            total_value REAL
        );
        CREATE TABLE Portfolio_Holdings (
            symbol TEXT,
            asset_category TEXT,
            quantity REAL,
            currency TEXT,
            PRIMARY KEY (symbol, asset_category)
        );
        CREATE TABLE Holdings_History (
            date TEXT,
            symbol TEXT,
            asset_category TEXT,
            quantity REAL,
            PRIMARY KEY (date, symbol, asset_category)
        );
        INSERT INTO Portfolio_Daily(date, total_value) VALUES ('2024-01-01', 10);
        """
    )
    connection.commit()
    connection.close()


def _columns(connection, table):
    return {
        row[1]
        for row in connection.execute(f'PRAGMA table_info("{table}")')
    }


def test_legacy_schema_is_backed_up_and_migrated_idempotently(tmp_path):
    path = tmp_path / "Portfolio.db"
    _legacy_database(path)

    create_tables(str(path))
    backups_after_first_run = list(tmp_path.glob("Portfolio.db.backup-*"))
    create_tables(str(path))

    connection = sqlite3.connect(path)
    try:
        versions = [
            row[0]
            for row in connection.execute(
                "SELECT version FROM schema_migrations ORDER BY version"
            )
        ]
        assert "cash_ccy_json" in _columns(connection, "Portfolio_Daily")
        assert "market_value_native" in _columns(connection, "Portfolio_Holdings")
        assert "market_value_native" in _columns(connection, "Holdings_History")
        assert connection.execute(
            "SELECT total_value FROM Portfolio_Daily WHERE date = '2024-01-01'"
        ).fetchone()[0] == 10
    finally:
        connection.close()

    assert versions == [1, 2]
    assert len(backups_after_first_run) == 1
    assert list(tmp_path.glob("Portfolio.db.backup-*")) == backups_after_first_run


def test_unrelated_migration_error_is_not_suppressed_and_rolls_back(tmp_path):
    path = tmp_path / "invalid.db"
    connection = sqlite3.connect(path)
    connection.execute("CREATE VIEW Portfolio_Holdings AS SELECT 1 AS value")
    connection.commit()
    connection.close()

    with pytest.raises(sqlite3.OperationalError):
        create_tables(str(path))

    connection = sqlite3.connect(path)
    try:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
    finally:
        connection.close()
    assert "Transactions" in tables
    connection = sqlite3.connect(path)
    try:
        versions = [
            row[0]
            for row in connection.execute(
                "SELECT version FROM schema_migrations ORDER BY version"
            )
        ]
    finally:
        connection.close()
    assert versions == [1]
