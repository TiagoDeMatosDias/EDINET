import sqlite3
from unittest.mock import patch

from src.orchestrator.update_stock_prices import update_stock_prices


def test_update_all_stock_prices_randomizes_ticker_order(tmp_path):
    db_path = tmp_path / "prices.db"
    connection = sqlite3.connect(db_path)
    try:
        connection.execute("CREATE TABLE CompanyInfo (Company_Ticker TEXT)")
        connection.executemany(
            "INSERT INTO CompanyInfo(Company_Ticker) VALUES (?)",
            [("13010",), ("72030",), ("67580",)],
        )
        connection.commit()
    finally:
        connection.close()

    attempted = []

    def reverse(values):
        attempted.extend(values)
        values.reverse()

    with patch.object(update_stock_prices.random, "shuffle", side_effect=reverse) as shuffle, patch.object(
        update_stock_prices, "_update_ticker", return_value=True
    ) as update:
        update_stock_prices.update_all_stock_prices(str(db_path))

    shuffle.assert_called_once()
    assert attempted == ["13010", "72030", "67580"]
    assert [call.args[2] for call in update.call_args_list] == [
        "67580",
        "72030",
        "13010",
    ]


def test_update_uses_company_universe_and_excludes_auxiliary_price_series(tmp_path):
    db_path = tmp_path / "prices.db"
    connection = sqlite3.connect(db_path)
    try:
        connection.execute("CREATE TABLE CompanyInfo (Company_Ticker TEXT)")
        connection.executemany(
            "INSERT INTO CompanyInfo(Company_Ticker) VALUES (?)",
            [("7203",), (" 6758 ",)],
        )
        connection.execute(
            "CREATE TABLE Stock_Prices "
            "(Date TEXT, Ticker TEXT, Currency TEXT, Price REAL)"
        )
        connection.executemany(
            "INSERT INTO Stock_Prices VALUES (?, ?, ?, ?)",
            [
                ("2026-01-01", "EUR", "USD", 1.1),
                ("2026-01-01", "Inflation_USD", "USD", 100.0),
                ("2026-01-01", "9999", "USD", 10.0),
            ],
        )
        connection.commit()
    finally:
        connection.close()

    with patch.object(update_stock_prices.random, "shuffle") as shuffle, patch.object(
        update_stock_prices, "_update_ticker", return_value=True
    ) as update:
        result = update_stock_prices.update_all_stock_prices(str(db_path))

    shuffle.assert_called_once_with(["7203", "6758"])
    assert {call.args[2] for call in update.call_args_list} == {"7203", "6758"}
    assert result == {
        "attempted": 2,
        "updated": 2,
        "failed": 0,
        "failed_tickers": [],
        "aborted_early": False,
        "skipped": 0,
    }


def test_update_fallback_excludes_auxiliary_price_series_when_company_table_empty(tmp_path):
    db_path = tmp_path / "prices.db"
    connection = sqlite3.connect(db_path)
    try:
        connection.execute(
            "CREATE TABLE Stock_Prices "
            "(Date TEXT, Ticker TEXT, Currency TEXT, Price REAL)"
        )
        connection.executemany(
            "INSERT INTO Stock_Prices VALUES (?, ?, ?, ?)",
            [
                ("2026-01-01", "EUR", "USD", 1.1),
                ("2026-01-01", "Inflation_USD", "USD", 100.0),
                ("2026-01-01", "7203", "JPY", 3000.0),
            ],
        )
        connection.commit()
    finally:
        connection.close()

    with patch.object(update_stock_prices.random, "shuffle") as shuffle, patch.object(
        update_stock_prices, "_update_ticker", return_value=True
    ) as update:
        update_stock_prices.update_all_stock_prices(str(db_path))

    shuffle.assert_called_once_with(["7203"])
    assert [call.args[2] for call in update.call_args_list] == ["7203"]


def test_update_stops_early_when_last_resort_provider_is_cooling_down(tmp_path):
    db_path = tmp_path / "prices.db"
    connection = sqlite3.connect(db_path)
    try:
        connection.execute("CREATE TABLE CompanyInfo (Company_Ticker TEXT)")
        connection.executemany(
            "INSERT INTO CompanyInfo(Company_Ticker) VALUES (?)",
            [("7203",), ("9984",), ("6758",)],
        )
        connection.commit()
    finally:
        connection.close()

    def fail_first_succeed_rest(conn, prices_table, ticker, currency="JPY", overwrite=False, savepoint_id=0):
        if ticker == "7203":
            return False
        return True

    with patch.object(update_stock_prices.random, "shuffle"), patch.object(
        update_stock_prices, "_update_ticker", side_effect=fail_first_succeed_rest
    ) as update, patch.object(
        update_stock_prices.stockprice_api,
        "primary_cooldown_remaining",
        return_value=250.0,
    ):
        result = update_stock_prices.update_all_stock_prices(str(db_path))

    assert [call.args[2] for call in update.call_args_list] == ["7203"]
    assert result == {
        "attempted": 1,
        "updated": 0,
        "failed": 1,
        "failed_tickers": ["7203"],
        "aborted_early": True,
        "skipped": 2,
    }
