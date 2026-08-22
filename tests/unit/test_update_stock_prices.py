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
