import os
import sqlite3
import tempfile
import unittest
from unittest.mock import Mock, patch

import pandas as pd
import requests

from src.orchestrator.import_stock_prices_csv.import_stock_prices_csv import import_stock_prices_csv
from src.utilities.stock_prices import (
    _create_prices_table,
    _fetch_jpx_history,
    _fetch_stooq_history,
    _fetch_yahoo_history,
    _jpx_symbol_for_ticker,
    _parse_jpx_history,
    _ProviderRateLimitError,
    _request_with_retries,
    _reset_provider_cooldowns,
    load_ticker_data,
)


class TestImportStockPricesCsv(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmpdir.name, "prices.db")
        self.csv_path = os.path.join(self.tmpdir.name, "prices.csv")

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_import_db_table_format_with_ticker_currency_columns(self):
        df = pd.DataFrame(
            {
                "Date": ["2026-01-01", "2026-01-02"],
                "Ticker": ["7203", "6758"],
                "Currency": ["JPY", "JPY"],
                "Price": [1000.5, 2000.0],
            }
        )
        df.to_csv(self.csv_path, index=False)

        inserted = import_stock_prices_csv(
            self.db_path,
            "stock_prices",
            self.csv_path,
            default_ticker="",
            default_currency="",
            date_column="Date",
            price_column="Price",
            ticker_column="Ticker",
            currency_column="Currency",
        )

        self.assertEqual(inserted, 2)

        conn = sqlite3.connect(self.db_path)
        try:
            rows = conn.execute(
                "SELECT Date, Ticker, Currency, Price FROM stock_prices ORDER BY Date"
            ).fetchall()
            self.assertEqual(rows[0], ("2026-01-01", "7203", "JPY", 1000.5))
            self.assertEqual(rows[1], ("2026-01-02", "6758", "JPY", 2000.0))
        finally:
            conn.close()

    def test_defaults_fill_blank_ticker_currency(self):
        df = pd.DataFrame(
            {
                "Date": ["2026-01-01"],
                "Ticker": [""],
                "Currency": [""],
                "Price": [100.0],
            }
        )
        df.to_csv(self.csv_path, index=False)

        inserted = import_stock_prices_csv(
            self.db_path,
            "stock_prices",
            self.csv_path,
            default_ticker="TPX",
            default_currency="JPY",
            date_column="Date",
            price_column="Price",
            ticker_column="Ticker",
            currency_column="Currency",
        )

        self.assertEqual(inserted, 1)

        conn = sqlite3.connect(self.db_path)
        try:
            row = conn.execute(
                "SELECT Date, Ticker, Currency, Price FROM stock_prices"
            ).fetchone()
            self.assertEqual(row, ("2026-01-01", "TPX", "JPY", 100.0))
        finally:
            conn.close()

    def test_import_auto_detects_backup_csv_columns_when_pipeline_defaults_are_stale(self):
        df = pd.DataFrame(
            {
                "Date": ["2026-01-01", "2026-01-02"],
                "Ticker": ["7203", "6758"],
                "Currency": ["JPY", "JPY"],
                "Price": [1000.5, 2000.0],
            }
        )
        df.to_csv(self.csv_path, index=False)

        inserted = import_stock_prices_csv(
            self.db_path,
            "stock_prices",
            self.csv_path,
            default_ticker="",
            default_currency="JPY",
            date_column="Date",
            price_column="Close",
            ticker_column="",
            currency_column="",
        )

        self.assertEqual(inserted, 2)

        conn = sqlite3.connect(self.db_path)
        try:
            rows = conn.execute(
                "SELECT Date, Ticker, Currency, Price FROM stock_prices ORDER BY Date"
            ).fetchall()
            self.assertEqual(
                rows,
                [
                    ("2026-01-01", "7203", "JPY", 1000.5),
                    ("2026-01-02", "6758", "JPY", 2000.0),
                ],
            )
        finally:
            conn.close()

    def test_jpx_symbol_maps_database_ticker(self):
        self.assertEqual(_jpx_symbol_for_ticker("31100"), "3110")
        self.assertEqual(_jpx_symbol_for_ticker("3110.T"), "3110")
        self.assertIsNone(_jpx_symbol_for_ticker("12345"))
        self.assertIsNone(_jpx_symbol_for_ticker("SPY"))

    def test_parse_jpx_historical_table(self):
        html = """
        <table id="historical">
          <tr><th>日付</th><th>始値</th><th>高値</th><th>安値</th><th>終値</th><th>売買高</th></tr>
          <tr data-value="20260629"><td>2026/06/29</td><td>3,875.0</td><td>4,580.0</td>
              <td>3,730.0</td><td>4,580.0</td><td>9,613,400</td></tr>
          <tr data-value="20260626"><td>2026/06/26</td><td>3,988.0</td><td>4,016.0</td>
              <td>3,776.0</td><td>3,926.0</td><td>7,342,500</td></tr>
        </table>
        """

        result = _parse_jpx_history(html)

        self.assertEqual(
            result.to_dict("records"),
            [
                {"Date": "2026-06-26", "Close": 3926.0},
                {"Date": "2026-06-29", "Close": 4580.0},
            ],
        )

    def test_fetch_jpx_history_establishes_navigation_referrers(self):
        html = """
        <table id="historical">
          <tr><th>日付</th><th>始値</th><th>高値</th><th>安値</th><th>終値</th></tr>
          <tr data-value="20260629"><td>2026/06/29</td><td>3,875</td><td>4,580</td>
              <td>3,730</td><td>4,580</td></tr>
        </table>
        """

        class FakeResponse:
            text = html

            def raise_for_status(self):
                return None

        class FakeSession:
            def __init__(self):
                self.calls = []

            def get(self, url, **kwargs):
                self.calls.append((url, kwargs))
                return FakeResponse()

        session = FakeSession()
        with patch("src.utilities.stock_prices.requests.Session", return_value=session):
            result = _fetch_jpx_history("3110")

        self.assertEqual(
            result.to_dict("records"),
            [{"Date": "2026-06-29", "Close": 4580.0}],
        )
        self.assertEqual(len(session.calls), 3)
        search_url, _search_kwargs = session.calls[0]
        self.assertIn("F=stock_search", search_url)
        detail_url, detail_kwargs = session.calls[1]
        self.assertEqual(detail_kwargs["params"], {"f": "stock_detail", "qcode": "3110"})
        self.assertEqual(detail_kwargs["headers"]["Referer"], search_url)
        self.assertEqual(session.calls[2][1]["params"], {
            "f": "stock_detail", "disptype": "historical", "qcode": "3110",
        })
        self.assertEqual(
            session.calls[2][1]["headers"]["Referer"],
            detail_url + "?f=stock_detail&qcode=3110",
        )

    def test_provider_request_retries_transient_http_failures(self):
        class FakeResponse:
            def __init__(self, status_code):
                self.status_code = status_code
                self.headers = {}

            def raise_for_status(self):
                if self.status_code >= 400:
                    raise requests.HTTPError(f"HTTP {self.status_code}")

        request_fn = Mock(side_effect=[FakeResponse(503), FakeResponse(200)])
        _reset_provider_cooldowns()
        try:
            with patch("src.utilities.stock_prices.time.sleep") as sleep:
                result = _request_with_retries("test-provider", request_fn, "https://example.test")

            self.assertEqual(result.status_code, 200)
            self.assertEqual(request_fn.call_count, 2)
            sleep.assert_called_once()
        finally:
            _reset_provider_cooldowns()

    def test_provider_rate_limit_enters_cooldown_after_retries(self):
        class FakeResponse:
            status_code = 429
            headers = {"Retry-After": "0"}

            @staticmethod
            def raise_for_status():
                raise requests.HTTPError("HTTP 429")

        request_fn = Mock(side_effect=[FakeResponse(), FakeResponse(), FakeResponse()])
        _reset_provider_cooldowns()
        try:
            with patch("src.utilities.stock_prices.time.sleep") as sleep:
                with self.assertRaises(_ProviderRateLimitError):
                    _request_with_retries("rate-limited-provider", request_fn, "https://example.test")

            self.assertEqual(request_fn.call_count, 3)
            self.assertEqual(sleep.call_count, 2)
            blocked_request = Mock()
            with self.assertRaises(_ProviderRateLimitError):
                _request_with_retries(
                    "rate-limited-provider", blocked_request, "https://example.test"
                )
            blocked_request.assert_not_called()
        finally:
            _reset_provider_cooldowns()

    def test_stooq_content_rate_limit_is_retried(self):
        class FakeResponse:
            status_code = 200
            headers = {}
            text = "Exceeded the daily hits limit"

            @staticmethod
            def raise_for_status():
                return None

        request_fn = Mock(return_value=FakeResponse())
        _reset_provider_cooldowns()
        try:
            with patch("src.utilities.stock_prices.requests.get", request_fn), patch(
                "src.utilities.stock_prices.time.sleep"
            ) as sleep:
                with self.assertRaises(_ProviderRateLimitError):
                    _fetch_stooq_history("3110.jp")

            self.assertEqual(request_fn.call_count, 3)
            self.assertEqual(sleep.call_count, 2)
        finally:
            _reset_provider_cooldowns()

    def test_yahoo_tries_second_chart_host_after_transient_failure(self):
        class FakeResponse:
            def __init__(self, status_code, payload=None):
                self.status_code = status_code
                self.headers = {}
                self._payload = payload

            def raise_for_status(self):
                if self.status_code >= 400:
                    raise requests.HTTPError(f"HTTP {self.status_code}")

            def json(self):
                return self._payload

        payload = {
            "chart": {
                "error": None,
                "result": [{
                    "timestamp": [1_735_689_600],
                    "indicators": {"quote": [{"close": [1_000.0]}]},
                    "events": {},
                }],
            }
        }
        request_fn = Mock(side_effect=[
            FakeResponse(503), FakeResponse(503), FakeResponse(503),
            FakeResponse(200, payload),
        ])
        _reset_provider_cooldowns()
        try:
            with patch("src.utilities.stock_prices.requests.get", request_fn), patch(
                "src.utilities.stock_prices.time.sleep"
            ):
                result, events = _fetch_yahoo_history("3110.T")

            self.assertEqual(len(result), 1)
            self.assertEqual(result.iloc[0]["Close"], 1_000.0)
            self.assertEqual(events, [])
            self.assertEqual(request_fn.call_count, 4)
        finally:
            _reset_provider_cooldowns()

    def test_load_ticker_data_prefers_jpx_history(self):
        db_path = os.path.join(self.tmpdir.name, "jpx-history.db")
        history = pd.DataFrame(
            {
                "Date": ["2026-01-01", "2026-01-02"],
                "Close": [810.0, 825.5],
            }
        )

        with patch("src.utilities.stock_prices._fetch_jpx_history", return_value=history) as fetch_jpx, patch(
            "src.utilities.stock_prices._fetch_stooq_history"
        ) as fetch_stooq, patch("src.utilities.stock_prices._fetch_yahoo_history") as fetch_yahoo:
            conn = sqlite3.connect(db_path)
            try:
                _create_prices_table(conn, "stock_prices")
                ok = load_ticker_data("13010", "stock_prices", conn)
                conn.commit()
                rows = conn.execute(
                    "SELECT Date, Ticker, Currency, Price, Price_Basis, Provider, Source_Revision "
                    "FROM stock_prices ORDER BY Date"
                ).fetchall()
            finally:
                conn.close()

        self.assertTrue(ok)
        self.assertEqual(fetch_jpx.call_args.args, ("1301",))
        self.assertEqual(fetch_jpx.call_args.kwargs, {"start_date": None})
        fetch_stooq.assert_not_called()
        fetch_yahoo.assert_not_called()
        self.assertEqual(
            rows,
            [
                ("2026-01-01", "13010", "JPY", 810.0, "adjusted", "JPX quote", "quote-jpx-historical-v1"),
                ("2026-01-02", "13010", "JPY", 825.5, "adjusted", "JPX quote", "quote-jpx-historical-v1"),
            ],
        )

    def test_jpx_limited_initial_history_falls_back_to_broad_provider(self):
        db_path = os.path.join(self.tmpdir.name, "jpx-limited.db")
        jpx_history = pd.DataFrame(
            {
                "Date": pd.date_range("2026-01-01", periods=50, freq="D"),
                "Close": [float(index) for index in range(50)],
            }
        )
        fallback_history = pd.DataFrame(
            {"Date": ["2025-01-01"], "Close": [100.0]}
        )

        with patch(
            "src.utilities.stock_prices._fetch_jpx_history",
            return_value=jpx_history,
        ) as fetch_jpx, patch(
            "src.utilities.stock_prices._fetch_stooq_history",
            return_value=fallback_history,
        ) as fetch_stooq:
            conn = sqlite3.connect(db_path)
            try:
                _create_prices_table(conn, "stock_prices")
                ok = load_ticker_data("13010", "stock_prices", conn)
            finally:
                conn.close()

        self.assertTrue(ok)
        self.assertEqual(fetch_jpx.call_args.args, ("1301",))
        self.assertEqual(fetch_stooq.call_args.args, ("1301.jp",))

    def test_load_ticker_data_falls_back_to_stooq_when_jpx_fails(self):
        db_path = os.path.join(self.tmpdir.name, "history.db")
        history = pd.DataFrame(
            {
                "Date": ["2026-01-01", "2026-01-02"],
                "Close": [810.0, 825.5],
            }
        )

        with patch("src.utilities.stock_prices._fetch_jpx_history", side_effect=RuntimeError("blocked")) as fetch_jpx, patch(
            "src.utilities.stock_prices._fetch_stooq_history", return_value=history
        ) as fetch_stooq, patch(
            "src.utilities.stock_prices._fetch_yahoo_history"
        ) as fetch_yahoo:
            conn = sqlite3.connect(db_path)
            try:
                _create_prices_table(conn, "stock_prices")
                ok = load_ticker_data("13010", "stock_prices", conn)
                conn.commit()
                rows = conn.execute(
                    "SELECT Date, Ticker, Currency, Price FROM stock_prices ORDER BY Date"
                ).fetchall()
            finally:
                conn.close()

        self.assertTrue(ok)
        self.assertEqual(fetch_jpx.call_args.args, ("1301",))
        self.assertEqual(fetch_stooq.call_args.args, ("1301.jp",))
        self.assertEqual(fetch_stooq.call_args.kwargs, {"start_date": None})
        fetch_yahoo.assert_not_called()
        self.assertEqual(
            rows,
            [
                ("2026-01-01", "13010", "JPY", 810.0),
                ("2026-01-02", "13010", "JPY", 825.5),
            ],
        )

    def test_load_ticker_data_falls_back_to_yahoo_when_stooq_fails(self):
        db_path = os.path.join(self.tmpdir.name, "fallback-history.db")
        history = pd.DataFrame(
            {
                ("Close", "1301.T"): [810.0, 825.5],
                ("Volume", "1301.T"): [1000, 1100],
            },
            index=pd.to_datetime(["2026-01-01", "2026-01-02"]),
        )
        history.index.name = "Date"

        with patch("src.utilities.stock_prices._fetch_jpx_history", side_effect=RuntimeError("blocked")) as fetch_jpx, patch(
            "src.utilities.stock_prices._fetch_stooq_history", side_effect=RuntimeError("blocked")
        ) as fetch_stooq, patch(
            "src.utilities.stock_prices._fetch_yahoo_history", return_value=(history, [])
        ) as fetch_yahoo:
            conn = sqlite3.connect(db_path)
            try:
                _create_prices_table(conn, "stock_prices")
                ok = load_ticker_data("13010", "stock_prices", conn)
                conn.commit()
                rows = conn.execute(
                    "SELECT Date, Ticker, Currency, Price FROM stock_prices ORDER BY Date"
                ).fetchall()
            finally:
                conn.close()

        self.assertTrue(ok)
        self.assertEqual(fetch_jpx.call_args.args, ("1301",))
        self.assertEqual(fetch_stooq.call_args.args, ("1301.jp",))
        self.assertEqual(fetch_yahoo.call_args.args, ("1301.T",))
        self.assertEqual(
            rows,
            [
                ("2026-01-01", "13010", "JPY", 810.0),
                ("2026-01-02", "13010", "JPY", 825.5),
            ],
        )

    def test_load_ticker_data_returns_false_when_all_providers_fail(self):
        db_path = os.path.join(self.tmpdir.name, "invalid-history.db")
        bad_history = pd.DataFrame({"Volume": [1000]}, index=pd.to_datetime(["2026-01-01"]))
        bad_history.index.name = "Date"

        with patch("src.utilities.stock_prices._fetch_jpx_history", side_effect=RuntimeError("blocked")), patch(
            "src.utilities.stock_prices._fetch_stooq_history", side_effect=RuntimeError("blocked")), patch(
            "src.utilities.stock_prices._fetch_yahoo_history", return_value=(bad_history, [])
        ):
            conn = sqlite3.connect(db_path)
            try:
                _create_prices_table(conn, "stock_prices")
                ok = load_ticker_data("13010", "stock_prices", conn)
            finally:
                conn.close()

        self.assertFalse(ok)


if __name__ == "__main__":
    unittest.main()
