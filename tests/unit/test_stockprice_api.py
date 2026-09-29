import json
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

    def test_provider_symbols_normalize_japanese_ticker_forms(self):
        from src.utilities.stock_prices import (
            _provider_symbol_for_ticker,
            _stooq_symbol_for_ticker,
        )

        # EDINET 5-digit form, suffixed 5-digit form, and 4-digit form must
        # all reach Yahoo as the 4-digit .T symbol.
        for stored in ("43960", "43960.T", "43960.jp", "4396", "4396.T"):
            self.assertEqual(_provider_symbol_for_ticker(stored), "4396.T")
        for stored in ("43960", "43960.T", "43960.jp", "4396", "4396.T", "4396.jp"):
            self.assertEqual(_stooq_symbol_for_ticker(stored), "4396.jp")

        # JPX Growth codes are four alphanumeric characters stored with a
        # trailing zero; they must map to the 4-char JPX/Yahoo form.
        self.assertEqual(_provider_symbol_for_ticker("302A0"), "302A.T")
        self.assertEqual(_stooq_symbol_for_ticker("302A0"), "302a.jp")
        self.assertEqual(_jpx_symbol_for_ticker("302A0"), "302A")
        self.assertEqual(_provider_symbol_for_ticker("302A.T"), "302A.T")

        # Symbols with real dots are preserved untouched.
        self.assertEqual(_provider_symbol_for_ticker("BRK.B"), "BRK.B")
        self.assertEqual(_provider_symbol_for_ticker("SXR8.DE"), "SXR8.DE")
        self.assertEqual(_stooq_symbol_for_ticker("SXR8.DE"), "sxr8.de")
        self.assertEqual(_provider_symbol_for_ticker("SXR8"), "SXR8")

    def test_parse_jpx_history_payload(self):
        payload = json.dumps(
            {
                "status": 0,
                "section1": {
                    "data": {
                        "4979/T": {
                            "A_HISTDAYL": (
                                "2026/06/26,3988.0,4016.0,3776.0,3926.0,7342500,\n"
                                "2026/06/29,3875.0,4580.0,3730.0,4580.0,9613400,\n"
                            )
                        }
                    }
                },
            }
        )

        result = _parse_jpx_history(payload)

        self.assertEqual(
            result.to_dict("records"),
            [
                {"Date": "2026-06-26", "Close": 3926.0},
                {"Date": "2026-06-29", "Close": 4580.0},
            ],
        )

    def test_parse_jpx_history_rejects_error_status(self):
        payload = json.dumps({"status": 503, "error": "リファラー："})

        with self.assertRaises(RuntimeError):
            _parse_jpx_history(payload)

    def test_fetch_jpx_history_requests_json_endpoint_with_referer(self):
        payload = json.dumps(
            {
                "status": 0,
                "section1": {
                    "data": {
                        "3110/T": {
                            "A_HISTDAYL": "2026/06/29,3875,4580,3730,4580,100,\n"
                        }
                    }
                },
            }
        )

        class FakeResponse:
            text = payload

            def raise_for_status(self):
                return None

        request_fn = Mock(return_value=FakeResponse())
        _reset_provider_cooldowns()
        try:
            with patch("src.utilities.stock_prices.requests.get", request_fn):
                result = _fetch_jpx_history("3110")
        finally:
            _reset_provider_cooldowns()

        self.assertEqual(result.to_dict("records"), [{"Date": "2026-06-29", "Close": 4580.0}])
        self.assertEqual(request_fn.call_count, 1)
        url = request_fn.call_args.args[0]
        self.assertEqual(url, "https://quote.jpx.co.jp/jpxhp/jcgi/wrap/qjsonp.aspx")
        kwargs = request_fn.call_args.kwargs
        self.assertEqual(kwargs["params"], {"F": "ctl/stock_detail", "qcode": "3110"})
        self.assertEqual(
            kwargs["headers"]["Referer"],
            "https://quote.jpx.co.jp/jpxhp/main/index.aspx"
            "?f=stock_detail&disptype=historical&qcode=3110",
        )

    def test_degenerate_history_gate(self):
        from src.utilities.stock_prices import (
            _ProviderCoverageError,
            _reject_degenerate_history,
        )

        # Monthly bars over multiple years are rejected as sparse.
        monthly = pd.DataFrame(
            {
                "Date": pd.date_range("2020-01-01", periods=60, freq="MS"),
                "Close": [100.0] * 60,
            }
        )
        with self.assertRaises(_ProviderCoverageError):
            _reject_degenerate_history(monthly, "72030")

        # Weekend-dated rows for a Japanese code are dropped; the rest stays.
        daily = pd.DataFrame(
            {
                "Date": ["2026-08-20", "2026-08-21", "2026-08-22"],
                "Close": [100.0, 101.0, 102.0],
            }
        )
        cleaned = _reject_degenerate_history(daily, "72030")
        self.assertEqual(cleaned["Date"].tolist(), ["2026-08-20", "2026-08-21"])

        # Weekend rows for a non-Japanese symbol pass through untouched.
        us = _reject_degenerate_history(daily, "AAPL")
        self.assertEqual(len(us), 3)

        # Dense business-day histories are accepted unchanged.
        dense = pd.DataFrame(
            {
                "Date": pd.date_range("2024-01-01", periods=300, freq="B"),
                "Close": [100.0] * 300,
            }
        )
        self.assertEqual(len(_reject_degenerate_history(dense, "AAPL")), 300)

    def test_jpx_error_payload_is_retried(self):
        error_payload = json.dumps({"status": 503, "error": "リファラー："})
        ok_payload = json.dumps(
            {"status": 0, "section1": {"data": {"3110/T": {"A_HISTDAYL": ""}}}}
        )

        class FakeResponse:
            def __init__(self, text):
                self.text = text
                self.status_code = 200

            def raise_for_status(self):
                return None

        from src.utilities.stock_prices import _validate_jpx_response

        request_fn = Mock(
            side_effect=[FakeResponse(error_payload), FakeResponse(ok_payload)]
        )
        _reset_provider_cooldowns()
        try:
            with patch("src.utilities.stock_prices.time.sleep") as sleep:
                response = _request_with_retries(
                    "test-jpx", request_fn, "https://example.test",
                    response_validator=_validate_jpx_response,
                )
            self.assertEqual(request_fn.call_count, 2)
            sleep.assert_called_once()
            self.assertEqual(json.loads(response.text)["status"], 0)
        finally:
            _reset_provider_cooldowns()

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

    def test_stooq_bot_verification_page_is_treated_as_rate_limit(self):
        class FakeResponse:
            status_code = 200
            headers = {}
            text = (
                "<!DOCTYPE html><html><head><meta charset=\"utf-8\">"
                "<noscript>This site requires JavaScript to verify your browser."
                "</noscript></head></html>"
            )

            @staticmethod
            def raise_for_status():
                return None

        request_fn = Mock(return_value=FakeResponse())
        _reset_provider_cooldowns()
        try:
            with patch("src.utilities.stock_prices.requests.get", request_fn), patch(
                "src.utilities.stock_prices.time.sleep"
            ):
                with self.assertRaises(_ProviderRateLimitError):
                    _fetch_stooq_history("4396.jp")

            self.assertEqual(request_fn.call_count, 3)
        finally:
            _reset_provider_cooldowns()

    def test_yahoo_404_does_not_mark_provider_cooldown(self):
        class FakeResponse:
            def __init__(self, status_code):
                self.status_code = status_code
                self.headers = {}

            def raise_for_status(self):
                if self.status_code >= 400:
                    raise requests.HTTPError(
                        f"HTTP {self.status_code}", response=self
                    )

        request_fn = Mock(return_value=FakeResponse(404))
        _reset_provider_cooldowns()
        try:
            with patch("src.utilities.stock_prices.requests.get", request_fn):
                with self.assertRaises(RuntimeError):
                    _fetch_yahoo_history("43960.T")

            # Only one host is tried for a permanent 404, and no cooldown
            # is recorded, so other tickers can still use Yahoo.
            self.assertEqual(request_fn.call_count, 1)
            self.assertEqual(
                _request_with_retries(
                    "Yahoo Finance chart",
                    Mock(return_value=FakeResponse(200)),
                    "https://example.test",
                ).status_code,
                200,
            )
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

    def test_recent_japanese_price_uses_only_jpx(self):
        db_path = os.path.join(self.tmpdir.name, "recent-jpx.db")
        last_date = (pd.Timestamp.today().normalize() - pd.Timedelta(days=10)).strftime("%Y-%m-%d")
        friday = (pd.Timestamp.today().normalize() - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        while pd.Timestamp(friday).dayofweek >= 5:
            friday = (pd.Timestamp(friday) - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        history = pd.DataFrame(
            {
                "Date": [last_date, friday],
                "Close": [810.0, 825.5],
            }
        )

        with patch("src.utilities.stock_prices._fetch_jpx_history", return_value=history) as fetch_jpx, patch(
            "src.utilities.stock_prices._fetch_stooq_history"
        ) as fetch_stooq, patch("src.utilities.stock_prices._fetch_yahoo_history") as fetch_yahoo:
            conn = sqlite3.connect(db_path)
            try:
                _create_prices_table(conn, "stock_prices")
                conn.execute(
                    "INSERT INTO stock_prices(Date, Ticker, Currency, Price) VALUES (?, ?, ?, ?)",
                    (last_date, "13010", "JPY", 800.0),
                )
                ok = load_ticker_data("13010", "stock_prices", conn)
            finally:
                conn.close()

        self.assertTrue(ok)
        self.assertEqual(fetch_jpx.call_args.args, ("1301",))
        self.assertEqual(fetch_jpx.call_args.kwargs, {"start_date": last_date})
        fetch_stooq.assert_not_called()
        fetch_yahoo.assert_not_called()

    def test_recent_jpx_update_replaces_overlapping_dates(self):
        db_path = os.path.join(self.tmpdir.name, "recent-jpx-idempotent.db")
        def latest_weekday(days_ago: int) -> str:
            # The loader drops weekend-dated rows, so both dates must be trading days.
            day = pd.Timestamp.today().normalize() - pd.Timedelta(days=days_ago)
            while day.dayofweek >= 5:
                day -= pd.Timedelta(days=1)
            return day.strftime("%Y-%m-%d")

        last_date = latest_weekday(10)
        friday = latest_weekday(1)
        history = pd.DataFrame(
            {
                "Date": [last_date, friday],
                "Close": [810.0, 825.5],
            }
        )

        conn = sqlite3.connect(db_path)
        try:
            _create_prices_table(conn, "stock_prices")
            conn.execute(
                "INSERT INTO stock_prices(Date, Ticker, Currency, Price) "
                "VALUES (?, ?, ?, ?)",
                (last_date, "13010", "JPY", 800.0),
            )
            with patch(
                "src.utilities.stock_prices._fetch_jpx_history",
                return_value=history,
            ):
                self.assertTrue(load_ticker_data("13010", "stock_prices", conn))
            conn.commit()
            rows = conn.execute(
                "SELECT Date, Price FROM stock_prices "
                "WHERE Ticker = ? ORDER BY Date",
                ("13010",),
            ).fetchall()
        finally:
            conn.close()

        self.assertEqual(rows, [(last_date, 810.0), (friday, 825.5)])
    def test_older_japanese_price_keeps_fallback_providers(self):
        db_path = os.path.join(self.tmpdir.name, "older-fallback.db")
        last_date = (pd.Timestamp.today().normalize() - pd.Timedelta(days=60)).strftime("%Y-%m-%d")
        friday = (pd.Timestamp.today().normalize() - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        while pd.Timestamp(friday).dayofweek >= 5:
            friday = (pd.Timestamp(friday) - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        history = pd.DataFrame(
            {"Date": [friday], "Close": [825.5]}
        )

        with patch(
            "src.utilities.stock_prices._fetch_jpx_history",
            side_effect=RuntimeError("historical gap"),
        ) as fetch_jpx, patch(
            "src.utilities.stock_prices._fetch_stooq_history",
            return_value=history,
        ) as fetch_stooq, patch(
            "src.utilities.stock_prices._fetch_yahoo_history"
        ) as fetch_yahoo:
            conn = sqlite3.connect(db_path)
            try:
                _create_prices_table(conn, "stock_prices")
                conn.execute(
                    "INSERT INTO stock_prices(Date, Ticker, Currency, Price) VALUES (?, ?, ?, ?)",
                    (last_date, "13010", "JPY", 800.0),
                )
                ok = load_ticker_data("13010", "stock_prices", conn)
            finally:
                conn.close()

        self.assertTrue(ok)
        self.assertEqual(fetch_jpx.call_args.args, ("1301",))
        self.assertEqual(fetch_stooq.call_args.args, ("1301.jp",))
        self.assertEqual(
            fetch_stooq.call_args.kwargs,
            {"start_date": (pd.Timestamp(last_date) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")},
        )
        fetch_yahoo.assert_not_called()

    def test_jpx_limited_initial_history_falls_back_to_broad_provider(self):
        db_path = os.path.join(self.tmpdir.name, "jpx-limited.db")
        jpx_history = pd.DataFrame(
            {
                "Date": pd.date_range("2019-01-01", periods=360, freq="B"),
                "Close": [float(index) for index in range(360)],
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
