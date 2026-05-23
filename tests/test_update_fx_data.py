import io
import os
import sqlite3
import tempfile
import unittest
import zipfile
from unittest.mock import Mock, patch

import pandas as pd

from src.orchestrator.update_fx_data.update_fx_data import (
    _download_ecb_fx_csv,
    _download_ecb_hicp,
    _download_fred_cpi,
    _fetch_all_inflation_prices,
    _fetch_ecb_fx_prices,
    _transform_ecb_fx_to_prices,
    _insert_new_pairs,
    update_fx_data,
)
from src.utilities.stock_prices import _create_prices_table


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_ecb_zip_csv(content: str) -> bytes:
    """Build an in-memory ECB-style ZIP containing 'eurofxref-hist.csv'."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("eurofxref-hist.csv", content)
    return buf.getvalue()


# Matches the real ECB format — trailing commas create an unnamed column
# which the transform filters out.
_SAMPLE_ECB_CSV = (
    "Date,USD,JPY,GBP,CHF,\n"
    "2026-05-18,1.1648,184.93,0.8702,0.9144,\n"
    "2026-05-15,1.1628,184.36,0.8705,0.9144,\n"
    "2026-05-14,1.1702,N/A,0.86618,0.915,\n"
)


# ---------------------------------------------------------------------------
# FX transform tests
# ---------------------------------------------------------------------------

class TestTransformEcbFxToPrices(unittest.TestCase):
    """Tests for _transform_ecb_fx_to_prices."""

    def test_melts_currencies_excluding_eur(self):
        raw = pd.DataFrame({
            "Date": ["2026-01-01", "2026-01-02"],
            "USD": [1.10, 1.11],
            "JPY": [140.0, 141.0],
            "EUR": [1.0, 1.0],
        })
        result = _transform_ecb_fx_to_prices(raw)

        self.assertEqual(len(result), 4)  # 2 dates × 2 currencies
        # Ticker is always "EUR", Currency is the target
        self.assertTrue((result["Ticker"] == "EUR").all())
        self.assertEqual(set(result["Currency"].unique()), {"USD", "JPY"})
        self.assertNotIn("EUR", result["Currency"].values)

    def test_drops_na_rates(self):
        raw = pd.DataFrame({
            "Date": ["2026-01-01", "2026-01-02"],
            "USD": ["1.10", "N/A"],
            "JPY": ["N/A", "141.0"],
        })
        result = _transform_ecb_fx_to_prices(raw)

        self.assertEqual(len(result), 2)
        usd_row = result[result["Currency"] == "USD"].iloc[0]
        jpy_row = result[result["Currency"] == "JPY"].iloc[0]
        self.assertEqual(usd_row["Price"], 1.10)
        self.assertEqual(jpy_row["Price"], 141.0)

    def test_dates_formatted_as_yyyy_mm_dd(self):
        raw = pd.DataFrame({
            "Date": ["2026-01-01"],
            "USD": [1.10],
        })
        result = _transform_ecb_fx_to_prices(raw)
        self.assertEqual(result.iloc[0]["Date"], "2026-01-01")

    def test_output_columns(self):
        raw = pd.DataFrame({
            "Date": ["2026-01-01"],
            "USD": [1.10],
        })
        result = _transform_ecb_fx_to_prices(raw)
        self.assertEqual(list(result.columns), ["Date", "Ticker", "Currency", "Price"])


# ---------------------------------------------------------------------------
# Inflation — FRED download tests
# ---------------------------------------------------------------------------

_FRED_CPI_SAMPLE = (
    "observation_date,CPIAUCSL\n"
    "2024-01-01,308.417\n"
    "2024-02-01,310.326\n"
    "2024-03-01,312.332\n"
)


class TestDownloadFredCpi(unittest.TestCase):

    def test_returns_date_price_dataframe(self):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = _FRED_CPI_SAMPLE
        mock_session = Mock()
        mock_session.get.return_value = mock_response

        df = _download_fred_cpi("CPIAUCSL", session=mock_session)

        self.assertEqual(len(df), 3)
        self.assertListEqual(list(df["Date"]), ["2024-01-01", "2024-02-01", "2024-03-01"])
        self.assertAlmostEqual(df.iloc[0]["Price"], 308.417)

    def test_returns_empty_on_http_error(self):
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = Exception("Server error")
        mock_session = Mock()
        mock_session.get.return_value = mock_response

        df = _download_fred_cpi("CPIAUCSL", session=mock_session)

        self.assertTrue(df.empty)
        self.assertListEqual(list(df.columns), ["Date", "Price"])

    def test_returns_empty_on_missing_column(self):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = "wrong_column,value\n2024-01-01,100\n"
        mock_session = Mock()
        mock_session.get.return_value = mock_response

        df = _download_fred_cpi("CPIAUCSL", session=mock_session)

        self.assertTrue(df.empty)


# ---------------------------------------------------------------------------
# Inflation — ECB HICP download tests
# ---------------------------------------------------------------------------

_ECB_HICP_SAMPLE = (
    "KEY,FREQ,TIME_PERIOD,OBS_VALUE,EXTRA\n"
    "ICP.M.U2.N.000000.4.INX,M,2024-01,124.53,A\n"
    "ICP.M.U2.N.000000.4.INX,M,2024-02,125.10,A\n"
    "ICP.M.U2.N.000000.4.INX,M,2024-03,125.80,A\n"
)


class TestDownloadEcbHicp(unittest.TestCase):

    def test_returns_date_price_dataframe(self):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = _ECB_HICP_SAMPLE
        mock_session = Mock()
        mock_session.get.return_value = mock_response

        df = _download_ecb_hicp(session=mock_session)

        self.assertEqual(len(df), 3)
        self.assertListEqual(list(df["Date"]), ["2024-01-01", "2024-02-01", "2024-03-01"])
        self.assertAlmostEqual(df.iloc[0]["Price"], 124.53)

    def test_returns_empty_on_http_error(self):
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = Exception("Server error")
        mock_session = Mock()
        mock_session.get.return_value = mock_response

        df = _download_ecb_hicp(session=mock_session)

        self.assertTrue(df.empty)

    def test_returns_empty_on_missing_columns(self):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = "KEY,FREQ,OTHER\nX,M,100\n"
        mock_session = Mock()
        mock_session.get.return_value = mock_response

        df = _download_ecb_hicp(session=mock_session)

        self.assertTrue(df.empty)


# ---------------------------------------------------------------------------
# _fetch_all_inflation_prices tests
# ---------------------------------------------------------------------------

class TestFetchAllInflationPrices(unittest.TestCase):

    @patch("src.orchestrator.update_fx_data.update_fx_data._download_ecb_hicp")
    @patch("src.orchestrator.update_fx_data.update_fx_data._download_fred_cpi")
    def test_assembles_all_currencies(self, mock_fred, mock_ecb):
        # ECB returns EUR HICP
        mock_ecb.return_value = pd.DataFrame({
            "Date": ["2024-01-01", "2024-02-01"],
            "Price": [124.53, 125.10],
        })

        # FRED returns data for each series
        def fred_side_effect(series_id, **_kwargs):
            ticker_map = {
                "CPIAUCSL":        "Inflation_USD",
                "JPNCPIALLMINMEI": "Inflation_JPY",
                "GBRCPIALLMINMEI": "Inflation_GBP",
                "AUSCPIALLMINMEI": "Inflation_AUD",
                "CANCPIALLMINMEI": "Inflation_CAD",
            }
            ticker = ticker_map.get(series_id, "UNKNOWN")
            return pd.DataFrame({
                "Date": ["2024-01-01"],
                "Price": [100.0],
            })
        mock_fred.side_effect = fred_side_effect

        result = _fetch_all_inflation_prices()

        self.assertEqual(len(result), 7)  # 2 EUR + 5×1 FRED
        expected_tickers = {
            "Inflation_EUR", "Inflation_USD", "Inflation_JPY",
            "Inflation_GBP", "Inflation_AUD", "Inflation_CAD",
        }
        self.assertEqual(set(result["Ticker"].unique()), expected_tickers)
        self.assertListEqual(list(result.columns), ["Date", "Ticker", "Currency", "Price"])

    @patch("src.orchestrator.update_fx_data.update_fx_data._download_ecb_hicp")
    @patch("src.orchestrator.update_fx_data.update_fx_data._download_fred_cpi")
    def test_skips_failed_sources(self, mock_fred, mock_ecb):
        mock_ecb.return_value = pd.DataFrame(columns=["Date", "Price"])
        # FRED returns empty for one series, data for another
        def fred_side_effect(series_id, **_kwargs):
            if series_id == "JPNCPIALLMINMEI":
                return pd.DataFrame(columns=["Date", "Price"])
            return pd.DataFrame({
                "Date": ["2024-01-01"],
                "Price": [100.0],
            })
        mock_fred.side_effect = fred_side_effect

        result = _fetch_all_inflation_prices()

        # EUR empty + JPY empty → 4 remaining FRED series
        self.assertEqual(len(result), 4)
        self.assertNotIn("Inflation_EUR", result["Ticker"].values)
        self.assertNotIn("Inflation_JPY", result["Ticker"].values)


# ---------------------------------------------------------------------------
# FX download tests (unchanged from original)
# ---------------------------------------------------------------------------

class TestDownloadEcbFx(unittest.TestCase):

    def test_returns_dataframe(self):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = _make_ecb_zip_csv(_SAMPLE_ECB_CSV)

        mock_session = Mock()
        mock_session.get.return_value = mock_response

        df = _download_ecb_fx_csv(session=mock_session)

        self.assertIsInstance(df, pd.DataFrame)
        self.assertIn("Date", df.columns)
        self.assertIn("USD", df.columns)
        self.assertIn("JPY", df.columns)
        self.assertEqual(len(df), 3)

    def test_raises_on_http_error(self):
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = Exception("Server error")

        mock_session = Mock()
        mock_session.get.return_value = mock_response

        with self.assertRaises(Exception):
            _download_ecb_fx_csv(session=mock_session)


# ---------------------------------------------------------------------------
# _insert_new_pairs tests
# ---------------------------------------------------------------------------

class TestInsertNewPairs(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmpdir.name, "insert_test.db")

    def tearDown(self):
        self.tmpdir.cleanup()

    def _read_all(self, table="Stock_Prices"):
        conn = sqlite3.connect(self.db_path)
        try:
            return pd.read_sql_query(
                f"SELECT Date, Ticker, Currency, Price FROM {table} ORDER BY Date, Ticker",
                conn,
            )
        finally:
            conn.close()

    def test_creates_table_and_inserts(self):
        df = pd.DataFrame({
            "Date": ["2024-01-01"],
            "Ticker": ["USD"],
            "Currency": ["EUR"],
            "Price": [1.10],
        })
        count = _insert_new_pairs(df, self.db_path, "Stock_Prices", label="FX")
        self.assertEqual(count, 1)

        result = self._read_all()
        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["Ticker"], "USD")

    def test_skips_duplicate_pairs(self):
        df = pd.DataFrame({
            "Date": ["2024-01-01", "2024-01-02"],
            "Ticker": ["USD", "USD"],
            "Currency": ["EUR", "EUR"],
            "Price": [1.10, 1.11],
        })
        _insert_new_pairs(df, self.db_path, "Stock_Prices", label="FX")
        count2 = _insert_new_pairs(df, self.db_path, "Stock_Prices", label="FX")
        self.assertEqual(count2, 0)

        result = self._read_all()
        self.assertEqual(len(result), 2)

    def test_returns_zero_for_empty_dataframe(self):
        df = pd.DataFrame(columns=["Date", "Ticker", "Currency", "Price"])
        count = _insert_new_pairs(df, self.db_path, "Stock_Prices", label="test")
        self.assertEqual(count, 0)


# ---------------------------------------------------------------------------
# update_fx_data integration tests
# ---------------------------------------------------------------------------

class TestUpdateFxDataIntegration(unittest.TestCase):
    """Integration tests using patched high-level fetch functions."""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmpdir.name, "full.db")

    def tearDown(self):
        self.tmpdir.cleanup()

    def _setup_table(self):
        conn = sqlite3.connect(self.db_path)
        try:
            _create_prices_table(conn, "Stock_Prices")
        finally:
            conn.close()

    def _read_prices(self):
        conn = sqlite3.connect(self.db_path)
        try:
            return pd.read_sql_query(
                "SELECT Date, Ticker, Currency, Price FROM Stock_Prices ORDER BY Date, Ticker",
                conn,
            )
        finally:
            conn.close()

    def _sample_fx_df(self):
        return _transform_ecb_fx_to_prices(pd.DataFrame({
            "Date": ["2024-01-01", "2024-01-02"],
            "USD": [1.10, 1.11],
            "JPY": [155.0, 156.0],
        }))

    def _sample_inflation_df(self):
        return pd.DataFrame({
            "Date": ["2024-01-01"],
            "Ticker": ["Inflation_USD"],
            "Currency": ["USD"],
            "Price": [308.417],
        })

    @patch("src.orchestrator.update_fx_data.update_fx_data._fetch_all_inflation_prices")
    @patch("src.orchestrator.update_fx_data.update_fx_data._fetch_ecb_fx_prices")
    def test_inserts_both_fx_and_inflation(self, mock_fx, mock_inflation):
        mock_fx.return_value = self._sample_fx_df()
        mock_inflation.return_value = self._sample_inflation_df()
        self._setup_table()

        result = update_fx_data(self.db_path, "Stock_Prices")

        self.assertEqual(result, {"fx": 4, "inflation": 1})

        prices = self._read_prices()
        self.assertEqual(len(prices), 5)
        # FX data: Ticker=EUR, Currency=target
        fx_currencies = set(prices[prices["Ticker"] == "EUR"]["Currency"])
        self.assertEqual(fx_currencies, {"USD", "JPY"})
        # Inflation data: Ticker starts with "Inflation_"
        inf = prices[prices["Ticker"].str.startswith("Inflation_")]
        inf_tickers = set(inf["Ticker"])
        self.assertEqual(inf_tickers, {"Inflation_USD"})

    @patch("src.orchestrator.update_fx_data.update_fx_data._fetch_all_inflation_prices")
    @patch("src.orchestrator.update_fx_data.update_fx_data._fetch_ecb_fx_prices")
    def test_dedup_across_runs(self, mock_fx, mock_inflation):
        mock_fx.return_value = self._sample_fx_df()
        mock_inflation.return_value = self._sample_inflation_df()
        self._setup_table()

        first = update_fx_data(self.db_path, "Stock_Prices")
        self.assertEqual(first, {"fx": 4, "inflation": 1})

        second = update_fx_data(self.db_path, "Stock_Prices")
        self.assertEqual(second, {"fx": 0, "inflation": 0})

        prices = self._read_prices()
        self.assertEqual(len(prices), 5)

    @patch("src.orchestrator.update_fx_data.update_fx_data._fetch_all_inflation_prices")
    @patch("src.orchestrator.update_fx_data.update_fx_data._fetch_ecb_fx_prices")
    def test_handles_empty_inflation(self, mock_fx, mock_inflation):
        mock_fx.return_value = self._sample_fx_df()
        mock_inflation.return_value = pd.DataFrame(
            columns=["Date", "Ticker", "Currency", "Price"]
        )
        self._setup_table()

        result = update_fx_data(self.db_path, "Stock_Prices")

        self.assertEqual(result, {"fx": 4, "inflation": 0})
        prices = self._read_prices()
        self.assertEqual(len(prices), 4)

    @patch("src.orchestrator.update_fx_data.update_fx_data._fetch_all_inflation_prices")
    @patch("src.orchestrator.update_fx_data.update_fx_data._fetch_ecb_fx_prices")
    def test_creates_table_when_not_exists(self, mock_fx, mock_inflation):
        mock_fx.return_value = self._sample_fx_df()
        mock_inflation.return_value = pd.DataFrame(
            columns=["Date", "Ticker", "Currency", "Price"]
        )

        update_fx_data(self.db_path, "Stock_Prices")

        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='Stock_Prices'"
            )
            self.assertIsNotNone(cursor.fetchone())
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
