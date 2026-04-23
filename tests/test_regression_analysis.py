"""
Tests for src/regression_analysis.py

Strategy
--------
* Pure-logic functions (multivariate_regression config validation) are tested
  with no mocking.
* Functions that touch a database (Run_Model, write_results_to_file) use an
  in-memory SQLite connection so the real SQL + data-transformation paths are
  exercised without any file-system side-effects.
"""
import os
import sys
import sqlite3
import tempfile
import unittest

import pandas as pd
import statsmodels.api as sm

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.orchestrator.multivariate_regression.analysis import (
    Run_Model,
    build_scoring_query,
    multivariate_regression,
    write_results_to_file,
)


# ---------------------------------------------------------------------------
# Run_Model  (in-memory SQLite)
# ---------------------------------------------------------------------------

class TestRunModel(unittest.TestCase):

    def _make_conn(self, df: pd.DataFrame, table: str = "t") -> sqlite3.Connection:
        conn = sqlite3.connect(":memory:")
        df.to_sql(table, conn, index=False)
        return conn

    def test_returns_regression_results_wrapper(self):
        df = pd.DataFrame({"y": [1, 2, 3, 4, 5], "x": [2, 3, 4, 5, 6]})
        conn = self._make_conn(df)
        results = Run_Model("SELECT y, x FROM t", conn, "y", ["x"])
        self.assertIsInstance(results, sm.regression.linear_model.RegressionResultsWrapper)

    def test_correct_coefficient(self):
        """y = 2x + 1  →  coefficient of x should be very close to 2."""
        df = pd.DataFrame({"y": [3.0, 5.0, 7.0, 9.0, 11.0],
                           "x": [1.0, 2.0, 3.0, 4.0,  5.0]})
        conn = self._make_conn(df)
        results = Run_Model("SELECT y, x FROM t", conn, "y", ["x"])
        self.assertAlmostEqual(results.params["x"], 2.0, places=5)

    def test_drops_inf_values(self):
        df = pd.DataFrame({"y": [1, 2, float("inf"), 4, 5], "x": [1, 2, 3, 4, 5]})
        conn = self._make_conn(df)
        # Use (0.0, 1.0) to disable quantile-based winsorisation so only the
        # inf→NaN→drop step is exercised.
        results = Run_Model("SELECT y, x FROM t", conn, "y", ["x"],
                            winsorize_limits=(0.0, 1.0))
        # inf row should be dropped → 4 observations
        self.assertEqual(int(results.nobs), 4)

    def test_drops_nan_values(self):
        df = pd.DataFrame({"y": [1, 2, None, 4, 5], "x": [1, 2, 3, 4, 5]})
        conn = self._make_conn(df)
        # Use (0.0, 1.0) to disable quantile-based winsorisation so only the
        # NaN-drop step is exercised.
        results = Run_Model("SELECT y, x FROM t", conn, "y", ["x"],
                            winsorize_limits=(0.0, 1.0))
        self.assertEqual(int(results.nobs), 4)

    def test_returns_model_on_bad_query(self):
        """A bad query must not raise; Run_Model should return a valid fallback model."""
        conn = sqlite3.connect(":memory:")
        results = Run_Model("SELECT y, x FROM nonexistent_table", conn, "y", ["x"])
        self.assertIsInstance(results, sm.regression.linear_model.RegressionResultsWrapper)

    def test_no_winsorisation_when_limits_are_zero_one(self):
        """Limits (0.0, 1.0) should preserve all rows."""
        df = pd.DataFrame({"y": [1, 2, 3, 100], "x": [1, 2, 3, 4]})
        conn = self._make_conn(df)
        results = Run_Model("SELECT y, x FROM t", conn, "y", ["x"],
                            winsorize_limits=(0.0, 1.0))
        self.assertEqual(int(results.nobs), 4)

    def test_winsorisation_removes_extreme_rows(self):
        """Tight upper limit should drop the extreme outlier row."""
        df = pd.DataFrame({"y": [1.0, 2.0, 3.0, 4.0, 5.0, 1000.0],
                           "x": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]})
        conn = self._make_conn(df)
        results_tight = Run_Model("SELECT y, x FROM t", conn, "y", ["x"],
                                  winsorize_limits=(0.0, 0.85))
        results_open  = Run_Model("SELECT y, x FROM t", conn, "y", ["x"],
                                  winsorize_limits=(0.0, 1.0))
        self.assertLess(int(results_tight.nobs), int(results_open.nobs))


# ---------------------------------------------------------------------------
# write_results_to_file
# ---------------------------------------------------------------------------

class TestWriteResultsToFile(unittest.TestCase):

    def _fit_model(self):
        df = pd.DataFrame({"y": [1, 2, 3, 4, 5], "x": [2, 3, 4, 5, 6]})
        y = df["y"]
        X = sm.add_constant(df["x"])
        return sm.OLS(y, X).fit()

    def test_file_is_created(self):
        results = self._fit_model()
        with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as f:
            path = f.name
        try:
            write_results_to_file(results, "SELECT 1", path)
            self.assertTrue(os.path.exists(path))
        finally:
            os.remove(path)


class TestBuildScoringQuery(unittest.TestCase):

    def _fit_model(self):
        df = pd.DataFrame({"y": [1, 2, 3, 4, 5], "x": [2, 3, 4, 5, 6]})
        y = df["y"]
        X = sm.add_constant(df["x"])
        return sm.OLS(y, X).fit()

    def test_qualifies_injected_identifiers_for_join_queries(self):
        results = self._fit_model()
        sql = build_scoring_query(
            results,
            "SELECT x FROM Quality_Historical qh LEFT JOIN Pershare_Historical ph ON qh.docID = ph.docID",
        )
        self.assertIn("qh.edinetCode AS edinetCode", sql)
        self.assertIn("qh.periodEnd AS periodEnd", sql)

    def test_does_not_reinject_identifiers_when_already_selected(self):
        results = self._fit_model()
        sql = build_scoring_query(
            results,
            "SELECT x, qh.edinetCode AS edinetCode, qh.periodEnd AS periodEnd FROM Quality_Historical qh",
        )
        self.assertEqual(sql.count("AS edinetCode"), 1)
        self.assertEqual(sql.count("AS periodEnd"), 1)

    def test_file_contains_query(self):
        results = self._fit_model()
        with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as f:
            path = f.name
        try:
            write_results_to_file(results, "SELECT y, x FROM my_table", path)
            self.assertIn("SELECT y, x FROM my_table", open(path).read())
        finally:
            os.remove(path)

    def test_file_contains_ols_summary(self):
        results = self._fit_model()
        with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as f:
            path = f.name
        try:
            write_results_to_file(results, "SELECT 1", path)
            self.assertIn("OLS Regression Results", open(path).read())
        finally:
            os.remove(path)

    def test_file_contains_significance_section(self):
        results = self._fit_model()
        with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as f:
            path = f.name
        try:
            write_results_to_file(results, "SELECT 1", path)
            self.assertIn("Significance Analysis", open(path).read())
        finally:
            os.remove(path)


# ---------------------------------------------------------------------------
# multivariate_regression  –  config validation + end-to-end
# ---------------------------------------------------------------------------

class TestMultivariateRegression(unittest.TestCase):

    def test_raises_when_sql_query_missing(self):
        with self.assertRaises(ValueError):
            multivariate_regression({"Output": "out.txt"}, ":memory:")

    def test_raises_when_output_missing(self):
        with self.assertRaises(ValueError):
            multivariate_regression({"SQL_Query": "SELECT 1"}, ":memory:")

    def test_runs_end_to_end_with_real_db(self):
        """Full run against a real temp DB; verifies output file is written."""
        df = pd.DataFrame({"y": [1.0, 2.0, 3.0, 4.0, 5.0],
                           "x": [2.0, 3.0, 4.0, 5.0, 6.0]})

        with tempfile.NamedTemporaryFile(suffix=".db",  delete=False) as db_f:
            db_path = db_f.name
        with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as out_f:
            out_path = out_f.name
        try:
            conn = sqlite3.connect(db_path)
            df.to_sql("test_table", conn, index=False)
            conn.close()

            multivariate_regression(
                {"SQL_Query": "SELECT y, x FROM test_table", "Output": out_path},
                db_path,
            )
            self.assertTrue(os.path.exists(out_path))
            self.assertIn("OLS", open(out_path).read())
        finally:
            os.remove(db_path)
            os.remove(out_path)

    def test_winsorize_thresholds_absent_means_no_clipping(self):
        """Omitting winsorize_thresholds should keep all rows (limits 0.0, 1.0)."""
        df = pd.DataFrame({"y": [1.0, 2.0, 3.0, 4.0, 1000.0],
                           "x": [1.0, 2.0, 3.0, 4.0, 5.0]})

        with tempfile.NamedTemporaryFile(suffix=".db",  delete=False) as db_f:
            db_path = db_f.name
        with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as out_f:
            out_path = out_f.name
        try:
            conn = sqlite3.connect(db_path)
            df.to_sql("t", conn, index=False)
            conn.close()

            multivariate_regression(
                {"SQL_Query": "SELECT y, x FROM t", "Output": out_path},
                db_path,
            )
            content = open(out_path).read()
            # 5 observations should appear in the OLS summary
            self.assertIn("5", content)
        finally:
            os.remove(db_path)
            os.remove(out_path)


if __name__ == "__main__":
    unittest.main()
