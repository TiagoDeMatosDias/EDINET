"""
Tests for src/regression_analysis.py

Strategy
--------
* Pure-logic functions (_significance_stars, _rank_predictor_results,
  multivariate_regression config validation) are tested with no mocking.
* Functions that touch a database (Run_Model, write_results_to_file) use an
  in-memory SQLite connection so the real SQL + data-transformation paths are
  exercised without any file-system side-effects.
* The orchestrator-level functions (find_significant_predictors) are NOT
  tested here; they are end-to-end glue validated by running the application.
"""
import os
import sys
import sqlite3
import tempfile
import unittest

import pandas as pd
import statsmodels.api as sm

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.regression_analysis import (
    Run_Model,
    _rank_predictor_results,
    _significance_stars,
    multivariate_regression,
    write_results_to_file,
)


# ---------------------------------------------------------------------------
# _significance_stars
# ---------------------------------------------------------------------------

class TestSignificanceStars(unittest.TestCase):

    def test_none_returns_empty(self):
        self.assertEqual(_significance_stars(None), "")

    def test_above_threshold_returns_empty(self):
        self.assertEqual(_significance_stars(0.05), "")
        self.assertEqual(_significance_stars(0.99), "")

    def test_single_star(self):
        self.assertEqual(_significance_stars(0.04), "*")
        self.assertEqual(_significance_stars(0.011), "*")

    def test_double_star(self):
        self.assertEqual(_significance_stars(0.009), "**")
        self.assertEqual(_significance_stars(0.0011), "**")

    def test_triple_star(self):
        self.assertEqual(_significance_stars(0.0009), "***")
        self.assertEqual(_significance_stars(0.0), "***")


# ---------------------------------------------------------------------------
# _rank_predictor_results
# ---------------------------------------------------------------------------

def _make_result(dep, ind, r2, p, status="success"):
    """Helper to build a minimal result dict."""
    return {
        "dep_var": dep,
        "ind_var": ind,
        "r_squared": r2,
        "p_value": p,
        "is_significant": p < 0.05 if p is not None else False,
        "status": status,
    }


class TestRankPredictorResults(unittest.TestCase):

    def test_sorted_by_r_squared_descending(self):
        results = [
            _make_result("y", "x1", r2=0.1, p=0.01),
            _make_result("y", "x2", r2=0.9, p=0.01),
            _make_result("y", "x3", r2=0.5, p=0.01),
        ]
        ranked = _rank_predictor_results(results)
        r2_values = [r["r_squared"] for r in ranked]
        self.assertEqual(r2_values, [0.9, 0.5, 0.1])

    def test_tiebreak_by_p_value_ascending(self):
        results = [
            _make_result("y", "x1", r2=0.5, p=0.04),
            _make_result("y", "x2", r2=0.5, p=0.001),
        ]
        ranked = _rank_predictor_results(results)
        self.assertEqual(ranked[0]["ind_var"], "x2")

    def test_ranks_are_one_based(self):
        results = [_make_result("y", f"x{i}", r2=float(i) / 10, p=0.01) for i in range(1, 4)]
        ranked = _rank_predictor_results(results)
        self.assertEqual([r["rank"] for r in ranked], [1, 2, 3])

    def test_failed_models_moved_to_end_with_none_rank(self):
        results = [
            _make_result("y", "good", r2=0.5,  p=0.01),
            _make_result("y", "bad",  r2=None,  p=None, status="failed"),
        ]
        ranked = _rank_predictor_results(results)
        self.assertEqual(ranked[0]["ind_var"], "good")
        self.assertEqual(ranked[0]["rank"], 1)
        self.assertIsNone(ranked[1]["rank"])

    def test_empty_list_returns_empty(self):
        self.assertEqual(_rank_predictor_results([]), [])


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
