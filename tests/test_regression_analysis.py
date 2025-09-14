
import unittest
from unittest.mock import patch, MagicMock
import os
import sys
import pandas as pd
import numpy as np
import statsmodels.api as sm
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src import regression_analysis

class TestRegression(unittest.TestCase):

    @patch('os.getenv')
    @patch('src.regression_analysis.load_config')
    @patch('src.regression_analysis.sqlite3.connect')
    @patch('src.regression_analysis.Generate_SQL_Query')
    @patch('src.regression_analysis.Run_Model')
    @patch('src.regression_analysis.write_results_to_file')
    def test_Regression(self, mock_write_results, mock_run_model, mock_generate_sql, mock_sqlite_connect, mock_load_config, mock_getenv):
        # Mock getenv to return a test database path
        mock_getenv.return_value = "test.db"

        # Mock the config loaded from JSON
        mock_load_config.return_value = {"Output": "test_output.txt"}

        # Mock the SQL query generation
        mock_generate_sql.return_value = {
            "Query": "SELECT * FROM test",
            "DependentVariable": "y",
            "IndependentVariables": ["x1", "x2"]
        }

        # Mock the model run
        mock_run_model.return_value = sm.OLS(pd.Series([1, 2]), pd.DataFrame({'x1': [1, 2], 'x2': [3, 4]})).fit()

        # Call the function
        regression_analysis.Regression()

        # Assert that the functions were called
        mock_load_config.assert_called_once()
        mock_getenv.assert_called_once_with("DB_PATH")
        mock_sqlite_connect.assert_called_once_with("test.db")
        mock_generate_sql.assert_called_once()
        mock_run_model.assert_called_once()
        mock_write_results.assert_called_once()

    def test_Run_Model(self):
        # Create a dummy dataframe
        df = pd.DataFrame({
            'y': [1, 2, 3, 4, 5],
            'x1': [2, 3, 4, 5, 6],
            'x2': [3, 4, 5, 6, 7]
        })
        conn = MagicMock()

        # Mock the read_sql_query
        with patch('pandas.read_sql_query', return_value=df):
            results = regression_analysis.Run_Model("query", conn, 'y', ['x1', 'x2'])

        # Check that the results are of the correct type
        self.assertIsInstance(results, sm.regression.linear_model.RegressionResultsWrapper)

    def test_Generate_SQL_Query(self):
        # Mock the config
        config = {
            "DependentVariable": {"Name": "y", "Formula": "y_formula"},
            "IndependentVariables": [
                {"Table_Alias": "t", "Name": "x1"},
                {"Table_Alias": "t", "Name": "x2"}
            ],
            "DB_Tables": [{"Name": "test_table", "Alias": "t"}],
            "NumberOfPeriods": 1
        }

        # Call the function
        query_data = regression_analysis.Generate_SQL_Query(config)

        # Check that the query is generated correctly
        self.assertIn("SELECT", query_data["Query"])
        self.assertIn("FROM", query_data["Query"])
        self.assertIn("WHERE", query_data["Query"])
        self.assertEqual(query_data["DependentVariable"], "y")
        self.assertEqual(query_data["IndependentVariables"], ["x1_0", "x2_0"])

if __name__ == '__main__':
    unittest.main()
