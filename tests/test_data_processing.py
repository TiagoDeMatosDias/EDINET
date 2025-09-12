
import unittest
from unittest.mock import patch, MagicMock
import os
import sys
import pandas as pd
import numpy as np
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.data_processing import data

class TestData(unittest.TestCase):

    def setUp(self):
        self.data = data()

    @patch('src.data_processing.sqlite3.connect')
    def test_Generate_Financial_Ratios(self, mock_sqlite_connect):
        # Mock the database connection and pandas read_sql_query
        mock_conn = MagicMock()
        mock_sqlite_connect.return_value = mock_conn
        
        # Create a dummy dataframe to be returned by read_sql_query
        dummy_df = pd.DataFrame({
            'edinetCode': ['E12345'],
            'docID': ['doc1'],
            'docTypeCode': ['120'],
            'periodStart': ['2024-01-01'],
            'periodEnd': ['2024-12-31'],
            'AccountingTerm': ['jppfs_cor:NetSales'],
            'Period': ['CurrentYearDuration'],
            'Amount': [1000]
        })
        with patch('pandas.read_sql_query', return_value=dummy_df):
            with patch.object(self.data, 'get_companyList', return_value=['E12345']):
                with patch('pandas.DataFrame.to_sql') as mock_to_sql:
                    self.data.Generate_Financial_Ratios('input_table', 'output_table')

        # Assert that to_sql was called, indicating the function ran to completion
        mock_to_sql.assert_called()

    @patch('src.data_processing.sqlite3.connect')
    def test_Generate_Aggregated_Ratios(self, mock_sqlite_connect):
        # Mock the database connection and pandas read_sql_query
        mock_conn = MagicMock()
        mock_sqlite_connect.return_value = mock_conn

        # Create a dummy dataframe to be returned by read_sql_query
        dummy_df = pd.DataFrame({
            'edinetCode': ['E12345', 'E12345'],
            'periodStart': ['2023-01-01', '2024-01-01'],
            'periodEnd': ['2023-12-31', '2024-12-31'],
            'Ratio_Current': [1.5, 2.0],
            'MarketCap': [100, 120],
            'PerShare_BookValue': [10, 12]
        })
        with patch('pandas.read_sql_query', side_effect=[dummy_df, pd.DataFrame()]) as mock_read_sql:
            with patch('pandas.DataFrame.to_sql') as mock_to_sql:
                self.data.Generate_Aggregated_Ratios('input_table', 'output_table')

        # Assert that to_sql was called, indicating the function ran to completion
        mock_to_sql.assert_called()

    @patch('src.data_processing.sqlite3.connect')
    def test_copy_table_to_Standard(self, mock_sqlite_connect):
        # Mock the database connection
        mock_conn = MagicMock()
        mock_sqlite_connect.return_value = mock_conn
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        # Call the method
        mock_copy_table = patch.object(self.data, 'copy_table').start()
        mock_rename = patch.object(self.data, 'rename_columns_to_Standard').start()
        mock_filter = patch.object(self.data, 'Filter_for_Relevant').start()
        mock_delete = patch.object(self.data, 'delete_table').start()

        self.data.copy_table_to_Standard('source_table', 'target_table', mock_conn)

        # Assert that the helper methods were called
        mock_copy_table.assert_called_once()
        mock_rename.assert_called_once()
        mock_filter.assert_called_once()
        mock_delete.assert_called_once()

        patch.stopall()

if __name__ == '__main__':
    unittest.main()
