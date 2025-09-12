
import unittest
from unittest.mock import patch, MagicMock, mock_open
import os
import sys
import sqlite3
import pandas as pd
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.edinet_api import Edinet

class TestEdinet(unittest.TestCase):

    def setUp(self):
        self.edinet = Edinet()

    @patch('src.edinet_api.requests.get')
    @patch('src.edinet_api.sqlite3.connect')
    def test_get_All_documents_withMetadata(self, mock_sqlite_connect, mock_requests_get):
        # Mock the requests response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "results": [
                {
                    "docID": "doc1",
                    "submitDateTime": "2025-01-01 10:00",
                    "docTypeCode": "120",
                    "secCode": "12345",
                    "csvFlag": "1"
                }
            ]
        }
        mock_requests_get.return_value = mock_response

        # Mock the database connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_sqlite_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = (0,)

        # Call the method
        self.edinet.get_All_documents_withMetadata(start_date="2025-01-01", end_date="2025-01-01")

        # Assert that the requests.get was called with the correct URL
        mock_requests_get.assert_called_with(f"{self.edinet.baseURL}.json?date=2025-01-01&type=2&Subscription-Key={self.edinet.key}")

        # Assert that the database was called correctly
        mock_sqlite_connect.assert_called_with(self.edinet.Database)
        mock_cursor.execute.assert_any_call("SELECT COUNT(*) FROM DocumentList WHERE docID = ?", ('doc1',))
        mock_cursor.execute.assert_any_call("INSERT INTO DocumentList VALUES (?, ?, ?, ?, ?, ?)", ('doc1', '2025-01-01 10:00', '120', '12345', '1', 'False'))
        self.assertEqual(mock_conn.commit.call_count, 2)
        mock_conn.close.assert_called_once()

    @patch('src.edinet_api.requests.get')
    @patch('builtins.open', new_callable=mock_open)
    def test_downloadDoc(self, mock_open_file, mock_requests_get):
        # Mock the requests response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b'zip_content'
        mock_requests_get.return_value = mock_response

        # Call the method
        docID = "test_doc_id"
        self.edinet.downloadDoc(docID)

        # Assert that the file was opened and written to correctly
        expected_path = os.path.join(self.edinet.defaultLocation, docID + '.zip')
        mock_open_file.assert_called_with(expected_path, 'wb')
        mock_open_file().write.assert_called_with(b'zip_content')

    @patch('src.edinet_api.Edinet.query_database_select')
    @patch('src.edinet_api.Edinet.create_folder')
    @patch('src.edinet_api.Edinet.downloadDoc')
    @patch('src.edinet_api.Edinet.list_files_in_folder')
    @patch('src.edinet_api.Edinet.unzip_files')
    @patch('src.edinet_api.Edinet.load_financial_data')
    @patch('src.edinet_api.Edinet.query_database_setColumn')
    @patch('src.edinet_api.Edinet.delete_folder')
    @patch('src.edinet_api.sqlite3.connect')
    def test_downloadDocs(self, mock_sqlite_connect, mock_delete_folder, mock_query_database_setColumn, mock_load_financial_data, mock_unzip_files, mock_list_files_in_folder, mock_downloadDoc, mock_create_folder, mock_query_database_select):
        # Mock the database connection
        mock_conn = MagicMock()
        mock_sqlite_connect.return_value = mock_conn

        # Mock the query_database_select to return a single document
        mock_query_database_select.return_value = [{"docID": "doc1"}]

        # Call the method
        self.edinet.downloadDocs("input_table")

        # Assert that the correct methods were called
        mock_query_database_select.assert_called_once()
        mock_create_folder.assert_called_once()
        mock_downloadDoc.assert_called_once_with("doc1", os.path.join(self.edinet.defaultLocation, "downloadeddocs", "doc1"))
        mock_list_files_in_folder.assert_called()
        mock_unzip_files.assert_called_once()
        mock_load_financial_data.assert_called_once()
        mock_query_database_setColumn.assert_called_once()
        mock_delete_folder.assert_called()

    @patch('src.edinet_api.pd.read_csv')
    @patch('src.edinet_api.Edinet.detect_file_encoding')
    @patch('src.edinet_api.sqlite3.connect')
    @patch('pandas.DataFrame.to_sql')
    def test_load_financial_data(self, mock_to_sql, mock_sqlite_connect, mock_detect_encoding, mock_read_csv):
        # Mock the database connection
        mock_conn = MagicMock()
        mock_sqlite_connect.return_value = mock_conn

        # Mock the detect_file_encoding to return a dummy encoding
        mock_detect_encoding.return_value = 'utf-8'

        # Mock the pandas read_csv to return a dummy dataframe
        mock_df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
        mock_read_csv.return_value = mock_df

        # Call the method
        doc = {"docID": "doc1", "edinetCode": "E12345", "docTypeCode": "120", "submitDateTime": "2025-01-01", "periodStart": "2024-01-01", "periodEnd": "2024-12-31"}
        self.edinet.load_financial_data(["file1.csv"], "output_table", doc, mock_conn)

        # Assert that the to_sql method was called on the dataframe
        mock_to_sql.assert_called()


if __name__ == '__main__':
    unittest.main()
