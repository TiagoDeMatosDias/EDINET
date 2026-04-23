import unittest
from unittest.mock import patch, MagicMock, mock_open
import base64
import io
import json
import os
import sys
import sqlite3
import pandas as pd
import tempfile
import zipfile
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.orchestrator.common.edinet import Edinet


def _make_edinet(**overrides):
    """Create an Edinet with test defaults; override any kwarg."""
    defaults = {
        "base_url": "http://test.com",
        "api_key": "test_key",
        "db_path": "dummy.db",
        "raw_docs_path": "test_location",
        "doc_list_table": "DocumentList",
        "company_info_table": "companyInfo",
        "taxonomy_table": "taxonomy",
    }
    defaults.update(overrides)
    return Edinet(**defaults)


def _build_mock_edinet_companyinfo_page(gx_state):
    escaped = json.dumps(gx_state).replace('"', '&quot;')
    return f"<html><body><input type=\"hidden\" name=\"GXState\" value='{escaped}' /></body></html>"


def _build_mock_edinet_download_response(zip_bytes):
    encoded = base64.b64encode(zip_bytes).decode("ascii")
    return {
        "gxProps": [
            {
                "TXTSCRIPT": {
                    "Caption": (
                        '<script type="text/javascript">'
                        f'const linkSource = "data:;base64,{encoded}";'
                        '</script>'
                    )
                }
            }
        ]
    }


def _build_mock_edinet_companyinfo_zip(csv_text, encoding="cp932"):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("EdinetcodeDlInfo.csv", csv_text.encode(encoding))
    return buffer.getvalue()


class TestEdinet(unittest.TestCase):

    OFFICIAL_ENGLISH_COMPANYINFO_CSV = "\n".join([
        "Date of download data creation,As Of 2026.04.21,Number of data,1",
        "EDINET Code,Type of Submitter,Listed company / Unlisted company,Consolidated / NonConsolidated,Capital stock,account closing date,Submitter Name,Submitter Name（alphabetic）,Submitter Name（phonetic）,Province,Submitter's industry,Securities Identification Code,Submitter's Japan Corporate Number",
        '"E00004","内国法人・組合","Listed company","Consolidated","1491","5.31","カネコ種苗株式会社","KANEKO SEEDS CO., LTD.","カネコシュビョウカブシキガイシャ","前橋市古市町一丁目５０番地１２","Fishery, Agriculture & Forestry","13760","5070001000715"',
    ])

    NORMALIZED_COMPANYINFO_CSV = "\n".join([
        "EdinetCode,Type of Submitter,Listed,Consolidated,Capital_Stock,Closing_Date,Submitter Name,Company_Name,Submitter Name（phonetic）,Province,Company_Industry,Company_Ticker,Company_Number",
        '"E00004","Domestic corporation","Listed company","Consolidated","1491","5.31","Kaneko Seeds Co Ltd","KANEKO SEEDS CO., LTD.","KANEKO SHUBYO KABUSHIKIGAISHA","Maebashi","Fishery, Agriculture & Forestry","13760","5070001000715"',
    ])

    def setUp(self):
        pass

    @patch('src.orchestrator.common.edinet.requests.get')
    @patch('src.orchestrator.common.edinet.sqlite3.connect')
    def test_get_All_documents_withMetadata(self, mock_sqlite_connect, mock_requests_get):
        self.edinet = _make_edinet()

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
        mock_requests_get.assert_called_with(
            f"{self.edinet.baseURL}.json?date=2025-01-01&type=2&Subscription-Key={self.edinet.key}",
            timeout=self.edinet.REQUEST_TIMEOUT_SECONDS,
        )

        # Assert that the database was called correctly
        mock_sqlite_connect.assert_called_with(self.edinet.Database)
        mock_cursor.execute.assert_any_call(f"SELECT COUNT(*) FROM \"{self.edinet.DB_DOC_LIST_TABLE}\" WHERE docID = ?", ('doc1',))
        self.assertEqual(mock_conn.commit.call_count, 1)
        mock_conn.close.assert_called_once()

    @patch('src.orchestrator.common.edinet.requests.get')
    @patch('builtins.open', new_callable=mock_open)
    def test_downloadDoc(self, mock_open_file, mock_requests_get):
        self.edinet = _make_edinet()

        # Mock the requests response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b'zip_content'
        mock_requests_get.return_value = mock_response

        # Call the method
        docID = "test_doc_id"
        was_downloaded = self.edinet.downloadDoc(docID)

        # Assert that the file was opened and written to correctly
        expected_path = os.path.join(self.edinet.defaultLocation, docID + '.zip')
        mock_open_file.assert_called_with(expected_path, 'wb')
        mock_open_file().write.assert_called_with(b'zip_content')
        self.assertTrue(was_downloaded)

    @patch('src.orchestrator.common.edinet.Edinet.query_database_select')
    @patch('src.orchestrator.common.edinet.Edinet.create_folder')
    @patch('src.orchestrator.common.edinet.Edinet.downloadDoc')
    @patch('src.orchestrator.common.edinet.Edinet.list_files_in_folder')
    @patch('src.orchestrator.common.edinet.Edinet.unzip_files')
    @patch('src.orchestrator.common.edinet.Edinet.load_financial_data')
    @patch('src.orchestrator.common.edinet.Edinet.query_database_setColumn')
    @patch('src.orchestrator.common.edinet.Edinet.delete_folder')
    @patch('src.orchestrator.common.edinet.sqlite3.connect')
    @patch('src.orchestrator.common.edinet.zipfile.is_zipfile', return_value=True)
    def test_downloadDocs(self, mock_is_zipfile, mock_sqlite_connect, mock_delete_folder, mock_query_database_setColumn, mock_load_financial_data, mock_unzip_files, mock_list_files_in_folder, mock_downloadDoc, mock_create_folder, mock_query_database_select):
        self.edinet = _make_edinet()

        # Mock the database connection
        mock_conn = MagicMock()
        mock_sqlite_connect.return_value = mock_conn

        # Mock the query_database_select to return a single document
        mock_query_database_select.return_value = [{"docID": "doc1"}]

        # list_files_in_folder is called twice: once for the zip files, once for unzipped
        mock_list_files_in_folder.side_effect = [
            ["fake/doc1.zip"],       # zipped files
            ["fake/unzipped/f.csv"], # financial files
        ]

        # Call the method
        self.edinet.downloadDocs("input_table")

        # Assert that the correct methods were called
        mock_query_database_select.assert_called_once()
        mock_create_folder.assert_called_once()
        mock_downloadDoc.assert_called_once_with("doc1", os.path.join(self.edinet.defaultLocation, "downloadeddocs", "doc1"))
        mock_list_files_in_folder.assert_called()
        mock_unzip_files.assert_called_once()
        mock_load_financial_data.assert_called_once()
        self.assertEqual(mock_query_database_setColumn.call_args[0][3], self.edinet.STATUS_DOWNLOADED)
        mock_delete_folder.assert_called()

    @patch('src.orchestrator.common.edinet.Edinet.query_database_select')
    @patch('src.orchestrator.common.edinet.Edinet.create_folder')
    @patch('src.orchestrator.common.edinet.Edinet.downloadDoc', return_value=False)
    @patch('src.orchestrator.common.edinet.Edinet.list_files_in_folder')
    @patch('src.orchestrator.common.edinet.Edinet.query_database_setColumn')
    @patch('src.orchestrator.common.edinet.Edinet.delete_folder')
    @patch('src.orchestrator.common.edinet.sqlite3.connect')
    def test_downloadDocs_sets_checked_unavailable_when_download_fails(self, mock_sqlite_connect, mock_delete_folder, mock_query_database_setColumn, mock_list_files_in_folder, mock_downloadDoc, mock_create_folder, mock_query_database_select):
        self.edinet = _make_edinet()

        mock_conn = MagicMock()
        mock_sqlite_connect.return_value = mock_conn
        mock_query_database_select.return_value = [{"docID": "doc1"}]

        self.edinet.downloadDocs("input_table")

        self.assertEqual(mock_query_database_setColumn.call_args[0][3], self.edinet.STATUS_CHECKED_UNAVAILABLE)
        mock_list_files_in_folder.assert_not_called()

    @patch('src.orchestrator.common.edinet.Edinet.query_database_select')
    @patch('src.orchestrator.common.edinet.Edinet.create_folder', side_effect=Exception("boom"))
    @patch('src.orchestrator.common.edinet.Edinet.query_database_setColumn')
    @patch('src.orchestrator.common.edinet.Edinet.delete_folder')
    @patch('src.orchestrator.common.edinet.sqlite3.connect')
    def test_downloadDocs_sets_checked_error_on_exception(self, mock_sqlite_connect, mock_delete_folder, mock_query_database_setColumn, mock_create_folder, mock_query_database_select):
        self.edinet = _make_edinet()

        mock_conn = MagicMock()
        mock_sqlite_connect.return_value = mock_conn
        mock_query_database_select.return_value = [{"docID": "doc1"}]

        self.edinet.downloadDocs("input_table")

        self.assertEqual(mock_query_database_setColumn.call_args[0][3], self.edinet.STATUS_CHECKED_ERROR)
        mock_conn.close.assert_called()

    @patch('src.orchestrator.common.edinet.Edinet.query_database_select')
    @patch('src.orchestrator.common.edinet.Edinet.create_folder')
    @patch('src.orchestrator.common.edinet.Edinet.downloadDoc')
    @patch('src.orchestrator.common.edinet.Edinet.delete_folder')
    def test_downloadDocs_handles_none_result(self, mock_delete_folder, mock_downloadDoc, mock_create_folder, mock_query_database_select):
        self.edinet = _make_edinet()

        # Simulate query failure path where select returns None
        mock_query_database_select.return_value = None

        # Should not raise TypeError on len(None)
        self.edinet.downloadDocs("input_table")

        # No download work should start
        mock_create_folder.assert_not_called()
        mock_downloadDoc.assert_not_called()
        mock_delete_folder.assert_not_called()

    @patch('src.orchestrator.common.edinet.pd.read_csv')
    @patch('src.orchestrator.common.edinet.Edinet.detect_file_encoding')
    @patch('src.orchestrator.common.edinet.sqlite3.connect')
    @patch('pandas.DataFrame.to_sql')
    def test_load_financial_data(self, mock_to_sql, mock_sqlite_connect, mock_detect_encoding, mock_read_csv):
        self.edinet = _make_edinet()

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

    def test_load_edinet_code_dataframe_from_path_preserves_normalized_csv_schema(self):
        self.edinet = _make_edinet(company_info_table="companyInfo")
        with tempfile.NamedTemporaryFile("wb", suffix=".csv", delete=False) as handle:
            handle.write(self.NORMALIZED_COMPANYINFO_CSV.encode("cp932"))
            csv_path = handle.name

        self.addCleanup(lambda: os.path.exists(csv_path) and os.remove(csv_path))

        df = self.edinet._load_edinet_code_dataframe_from_path(csv_path)

        self.assertEqual(df.columns.tolist(), list(self.edinet.EDINET_CODE_LIST_TARGET_COLUMNS))
        self.assertEqual(df.iloc[0]["EdinetCode"], "E00004")
        self.assertEqual(df.iloc[0]["Company_Name"], "KANEKO SEEDS CO., LTD.")
        self.assertEqual(df.iloc[0]["Company_Ticker"], "13760")

    @patch('src.orchestrator.common.edinet.requests.Session')
    def test_load_edinet_code_dataframe_downloads_official_english_zip_when_csv_not_provided(self, mock_session_cls):
        self.edinet = _make_edinet(company_info_table="companyInfo")
        gx_state = {
            "vPGMNAME": "WEEE0020",
            "vPGMDESC": "EDINET TAXONOMY&CODE LIST",
            "gxhash_vPGMNAME": "hash-name",
            "gxhash_vPGMDESC": "hash-desc",
            "GX_AJAX_IV": "ABCDEF0123456789",
            "AJAX_SECURITY_TOKEN": "ajax-security-token",
            "GX_AUTH_WEEE0020": "page-auth-token",
        }
        zip_bytes = _build_mock_edinet_companyinfo_zip(self.OFFICIAL_ENGLISH_COMPANYINFO_CSV)

        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session

        mock_page_response = MagicMock()
        mock_page_response.text = _build_mock_edinet_companyinfo_page(gx_state)
        mock_page_response.raise_for_status.return_value = None

        mock_download_response = MagicMock()
        mock_download_response.raise_for_status.return_value = None
        mock_download_response.json.return_value = _build_mock_edinet_download_response(zip_bytes)

        mock_session.get.return_value = mock_page_response
        mock_session.post.return_value = mock_download_response

        df = self.edinet._load_edinet_code_dataframe("")

        mock_session.get.assert_called_once_with(
            self.edinet.EDINET_CODE_LIST_PAGE_URLS["en"],
            timeout=self.edinet.REQUEST_TIMEOUT_SECONDS,
        )
        self.assertTrue(mock_session.post.called)

        post_url = mock_session.post.call_args.args[0]
        self.assertTrue(post_url.startswith(self.edinet.EDINET_CODE_LIST_PAGE_URLS["en"] + "?abcdef0123456789,gx-no-cache="))

        payload = json.loads(mock_session.post.call_args.kwargs["data"])
        self.assertEqual(payload["events"], [self.edinet.EDINET_CODE_LIST_DOWNLOAD_EVENT])
        self.assertEqual(payload["parms"], [gx_state["vPGMNAME"], gx_state["vPGMDESC"]])

        headers = mock_session.post.call_args.kwargs["headers"]
        self.assertEqual(headers["Content-Type"], "application/json")
        self.assertEqual(headers["Accept"], "*/*")
        self.assertEqual(headers["GXAjaxRequest"], "1")
        self.assertEqual(headers["AJAX_SECURITY_TOKEN"], gx_state["AJAX_SECURITY_TOKEN"])
        self.assertEqual(headers["X-GXAuth-Token"], gx_state["GX_AUTH_WEEE0020"])
        self.assertEqual(headers["Origin"], "https://disclosure2.edinet-fsa.go.jp")
        self.assertEqual(headers["Referer"], self.edinet.EDINET_CODE_LIST_PAGE_URLS["en"])

        self.assertEqual(df.columns.tolist(), list(self.edinet.EDINET_CODE_LIST_TARGET_COLUMNS))
        self.assertEqual(df.iloc[0]["EdinetCode"], "E00004")
        self.assertEqual(df.iloc[0]["Company_Name"], "KANEKO SEEDS CO., LTD.")
        self.assertEqual(df.iloc[0]["Company_Ticker"], "13760")
        self.assertEqual(df.iloc[0]["Company_Industry"], "Fishery, Agriculture & Forestry")

    @patch('src.orchestrator.common.edinet.sqlite3.connect')
    @patch('pandas.DataFrame.to_sql')
    @patch('src.orchestrator.common.edinet.Edinet._load_edinet_code_dataframe')
    def test_store_edinetCodes_uses_target_database(self, mock_load_dataframe, mock_to_sql, mock_sqlite_connect):
        self.edinet = _make_edinet(company_info_table="companyInfo")
        mock_load_dataframe.return_value = pd.DataFrame({"A": [1]})
        mock_conn = MagicMock()
        mock_sqlite_connect.return_value = mock_conn

        self.edinet.store_edinetCodes("codes.csv", target_database="custom.db")

        mock_load_dataframe.assert_called_once_with("codes.csv")
        mock_sqlite_connect.assert_called_with("custom.db")
        mock_to_sql.assert_called_with("companyInfo", mock_conn, if_exists="replace", index=False)


if __name__ == '__main__':
    unittest.main()
