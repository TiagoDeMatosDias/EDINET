import requests
from datetime import datetime, timedelta
import logging
import src.utils as h
import sqlite3
import os
import zipfile
import chardet
import csv
import shutil
import base64
import io
import json
import pandas as pd
import re
import time
from html.parser import HTMLParser
from urllib.parse import urlsplit

logger = logging.getLogger(__name__)


class _HiddenInputValueParser(HTMLParser):
    """Extract a hidden input value from an HTML document."""

    def __init__(self, target_id):
        super().__init__()
        self.target_id = target_id
        self.value = None

    def handle_starttag(self, tag, attrs):
        if tag.lower() != "input" or self.value is not None:
            return
        attr_map = {key.lower(): value for key, value in attrs}
        if attr_map.get("id") == self.target_id or attr_map.get("name") == self.target_id:
            self.value = attr_map.get("value")

class Edinet:
    REQUEST_TIMEOUT_SECONDS = 30
    STATUS_PENDING = "False"
    STATUS_DOWNLOADED = "True"
    STATUS_CHECKED_UNAVAILABLE = "Checked_Unavailable"
    STATUS_CHECKED_ERROR = "Checked_Error"
    ALLOWED_FILTER_OPERATORS = {"=", "<", ">", "<=", ">=", "!=", "<>", "LIKE"}
    EDINET_CODE_LIST_PAGE_URLS = {
        "en": "https://disclosure2.edinet-fsa.go.jp/weee0020.aspx",
    }
    EDINET_CODE_LIST_DOWNLOAD_EVENT = "'DODOWNLOADEDINET'"
    EDINET_CODE_LIST_TARGET_COLUMNS = (
        "EdinetCode",
        "Type of Submitter",
        "Listed",
        "Consolidated",
        "Capital_Stock",
        "Closing_Date",
        "Submitter Name",
        "Company_Name",
        "Submitter Name（phonetic）",
        "Province",
        "Company_Industry",
        "Company_Ticker",
        "Company_Number",
    )
    EDINET_CODE_LIST_COLUMN_ALIASES = {
        "EdinetCode": ("EdinetCode", "EDINET Code", "edinetCode"),
        "Type of Submitter": ("Type of Submitter",),
        "Listed": ("Listed", "Listed company / Unlisted company"),
        "Consolidated": ("Consolidated", "Consolidated / NonConsolidated"),
        "Capital_Stock": ("Capital_Stock", "Capital stock"),
        "Closing_Date": ("Closing_Date", "account closing date"),
        "Submitter Name": ("Submitter Name",),
        "Company_Name": (
            "Company_Name",
            "Company Name",
            "Submitter Name（alphabetic）",
            "Submitter Name(alphabetic)",
        ),
        "Submitter Name（phonetic）": (
            "Submitter Name（phonetic）",
            "Submitter Name(phonetic)",
        ),
        "Province": ("Province",),
        "Company_Industry": ("Company_Industry", "Submitter's industry"),
        "Company_Ticker": ("Company_Ticker", "Securities Identification Code"),
        "Company_Number": (
            "Company_Number",
            "Submitter's Japan Corporate Number",
        ),
    }
    EDINET_CODE_LIST_ENCODING_CANDIDATES = (
        "cp932",
        "shift_jis",
        "utf-8-sig",
        "utf-8",
    )

    def __init__(self, base_url, api_key, db_path, raw_docs_path=None,
                 doc_list_table=None, company_info_table=None,
                 taxonomy_table=None):
        self.baseURL = base_url
        self.key = api_key
        self.defaultLocation = raw_docs_path
        self.Database = db_path
        self.DB_COMPANY_INFO_TABLE = company_info_table
        self.DB_TAXONOMY_TABLE = taxonomy_table
        self.DB_DOC_LIST_TABLE = doc_list_table

    def get_All_documents_withMetadata(self, start_date="2015-01-01", end_date=None):
        """
        Fetch all available document IDs for a given time period from the EDINET API and store them in the database.

        Args:
            start_date (str): The start date in YYYY-MM-DD format. Defaults to '2015-01-01'.
            end_date (str): The end date in YYYY-MM-DD format. Defaults to today's date.

        Returns:
            list: List of document IDs for the given date range.
        """
        
        if end_date is None:
            end_date = datetime.today().strftime("%Y-%m-%d")
        
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
        end_date = datetime.strptime(end_date, "%Y-%m-%d")
        
        current_date = start_date

        # Connect to the SQLite database
        conn = sqlite3.connect(self.Database)
        cursor = conn.cursor()
        
        while current_date <= end_date:
            date_str = current_date.strftime("%Y-%m-%d")
            url = f"{self.baseURL}.json?date={date_str}&type=2&Subscription-Key={self.key}"
            print("URL: " + url)
            try:
                response = requests.get(url, timeout=self.REQUEST_TIMEOUT_SECONDS)
                if response.status_code == 200:
                    data = response.json()

                    results = data.get("results") or []
                    if not isinstance(results, list):
                        print(f"Skipping date {date_str}: unexpected API payload for results.")
                        current_date += timedelta(days=1)
                        continue

                    if not results:
                        print(f"No documents found for date {date_str}.")
                        current_date += timedelta(days=1)
                        continue
                    else:
                        print(f"Found {len(results)} documents for date {date_str}.")

                        # Create the DB table if it doesn't exist
                        columns = list(results[0].keys())
                        columns.append("Downloaded")
                        
                        self.create_table(self.DB_DOC_LIST_TABLE, columns, conn)

                        # Insert documents into the database
                        for entry in results:
                            # Check if the document already exists in the table
                            quoted_table = self._quote_identifier(self.DB_DOC_LIST_TABLE)
                            cursor.execute(f"SELECT COUNT(*) FROM {quoted_table} WHERE docID = ?", (entry["docID"],))
                            if cursor.fetchone()[0] == 0:
                                entry["Downloaded"] = self.STATUS_PENDING
                                placeholders = ", ".join(["?" for _ in entry])
                                cursor.execute(f"INSERT INTO {quoted_table} VALUES ({placeholders})", tuple(entry.values()))
                        
                        conn.commit()
            
            except Exception as e:
                print(f"Error fetching data for {date_str}: {e}")
            current_date += timedelta(days=1)  # Move to the next day

        conn.close()

    def downloadDoc(self, docID, fileLocation=None, docTypeCode=None):
        """Download a single EDINET document and save it as a ZIP file.

        Args:
            docID (str): The EDINET document identifier to download.
            fileLocation (str, optional): Directory where the ZIP file will be
                saved. Defaults to the configured default location.
            docTypeCode (str, optional): Override for the document type code
                used when building the download URL.

        Returns:
            bool: ``True`` when the document ZIP is downloaded successfully,
            otherwise ``False``.
        """
        if fileLocation is None:
            fileLocation = self.defaultLocation
        
        fullURL = h.generateURL(docID, self.baseURL, self.key, docTypeCode)
        logger.info(f"Downloading document {docID} from {fullURL}...")
        # Send a GET request to download the file
        try:
            response = requests.get(fullURL, timeout=self.REQUEST_TIMEOUT_SECONDS)
        except requests.RequestException as e:
            logger.error(f"Request failed for document {docID}: {e}")
            return False

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
        # Save the content as a ZIP file on disk
            filename = fileLocation + "\\" + docID +'.zip'
            with open(filename, 'wb') as f:
                f.write(response.content)
            logger.info(f"File downloaded and saved as {filename}.")
            return True
        else:
            logger.error(f"Failed to download file. Status code: {response.status_code}")
            return False

    def downloadDocs(self, input_table, output_table=None, filter=None):
        """Download all documents listed in the database that have not yet been downloaded.

        Iterates over documents from ``input_table``, downloads each as a ZIP,
        extracts the financial CSV files, loads them into ``output_table``, and
        marks each document as downloaded.

        Args:
            input_table (str): Name of the database table containing the document list.
            output_table (str, optional): Name of the database table where
                extracted financial data will be stored.
            filter (dict, optional): Filter dictionary to restrict which
                documents are downloaded. Defaults to filtering on
                ``Downloaded = 'False'``.

        Returns:
            None
        """
        # Connect to the SQLite database
        if filter is None:
            filter = self.generate_filter("Downloaded", "=", self.STATUS_PENDING)

        docList = self.query_database_select(input_table,filter)

        if docList is None:
            print("No documents to download: query returned no results.")
            return

        logger.info(f"Number of documents to download: {len(docList)}")
        
        for doc in docList:
            connection = None
            doc_id = doc.get("docID")
            doc_filter = self.generate_filter("docID", "=", doc_id)
            doc_status = self.STATUS_CHECKED_ERROR
            folder = os.path.join(self.defaultLocation, "downloadeddocs", str(doc_id))

            if not doc_id:
                print("Skipping document with missing docID.")
                continue

            try:
                connection = sqlite3.connect(self.Database)

                #create folders and download  files
                self.create_folder(folder)
                downloaded = self.downloadDoc(doc_id, folder)

                if not downloaded:
                    print(f"Skipping document {doc_id}: downloaded file is not a valid ZIP.")
                    doc_status = self.STATUS_CHECKED_UNAVAILABLE
                else:
                    #Unzip files
                    zipped_files = self.list_files_in_folder(folder)
                    zipped_files = [f for f in zipped_files if zipfile.is_zipfile(f)]
                    if not zipped_files:
                        print(f"Skipping document {doc_id}: downloaded file is not a valid ZIP.")
                        doc_status = self.STATUS_CHECKED_UNAVAILABLE
                    else:
                        unzipped_folder = os.path.join(folder, "unzipped")
                        self.unzip_files(zipped_files, unzipped_folder)
                        financialFiles = self.list_files_in_folder(unzipped_folder, True)

                        if not financialFiles:
                            print(f"No financial files found for document {doc_id}.")
                            doc_status = self.STATUS_CHECKED_UNAVAILABLE
                        else:
                            #Load files to DB
                            self.load_financial_data(financialFiles, output_table, doc, connection)
                            doc_status = self.STATUS_DOWNLOADED
            except Exception as e:
                doc_status = self.STATUS_CHECKED_ERROR
                print(f"Error downloading document {doc_id}: {e}")
            finally:
                if connection is not None:
                    try:
                        self.query_database_setColumn(input_table, doc_filter, "Downloaded", doc_status, connection)
                    except Exception as e:
                        print(f"Failed to persist status for document {doc_id}: {e}")
                    connection.close()

                if folder:
                    self.delete_folder(folder)
        
        self.delete_folder(os.path.join(self.defaultLocation, "downloadeddocs"))

        print("All files downloaded successfully.")

    def load_financial_data(self, financialFiles, table_name, doc, connection=None):
        """Read financial data from extracted CSV files and load it into the database.

        Args:
            financialFiles (list): List of paths to the financial data CSV files.
            table_name (str): Name of the destination table in the SQLite database.
            doc (dict): Document metadata dict (e.g. docID, edinetCode, periodStart).
            connection (sqlite3.Connection, optional): Existing database
                connection. A new connection is opened and closed automatically
                when omitted.

        Returns:
            None
        """
        try:
            if connection is None:
                conn = sqlite3.connect(self.Database)
            else:
                conn = connection

            cursor = conn.cursor()
            for csv_file in financialFiles:
                # Detect file encoding
                encoding = self.detect_file_encoding(csv_file)

                # Read CSV file using pandas
                df = pd.read_csv(csv_file, delimiter='\t', quotechar='"', encoding=encoding)

                # Add document metadata to DataFrame
                df["docID"] = doc.get("docID")            
                df["edinetCode"] = doc.get("edinetCode")            
                df["docTypeCode"] = doc.get("docTypeCode")            
                df["submitDateTime"] = doc.get("submitDateTime")    
                df["periodStart"] = doc.get("periodStart")    
                df["periodEnd"] = doc.get("periodEnd")

                # Create table if it doesn't exist
                columns = list(df.columns)
                self.create_table(table_name, columns, connection)

                # Insert data into table, ignoring duplicate constraint violations
                try:
                    df.to_sql(table_name, conn, if_exists='append', index=False)
                except sqlite3.IntegrityError:
                    # Ignore duplicate constraint violations
                    pass

            conn.commit()
        except FileNotFoundError:
            print(f"File '{csv_file}' not found.")
        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            if connection is None:
                conn.close()

    def add_document_metadata_to_List(self, data, doc, columns=False):
        """Add document metadata fields to a data dict or a columns list.

        Args:
            data (dict or list): The object to add metadata to. When
                ``columns=True`` this should be a list; otherwise a dict.
            doc (dict): Document metadata containing keys such as ``docID``,
                ``edinetCode``, and ``periodStart``.
            columns (bool): When ``True``, only the column name strings are
                appended to ``data`` (used to build a column-headers list).
                When ``False``, the actual metadata values are added.

        Returns:
            dict or list: The updated ``data`` object.
        """
        if columns:
            data.append("docID")
            data.append("edinetCode")
            data.append("docTypeCode")
            data.append("submitDateTime")
            data.append("periodStart")
            data.append("periodEnd")
        else:
            data["docID"] = doc.get("docID")            
            data["edinetCode"] = doc.get("edinetCode")            
            data["docTypeCode"] = doc.get("docTypeCode")            
            data["submitDateTime"] = doc.get("submitDateTime")    
            data["periodStart"] = doc.get("periodStart")    
            data["periodEnd"] = doc.get("periodEnd")
        return data

    def clear_table(self, table_name="temp_data"):
        """Drop a table from the SQLite database if it exists.

        Args:
            table_name (str): Name of the table to drop. Defaults to
                ``'temp_data'``.

        Returns:
            None
        """
        try:
            conn = sqlite3.connect(self.Database)
            cursor = conn.cursor()

            quoted_table = self._quote_identifier(table_name)
            cursor.execute(f"DROP TABLE IF EXISTS {quoted_table}")
            conn.commit()
        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            try:
                conn.close()
            except UnboundLocalError:
                pass

    def list_files_in_folder(self, folder_path, recursive=False):
        """Return a list of file paths found in the given folder.

        Args:
            folder_path (str): Path of the folder to search.
            recursive (bool): When ``True``, descends into sub-folders.
                Defaults to ``False``.

        Returns:
            list: List of absolute file path strings found in the folder.
        """
        try:
            if recursive:
                files = []
                for root, _, filenames in os.walk(folder_path):
                    for filename in filenames:
                        files.append(os.path.join(root, filename))
            else:
                files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]
            print(f"Number of Files found: {len(files)}")

            return files
        except FileNotFoundError:
            print(f"Folder '{folder_path}' not found.")
            return []
        except Exception as e:
            print(f"An error occurred: {e}")
            return []

    def unzip_file(self, zip_file, output_dir):
        """Extract the contents of a single ZIP file to the specified directory.

        Args:
            zip_file (str): Path to the ZIP file to extract.
            output_dir (str): Directory where the contents will be extracted.

        Returns:
            None
        """
        try:
            with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                zip_ref.extractall(output_dir)
        except FileNotFoundError:
            print(f"File '{zip_file}' not found.")
        except Exception as e:
            print(f"An error occurred: {e}")

    def unzip_files(self, zip_files, output_dir):
        """Extract the contents of multiple ZIP files to the specified directory.

        Args:
            zip_files (list): List of paths to the ZIP files to extract.
            output_dir (str): Directory where the contents will be extracted.

        Returns:
            None
        """
        print(f"Unzipping {len(zip_files)} files to {output_dir}...")
        for zip_file in zip_files:
            self.unzip_file(zip_file, output_dir)

    def create_folder(self, folder_path):
        """Create a folder at the given path if it does not already exist.

        Args:
            folder_path (str): Path of the folder to create.

        Returns:
            None
        """
        try:
            if not os.path.exists(folder_path):
                os.makedirs(folder_path)
                print(f"Folder created: {folder_path}")
            else:
                print(f"Folder already exists: {folder_path}")
        except Exception as e:
            print(f"An error occurred while creating folder {folder_path}: {e}")

    def delete_folder(self, folder_path):
        """Delete a folder and all its contents from the filesystem.

        Args:
            folder_path (str): Path of the folder to delete.

        Returns:
            None
        """
        if os.path.exists(folder_path):
            shutil.rmtree(folder_path)
            print(f"Folder deleted: {folder_path}")
        else:
            print(f"Folder not found: {folder_path}")

    def generate_filter(self, column, filter_type, value, existing_filters=None):
        """Build or extend a filter dictionary for use with database query methods.

        Args:
            column (str): Name of the column to filter on.
            filter_type (str): Comparison operator (e.g. ``'='``, ``'<'``,
                ``'>'``, ``'LIKE'``).
            value: Value to compare against.
            existing_filters (dict, optional): An existing filter dict to extend.
                When provided a copy is made so the original is not mutated.
                Defaults to an empty dict.

        Returns:
            dict: Updated filter dict with column names as keys and
            ``(filter_type, value)`` tuples as values.
        """
        if existing_filters is None:
            existing_filters = {}
        else:
            existing_filters = dict(existing_filters)
        existing_filters[column] = (filter_type, value)
        print(existing_filters)
        return existing_filters

    def query_database_select(self, table, filters=None, output_table=None):
        """Query a database table with optional filters and return or store the results.

        Args:
            table (str): Name of the table to query.
            filters (dict, optional): Column filters in the form
                ``{"column": ("operator", value)}``.
                Example: ``{"docTypeCode": ("=", "120"), "Downloaded": ("<", "True")}``.
            output_table (str, optional): When provided, the query results are
                copied into this table instead of being returned.

        Returns:
            list or None: A list of row dicts when ``output_table`` is not
            provided, or ``None`` when results are copied to ``output_table``.
        """
        conn = None
        try:
            conn = sqlite3.connect(self.Database)
            cursor = conn.cursor()
            quoted_table = self._quote_identifier(table)
            
            # Construct the WHERE clause with multiple filters
            if filters is not None:
                filter_clauses = []
                for col, (op, _) in filters.items():
                    self._validate_filter_operator(op)
                    filter_clauses.append(f"{self._quote_identifier(col)} {op} ?")
                where_clause = " AND ".join(filter_clauses)
                query = f"SELECT * FROM {quoted_table} WHERE {where_clause}"
                
                filter_values = [value for _, (_, value) in filters.items()]
                cursor.execute(query, tuple(filter_values))
            else:
                cursor.execute(f"SELECT * FROM {quoted_table}")

            rows = cursor.fetchall()
            
            # Get column names
            column_names = [description[0] for description in cursor.description]
            
            if output_table:
                self.create_table(output_table, column_names)
                self.insert_data(output_table, column_names, rows)
                print(f"Data copied to table {output_table}.")
                return None
            else:
                # Convert rows to list of dictionaries
                result = [dict(zip(column_names, row)) for row in rows]
                return result
        except Exception as e:
            print(f"An error occurred: {e}")
            return None
        finally:
            if conn is not None:
                conn.close()

    def create_table(self, table_name, columns, connection=None):
        """Create a SQLite table with TEXT columns if it does not already exist.

        Args:
            table_name (str): Name of the table to create.
            columns (list): List of column name strings.
            connection (sqlite3.Connection, optional): Existing database
                connection. A new connection is opened and closed automatically
                when omitted.

        Returns:
            None
        """
        try:
            if connection is None:
                conn = sqlite3.connect(self.Database)
            else:
                conn = connection
            cursor = conn.cursor()

            quoted_table = self._quote_identifier(table_name)
            column_definitions = ", ".join([f"{self._quote_identifier(col)} TEXT" for col in columns])
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {quoted_table} ({column_definitions})")
        except Exception as e:
            print(f"An error occurred while creating table {table_name}: {e}")
        finally:
            
            if connection is None:
                conn.close()

    def insert_data(self, table_name, columns, rows, connection=None):
        """Insert rows into a SQLite table.

        Args:
            table_name (str): Name of the destination table.
            columns (list): List of column name strings matching the order of values in each row.
            rows (list or dict): Rows to insert. Can be a list of tuples or a
                single dict mapping column names to values.
            connection (sqlite3.Connection, optional): Existing database
                connection. A new connection is opened and closed automatically
                when omitted.

        Returns:
            None
        """
        try:
            if connection is None:
                conn = sqlite3.connect(self.Database)
            else:
                conn = connection
            cursor = conn.cursor()

            quoted_table = self._quote_identifier(table_name)
            placeholders = ", ".join(["?" for _ in columns])
            if isinstance(rows, dict):
                row_columns = list(rows.keys())
                row_placeholders = ", ".join(["?" for _ in row_columns])
                quoted_columns = ", ".join([self._quote_identifier(col) for col in row_columns])
                cursor.executemany(
                    f"INSERT INTO {quoted_table} ({quoted_columns}) VALUES ({row_placeholders})",
                    [tuple(rows.values())],
                )
            else:
                cursor.executemany(f"INSERT INTO {quoted_table} VALUES ({placeholders})", rows)
            conn.commit()
        except Exception as e:
            print(f"An error occurred while inserting data into table {table_name}: {e}")
        finally:
            if connection is None:
                conn.close()

    def detect_file_encoding(self, file_path):
        """
        Detect the encoding of a file.

        Args:
            file_path (str): The path to the file whose encoding is to be detected.

        Returns:
            str: The detected encoding of the file.
        """
        with open(file_path, 'rb') as file:
            raw_data = file.read()
            result = chardet.detect(raw_data)
            return result['encoding']

    def query_database_setColumn(self, table, filter, column, value, connection):
        """Update a column value in a SQLite table for rows matching the given filter.

        Args:
            table (str): Name of the table to update.
            filter (dict): Column filters in the form
                ``{"column": ("operator", value)}``.
                Example: ``{"docID": ("=", "S100ABC1")}``.
            column (str): Name of the column to update.
            value: New value to set in the column.
            connection (sqlite3.Connection): Active database connection.

        Returns:
            None
        """
        try:
            if connection is None:
                conn = sqlite3.connect(self.Database)
            else:
                conn = connection
            cursor = conn.cursor()
            quoted_table = self._quote_identifier(table)
            quoted_column = self._quote_identifier(column)
            
            # Construct the WHERE clause with multiple filters
            filter_clauses = []
            for col, (op, _) in filter.items():
                self._validate_filter_operator(op)
                filter_clauses.append(f"{self._quote_identifier(col)} {op} ?")
            where_clause = " AND ".join(filter_clauses)
            query = f"UPDATE {quoted_table} SET {quoted_column} = ? WHERE {where_clause}"
            
            filter_values = [value for _, (_, value) in filter.items()]
            cursor.execute(query, (value,) + tuple(filter_values))
            conn.commit()
        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            if connection is None:
                conn.close()

    @staticmethod
    def _quote_identifier(identifier):
        """Safely quote SQL identifiers such as table and column names."""
        if not isinstance(identifier, str) or not identifier.strip():
            raise ValueError("SQL identifier must be a non-empty string.")
        return '"' + identifier.replace('"', '""') + '"'

    def _validate_filter_operator(self, operator):
        if operator not in self.ALLOWED_FILTER_OPERATORS:
            raise ValueError(f"Unsupported filter operator: {operator}")

    @classmethod
    def _canonicalize_company_info_column_name(cls, column_name):
        value = str(column_name or "").strip().replace("\ufeff", "")
        value = value.replace("（", "(").replace("）", ")")
        value = value.replace("_", " ")
        value = re.sub(r"\s+", " ", value)
        return value.lower()

    @classmethod
    def _find_company_info_column(cls, columns, aliases):
        alias_keys = {
            cls._canonicalize_company_info_column_name(alias)
            for alias in aliases
        }
        for column in columns:
            if cls._canonicalize_company_info_column_name(column) in alias_keys:
                return column
        return None

    def _looks_like_edinet_code_dataframe(self, df):
        if df is None or df.empty:
            return False

        columns = list(df.columns)
        has_edinet_code = self._find_company_info_column(
            columns,
            self.EDINET_CODE_LIST_COLUMN_ALIASES["EdinetCode"],
        )
        has_company_name = self._find_company_info_column(
            columns,
            self.EDINET_CODE_LIST_COLUMN_ALIASES["Company_Name"],
        )
        has_ticker = self._find_company_info_column(
            columns,
            self.EDINET_CODE_LIST_COLUMN_ALIASES["Company_Ticker"],
        )
        return bool(has_edinet_code and (has_company_name or has_ticker))

    def _normalize_edinet_code_dataframe(self, df):
        normalized_df = pd.DataFrame()
        working_df = df.copy().fillna("")
        used_columns = set()
        source_columns = list(working_df.columns)

        for target_column in self.EDINET_CODE_LIST_TARGET_COLUMNS:
            match = self._find_company_info_column(
                source_columns,
                self.EDINET_CODE_LIST_COLUMN_ALIASES[target_column],
            )
            if match is None or match in used_columns:
                continue
            normalized_df[target_column] = working_df[match].astype(str)
            used_columns.add(match)

        for column in source_columns:
            if column in used_columns or column in normalized_df.columns:
                continue
            normalized_df[column] = working_df[column].astype(str)

        return normalized_df

    def _parse_edinet_code_csv_bytes(self, csv_bytes):
        errors = []
        for encoding in self.EDINET_CODE_LIST_ENCODING_CANDIDATES:
            for header_row in (0, 1):
                try:
                    df = pd.read_csv(
                        io.BytesIO(csv_bytes),
                        encoding=encoding,
                        header=header_row,
                        dtype=str,
                        keep_default_na=False,
                    )
                except (UnicodeDecodeError, pd.errors.ParserError, ValueError) as exc:
                    errors.append(f"encoding={encoding}, header={header_row}: {exc}")
                    continue

                if self._looks_like_edinet_code_dataframe(df):
                    logger.info(
                        "Loaded EDINET code list CSV with encoding=%s header=%s",
                        encoding,
                        header_row,
                    )
                    return self._normalize_edinet_code_dataframe(df)

        raise ValueError(
            "Unable to parse the EDINET code list CSV. "
            + (" Attempts: " + " | ".join(errors) if errors else "")
        )

    def _extract_csv_from_edinet_code_zip(self, zip_bytes):
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as archive:
            csv_entries = [
                info for info in archive.infolist()
                if not info.is_dir() and info.filename.lower().endswith(".csv")
            ]
            if not csv_entries:
                raise ValueError("Downloaded EDINET code list ZIP does not contain a CSV file.")

            entry = csv_entries[0]
            return entry.filename, archive.read(entry)

    def _load_edinet_code_dataframe_from_path(self, csv_file):
        if not os.path.exists(csv_file):
            raise FileNotFoundError(f"EDINET company info source file not found: {csv_file}")

        if zipfile.is_zipfile(csv_file):
            with open(csv_file, "rb") as handle:
                _, csv_bytes = self._extract_csv_from_edinet_code_zip(handle.read())
            return self._parse_edinet_code_csv_bytes(csv_bytes)

        with open(csv_file, "rb") as handle:
            return self._parse_edinet_code_csv_bytes(handle.read())

    def _extract_hidden_input_value(self, html_text, input_id):
        parser = _HiddenInputValueParser(input_id)
        parser.feed(html_text)
        if parser.value is None:
            raise ValueError(f"Hidden input '{input_id}' not found in EDINET download page.")
        return parser.value

    def _build_edinet_code_download_payload(self, gx_state):
        required_keys = (
            "vPGMNAME",
            "vPGMDESC",
            "gxhash_vPGMNAME",
            "gxhash_vPGMDESC",
        )
        missing = [key for key in required_keys if not gx_state.get(key)]
        if missing:
            raise ValueError(
                "Missing GeneXus state required for EDINET code-list download: "
                + ", ".join(missing)
            )

        return {
            "MPage": False,
            "cmpCtx": "",
            "parms": [gx_state["vPGMNAME"], gx_state["vPGMDESC"]],
            "hsh": [
                {"hsh": gx_state["gxhash_vPGMNAME"], "row": ""},
                {"hsh": gx_state["gxhash_vPGMDESC"], "row": ""},
            ],
            "objClass": "weee0020",
            "pkgName": "GeneXus.Programs",
            "events": [self.EDINET_CODE_LIST_DOWNLOAD_EVENT],
            "grids": {},
        }

    def _build_edinet_code_download_headers(self, gx_state, page_url):
        origin_parts = urlsplit(page_url)
        origin = f"{origin_parts.scheme}://{origin_parts.netloc}"
        headers = {
            "Content-Type": "application/json",
            "Accept": "*/*",
            "GXAjaxRequest": "1",
            "Origin": origin,
            "Referer": page_url,
        }

        ajax_security_token = gx_state.get("AJAX_SECURITY_TOKEN")
        if ajax_security_token:
            headers["AJAX_SECURITY_TOKEN"] = ajax_security_token

        auth_token = gx_state.get(f"GX_AUTH_{gx_state.get('vPGMNAME', '')}")
        if auth_token:
            headers["X-GXAuth-Token"] = auth_token

        return headers

    def _extract_edinet_code_zip_bytes(self, download_response):
        gx_props = download_response.get("gxProps") or []
        if not gx_props:
            raise ValueError("EDINET code-list download response is missing gxProps.")

        script_caption = (gx_props[0].get("TXTSCRIPT") or {}).get("Caption") or ""
        match = re.search(r"base64,([^\"']+)", script_caption)
        if not match:
            raise ValueError("Unable to find the EDINET code-list ZIP payload in the response.")

        return base64.b64decode(match.group(1))

    def _download_official_edinet_code_dataframe(self, language="en"):
        page_url = self.EDINET_CODE_LIST_PAGE_URLS.get((language or "en").lower())
        if not page_url:
            raise ValueError(f"Unsupported EDINET code-list language: {language}")

        session = requests.Session()
        try:
            page_response = session.get(page_url, timeout=self.REQUEST_TIMEOUT_SECONDS)
            page_response.raise_for_status()

            gx_state_raw = self._extract_hidden_input_value(page_response.text, "GXState")
            gx_state = json.loads(gx_state_raw)
            ajax_iv = gx_state.get("GX_AJAX_IV")
            if not ajax_iv:
                raise ValueError("EDINET download page is missing the GX_AJAX_IV token.")

            payload = self._build_edinet_code_download_payload(gx_state)
            headers = self._build_edinet_code_download_headers(gx_state, page_url)
            post_url = f"{page_url}?{str(ajax_iv).lower()},gx-no-cache={int(time.time() * 1000)}"
            download_response = session.post(
                post_url,
                data=json.dumps(payload, separators=(",", ":")),
                headers=headers,
                timeout=self.REQUEST_TIMEOUT_SECONDS,
            )
            download_response.raise_for_status()

            zip_bytes = self._extract_edinet_code_zip_bytes(download_response.json())
            entry_name, csv_bytes = self._extract_csv_from_edinet_code_zip(zip_bytes)
            logger.info("Downloaded EDINET code list entry %s", entry_name)
            return self._parse_edinet_code_csv_bytes(csv_bytes)
        finally:
            session.close()

    def _load_edinet_code_dataframe(self, csv_file=None):
        if csv_file:
            logger.info("Loading EDINET company info from %s", csv_file)
            return self._load_edinet_code_dataframe_from_path(csv_file)

        logger.info("Downloading the official EDINET code list (English)")
        return self._download_official_edinet_code_dataframe(language="en")

    def store_edinetCodes(self, csv_file, target_database=None, table_name=None):
        """Store EDINET company codes from a CSV file into the SQLite database.

        Args:
            csv_file (str): Optional path to a local CSV or ZIP file containing
                EDINET company codes. When omitted or blank, the official
                English EDINET code list is downloaded from the EDINET site.
            target_database (str, optional): Destination SQLite DB path.
                Defaults to the configured DB path.
            table_name (str, optional): Destination table name.
                Defaults to configured company-info table.

        Returns:
            None
        """
          
        try:
            df = self._load_edinet_code_dataframe(csv_file)
            
            # Connect to SQLite database
            destination_db = target_database or self.Database
            destination_table = table_name or self.DB_COMPANY_INFO_TABLE
            conn = sqlite3.connect(destination_db)

            # Insert data into the table
            df.to_sql(destination_table, conn, if_exists="replace", index=False)
            
            # Commit and close
            conn.commit()
            conn.close()
            print("EDINET codes successfully stored in the database.")
        except Exception as e:
            print(f"Error downloading or storing EDINET codes: {e}")