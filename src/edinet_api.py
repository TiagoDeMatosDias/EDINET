import config as c
import requests
from datetime import datetime, timedelta
import src.utils as h
import sqlite3
import os
import zipfile
import chardet
import csv
import shutil
import pandas as pd

class Edinet:
    def __init__(self):
        self.config = c.Config()
        self.baseURL = self.config.get("baseURL")
        self.key = self.config.get("API_KEY")
        self.defaultLocation = self.config.get("RAW_DOCUMENTS_PATH")
        self.Database = self.config.get("DB_PATH")
        self.DB_COMPANY_INFO_TABLE = self.config.get("DB_COMPANY_INFO_TABLE")
        self.DB_TAXONOMY_TABLE = self.config.get("DB_TAXONOMY_TABLE")
        self.DB_DOC_LIST_TABLE = self.config.get("DB_DOC_LIST_TABLE")

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
                response = requests.get(url)
                if response.status_code == 200:
                    data = response.json()
                    
                    if data.get("results") == []:
                        print(f"No documents found for date {date_str}.")
                        current_date += timedelta(days=1)
                        continue
                    else:
                        print(f"Found {len(data.get('results'))} documents for date {date_str}.")

                        # Create the DB table if it doesn't exist
                        columns = list(data.get("results")[0].keys())
                        columns.append("Downloaded")
                        
                        self.create_table(self.DB_DOC_LIST_TABLE, columns, conn)

                        # Insert documents into the database
                        for entry in data.get("results", []):
                            # Check if the document already exists in the table
                            cursor.execute(f"SELECT COUNT(*) FROM {self.DB_DOC_LIST_TABLE} WHERE docID = ?", (entry["docID"],))
                            if cursor.fetchone()[0] == 0:
                                entry["Downloaded"] = "False"
                                placeholders = ", ".join(["?" for _ in entry])
                                cursor.execute(f"INSERT INTO {self.DB_DOC_LIST_TABLE} VALUES ({placeholders})", tuple(entry.values()))
                        
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
            None
        """
        if fileLocation is None:
            fileLocation = self.defaultLocation
        
        fullURL = h.generateURL(docID, self.config ,docTypeCode)
        # Send a GET request to download the file
        response = requests.get(fullURL)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
        # Save the content as a ZIP file on disk
            filename = fileLocation + "\\" + docID +'.zip'
            with open(filename, 'wb') as f:
                f.write(response.content)
            print("File downloaded and saved as " + filename + ".")
        else:
            print(f"Failed to download file. Status code: {response.status_code}")
            return null

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
            filter = self.generate_filter("Downloaded", "=", "False")

        docList = self.query_database_select(input_table,filter)

        print(f"Number of documents to download: {len(docList)}")
        
        for doc in docList:
            try:
                #get defaults
                doc_id = doc.get("docID")
                folder = self.defaultLocation + "\\downloadeddocs\\" + doc_id
                filter = self.generate_filter("docID", "=", doc_id)
                connection = sqlite3.connect(self.Database)

                #create folders and download  files
                self.create_folder(folder)
                self.downloadDoc(doc_id, folder)

                #Unzip files
                zipped_files = self.list_files_in_folder(folder)
                self.unzip_files(zipped_files, folder + "\\unzipped")
                financialFiles = self.list_files_in_folder(folder + "\\unzipped", True)

                #Load files to DB
                self.load_financial_data(financialFiles, output_table, doc, connection)

                #Update downloaded status and clean the environment
                self.query_database_setColumn(input_table,filter, "Downloaded", "True", connection)
                self.delete_folder(folder)
                connection.close()
            except Exception as e:
                print(f"Error downloading document {doc_id}: {e}")       
        
        self.delete_folder(self.defaultLocation + "\\downloadeddocs" )

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
            
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
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
        try:
            conn = sqlite3.connect(self.Database)
            cursor = conn.cursor()
            
            # Construct the WHERE clause with multiple filters
            if filters is not None:
                filter_clauses = [f"{col} {op} ?" for col, (op, _) in filters.items()]
                where_clause = " AND ".join(filter_clauses)
                query = f"SELECT * FROM {table} WHERE {where_clause}"
                
                filter_values = [value for _, (_, value) in filters.items()]
                cursor.execute(query, tuple(filter_values))
            else:
                cursor.execute(f"SELECT * FROM {table}")

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
            
            column_definitions = ", ".join([f"{col} TEXT" for col in columns])
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({column_definitions})")
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
            
            placeholders = ", ".join(["?" for _ in columns])
            if isinstance(rows, dict):
                cursor.executemany(f"INSERT INTO {table_name} ({', '.join(rows.keys())}) VALUES ({placeholders})", [tuple(rows.values())])
            else:
                cursor.executemany(f"INSERT INTO {table_name} VALUES ({placeholders})", rows)
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
            
            # Construct the WHERE clause with multiple filters
            filter_clauses = [f"{col} {op} ?" for col, (op, _) in filter.items()]
            where_clause = " AND ".join(filter_clauses)
            query = f"UPDATE {table} SET {column} = ? WHERE {where_clause}"
            
            filter_values = [value for _, (_, value) in filter.items()]
            cursor.execute(query, (value,) + tuple(filter_values))
            conn.commit()
        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            if connection is None:
                conn.close()

    def store_edinetCodes(self, csv_file):
        """Store EDINET company codes from a CSV file into the SQLite database.

        Args:
            csv_file (str): Path to the CSV file containing EDINET codes.

        Returns:
            None
        """
          
        try:

            # Load CSV into a pandas DataFrame
            df = pd.read_csv(csv_file, encoding=self.detect_file_encoding(csv_file))
            
            # Connect to SQLite database
            conn = sqlite3.connect(self.Database)
            cursor = conn.cursor()
            
            
            # Insert data into the table
            df.to_sql(self.DB_COMPANY_INFO_TABLE, conn, if_exists="replace", index=False)
            
            # Commit and close
            conn.commit()
            conn.close()
            print("EDINET codes successfully stored in the database.")
        except Exception as e:
            print(f"Error downloading or storing EDINET codes: {e}")