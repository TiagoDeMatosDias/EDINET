import config as c
import requests
from datetime import datetime, timedelta
import classes.helper as h
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
        self.key = self.config.get("apikey")
        self.defaultLocation = self.config.get("defaultLocation")
        self.Database = self.config.get("Database")

    def get_All_documents_withMetadata(self, start_date="2015-01-01", end_date=None, Database_DocumentList="DocumentList"):
        """
        Fetch all available document IDs for a given company from the EDINET API and store them in the database.
        
        Parameters:
        - start_date (str): The start date in YYYY-MM-DD format (default: 2015-01-01).
        - end_date (str): The end date in YYYY-MM-DD format (default: today).
        
        Returns:
        - List of document IDs for the given company.
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
                    
                    # Create the DB table if it doesn't exist
                    columns = list(data.get("results")[0].keys())
                    columns.append("Downloaded")
                    
                    self.create_table(Database_DocumentList, columns)

                    # Insert documents into the database
                    for entry in data.get("results", []):
                        # Check if the document already exists in the table
                        cursor.execute(f"SELECT COUNT(*) FROM {Database_DocumentList} WHERE docID = ?", (entry["docID"],))
                        if cursor.fetchone()[0] == 0:
                            entry["Downloaded"] = "False"
                            placeholders = ", ".join(["?" for _ in entry])
                            cursor.execute(f"INSERT INTO {Database_DocumentList} VALUES ({placeholders})", tuple(entry.values()))
                    
                    conn.commit()
            
            except Exception as e:
                print(f"Error fetching data for {date_str}: {e}")
            current_date += timedelta(days=1)  # Move to the next day

        conn.close()

    def downloadDoc(self, docID, fileLocation=None, docTypeCode=None):

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
        
        # Connect to the SQLite database
        if filter is None:
            filter = self.generate_filter("Downloaded", "=", "False")

        docList = self.query_database_select(input_table,filter)
        
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
        """
        This function reads the financial data from the unzipped files and loads it into the SQLite database.
        :param financialFiles: List of financial data files
        :param table_name: Name of the table in the SQLite database
        :param doc: Document metadata
        :return: None
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

                # Insert data into table
                df.to_sql(table_name, conn, if_exists='append', index=False)

            conn.commit()
        except FileNotFoundError:
            print(f"File '{csv_file}' not found.")
        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            if connection is None:
                conn.close()

    def add_document_metadata_to_List(self, data, doc, columns=False):
        """
        This function adds the metadata of a document to the list of documents.
        :param data: List of documents
        :param doc: Document metadata
        :param columns: determined whether only the column name is added
        :return: None
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
        """
        This function clears the temporary table in the SQLite database.
        :param SQLITE_DB: Location of the SQLite database
        :return: None
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
        """
        Returns a list of files in the provided folder.
        
        :param folder_path: Path of the folder
        :param recursive: Boolean indicating whether to search subfolders recursively (default: False)
        :return: List of file names
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
        """
        This function unzips a ZIP file to the specified output directory.
        :param zip_file: Location of the ZIP file
        :param output_dir: Location to extract the contents of the ZIP file
        :return: None
        """
        try:
            with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                zip_ref.extractall(output_dir)
        except FileNotFoundError:
            print(f"File '{zip_file}' not found.")
        except Exception as e:
            print(f"An error occurred: {e}")

    def unzip_files(self, zip_files, output_dir):
        """
        This function unzips multiple ZIP files to the specified output directory.
        :param zip_files: List of ZIP file locations
        :param output_dir: Location to extract the contents of the ZIP files
        :return: None
        """
        print(f"Unzipping {len(zip_files)} files to {output_dir}...")
        for zip_file in zip_files:
            self.unzip_file(zip_file, output_dir)

    def create_folder(self, folder_path):
        """
        Creates a folder in the filesystem if it doesn't already exist.
        
        :param folder_path: Path of the folder to create
        :return: None
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
        """
        Deletes a folder in the filesystem if it exists.
        
        :param folder_path: Path of the folder to delete
        :return: None
        """
        if os.path.exists(folder_path):
            shutil.rmtree(folder_path)
            print(f"Folder deleted: {folder_path}")
        else:
            print(f"Folder not found: {folder_path}")

    def generate_filter(self, column, filter_type, value, existing_filters=None):
        """
        Generates a filter dictionary for querying the SQLite database.
        :param column: Name of the column
        :param filter_type: Type of filter (e.g., "=", "<", ">", "LIKE")
        :param value: Value to filter on
        :param existing_filters: Existing filter dictionary to add the new filter to (default: None)
        :return: Dictionary with column name as key and filter tuple as value
        """
        if existing_filters is None:
            existing_filters = {}
        else:
            existing_filters = dict(existing_filters)
        existing_filters[column] = (filter_type, value)
        print(existing_filters)
        return existing_filters

    def query_database_select(self, table, filters=None, output_table=None):
        """
        This function executes a custom query on a SQLite database and either returns the result as a JSON object
        or copies the data to another table if output_table is provided.
        :param table: Name of the table to query
        :param filters: Dictionary of column names and their corresponding filter values and types
                Example: {"column1": ("=", value1), "column2": ("<", value2)}
        :param output_table: Name of the table to copy the data to (default: None)
        :return: JSON object with the query results or None if output_table is provided
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
        """
        This function creates a table in the SQLite database with the given columns.
        :param table_name: Name of the table to create
        :param columns: List of column names
        :return: None
        """
        try:
            if connection is None:
                conn = sqlite3.connect(self.Database)
            else:
                conn = connection
            cursor = conn.cursor()
            
            column_definitions = ", ".join([f"{col} TEXT" for col in columns])
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({column_definitions})")
            conn.commit()
        except Exception as e:
            print(f"An error occurred while creating table {table_name}: {e}")
        finally:
            
            if connection is None:
                conn.close()

    def insert_data(self, table_name, columns, rows, connection=None):
        """
        This function inserts data into a table in the SQLite database.
        :param table_name: Name of the table to insert data into
        :param columns: List of column names
        :param rows: List of rows to insert
        :return: None
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
        """
        This function executes a custom query on a SQLite database and updates the value of a specific column.
        :param table: Name of the table to query
        :param filter: Dictionary of column names and their corresponding filter values and types
                Example: {"column1": ("=", value1), "column2": ("<", value2)}
        :param column: Name of the column to update
        :param value: New value for the column
        :return: None
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
        """
        Stores EDINET codes from a CSV file into an SQLite database.
        Args:
            csv_file (str): The path to the CSV file containing EDINET codes.
        Raises:
            Exception: If there is an error in reading the CSV file or storing data into the database.
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
            df.to_sql("edinet_codes", conn, if_exists="replace", index=False)
            
            # Commit and close
            conn.commit()
            conn.close()
            print("EDINET codes successfully stored in the database.")
        except Exception as e:
            print(f"Error downloading or storing EDINET codes: {e}")


    # Press the green button in the gutter to run the script.