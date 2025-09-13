import config as c
import pandas as pd
import sqlite3
import xml.etree.ElementTree as ET
import random
import numpy as np
import json

class data:
    def __init__(self):
        self.config = c.Config()
        self.baseURL = self.config.get("baseURL")
        self.key = self.config.get("API_KEY")
        self.Database = self.config.get("DB_PATH")
        self.Database_DocumentList = self.config.get("DB_DOC_LIST_TABLE")


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

    def Filter_for_Relevant(self, input_table, output_table):
        """
        Generates financial statements by querying data from the input table and
        storing the results in the output table, keeping only specific columns.

        Args:
            input_table (str): The name of the input table to query data from.
            output_table (str): The name of the output table to store the results.

        Returns:
            None
        """
        conn = sqlite3.connect(self.Database)
        cursor = conn.cursor()

        # Load configuration
        with open('config/financial_ratios_config.json', 'r') as f:
            config = json.load(f)
        
        accounting_term_conditions = config['accounting_term_conditions']
        period_conditions = config['period_conditions']
        query_template = config['query_template']

        # Build the SQL query dynamically
        accounting_term_query = " OR ".join([f"AccountingTerm LIKE '{term}'" for term in accounting_term_conditions])
        period_query = " OR ".join([f"Period = '{period}'" for period in period_conditions])

        query = query_template.format(
            output_table=output_table,
            input_table=input_table,
            accounting_term_query=accounting_term_query,
            period_query=period_query
        )

        # Execute the query
        cursor.execute(query)
        conn.commit()
        conn.close()


    def evaluate_expression(self, df, expression):
        if "value" in expression:
            return expression["value"]

        if "column" in expression:
            col_name = expression["column"]
            series = df[col_name]
            if "fillna" in expression:
                fillna_config = expression["fillna"]
                if isinstance(fillna_config, dict) and "column" in fillna_config:
                    fallback_series = self.evaluate_expression(df, fillna_config)
                    series = series.fillna(fallback_series)
                else:
                    series = series.fillna(fillna_config)
            return series

        op = expression["operator"]
        operands = [self.evaluate_expression(df, operand) for operand in expression["operands"]]

        if op == '+':
            return operands[0] + operands[1]
        if op == '-':
            return operands[0] - operands[1]
        if op == '*':
            return operands[0] * operands[1]
        if op == '/':
            return operands[0] / operands[1]
        return None

    def Generate_Financial_Ratios(self, input_table, output_table):
        # This function will take the input table and generate the financial ratios
        # The output will be stored in the output_table

        # Connect to the database
        conn = sqlite3.connect(self.Database)

        # Load configuration
        with open('config/financial_ratios_config.json', 'r') as f:
            config = json.load(f)
        columns_mapping = config['mappings']
        ratios_definitions = config['ratios']

        # Get the list of companies
        companies = self.get_companyList(input_table, conn)
        exists = False
        for company in companies:
            # Get the data for the company
            df = pd.read_sql_query(f"""SELECT * FROM {input_table} WHERE edinetCode = '{company}' """, conn)

            # Create a combined column for AccountingTerm and Period
            df['AccountingTerm_Period'] = df['AccountingTerm'] + '_' + df['Period']
                    
            RatiosTable = df.pivot_table(
                index=['edinetCode', 'docID',  'docTypeCode', 'periodStart', 'periodEnd'],
                columns=['AccountingTerm_Period'],
                values='Amount',
                aggfunc='first'
            ).reset_index()

            # Flatten the columns
            RatiosTable.columns.name = None
            RatiosTable.columns = [col if isinstance(col, str) else col[1] for col in RatiosTable.columns]
            RatiosTable = pd.DataFrame(RatiosTable)

            # Convert any non-numeric columns to numeric
            numeric_columns = RatiosTable.columns.difference(['edinetCode', 'docID', 'Currency', 'docTypeCode', 'periodStart', 'periodEnd'])
            RatiosTable[numeric_columns] = RatiosTable[numeric_columns].apply(pd.to_numeric, errors='coerce')

            # Populate the new columns using a lambda function
            for new_col, relevant_cols in columns_mapping.items():
                RatiosTable[new_col] = RatiosTable.apply(lambda row: next((row[col] for col in relevant_cols if col in row and pd.notnull(row[col])), np.nan), axis=1)

            # Flatten the list of all relevant columns from the columns_mapping dictionary
            columns_to_remove = [col for relevant_cols in columns_mapping.values() for col in relevant_cols]

            # Remove the relevant columns from the RatiosTable
            RatiosTable.drop(columns=columns_to_remove, inplace=True, errors='ignore')

            # Remove any columns whose name begins with jppfs_cor: or jpcrp_cor:
            RatiosTable = RatiosTable.loc[:, ~RatiosTable.columns.str.startswith('jppfs_cor:')]
            RatiosTable = RatiosTable.loc[:, ~RatiosTable.columns.str.startswith('jpcrp_cor:')]

            RatiosTable.reset_index(drop=True, inplace=True)

            # Calculate ratios from config
            RatiosTable_calcs = RatiosTable.copy()
            for ratio_def in ratios_definitions:
                output_col = ratio_def["output"]
                expression = ratio_def["expression"]
                RatiosTable[output_col] = self.evaluate_expression(RatiosTable_calcs, expression)
                RatiosTable_calcs[output_col] = RatiosTable[output_col]

            # Calculate the 3 year and 5 year averages
            RatiosTable["Ratio_PriceBook_3Year_Average"] = RatiosTable["Ratio_PriceBook"].rolling(window=3, min_periods=1).mean()
            RatiosTable["Ratio_PriceBook_5Year_Average"] = RatiosTable["Ratio_PriceBook"].rolling(window=5, min_periods=1).mean()
            RatiosTable["Ratio_PriceEarnings_3Year_Average"] = RatiosTable["Ratio_PriceEarnings"].rolling(window=3, min_periods=1).mean()
            RatiosTable["Ratio_PriceEarnings_5Year_Average"] = RatiosTable["Ratio_PriceEarnings"].rolling(window=5, min_periods=1).mean()

            # Round the values to 2 decimal places
            RatiosTable = RatiosTable.round(2)

            # Store the data back to the database
            if exists:
                self.add_missing_columns(conn, output_table, RatiosTable)
            RatiosTable.to_sql(output_table, conn, if_exists='append')
            conn.commit()
            exists = True
        
        conn.close()

        pass


    def Generate_Aggregated_Ratios(self, input_table, output_table):
        """
        This function aggregates financial ratios from the input table and stores the results in the output table.
        It calculates the average of each ratio (columns with prefix "Ratio_") for each company and adds a column
        for the count of periods aggregated. Additionally, it includes the latest value of the MarketCap column
        based on the most recent period.
        """
        conn = sqlite3.connect(self.Database)
        
        # Process data in chunks to handle large datasets
        chunk_size = 10000
        offset = 0
        first_chunk = True

        while True:
            # Get a chunk of data from the input table
            df = pd.read_sql_query(f"SELECT * FROM {input_table} LIMIT {chunk_size} OFFSET {offset}", conn)
            if df.empty:
                break

            # Filter columns with prefix "Ratio_"
            ratio_columns = [col for col in df.columns if col.startswith("Ratio_")]
            if not ratio_columns:
                raise ValueError("No columns with prefix 'Ratio_' found in the input table.")

            # Group by edinetCode and calculate the mean for ratio columns
            aggregated_df = df.groupby('edinetCode')[ratio_columns].mean().reset_index()
           

            # Filter columns with prefix "PerShare_"
            PerShare_columns = [col for col in df.columns if col.startswith("PerShare_")]
            if not ratio_columns:
                raise ValueError("No columns with prefix 'Ratio_' found in the input table.")

            # for the pershare columns we get the standard deviation and the growth from the first period_start to the latest period_start
            aggregated_df_PerShare = df.groupby('edinetCode')[PerShare_columns].agg(['mean', 'std']).reset_index()
            # Flatten the MultiIndex columns
            aggregated_df_PerShare.columns = ['_'.join(col).strip() for col in aggregated_df_PerShare.columns.values]
            # Rename the edinet_ column back to edinetCode
            aggregated_df_PerShare.rename(columns={'edinetCode_': 'edinetCode'}, inplace=True)

            # Ensure the DataFrame is sorted by edinetCode and period_start
            df = df.sort_values(by=['edinetCode', 'periodStart'])
            # for the pershare columns and for each edinetcode we get the growth from the first period_start to the latest period_start
            for col in PerShare_columns:
                # Get the first and last periodStart for each edinetCode
                first_value = df.groupby('edinetCode')[col].first().reset_index()
                last_value = df.groupby('edinetCode')[col].last().reset_index()

                # Calculate the growth
                growth_col_name = f"{col}_Growth"
                # Calculate the number of years between the first and last periodStart
                first_period = pd.to_datetime(df.groupby('edinetCode')['periodStart'].first().reset_index()['periodStart'])
                last_period = pd.to_datetime(df.groupby('edinetCode')['periodStart'].last().reset_index()['periodStart'])
                years_difference = (last_period - first_period).dt.days / 365.0

                # Calculate the annualized growth
                aggregated_df_PerShare[growth_col_name] = ((last_value[col] / first_value[col]) ** (1 / years_difference)) - 1
            

            # Add a column for the count of periods aggregated
            count_df = df.groupby('edinetCode').size().reset_index(name='number_of_Periods')
            aggregated_df = pd.merge(aggregated_df, count_df, on='edinetCode')
            aggregated_df = pd.merge(aggregated_df, aggregated_df_PerShare, on='edinetCode', how='left')


            # Add the latest MarketCap value for each company based on the most recent period
            latest_marketcap_df = df.sort_values('periodEnd').groupby('edinetCode').last().reset_index()[['edinetCode', 'MarketCap']]
            aggregated_df = pd.merge(aggregated_df, latest_marketcap_df, on='edinetCode', how='left')

            # Round the values to 2 decimal places
            aggregated_df = aggregated_df.round(2)

            # Store the aggregated data back to the database
            aggregated_df.to_sql(output_table, conn, if_exists='replace' if first_chunk else 'append', index=False)
            conn.commit()

            offset += chunk_size
            first_chunk = False

        conn.close()




    def get_first_existing_column(self, df, columns):
        """
        This function returns the first column from the list that contains numeric values.
        :param df: DataFrame to search
        :param columns: List of column names to check
        :return: Series with the first existing column's values
        """
        for column in columns:
            if column in df.columns and pd.api.types.is_numeric_dtype(df[column]) :
                return df[column]
        return pd.Series([np.nan] * len(df))





    def add_missing_columns(self, conn, table_name, df):
        """
        Adds missing columns to the SQLite table based on the DataFrame columns.
        
        :param conn: SQLite connection object
        :param table_name: Name of the table to modify
        :param df: DataFrame with the new columns
        :return: None
        """
        cursor = conn.cursor()
        
        # Get existing columns in the table
        cursor.execute(f"PRAGMA table_info({table_name})")
        existing_columns = [info[1] for info in cursor.fetchall()]
        
        # Add missing columns
        for column in df.columns:
            if column not in existing_columns:
                cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN '{column}' TEXT")
        
        conn.commit()


    def delete_table(self, table_name, connection=None):
        """
        This function deletes a table from the SQLite database.
        :param table_name: Name of the table to delete
        :return: None
        """
        try:
            if connection is None:
                conn = sqlite3.connect(self.Database)
            else:
                conn = connection
            cursor = conn.cursor()
            
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            conn.commit()
        except Exception as e:
            print(f"An error occurred while deleting table {table_name}: {e}")
        finally:
            if connection is None:
                conn.close()

    def get_companyList(self, input_table, conn, company_column="edinetCode"):
        df = pd.read_sql_query(f"SELECT DISTINCT {company_column} FROM {input_table}", conn)
        return df[company_column].tolist()


    def rename_columns(self, conn, table_name, column_mapping):
        """
        Renames columns in the SQLite table based on the mapping provided.
        
        :param conn: SQLite connection object
        :param table_name: Name of the table to modify
        :param column_mapping: Dictionary with old column names as keys and new column names as values
        :return: None
        """
        cursor = conn.cursor()
        
        for old_name, new_name in column_mapping.items():
            cursor.execute(f"ALTER TABLE {table_name} RENAME COLUMN '{old_name}' TO '{new_name}'")
        
        conn.commit()

    def rename_columns_to_Standard(self, conn, table_name):
        # Example usage
        column_mapping = {
            "要素ID": "AccountingTerm",
            "コンテキストID": "Period",
            "ユニットID": "Currency",
            "値": "Amount"
        }
        self.rename_columns(conn, table_name, column_mapping)

    def copy_table(self, conn, source_table, target_table, columns=None):
        """
        Copies data from one table to another in the SQLite database.
        
        :param conn: SQLite connection object
        :param source_table: Name of the source table
        :param target_table: Name of the target table
        :param columns: List of columns to copy (default is all columns)
        :return: None
        """
        cursor = conn.cursor()        
        # Create the target table if it doesn't exist
        cursor.execute(f"CREATE TABLE {target_table} AS SELECT * FROM {source_table}")        
        
        conn.commit()

    def copy_table_to_Standard(self, source_table, target_table,conn=None):
        """
        Copies data from a source table to a target table with standardized column names and generates financial statements.

        Args:
            conn: Database connection object.
            source_table (str): Name of the source table from which data is to be copied.
            target_table (str): Name of the target table to which data is to be copied.

        Returns:
            None
        """
        if conn is None:
            conn = sqlite3.connect(self.Database)
        
        columns = ["要素ID", "コンテキストID", "ユニットID", "値", "docID", "edinetCode", "docTypeCode", "submitDateTime", "periodStart", "periodEnd"]
        tempTable = "TempTable_" + random.choice("132465sadf")
        self.copy_table(conn, source_table, tempTable, columns)
        self.rename_columns_to_Standard(conn, tempTable)
        self.Filter_for_Relevant(tempTable, target_table)
        self.delete_table(tempTable, conn)
    


    def parse_edinet_taxonomy(self, xsd_file, table_name, connection=None):
        """
        Parses an EDINET Taxonomy XSD file and stores relevant elements in an SQLite database.
        
        :param xsd_file: Path to the EDINET XSD file.
        :param db_file: Path to the SQLite database file.
        """
        # Parse the XSD file
        tree = ET.parse(xsd_file)
        root = tree.getroot()
        
        # Define XML namespace (assuming the namespace does not change)
        namespace = "{http://www.w3.org/2001/XMLSchema}"
        
        # Extract elements
        elements = []
        for elem in root.findall(f"{namespace}element"):
            name = elem.get("name")
            elem_id = elem.get("id")
            abstract = elem.get("abstract", "false")
            balance = elem.get("{http://www.xbrl.org/2003/instance}balance")
            period_type = elem.get("{http://www.xbrl.org/2003/instance}periodType")

            Id = self.adjust_string(elem_id, "jppfs_cor_", "jppfs_cor:")
            if period_type == "instant" and abstract == "false":
                Statement = "Balance Sheet"
            elif period_type == "duration" and abstract == "false" and balance is not None:
                Statement = "Income Statement"
            elif period_type == "duration" and abstract == "false" and balance is None:
                Statement = "Cashflow Statement"
            else:
                Statement = "Other Statement"

            if Statement == "Balance Sheet" and balance == "credit":
                Type = "Liability"
            elif Statement == "Balance Sheet" and balance == "debit":
                Type = "Asset"
            elif Statement == "Income Statement" and balance == "debit":
                Type = "Expense"
            elif Statement == "Income Statement" and balance == "credit":
                Type = "Income"
            else:
                Type = "Other"
                

            
            if elem_id and name:
                elements.append((Id, name, Statement, Type))
        
        # Store in SQLite database
        
        if connection is None:
                conn = sqlite3.connect(self.Database)
        else:
            conn = connection
        cursor = conn.cursor()
            


        # Create table
        self.create_table(table_name, ["Id", "Name", "Statement", "Type"], conn)


        
        # Insert data
        self.insert_data(table_name, ["Id", "Name", "Statement", "Type"], elements, conn)

        
        conn.commit()
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

    def adjust_string(self, input_string,check_substring, replace_substring ):
        """
        Adjusts the input string by replacing "jppfs_cor_" with "jppfs_cor:" if it starts with "jppfs_cor_".
        
        :param input_string: The string to adjust
        :return: The adjusted string
        """
        if input_string.startswith(check_substring):
            return input_string.replace(check_substring, replace_substring, 1)
        return input_string
        

    def SQL_to_CSV(self, input_table, CSV_Name, Query_Modifier = None , conn=None):

        if conn is None:
                conn = sqlite3.connect(self.Database)
        else:
            conn = connection
        cursor = conn.cursor()

        df = pd.read_sql_query(f"SELECT * FROM {input_table} {Query_Modifier}", conn)
        df.to_csv(CSV_Name)
