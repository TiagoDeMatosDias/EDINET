import config as c
import pandas as pd
import sqlite3
import xml.etree.ElementTree as ET
import random
import numpy as np

class data:
    def __init__(self):
        self.config = c.Config()
        self.baseURL = self.config.get("baseURL")
        self.key = self.config.get("apikey")
        self.defaultLocation = self.config.get("defaultLocation")
        self.Database = self.config.get("Database")
        self.Database_DocumentList = self.config.get("Database_DocumentList")


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

    def Filter_for_Relevant(self, input_table, output_table, input_column="AccountingTerm"):
        """
        Generates financial statements by querying data from the input table and 
        storing the results in the output table.

        Args:
            input_table (str): The name of the input table to query data from.
            output_table (str): The name of the output table to store the results.

        Returns:
            None
        """
        conn = sqlite3.connect(self.Database)
        cursor = conn.cursor()
        
        # Create the output table with the same schema as the input table
        cursor.execute(f"""CREATE TABLE IF NOT EXISTS {output_table} AS SELECT * FROM {input_table} WHERE   
            (
            AccountingTerm like 'jppfs_cor:NetSales'
            OR
            AccountingTerm like '%OrdinaryIncome'
            OR
            AccountingTerm like 'jppfs_cor:NetIncome'
            OR
            AccountingTerm like '%ProfitLoss'
            OR
            AccountingTerm like 'jppfs_cor:OperatingIncome'
            OR
            AccountingTerm like 'jppfs_cor:CostOfSales'
            OR
            AccountingTerm like 'jppfs_cor:TotalAssets'
            OR
            AccountingTerm like 'jppfs_cor:Assets'
            OR
            AccountingTerm like 'jppfs_cor:ShareholdersEquity'
            OR
            AccountingTerm like 'jppfs_cor:CashDividendsPaidFinCF'
            OR
            AccountingTerm like 'jppfs_cor:PurchaseOfTreasuryStockFinCF'
            OR
            AccountingTerm like 'jppfs_cor:NetCashProvidedByUsedInOperatingActivities'
            OR
            AccountingTerm like 'jppfs_cor:NetCashProvidedByUsedInInvestmentActivities'
            OR
            AccountingTerm like 'jppfs_cor:NetCashProvidedByUsedInFinancingActivities'
            OR
            AccountingTerm like 'jppfs_cor:CurrentAssets'
            OR
            AccountingTerm like 'jppfs_cor:CurrentLiabilities'
            OR
            AccountingTerm like 'jppfs_cor:Inventories'
            OR
            AccountingTerm like 'jppfs_cor:TotalDebt'
            OR
            AccountingTerm like 'jppfs_cor:TotalDebt'
            OR
            AccountingTerm like 'jppfs_cor:GrossProfit'
            )
            AND
            (                
            Period = 'CurrentYearDuration'
            OR            
            Period = 'CurrentYearInstant'            
            )
            AND
            (
            AccountingTerm LIKE 'jppfs_cor:%'
            )""")
        conn.commit()
        conn.close()


    def Generate_Financial_Ratios(self, input_table, output_table):
        # This function will take the input table and generate the financial ratios
        # Each ratio shall have both on a yearly and 3 and 5 year average
        # The output will be stored in the output_table
        # Ratio 1: Current Ratio = Current Assets / Current Liabilities
        # Ratio 2: Quick Ratio = (Current Assets - Inventory) / Current Liabilities
        # Ratio 3: Debt to Equity Ratio = Total Debt / Total Equity
        # Ratio 4: Return on Equity = Net Income / Total Equity
        # Ratio 5: Return on Assets = Net Income / Total Assets
        # Ratio 6: Gross Margin = Gross Profit / Revenue
        # Ratio 7: Operating Margin = Operating Income / Revenue
        # Ratio 8: Net Profit Margin = Net Income / Revenue
        # Ratio 9: Asset Turnover = Revenue / Total Assets
        # Ratio 10: Inventory Turnover = Cost of Goods Sold / Inventory

        # Connect to the database
        conn = sqlite3.connect(self.Database)

        # Get the list of companies
        companies = self.get_companyList(input_table, conn)
        # Per company
        exists = False
        for company in companies:
            # Get the data for the company
            df = pd.read_sql_query(f"""SELECT * FROM {input_table} WHERE edinetCode = '{company}' """, conn)

            # Calculate the ratios
                    
            RatiosTable = df.pivot_table(
                index=['edinetCode', 'docID',  'Currency', 'docTypeCode', 'periodStart', 'periodEnd'],
                columns='AccountingTerm',
                values='Amount',
                aggfunc='first'
            ).reset_index()

            # Flatten the columns
            RatiosTable.columns.name = None
            RatiosTable.columns = [col if isinstance(col, str) else col[1] for col in RatiosTable.columns]
            RatiosTable = pd.DataFrame(RatiosTable)

            # Convert any non-numeric columns to numeric, except for the following columns 'edinetCode', 'docID',  'Currency', 'docTypeCode', 'periodStart', 'periodEnd'
            numeric_columns = RatiosTable.columns.difference(['edinetCode', 'docID', 'Currency', 'docTypeCode', 'periodStart', 'periodEnd'])
            RatiosTable[numeric_columns] = RatiosTable[numeric_columns].apply(pd.to_numeric, errors='coerce')

            # Define the relevant columns for each parameter
            columns_mapping = {
                "netIncome": ["jppfs_cor:NetIncome", "jppfs_cor:ProfitLoss"],
                "netSales": ["jppfs_cor:NetSales"],
                "operatingIncome": ["jppfs_cor:OperatingIncome"],
                "grossProfit": ["jppfs_cor:GrossProfit"],
                "totalAssets": ["jppfs_cor:Assets", "jppfs_cor:TotalAssets"],
                "totalDebt": ["jppfs_cor:TotalDebt", "jppfs_cor:LongTermLoansPayable"],
                "shareholdersEquity": ["jppfs_cor:ShareholdersEquity"],
                "currentAssets": ["jppfs_cor:CurrentAssets"],
                "currentLiabilities": ["jppfs_cor:CurrentLiabilities"],
                "inventories": ["jppfs_cor:Inventories"],
                "costOfSales": ["jppfs_cor:CostOfSales"]
            }

            # Populate the new columns using a lambda function
            for new_col, relevant_cols in columns_mapping.items():
                RatiosTable[new_col] = RatiosTable.apply(lambda row: next((row[col] for col in relevant_cols if col in row and pd.notnull(row[col])), np.nan), axis=1)

            # Calculate the ratios with default value as zero for nulls
            RatiosTable["CurrentRatio"] = RatiosTable["currentAssets"].fillna(0) / RatiosTable["currentLiabilities"].fillna(1)
            RatiosTable["QuickRatio"] = (RatiosTable["currentAssets"].fillna(0) - RatiosTable["inventories"].fillna(0)) / RatiosTable["currentLiabilities"].fillna(1)
            RatiosTable["LiquidAssets"] = RatiosTable["currentAssets"].fillna(0) / RatiosTable["totalAssets"].fillna(1)
            RatiosTable["DebtToEquityRatio"] = RatiosTable["totalDebt"].fillna(0) / RatiosTable["shareholdersEquity"].fillna(1)
            RatiosTable["DebtToAssetsRatio"] = RatiosTable["totalDebt"].fillna(0) / RatiosTable["totalAssets"].fillna(1)
            RatiosTable["ReturnOnEquity"] = RatiosTable["netIncome"].fillna(0) / RatiosTable["shareholdersEquity"].fillna(1)
            RatiosTable["ReturnOnAssets"] = RatiosTable["netIncome"].fillna(0) / RatiosTable["totalAssets"].fillna(1)
            RatiosTable["GrossMargin"] = RatiosTable["grossProfit"].fillna(0) / RatiosTable["netSales"].fillna(1)
            RatiosTable["OperatingMargin"] = RatiosTable["operatingIncome"].fillna(0) / RatiosTable["netSales"].fillna(1)
            RatiosTable["NetProfitMargin"] = RatiosTable["netIncome"].fillna(0) / RatiosTable["netSales"].fillna(1)
            RatiosTable["AssetTurnover"] = RatiosTable["netSales"].fillna(0) / RatiosTable["totalAssets"].fillna(1)
            RatiosTable["InventoryTurnover"] = RatiosTable["costOfSales"].fillna(0) / RatiosTable["inventories"].fillna(1)

            """
            # Calculate the rankings per column
            RatiosTable["Ranking_CurrentRatio"] = RatiosTable["CurrentRatio"].rank(ascending=False )            
            RatiosTable["Ranking_QuickRatio"] = RatiosTable["QuickRatio"].rank(ascending=False)            
            RatiosTable["Ranking_LiquidAssets"] = RatiosTable["LiquidAssets"].rank(ascending=False)            
            RatiosTable["Ranking_DebtToEquityRatio"] = RatiosTable["DebtToEquityRatio"].rank(ascending=True)            
            RatiosTable["Ranking_DebtToAssetsRatio"]  = RatiosTable["DebtToAssetsRatio"].rank(ascending=True)            
            RatiosTable["Ranking_ReturnOnEquity"]  = RatiosTable["ReturnOnEquity"].rank(ascending=False)            
            RatiosTable["Ranking_ReturnOnAssets"] = RatiosTable["ReturnOnAssets"].rank(ascending=False)            
            RatiosTable["Ranking_GrossMargin"] = RatiosTable["GrossMargin"].rank(ascending=False)            
            RatiosTable["Ranking_OperatingMargin"]  = RatiosTable["OperatingMargin"].rank(ascending=False)            
            RatiosTable["Ranking_NetProfitMargin"]  = RatiosTable["NetProfitMargin"].rank(ascending=False)            
            RatiosTable["Ranking_AssetTurnover"]  = RatiosTable["AssetTurnover"].rank(ascending=False)            
            RatiosTable["Ranking_InventoryTurnover"] = RatiosTable["InventoryTurnover"].rank(ascending=False)  
              
            RatiosTable["Ranking_Overall"] = 1.5 * RatiosTable["Ranking_CurrentRatio"] +  RatiosTable["Ranking_QuickRatio"] + RatiosTable["Ranking_LiquidAssets"] + 1.5 * RatiosTable["Ranking_DebtToEquityRatio"] + 1.5 * RatiosTable["Ranking_DebtToAssetsRatio"] + 1.5 * RatiosTable["Ranking_ReturnOnEquity"] + 2 * RatiosTable["Ranking_ReturnOnAssets"] + RatiosTable["Ranking_GrossMargin"] + RatiosTable["Ranking_OperatingMargin"] + RatiosTable["Ranking_NetProfitMargin"] + 0.5 * RatiosTable["Ranking_AssetTurnover"] + 0.5 * RatiosTable["Ranking_InventoryTurnover"] 
            """

            # remove unnecessary columns
            columns_to_keep = ['edinetCode', 'docID', 'Currency', 'docTypeCode', 'periodStart', 'periodEnd', "CurrentRatio", "QuickRatio", "LiquidAssets", "DebtToEquityRatio", "DebtToAssetsRatio", "ReturnOnEquity", "ReturnOnAssets", "GrossMargin", "OperatingMargin", "NetProfitMargin", "AssetTurnover", "InventoryTurnover"]
            RatiosTable = RatiosTable.loc[:, columns_to_keep]


            # Store the data back to the database
            if exists:
                self.add_missing_columns(conn, output_table, RatiosTable)
            RatiosTable.to_sql(output_table, conn, if_exists='append')
            conn.commit()
            exists = True
        
        
        
        conn.close()

        pass


    def Generate_Rankings(self, input_table, output_table, columns):
        """
        This function takes the data from an input table and ranks the columns (either ascending or descending), it also generates a weighted rank of all the columns.
        The output is placed in the output table.
        The columns parameter is a dictionary containing the column names, whether it is to be ranked in an ascending or descending manner, and the weight it should have in the overall ranking.
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

            # Rank the columns
            for column, (ascending, weight) in columns.items():
                df[f"Ranking_{column}"] = df[column].rank(ascending=ascending)

            # Store the data back to the database
            df.to_sql(output_table, conn, if_exists='replace' if first_chunk else 'append', index=False)
            conn.commit()

            offset += chunk_size
            first_chunk = False
 

        # Calculate the average ranks for each unique edinetCode
        df = pd.read_sql_query(f"SELECT * FROM {output_table}", conn)
        # Select only numeric columns for mean calculation
        numeric_columns = df.select_dtypes(include=[np.number]).columns.tolist()
        avg_ranks = df.groupby('edinetCode')[numeric_columns].mean().reset_index()

        # Generate the weighted rank
        avg_ranks["Weighted_Rank"] = avg_ranks[[f"Ranking_{column}" for column in columns]].dot([weight for _, weight in columns.values()])

        # Group by edinetCode and sum the values in other columns
        grouped_df = avg_ranks.groupby('edinetCode').mean().reset_index()

        # Store the grouped data back to the database
        grouped_df.to_sql(output_table, conn, if_exists='replace', index=False)
        conn.commit()

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
