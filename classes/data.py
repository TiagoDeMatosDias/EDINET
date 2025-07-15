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
        
        # Define the LIKE arguments for AccountingTerm and Period
        accounting_term_conditions = [
            "jppfs_cor:NetSales",
            "jppfs_cor:OperatingRevenue1",
            "jppfs_cor:IncomeBeforeIncomeTaxes",
            "%OrdinaryIncome",
            "jppfs_cor:NetIncome",
            "%ProfitLoss",
            "jppfs_cor:OperatingIncome",
            "jppfs_cor:CostOfSales",
            "jppfs_cor:TotalAssets",
            "jppfs_cor:Assets",
            "jppfs_cor:ShareholdersEquity",
            "jppfs_cor:CashDividendsPaidFinCF",
            "jppfs_cor:PurchaseOfTreasuryStockFinCF",
            "jppfs_cor:NetCashProvidedByUsedInOperatingActivities",
            "jppfs_cor:NetCashProvidedByUsedInInvestmentActivities",
            "jppfs_cor:NetCashProvidedByUsedInFinancingActivities",
            "jppfs_cor:CurrentAssets",
            "jppfs_cor:CurrentLiabilities",
            "jppfs_cor:Inventories",
            "jppfs_cor:TotalDebt",
            "jppfs_cor:GrossProfit"
        ]

        period_conditions = [
            "CurrentYearDuration",
            "CurrentYearInstant",
            "Prior1YearDuration"
        ]

        # Build the SQL query dynamically
        accounting_term_query = " OR ".join([f"AccountingTerm LIKE '{term}'" for term in accounting_term_conditions])
        period_query = " OR ".join([f"Period = '{period}'" for period in period_conditions])

        query = f"""
            CREATE TABLE IF NOT EXISTS {output_table} AS 
            SELECT 
                AccountingTerm,
                Period,
                Currency,
                Amount,
                docID,
                edinetCode,
                docTypeCode,
                submitDateTime,
                periodStart,
                periodEnd
            FROM {input_table} WHERE (
            ({accounting_term_query}) AND ({period_query}) AND (AccountingTerm LIKE 'jppfs_cor:%')
            ) OR (
            AccountingTerm = 'jpcrp_cor:TotalNumberOfIssuedSharesSummaryOfBusinessResults' AND 
            Period = 'CurrentYearInstant_NonConsolidatedMember'
            ) OR (
            AccountingTerm = 'jpcrp_cor:PriceEarningsRatioSummaryOfBusinessResults' AND 
            Period = 'CurrentYearDuration'
            )
        """

        # Execute the query
        cursor.execute(query)
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

            # Convert any non-numeric columns to numeric, except for the following columns 'edinetCode', 'docID',  'Currency', 'docTypeCode', 'periodStart', 'periodEnd'
            numeric_columns = RatiosTable.columns.difference(['edinetCode', 'docID', 'Currency', 'docTypeCode', 'periodStart', 'periodEnd'])
            RatiosTable[numeric_columns] = RatiosTable[numeric_columns].apply(pd.to_numeric, errors='coerce')

            # Define the relevant columns for each parameter
            columns_mapping = {
                "netIncome": ["jppfs_cor:NetIncome_CurrentYearDuration", "jppfs_cor:ProfitLoss_CurrentYearDuration"],
                "netIncome_PriorYear": ["jppfs_cor:NetIncome_Prior1YearDuration", "jppfs_cor:ProfitLoss_Prior1YearDuration"],
                "netSales": ["jppfs_cor:NetSales_CurrentYearDuration", "jppfs_cor:OperatingRevenue1_CurrentYearDuration"],
                "netSales_PriorYear": ["jppfs_cor:NetSales_Prior1YearDuration", "jppfs_cor:OperatingRevenue1_Prior1YearDuration"],
                "operatingIncome": ["jppfs_cor:OperatingIncome_CurrentYearDuration"],
                "operatingIncome_PriorYear": ["jppfs_cor:OperatingIncome_Prior1YearDuration"],
                "grossProfit": ["jppfs_cor:GrossProfit_CurrentYearDuration"],
                "grossProfit_PriorYear": ["jppfs_cor:GrossProfit_Prior1YearDuration"],
                "totalAssets": ["jppfs_cor:Assets_CurrentYearInstant", "jppfs_cor:TotalAssets_CurrentYearInstant"],
                "totalAssets_PriorYear": ["jppfs_cor:Assets_Prior1YearInstant", "jppfs_cor:TotalAssets_Prior1YearInstant"],
                "totalDebt": ["jppfs_cor:TotalDebt_CurrentYearInstant", "jppfs_cor:LongTermLoansPayable_CurrentYearInstant"],
                "totalDebt_PriorYear": ["jppfs_cor:TotalDebt_Prior1YearInstant", "jppfs_cor:LongTermLoansPayable_Prior1YearInstant"],
                "shareholdersEquity": ["jppfs_cor:ShareholdersEquity_CurrentYearInstant"],
                "shareholdersEquity_PriorYear": ["jppfs_cor:ShareholdersEquity_Prior1YearInstant"],
                "currentAssets": ["jppfs_cor:CurrentAssets_CurrentYearInstant"],
                "currentAssets_PriorYear": ["jppfs_cor:CurrentAssets_Prior1YearInstant"],
                "currentLiabilities": ["jppfs_cor:CurrentLiabilities_CurrentYearInstant"],
                "currentLiabilities_PriorYear": ["jppfs_cor:CurrentLiabilities_Prior1YearInstant"],
                "inventories": ["jppfs_cor:Inventories_CurrentYearInstant"],
                "inventories_PriorYear": ["jppfs_cor:Inventories_Prior1YearInstant"],
                "costOfSales": ["jppfs_cor:CostOfSales_CurrentYearDuration"],
                "costOfSales_PriorYear": ["jppfs_cor:CostOfSales_Prior1YearDuration"],
                "dividends": ["jppfs_cor:CashDividendsPaidFinCF_CurrentYearDuration"],
                "dividends_PriorYear": ["jppfs_cor:CashDividendsPaidFinCF_Prior1YearDuration"],
                "buybacks": ["jppfs_cor:PurchaseOfTreasuryStockFinCF_CurrentYearDuration"],
                "buybacks_PriorYear": ["jppfs_cor:PurchaseOfTreasuryStockFinCF_Prior1YearDuration"],
                "operatingCashflow": ["jppfs_cor:NetCashProvidedByUsedInOperatingActivities_CurrentYearDuration"],
                "operatingCashflow_PriorYear": ["jppfs_cor:NetCashProvidedByUsedInOperatingActivities_Prior1YearDuration"],
                "investmentCashflow": ["jppfs_cor:NetCashProvidedByUsedInInvestmentActivities_CurrentYearDuration"],
                "investmentCashflow_PriorYear": ["jppfs_cor:NetCashProvidedByUsedInInvestmentActivities_Prior1YearDuration"],
                "financingCashflow": ["jppfs_cor:NetCashProvidedByUsedInFinancingActivities_CurrentYearDuration"],
                "financingCashflow_PriorYear": ["jppfs_cor:NetCashProvidedByUsedInFinancingActivities_Prior1YearDuration"],
                "SharesOutstanding" : ["jpcrp_cor:TotalNumberOfIssuedSharesSummaryOfBusinessResults_CurrentYearInstant_NonConsolidatedMember"],
                "PE_Ratio" : ["jpcrp_cor:PriceEarningsRatioSummaryOfBusinessResults_CurrentYearDuration"]
            }

            # Populate the new columns using a lambda function
            for new_col, relevant_cols in columns_mapping.items():
                RatiosTable[new_col] = RatiosTable.apply(lambda row: next((row[col] for col in relevant_cols if col in row and pd.notnull(row[col])), np.nan), axis=1)

            # Flatten the list of all relevant columns from the columns_mapping dictionary
            columns_to_remove = [col for relevant_cols in columns_mapping.values() for col in relevant_cols]

            # Remove the relevant columns from the RatiosTable
            RatiosTable.drop(columns=columns_to_remove, inplace=True, errors='ignore')

            # Remove any columns whose name begins with jppfs_cor:
            RatiosTable = RatiosTable.loc[:, ~RatiosTable.columns.str.startswith('jppfs_cor:')]
            # Remove any columns whose name begins with jpcrp_cor:
            RatiosTable = RatiosTable.loc[:, ~RatiosTable.columns.str.startswith('jpcrp_cor:')]

            RatiosTable.reset_index( drop=True, inplace=True)


            # Calculate additional data
            RatiosTable["Cashflow_free"] = (RatiosTable["operatingCashflow"].fillna(0) + RatiosTable["investmentCashflow"].fillna(0))
            RatiosTable["Cashflow_equity"] = ( RatiosTable["dividends"].fillna(0) + RatiosTable["buybacks"].fillna(0) )
            RatiosTable["Cashflow_debt"] = (RatiosTable["financingCashflow"].fillna(0) - RatiosTable["Cashflow_equity"].fillna(0))
            RatiosTable["NCAV"] = RatiosTable["currentAssets"].fillna(0) - (RatiosTable["totalAssets"].fillna(1) - RatiosTable["shareholdersEquity"].fillna(1))


            # Calculate the ratios with default value as zero for nulls
            RatiosTable["Ratio_Current"] = RatiosTable["currentAssets"].fillna(0) / RatiosTable["currentLiabilities"].fillna(1)
            RatiosTable["Ratio_QuickRatio"] = (RatiosTable["currentAssets"].fillna(0) - RatiosTable["inventories"].fillna(0)) / RatiosTable["currentLiabilities"].fillna(1)
            RatiosTable["Ratio_LiquidAssets"] = RatiosTable["currentAssets"].fillna(0) / RatiosTable["totalAssets"].fillna(1)
            RatiosTable["Ratio_DebtToEquity"] = RatiosTable["totalDebt"].fillna(0) / RatiosTable["shareholdersEquity"].fillna(1)
            RatiosTable["Ratio_DebtToAssets"] = RatiosTable["totalDebt"].fillna(0) / RatiosTable["totalAssets"].fillna(1)
            RatiosTable["Ratio_ReturnOnEquity"] = RatiosTable["netIncome"].fillna(0) / RatiosTable["shareholdersEquity"].fillna(1)
            RatiosTable["Ratio_ReturnOnAssets"] = RatiosTable["netIncome"].fillna(0) / RatiosTable["totalAssets"].fillna(1)
            RatiosTable["Ratio_GrossMargin"] = RatiosTable["grossProfit"].fillna(0) / RatiosTable["netSales"].fillna(1)
            RatiosTable["Ratio_OperatingMargin"] = RatiosTable["operatingIncome"].fillna(0) / RatiosTable["netSales"].fillna(1)
            RatiosTable["Ratio_NetProfitMargin"] = RatiosTable["netIncome"].fillna(0) / RatiosTable["netSales"].fillna(1)
            RatiosTable["Ratio_AssetTurnover"] = RatiosTable["netSales"].fillna(0) / RatiosTable["totalAssets"].fillna(1)
            RatiosTable["Ratio_InventoryTurnover"] = RatiosTable["costOfSales"].fillna(0) / RatiosTable["inventories"].fillna(1)                        
            RatiosTable["Ratio_BuybackPayout"] = (  -1 * RatiosTable["buybacks"].fillna(0)  )/ RatiosTable["netIncome"].fillna(1)
            RatiosTable["Ratio_ShareholderPayout"] = ( -1 * RatiosTable["Cashflow_equity"] )/ RatiosTable["netIncome"].fillna(1)
            RatiosTable["Ratio_DividendPayout"] = ( -1 * RatiosTable["dividends"].fillna(0)  )/ RatiosTable["netIncome"].fillna(1)
            RatiosTable["Ratio_FreeCashflowMargin"] = RatiosTable["Cashflow_free"].fillna(0) / RatiosTable["netSales"].fillna(1)
            RatiosTable["Ratio_CashflowToEquityMargin"] = RatiosTable["Cashflow_equity"].fillna(0) / RatiosTable["netSales"].fillna(1)
            RatiosTable["Ratio_CashflowToDebtMargin"] = RatiosTable["Cashflow_debt"].fillna(0) / RatiosTable["netSales"].fillna(1)
            RatiosTable["Ratio_netSales_Growth"] = (RatiosTable["netSales"].fillna(0) / RatiosTable["netSales_PriorYear"].fillna(RatiosTable["netSales"].fillna(1))) - 1
            RatiosTable["Ratio_netIncome_Growth"] = (RatiosTable["netIncome"].fillna(0) / RatiosTable["netIncome_PriorYear"].fillna(RatiosTable["netIncome"].fillna(1))) - 1

            # Calculate Share 
            RatiosTable["PerShare_BookValue"] = RatiosTable["shareholdersEquity"].fillna(0) / RatiosTable["SharesOutstanding"].fillna(1)
            RatiosTable["PerShare_Earnings"] = (RatiosTable["netIncome"].fillna(0) / RatiosTable["SharesOutstanding"].fillna(1)) 
            RatiosTable["PerShare_Sales"] = (RatiosTable["netSales"].fillna(0) / RatiosTable["SharesOutstanding"].fillna(1)) 
            RatiosTable["PerShare_NCAV"] = (RatiosTable["NCAV"].fillna(0) / RatiosTable["SharesOutstanding"].fillna(1)) 
            RatiosTable["PerShare_Cashflow_free"] = (RatiosTable["Cashflow_free"].fillna(0) / RatiosTable["SharesOutstanding"].fillna(1)) 
            RatiosTable["PerShare_Cashflow_equity"] = (RatiosTable["Cashflow_equity"].fillna(0) / RatiosTable["SharesOutstanding"].fillna(1)) 
            RatiosTable["PerShare_Cashflow_debt"] = (RatiosTable["Cashflow_debt"].fillna(0) / RatiosTable["SharesOutstanding"].fillna(1)) 
            RatiosTable["PerShare_Dividends"] = (-RatiosTable["dividends"].fillna(0) / RatiosTable["SharesOutstanding"].fillna(1)) 
            RatiosTable["PerShare_Buybacks"] = (-RatiosTable["buybacks"].fillna(0) / RatiosTable["SharesOutstanding"].fillna(1))             
            RatiosTable["PerShare_TotalPayout"] = RatiosTable["PerShare_Buybacks"].fillna(0) + RatiosTable["PerShare_Dividends"].fillna(0)
            RatiosTable["PerShare_SharePrice"] = RatiosTable["PerShare_Earnings"].fillna(0) * RatiosTable["PE_Ratio"].fillna(1)

            # Price Ratios
            RatiosTable["Ratio_PriceEarnings"] = RatiosTable["PerShare_SharePrice"].fillna(0) / RatiosTable["PerShare_Earnings"].fillna(1)
            RatiosTable["Ratio_EarningsYield"] = RatiosTable["PerShare_Earnings"].fillna(0) / RatiosTable["PerShare_SharePrice"].fillna(1)
            RatiosTable["Ratio_PriceBook"] = RatiosTable["PerShare_SharePrice"].fillna(0) / RatiosTable["PerShare_BookValue"].fillna(1)
            RatiosTable["Ratio_PriceNCAV"] = RatiosTable["PerShare_SharePrice"].fillna(0) / RatiosTable["PerShare_NCAV"].fillna(1)
            RatiosTable["Ratio_FreeCashflowYield"] = RatiosTable["PerShare_Cashflow_free"].fillna(0) / RatiosTable["PerShare_SharePrice"].fillna(1)
            RatiosTable["Ratio_DividendsYield"] = RatiosTable["PerShare_Dividends"].fillna(0) / RatiosTable["PerShare_SharePrice"].fillna(1)
            RatiosTable["Ratio_BuybacksYield"] = RatiosTable["PerShare_Buybacks"].fillna(0) / RatiosTable["PerShare_SharePrice"].fillna(1)
            RatiosTable["Ratio_TotalPayoutYield"] = RatiosTable["PerShare_TotalPayout"].fillna(0) / RatiosTable["PerShare_SharePrice"].fillna(1)

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
        for the count of periods aggregated, processing data in chunks to handle large datasets.
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

            # Add a column for the count of periods aggregated
            count_df = df.groupby('edinetCode').size().reset_index(name='number_of_Periods')
            aggregated_df = pd.merge(aggregated_df, count_df, on='edinetCode')

            # Round the values to 2 decimal places
            aggregated_df = aggregated_df.round(2)

            # Store the aggregated data back to the database
            aggregated_df.to_sql(output_table, conn, if_exists='replace' if first_chunk else 'append', index=False)
            conn.commit()

            offset += chunk_size
            first_chunk = False

        conn.close()


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
        avg_ranks = df.groupby('edinetCode')[numeric_columns].median().reset_index()

        # Generate the weighted rank
        avg_ranks["Weighted_Rank"] = avg_ranks[[f"Ranking_{column}" for column in columns]].dot([weight for _, weight in columns.values()])

        # Group by edinetCode and sum the values in other columns
        grouped_df = avg_ranks.groupby('edinetCode').mean().reset_index()

        # Simple Valuation
        grouped_df["netIncomePerShare"] = (grouped_df["netIncome"] /  grouped_df["SharesOutstanding"]).round(0)
        grouped_df["NCAV_PerShare"] = (grouped_df["NCAV"] /  grouped_df["SharesOutstanding"]).round(0)

        grouped_df["Valuation_PerShare_netIncomeDiscount_10pct"] = (grouped_df["netIncomePerShare"]/(0.1 - grouped_df["netSales_Growth"].apply(lambda x: min(x, 0.08)))).round(0)
        grouped_df["Valuation_PerShare_netIncomeDiscount_12pct"] = (grouped_df["netIncomePerShare"]/(0.12 - grouped_df["netSales_Growth"].apply(lambda x: min(x, 0.08)))).round(0)
        grouped_df["Valuation_PerShare_netIncomeDiscount_8pct"] = (grouped_df["netIncomePerShare"]/(0.08 - grouped_df["netSales_Growth"].apply(lambda x: min(x, 0.08)))).round(0)

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
