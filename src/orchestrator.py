import json
import src.edinet_api as e
from config import Config
import src.data_processing as d
import src.yahoofinance as y

def run(edinet=None, data=None):
    """
    Orchestrates the execution of the application based on the run_config.json file.
    """
    print('Starting Program')

    print('Loading Config')
    config = Config()
    run_steps = config.get("run_steps", {})

    # Access values from main config
    Database_DocumentList = config.get("DB_DOC_LIST_TABLE")
    FinancialData = config.get("DB_FINANCIAL_DATA_TABLE")
    Database_Standardized = config.get("DB_STANDARDIZED_TABLE")

    if not edinet:
        edinet = e.Edinet()
    if not data:
        data = d.data()

    if run_steps.get("get_documents"):
        try:
            print("Getting all documents with metadata...")
            edinet.get_All_documents_withMetadata("2025-07-01", "2025-08-01", Database_DocumentList)
        except Exception as e:
            print(f"Error getting documents: {e}")

    if run_steps.get("download_documents"):
        try:
            print("Downloading documents...")
            filters = edinet.generate_filter("docTypeCode", "=", "120")
            filters = edinet.generate_filter("csvFlag", "=", "1", filters)
            filters = edinet.generate_filter("secCode", "!=", "", filters)
            filters = edinet.generate_filter("Downloaded", "=", "False", filters)
            edinet.downloadDocs(Database_DocumentList, FinancialData, filters)
        except Exception as e:
            print(f"Error downloading documents: {e}")

    if run_steps.get("standardize_data"):
        try:
            print("Standardizing data...")
            data.copy_table_to_Standard(FinancialData, Database_Standardized + "_2")
        except Exception as e:
            print(f"Error standardizing data: {e}")

    if run_steps.get("generate_financial_ratios"):
        try:
            print("Generating financial ratios...")
            data.Generate_Financial_Ratios(Database_Standardized + "_2", Database_Standardized + "_Ratios_2")
        except Exception as e:
            print(f"Error generating financial ratios: {e}")

    if run_steps.get("aggregate_ratios"):
        try:
            print("Aggregating ratios...")
            data.Generate_Aggregated_Ratios(Database_Standardized + "_Ratios_2", Database_Standardized + "_Ratios_Aggregated_2")
        except Exception as e:
            print(f"Error aggregating ratios: {e}")

    if run_steps.get("update_stock_prices"):
        try:
            print("Updating stock prices...")
            y.update_all_stock_prices(config.get("DB_PATH"), only_update_empty=True)
        except Exception as e:
            print(f"Error updating stock prices: {e}")

    print('Program Ended')