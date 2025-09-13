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
            get_documents_config = config.get("get_documents_config", {})
            startDate = get_documents_config.get("startDate")
            endDate = get_documents_config.get("endDate")
            edinet.get_All_documents_withMetadata(startDate, endDate, Database_DocumentList)
        except Exception as e:
            print(f"Error getting documents: {e}")

    if run_steps.get("download_documents"):
        try:
            print("Downloading documents...")
            download_documents_config = config.get("download_documents_config", {})
            docTypeCode = download_documents_config.get("docTypeCode")
            csvFlag = download_documents_config.get("csvFlag")
            secCode = download_documents_config.get("secCode")
            downloaded_flag = download_documents_config.get("Downloaded")

            filters = edinet.generate_filter("docTypeCode", "=", docTypeCode)
            filters = edinet.generate_filter("csvFlag", "=", csvFlag, filters)
            filters = edinet.generate_filter("secCode", "!=", secCode, filters)
            filters = edinet.generate_filter("Downloaded", "=", downloaded_flag, filters)
            edinet.downloadDocs(Database_DocumentList, FinancialData, filters)
        except Exception as e:
            print(f"Error downloading documents: {e}")

    if run_steps.get("standardize_data"):
        try:
            print("Standardizing data...")
            data.copy_table_to_Standard(FinancialData, Database_Standardized )
        except Exception as e:
            print(f"Error standardizing data: {e}")

    if run_steps.get("generate_financial_ratios"):
        try:
            print("Generating financial ratios...")
            data.Generate_Financial_Ratios(Database_Standardized, Database_Standardized + "_Ratios")
        except Exception as e:
            print(f"Error generating financial ratios: {e}")

    if run_steps.get("aggregate_ratios"):
        try:
            print("Aggregating ratios...")
            data.Generate_Aggregated_Ratios(Database_Standardized + "_Ratios", Database_Standardized + "_Ratios_Aggregated")
        except Exception as e:
            print(f"Error aggregating ratios: {e}")

    if run_steps.get("update_stock_prices"):
        try:
            print("Updating stock prices...")
            y.update_all_stock_prices(config.get("DB_PATH"), only_update_empty=True)
        except Exception as e:
            print(f"Error updating stock prices: {e}")

    print('Program Ended')