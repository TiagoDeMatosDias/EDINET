import json
import src.edinet_api as edinet_api
from config import Config
import src.data_processing as d
import src.yahoofinance as y
import src.regression_analysis as r

def run(edinet=None, data=None):
    """
    Orchestrates the execution of the application based on the run config file.
    """
    print('Starting Program')

    print('Loading Config')
    config = Config()
    run_steps = config.get("run_steps", {})

    # Access values from main config
    DB_DOC_LIST_TABLE = config.get("DB_DOC_LIST_TABLE")
    DB_FINANCIAL_DATA_TABLE = config.get("DB_FINANCIAL_DATA_TABLE")
    DB_STANDARDIZED_TABLE = config.get("DB_STANDARDIZED_TABLE")
    DB_STANDARDIZED_RATIOS_TABLE = config.get("DB_STANDARDIZED_RATIOS_TABLE")
    DB_PATH = config.get("DB_PATH")
    DB_COMPANY_INFO_TABLE = config.get("DB_COMPANY_INFO_TABLE")
    DB_STOCK_PRICES_TABLE = config.get("DB_STOCK_PRICES_TABLE")

    if not edinet:
        edinet = edinet_api.Edinet()
    if not data:
        data = d.data()


    if run_steps.get("get_documents"):
        try:
            print("Getting all documents with metadata...")
            get_documents_config = config.get("get_documents_config", {})
            startDate = get_documents_config.get("startDate")
            endDate = get_documents_config.get("endDate")
            edinet.get_All_documents_withMetadata(startDate, endDate)
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
            edinet.downloadDocs(DB_DOC_LIST_TABLE, DB_FINANCIAL_DATA_TABLE, filters)
        except Exception as e:
            print(f"Error downloading documents: {e}")

    if run_steps.get("standardize_data"):
        try:
            print("Standardizing data...")
            data.copy_table_to_Standard(DB_FINANCIAL_DATA_TABLE, DB_STANDARDIZED_TABLE)
        except Exception as e:
            print(f"Error standardizing data: {e}")

    if run_steps.get("generate_financial_ratios"):
        try:
            print("Generating financial ratios...")
            data.Generate_Financial_Ratios(DB_STANDARDIZED_TABLE, DB_STANDARDIZED_RATIOS_TABLE)
        except Exception as e:
            print(f"Error generating financial ratios: {e}")


    if run_steps.get("update_stock_prices"):
        try:
            print("Updating stock prices...")
            y.update_all_stock_prices(DB_PATH, DB_COMPANY_INFO_TABLE, DB_STOCK_PRICES_TABLE)
        except Exception as e:
            print(f"Error updating stock prices: {e}")

    if run_steps.get("run_regression"):
        try:
            print("Running regression...")
            r.Regression(config, DB_PATH)
        except Exception as e:
            print(f"Error running regression: {e}")

    print('Program Ended')