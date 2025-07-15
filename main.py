# This is a sample Python script.
import classes.EDINET as e
from config import Config
import classes.helper as h
import classes.data as d
import classes.yahoofinance as y

from datetime import datetime
# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
# Collect data from API
def get_FileswithMeta(start_date="2015-01-01", end_date=None):
    fileswithmeta = edinet.get_All_documents_withMetadata(start_date, end_date)
    fileswithMetaLocation = defaultLocation + "\\" + datetime.now().strftime("%Y-%m-%d_%H-%M-%S") + "-EDINET-File-List.csv"
    h.json_list_to_csv(fileswithmeta, fileswithMetaLocation)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print('Starting Program')

    print('Loading Config')
    config = Config()  # Create instance

    # Access values
    baseURL = config.get("baseURL")
    apikey = config.get("apikey")
    defaultLocation = config.get("defaultLocation")
    Database_DocumentList = config.get("Database_DocumentList")
    Database_downloadList = config.get("Database_downloadList")
    FinancialData = config.get("Database_FinancialData")
    Database_Standardized = config.get("Database_Standardized")


    print(f"baseURL: {baseURL}")
    print(f"apikey: {apikey}")
    print(f"defaultLocation: {defaultLocation}")
    print(f"Database_DocumentList: {Database_DocumentList}")
    print(f"FinancialData: {FinancialData}")

    edinet = e.Edinet()    
    #Working!!!
    # Get all documents released between two dates
    #edinet.get_All_documents_withMetadata("2025-01-01", "2025-07-01",Database_DocumentList)

    # Add the documents to the database that match the criteria
    # You need the following filters for the baseline annual reports only
    filters = edinet.generate_filter("docTypeCode", "=", "120")
    filters = edinet.generate_filter("csvFlag", "=", "1", filters)
    filters = edinet.generate_filter("secCode", "!=", "", filters)
    filters = edinet.generate_filter("Downloaded", "=", "False", filters)
    #edinet.downloadDocs(Database_DocumentList, FinancialData, filters)

    # Data class instantiation
    data = d.data()

    # Standardize the data you have downloaded    
    #data.copy_table_to_Standard(FinancialData, Database_Standardized)

    # Generate financial ratios for the standardized data
    # data.Generate_Financial_Ratios(Database_Standardized, Database_Standardized + "_Ratios")

    # Aggregate the ratios
    data.Generate_Aggregated_Ratios(Database_Standardized + "_Ratios", Database_Standardized + "_Ratios_Aggregated")

    #data = d.data()
    #data.copy_table_to_Standard(FinancialData, "Standardized_Data_Complete")
    #data.Generate_Financial_Ratios("Standardized_Data_Complete", "Standardized_Data_Complete_Ratios")
    #data.parse_edinet_taxonomy("testdata\\inputs\\Taxonomy\\taxonomy\\jppfs\\2024-11-01\\jppfs_cor_2024-11-01.xsd", "TAXONOMY_JPFS_COR")

    #data.copy_table_to_Standard("financialData_full", "Standardized_Data_Complete")
    #data.Generate_Financial_Ratios("Standardized_Data_Complete", "Standardized_Data_Complete_Ratios")

    columns = {
    'CurrentRatio': (False, 1.5),
    'QuickRatio': (False, 1.0),
    'LiquidAssets': (False, 1.0),
    'DebtToEquityRatio': (True, 1.0),
    'DebtToAssetsRatio': (True, 1.5),
    'ReturnOnEquity': (False, 1.0),
    'ReturnOnAssets': (False, 2),
    'GrossMargin': (False, 1),
    'OperatingMargin': (False, 1.5),
    'NetProfitMargin': (False, 2.0),
    'AssetTurnover': (False, 0.5),
    'InventoryTurnover': (False, 0.5),
    'ShareholderPayout': (False, 1),
    'FreeCashflowMargin': (False, 1.5),
    'netSales_Growth' : (False, 2),
    'netIncome_Growth' : (False, 2)
    }
    #data.Generate_Rankings("Standardized_Data_Complete_Ratios", "Standardized_Data_Complete_Ratios_Rankings", columns)
    
    #y.update_all_stock_prices(config.get("Database"), only_update_empty=True)

    print('Program Ended')


