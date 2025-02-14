# This is a sample Python script.
import classes.EDINET as e
from config import Config
import classes.helper as h
import classes.data as d

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

    print(f"baseURL: {baseURL}")
    print(f"apikey: {apikey}")
    print(f"defaultLocation: {defaultLocation}")

    edinet = e.Edinet()    
    #Working!!!
    #edinet.get_All_documents_withMetadata("2015-01-01", "2025-01-31")

    #Working!!!
    #filters = edinet.generate_filter("edinetCode", "=", "E01296")
    #filters = edinet.generate_filter("docTypeCode", "=", "120")
    #filters = edinet.generate_filter("csvFlag", "=", "1", filters)
    #filters = edinet.generate_filter("secCode", "", "", filters, True)
    #docs = edinet.query_database_select("downloadList", filters , "DocsToDownload")
    #edinet.downloadDoc("S1004542","C:\\programming\\Finance\\EDINET\\EDINET\\testdata\\inputs", "1")

    #edinet.downloadDocs("downloadList", "financialData_full")
    #edinet.clear_table("DocsToDownload")
    #edinet.store_edinetCodes("testdata\\inputs\\EdinetcodeDlInfo.csv")

    data = d.data()
    #data.copy_table_to_Standard("financialData_full", "Standardized_Data_Complete")
    #data.Generate_Financial_Ratios("Standardized_Data_Complete", "Standardized_Data_Complete_Ratios")
    #data.parse_edinet_taxonomy("testdata\\inputs\\Taxonomy\\taxonomy\\jppfs\\2024-11-01\\jppfs_cor_2024-11-01.xsd", "TAXONOMY_JPFS_COR")

    columns = {
    'CurrentRatio': (False, 1.5),
    'QuickRatio': (False, 1.0),
    'LiquidAssets': (False, 1.0),
    'DebtToEquityRatio': (True, 1.0),
    'DebtToAssetsRatio': (True, 1.5),
    'ReturnOnEquity': (False, 1.0),
    'ReturnOnAssets': (False, 1.2),
    'GrossMargin': (False, 1.5),
    'OperatingMargin': (False, 1.5),
    'NetProfitMargin': (False, 2.0),
    'AssetTurnover': (False, 0.5),
    'InventoryTurnover': (False, 0.5)
    }

    #data.Generate_Rankings("Standardized_Data_Complete_Ratios", "Standardized_Data_Complete_Ratios_Rankings", columns)
    data.SQL_to_CSV("Standardized_Data_Complete_Ratios_Rankings", "Standardized_Data_Complete_Ratios_Rankings.csv", "left join edinet_codes on edinet_codes.EdinetCode = Standardized_Data_Complete_Ratios_Rankings.edinetCode")
    print('Program Ended')


