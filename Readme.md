# EDINET Data Downloader and Screener

This program is designed to download financial data from the Japanese Securities Regulator (EDINET) and store it in a SQLite database. The main goal is to create a financial screener that allows users to query and analyze the data for future processing.

## Key Functionality

1. **Download Data**: Fetches financial documents from the EDINET API and stores them in a local database.
2. **Data Processing**: Processes the downloaded data to generate financial ratios and rankings.
3. **Database Management**: Manages the storage and retrieval of data in a SQLite database.
4. **Data Export**: Exports data from the database to CSV files for further analysis.

## Main Functions and Methods

### 1. Download Data

- **

get_All_documents_withMetadata(start_date, end_date)

**: Fetches all available document IDs for a given company from the EDINET API and stores them in the database.
- **

downloadDoc(docID, fileLocation, docTypeCode)

**: Downloads a specific document from EDINET and saves it as a ZIP file.
- **

downloadDocs(input_table, output_table)

**: Downloads multiple documents based on a list of document IDs from the database.

### 2. Data Processing

- **

load_financial_data(financialFiles, table_name, doc, connection)

**: Reads financial data from unzipped files and loads it into the SQLite database.
- **

Generate_Financial_Ratios(input_table, output_table)

**: Generates financial ratios from the input table and stores the results in the output table.
- **

Generate_Rankings(input_table, output_table, columns)

**: Ranks the columns based on specified criteria and generates a weighted rank of all the columns.

### 3. Database Management

- **

create_table(table_name, columns, connection)

**: Creates a table in the SQLite database with the given columns.
- **

insert_data(table_name, columns, rows, connection)

**: Inserts data into a table in the SQLite database.
- **

query_database_select(table, filters, output_table)

**: Executes a custom query on the SQLite database and returns the result as a JSON object or copies the data to another table.
- **

clear_table(table_name)

**: Clears the specified table in the SQLite database.

### 4. Data Export

- **

SQL_to_CSV(input_table, CSV_Name, Query_Modifier, conn)

**: Exports data from the specified table to a CSV file.

## Usage

1. **Configuration**: Ensure that the 

config.json

 file is properly set up with the necessary API keys and database paths.
2. **Running the Script**: Execute the 

main.py

 script to start the program.
3. **Downloading Data**: Use the provided functions to download and process data from EDINET.
4. **Exporting Data**: Export the processed data to CSV files for further analysis.

## Example

```python
# Example usage in main.py
if __name__ == '__main__':
    print('Starting Program')

    # Load configuration
    config = Config()

    # Initialize EDINET and Data classes
    edinet = Edinet()
    data = data()

    # Download documents
    edinet.downloadDocs("downloadList", "financialData_full")

    # Generate financial ratios
    data.Generate_Financial_Ratios("financialData_full", "financialData_ratios")

    # Define columns for ranking
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

    # Generate rankings
    data.Generate_Rankings("financialData_ratios", "financialData_rankings", columns)

    # Export rankings to CSV
    data.SQL_to_CSV("financialData_rankings", "financialData_rankings.csv")

    print('Program Ended')
```


# Key Document Types for Annual Reports:
Document Type |	Japanese Name |	Document Code |
|---|---|---|
Securities Report (Annual Report)|	有価証券報告書	|120
Quarterly Securities Report|	四半期報告書	|140
Semi-Annual Securities Report|	半期報告書	|150



This README provides an overview of the program's functionality, highlights the key methods and functions, and includes an example of how to use the program to download, process, and export financial data from EDINET.