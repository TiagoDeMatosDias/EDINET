# EDINET Data Downloader and Screener

This program is designed to download financial data from the Japanese Securities Regulator (EDINET) and store it in a SQLite database. The main goal is to create a financial screener that allows users to query and analyze the data for future processing.

Additionally the program can run a simple regression analysis to identify which financial ratios are most predictive of specific parameters such as stock price performance, earnings growth, or other financial metrics. The regression analysis can be configured to use different sets of independent variables (financial ratios) and dependent variables (e.g., stock price performance) based on the user's needs.

## Key Functionality

1. **Download Data**: Fetches financial documents from the EDINET API and stores them in a local database.
2. **Data Processing**: Processes the downloaded data to generate financial ratios.
3. **Database Management**: Manages the storage and retrieval of data in a SQLite database.
4. **Regression Analysis**: Performs regression analysis to identify key financial ratios that predict specific outcomes.

## Configuration

The program requires a set of configurations to run properly. These configurations include API keys, database paths, and parameters for data processing and regression analysis. 

The following files are used for configuration:
- `config/run_config.json`: Contains settings for controlling the execution flow of the application, including which steps to run and the parameters for each step.
- `config/app_config.json`: Contains application-specific settings such as EDINET base urls and default locations for files.
- `config/financial_ratios_config.json`: Contains definitions and formulas for calculating financial ratios, allowing users to customize which ratios to calculate and how they are defined.
- `config/regression_config.json`: Contains settings for the regression analysis, including which financial ratios to use as independent variables and which outcomes to predict as dependent variables.
- `.env`: Contains environment variables such as API keys and database credentials.

Most configurations are stored in JSON format for easy editing and readability. The `.env` file is used to store sensitive information such as API keys and database paths, which should not be hardcoded in the source code for security reasons.

Example .Env file:
```
API_KEY=[Your EDINET API Key Here]
DB_PATH=D:\programming\EDINET\Base.db
DB_DOC_LIST_TABLE=DocumentList
DB_STANDARDIZED_TABLE=Standard_Data
DB_FINANCIAL_DATA_TABLE=financialData_full
```


## Usage

1. **Configuration**: Ensure that the config files and `.env` file are properly set up with the necessary API keys and database paths.
2. **Running the Script**: Execute the main.py script to start the program.
3. **Downloading Data**: Use the provided functions to download and process data from EDINET.
4. **Handling Data**: You can query the SQLite database to retrieve and analyze the financial data as needed. A ols regression results is generated in the relevant output folder. See config/regression_config.json for details on how to configure the regression analysis.


# Key Document Types for Annual Reports:
Document Type |	Japanese Name |	Document Code |
|---|---|---|
Securities Report (Annual Report)|	有価証券報告書	|120
Quarterly Securities Report|	四半期報告書	|140
Semi-Annual Securities Report|	半期報告書	|150


