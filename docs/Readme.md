# EDINET Financial Data Tool

Downloads financial filings from the Japanese securities regulator (EDINET), processes them into a structured SQLite database, and runs statistical analysis to identify relationships between financial ratios and stock valuations.

## Quick Start (Alpha Release)

1. Download the latest release from [Releases](https://github.com/TiagoDeMatosDias/EDINET/releases)
2. Extract `EDINET-0.1.0-alpha.zip`
3. Copy `config/run_config.example.json` to `config/run_config.json` and configure your settings
4. Create a `.env` file with your API keys (see Setup section below)
5. Run `EDINET.exe` (Windows) or the binary for your OS

## What it does

1. **Fetch document list** - queries the EDINET API for available filings in a given date range.
2. **Download documents** - downloads the XBRL/CSV filings that match the filter criteria.
3. **Standardise data** - normalises raw XBRL data into a clean, consistently named table.
4. **Generate financial ratios** - calculates per-share values, valuation ratios, rolling averages, growth rates and z-scores for every company.
5. **Update stock prices** - fetches historical share prices via Stooq API (filtered to companies with financial data).
6. **Populate company info** - loads the EDINET company code list from CSV into the database.
7. **Parse taxonomy** - parses an EDINET XBRL taxonomy XSD file and stores element metadata in the database.
8. **Find significant predictors** - univariate OLS sweep ranking which ratio columns best predict a given dependent variable.
9. **Multivariate regression** - user-defined multivariate OLS regression specified as a SQL query.

## Setup

### From Source

#### 1. Install dependencies

```
pip install -r requirements.txt
```

#### 2. Create a `.env` file

Copy the template below into a `.env` file in the project root and fill in your values:

```
API_KEY=<your_edinet_api_key>
baseURL=https://api.edinet-fsa.go.jp/api/v2/documents
doctype=5

RAW_DOCUMENTS_PATH=<your_documents_path>
DB_PATH=<your_sqlite_db_path>

DB_DOC_LIST_TABLE=DocumentList
DB_FINANCIAL_DATA_TABLE=financialData_full
DB_STANDARDIZED_TABLE=Standard_Data
DB_STANDARDIZED_RATIOS_TABLE=Standard_Data_Ratios
DB_COMPANY_INFO_TABLE=CompanyInfo
DB_STOCK_PRICES_TABLE=Stock_Prices
DB_TAXONOMY_TABLE=TAXONOMY_JPFS_COR
DB_SIGNIFICANT_PREDICTORS_TABLE=Significant_Predictors
```

#### 3. Configure and run

Edit `config/run_config.json` to enable the steps you want, then:

```
python main.py
```

### From Release

1. Extract the release ZIP file
2. Edit the `.env` file with your configuration
3. Run the executable

All output is logged to timestamped files in the `logs/` directory. See [LOGGING.md](LOGGING.md) for details.

## Documentation

- [RUNNING.md](RUNNING.md) - Full description of every step and configuration options
- [LOGGING.md](LOGGING.md) - Logging system documentation
- [CHANGELOG.md](../../CHANGELOG.md) - Version history and changes

## Building an executable

The project can be packaged into a single \.exe\ with [PyInstaller](https://pyinstaller.org).
The exe resolves all config paths relative to the folder it lives in, so \config/\, \.env\,
and the output \data/\ folder just need to sit alongside it.

### 1. Install PyInstaller

\pip install pyinstaller
\
### 2. Build

Run from the project root:

\pyinstaller --onefile --name EDINET main.py
\
The exe is written to \dist/EDINET.exe\.

### 3. Prepare the distribution folder

Create a deployment folder and copy the required items into it:

\EDINET.exe                        <- built by PyInstaller (from dist/)
.env                              <- your API keys and DB paths
config/
    run_config.json
    financial_ratios_config.json
    EdinetcodeDlInfo.csv
    jppfs_cor_2013-08-31.xsd
data/
    ols_results/                  <- must exist for regression output steps
\
### 4. Run

Double-click \EDINET.exe\ or launch it from a terminal.
It will look for \config/\ and \.env\ in the same folder as the exe.

> **Note:** Large dependencies (pandas, statsmodels, scipy) make the final exe around 200-300 MB.
> Build time is a few minutes on the first run.

## Configuration files

| File | Purpose |
|---|---|
| \config/run_config.json\ | Controls which steps run and their parameters |
| \config/financial_ratios_config.json\ | Ratio definitions and formulas |
| \config/EdinetcodeDlInfo.csv\ | EDINET company code list (used by populate_company_info) |
| \config/jppfs_cor_2013-08-31.xsd\ | XBRL taxonomy file (used by parse_taxonomy) |
| \.env\ | API keys, file paths, and database table names |

## Key EDINET document type codes

| Code | Document type |
|---|---|
| 120 | Securities Report (Annual Report - 有価証券報告書) |
| 140 | Quarterly Securities Report (四半期報告書) |
| 150 | Semi-Annual Securities Report (半期報告書) |
