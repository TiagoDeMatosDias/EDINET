# EDINET Financial Data Tool

Downloads financial filings from the Japanese securities regulator (EDINET), processes them into a structured SQLite database, and runs statistical analysis to identify relationships between financial ratios and stock valuations.

## What it does

1. **Fetch document list**  queries the EDINET API for available filings in a given date range.
2. **Download documents**  downloads the XBRL/CSV filings that match the filter criteria.
3. **Standardise data**  normalises raw XBRL data into a clean, consistently named table.
4. **Generate financial ratios**  calculates per-share values, valuation ratios, rolling averages, growth rates and z-scores for every company.
5. **Update stock prices**  fetches historical share prices via Yahoo Finance.
6. **Parse taxonomy**  parses an EDINET XBRL taxonomy XSD file and stores element metadata in the database.
7. **Find significant predictors**  univariate OLS sweep ranking which ratio columns best predict a given dependent variable.
8. **Multivariate regression**  user-defined multivariate OLS regression specified as a SQL query.

## Setup

### 1. Install dependencies

```
pip install -r requirements.txt
```

### 2. Create a `.env` file

Copy the template below into a `.env` file in the project root and fill in your values.

```dotenv
# EDINET API
API_KEY=<your_edinet_api_key>
baseURL=https://api.edinet-fsa.go.jp/api/v2/documents
doctype=5

# File storage
RAW_DOCUMENTS_PATH=D:\path\to\files

# Database
DB_PATH=D:\path\to\Base.db
DB_DOC_LIST_TABLE=DocumentList
DB_FINANCIAL_DATA_TABLE=financialData_full
DB_STANDARDIZED_TABLE=Standard_Data
DB_STANDARDIZED_RATIOS_TABLE=Standard_Data_Ratios
DB_COMPANY_INFO_TABLE=CompanyInfo
DB_STOCK_PRICES_TABLE=Stock_Prices
DB_TAXONOMY_TABLE=TAXONOMY_JPFS_COR
DB_SIGNIFICANT_PREDICTORS_TABLE=Significant_Predictors
```

### 3. Configure and run

Edit `config/run_config.json` to enable the steps you want, then:

```
python main.py
```

See [RUNNING.md](RUNNING.md) for a full description of every step and its config options.

## Configuration files

| File | Purpose |
|---|---|
| `config/run_config.json` | Controls which steps run and their parameters |
| `config/financial_ratios_config.json` | Ratio definitions and formulas |
| `.env` | API keys, file paths, and database table names |

## Key EDINET document type codes

| Code | Document type |
|---|---|
| 120 | Securities Report (Annual Report  有価証券報告書) |
| 140 | Quarterly Securities Report (四半期報告書) |
| 150 | Semi-Annual Securities Report (半期報告書) |
