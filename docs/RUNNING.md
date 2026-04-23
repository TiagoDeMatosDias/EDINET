# Running the Application

## Execution Modes

The application supports two execution modes:

- **GUI mode** (default): `python main.py` — launches the Tk desktop UI where you can configure steps and run the pipeline visually.
- **CLI mode**: `python main.py --cli` — reads `config/state/run_config.json` and executes enabled steps headlessly.

Most steps now require an explicit source or target database path in their step configuration. The GUI exposes those paths directly in each step's config dialog.

## Configuration Format

All execution is controlled by `config/state/run_config.json`. Each step is an object with `enabled` and `overwrite` flags:

```json
"run_steps": {
  "get_documents": { "enabled": true, "overwrite": false },
  "generate_financial_statements": { "enabled": true, "overwrite": false },
  "populate_business_descriptions_en": { "enabled": true, "overwrite": false },
  ...
}
```

- `enabled` — set to `true` to run the step, `false` to skip it.
- `overwrite` — when `true`, the step rebuilds or refreshes the step output. Supported by: `generate_financial_statements`, `populate_business_descriptions_en`, `generate_ratios`, `generate_historical_ratios`.

Steps execute in the order they appear in the `run_steps` object. In the GUI, you can reorder steps by dragging them.

## Pre-flight Validation

Before any step runs, the orchestrator checks that all required `.env` / config keys are set for every enabled step. If anything is missing, execution halts with a clear error listing the missing keys and which steps need them.

## Business Description Translation APIs

The Security Analysis view no longer performs runtime model translation. Instead, `FinancialStatements` includes a `DescriptionOfBusiness_EN` column that is populated ahead of time by the `populate_business_descriptions_en` pipeline step.

Translation providers are defined in `config/reference/business_description_translation_providers.example.json`. Providers are tried in the order listed, so if one API is rate-limited or unavailable the step automatically falls back to the next enabled provider.

The bundled example config includes:

- a LibreTranslate-compatible endpoint
- the free MyMemory API

You can reorder providers, disable them, or add new ones later by editing that JSON file.

---

## Steps

### `get_documents`
Fetches the list of available filings from the EDINET API and stores document metadata in the database.

```json
"get_documents_config": {
  "startDate": "2026-02-15",
  "endDate":   "2026-02-21",
  "Target_Database": "C:/path/to/base.db"
}
```

- `Target_Database` — database where the EDINET document list table will be written.

---

### `download_documents`
Downloads filings for documents already in the document list that match the filter criteria.

```json
"download_documents_config": {
  "docTypeCode": "120",
  "csvFlag":     "1",
  "secCode":     "",
  "Downloaded":  "False",
  "Target_Database": "C:/path/to/base.db"
}
```

- `docTypeCode` — EDINET document type (e.g. `120` = annual report).
- `csvFlag` — `"1"` to download the XBRL-to-CSV version.
- `secCode` — filter by security code; leave blank for all.
- `Downloaded` — `"False"` to skip already-downloaded documents.
- `Target_Database` — database containing the document list table and destination financial data table.

---

### `populate_company_info`
Loads the EDINET company code list into the database.

When `csv_file` is blank, the app downloads the official English EDINET code list ZIP from the EDINET code-list page, reads the CSV inside it, normalizes the column names to the existing schema, and stores the result in the company info table. When `csv_file` is provided, that local file is used instead.

```json
"populate_company_info_config": {
  "csv_file": "",
  "Target_Database": "C:/path/to/standardized.db"
}
```

- `Target_Database` — database where the company info table will be written.
- `csv_file` — optional local CSV override. Leave blank to download the official English EDINET code list.

---

### `import_stock_prices_csv`
Imports historical stock prices from a user-supplied CSV file into the `stock_prices` database table. Duplicate dates for the same ticker are automatically skipped.

```json
"import_stock_prices_csv_config": {
  "Target_Database": "C:/path/to/standardized.db",
  "csv_file": "C:/path/to/prices.csv",
  "default_ticker": "TPX",
  "default_currency": "JPY",
  "date_column": "Date",
  "price_column": "Price",
  "ticker_column": "Ticker",
  "currency_column": "Currency"
}
```

- `Target_Database` — database where the stock prices table will be written.
- `csv_file` — absolute path to the CSV file. In the GUI, use the file picker in the config dialog.
- `default_ticker` — fallback ticker assigned when the CSV has no ticker column or the row value is blank.
- `default_currency` — fallback currency assigned when the CSV has no currency column or the row value is blank.
- `date_column` — name of the CSV column that contains dates.
- `price_column` — name of the CSV column that contains the price values. The standardized backup CSV uses `Price`; common alternatives such as `Close` are also detected automatically.
- `ticker_column` — optional ticker column in the CSV. If left blank, the importer auto-detects `Ticker` when present before falling back to `default_ticker`.
- `currency_column` — optional currency column in the CSV. If left blank, the importer auto-detects `Currency` when present before falling back to `default_currency`.

Example CSV format:
```
Date,Ticker,Currency,Price
2015-01-05,13010,JPY,1401.09
2015-01-06,13010,JPY,1361.14
```

---

### `update_stock_prices`
Fetches historical share prices from the Stooq API for all companies in the selected database that have financial data, with a Yahoo Finance chart fallback if Stooq is unavailable.

```json
"update_stock_prices_config": {
  "Target_Database": "C:/path/to/standardized.db"
}
```

- `Target_Database` — database containing the company info and financial data tables, and where stock prices will be updated.

---

### `parse_taxonomy`
Syncs EDINET taxonomy releases into normalized taxonomy tables, or imports a local XSD file for offline use.

```json
"parse_taxonomy_config": {
  "xsd_file": "",
  "namespace_prefix": "jppfs_cor",
  "release_label": "",
  "release_year": "",
  "taxonomy_date": "",
  "release_selection": "all",
  "release_years": [],
  "namespaces": ["jppfs_cor", "jpcrp_cor"],
  "download_dir": "assets/taxonomy",
  "force_download": "False",
  "force_reparse": "False",
  "Target_Database": "C:/path/to/standardized.db"
}
```

- Leave `xsd_file` empty to download and parse official EDINET taxonomy releases.
- Set `xsd_file` to import a local XSD instead; `namespace_prefix`, `release_label`, `release_year`, and `taxonomy_date` are only used in that local-import mode.
- `release_selection`, `release_years`, and `namespaces` control which official releases are synced. The default `all` setting downloads the full historical set for the selected namespaces.
- `download_dir` stores downloaded taxonomy ZIP archives locally.
- `force_download` redownloads archives even if they already exist locally.
- `force_reparse` rebuilds normalized taxonomy tables even if the archive hash is unchanged.
- `Target_Database` — database where the normalized taxonomy tables will be written.

---

### `generate_financial_statements`
Extracts tagged XBRL values from the raw financial data table into structured per-company financial tables.

Supports `overwrite` — when enabled, the output tables are dropped and fully rebuilt.

```json
"generate_financial_statements_config": {
  "Source_Database": "C:/path/to/base.db",
  "Source_Table": "financialData_full",
  "Target_Database": "C:/path/to/standardized.db",
  "Company_Info_Table": "",
  "Stock_Prices_Table": "",
  "Mappings_Config": "config/reference/canonical_metrics_config.json",
  "max_line_depth": 3,
  "batch_size": 2500
}
```

- `Source_Database` — database containing the raw EDINET financial data.
- `Source_Table` — the raw financial data table to read from (default: `financialData_full`).
- `Target_Database` — database where `FinancialStatements`, `statement_line_items`, and the wide taxonomy-backed `IncomeStatement`, `BalanceSheet`, and `CashflowStatement` tables are written.
- `FinancialStatements.DescriptionOfBusiness_EN` is created automatically as an empty `TEXT` column and preserved on reruns so it can be populated by the translation step.
- `Company_Info_Table` — optional override for the company info table name.
- `Stock_Prices_Table` — optional override for the stock prices table name.
- `Mappings_Config` — JSON registry used for doc-level `FinancialStatements` fields such as share-count and filing-description values. When taxonomy metadata has not been loaded yet, it also provides a fallback concept list for statement generation.
- `max_line_depth` — maximum taxonomy presentation depth to materialize into the three main statement tables. The tables always use the primary current-year contexts only: `CurrentYearInstant` for `BalanceSheet`, `CurrentYearDuration` for `IncomeStatement` and `CashflowStatement`.
- `batch_size` — rows/documents processed per batch.

Runtime notes:

- `IncomeStatement`, `BalanceSheet`, and `CashflowStatement` now contain `docID` plus taxonomy-label columns only.
- `statement_line_items` stores the hierarchy metadata for those columns: concept QName, label, parent concept, parent column, order, depth, and statement family.
- `taxonomy_levels` is populated by `parse_taxonomy` and gives a release-scoped one-row-per-concept table with `release_id`, `statement_family`, `data_type`, `namespace_prefix`, `concept_qname`, `primary_label_en`, `parent_concept_qname`, and `level`. These rows are derived from one canonical standard EDINET role per statement family, with root wrapper concepts compressed out and a headings-first projection applied so abstract section concepts define the visible structure. `generate_financial_statements` uses this table first when it is available.
- The legacy normalized statement-storage tables (`statement_documents`, `statement_contexts`, `statement_facts`, `statement_fact_dimensions`) are no longer part of the generated output and are dropped on rerun.

---

### `populate_business_descriptions_en`
Populates `FinancialStatements.DescriptionOfBusiness_EN` by translating `DescriptionOfBusiness` through an ordered list of HTTP providers.

Supports `overwrite`.

```json
"populate_business_descriptions_en_config": {
  "Target_Database": "C:/path/to/standardized.db",
  "Table_Name": "FinancialStatements",
  "DocID_Column": "docID",
  "Source_Column": "DescriptionOfBusiness",
  "Target_Column": "DescriptionOfBusiness_EN",
  "Providers_Config": "config/reference/business_description_translation_providers.example.json",
  "Source_Language": "ja",
  "Target_Language": "en",
  "batch_size": 25
}
```

- `Target_Database` — database containing the `FinancialStatements` table to update.
- `Table_Name` — target table containing the description columns (default: `FinancialStatements`).
- `DocID_Column` — unique identifier column used for updates (default: `docID`).
- `Source_Column` — source description column to translate (default: `DescriptionOfBusiness`).
- `Target_Column` — destination English column to populate (default: `DescriptionOfBusiness_EN`).
- `Providers_Config` — JSON file defining enabled providers and their fallback order.
- `Source_Language` — source language passed to the providers (default: `ja`).
- `Target_Language` — target language passed to the providers (default: `en`).
- `batch_size` — number of rows to process per batch.

The provider config supports a top-level `chunk_char_limit` for long descriptions and `row_delay_seconds` if you want to slow requests to stay under free-tier limits.

---

### `generate_ratios`
Calculates per-share values and valuation ratios for every company. Formula definitions are controlled by `config/reference/generate_ratios_formulas_config.json`.

Supports `overwrite`. In incremental mode (the default), documents already processed are skipped.

```json
"generate_ratios_config": {
  "Source_Database": "C:/path/to/standardized.db",
  "Target_Database": "C:/path/to/standardized.db",
  "Formulas_Config": "config/reference/generate_ratios_formulas_config.json",
  "batch_size": 5000
}
```

- `Source_Database` — database containing the financial statement tables.
- `Target_Database` — database where the `PerShare`, `Valuation`, and `Quality` tables are written.
- `Formulas_Config` — JSON formula file used to derive ratio fields.
- `batch_size` — rows/documents processed per batch.

---

### `generate_historical_ratios`
Computes rolling averages, growth rates, and z-scores over the ratio tables produced by `generate_ratios`.

Supports `overwrite`.

```json
"generate_historical_ratios_config": {
  "Source_Database": "C:/path/to/standardized.db",
  "Target_Database": "C:/path/to/standardized.db",
  "company_batch_size": 200
}
```

- `Source_Database` — database containing the current ratio tables.
- `Target_Database` — database where the historical ratio tables are written.
- `company_batch_size` — number of companies processed per batch.

---

### `Multivariate_Regression`
Runs a multivariate OLS regression defined entirely by a SQL query. The **first column** in the query is the dependent variable; all remaining columns are the independent variables.

```json
"Multivariate_Regression_config": {
  "Source_Database": "C:/path/to/standardized.db",
  "Output": "data/ols_results/ols_results_summary.txt",
  "winsorize_thresholds": { "lower": 0.05, "upper": 0.95 },
  "SQL_Query": "SELECT dep_var, ind_var_1, ind_var_2 FROM Quality_Historical"
}
```

- `Source_Database` — database queried by the regression SQL.
- `winsorize_thresholds` — optional; omit the key entirely to skip winsorisation.
- `SQL_Query` — any valid SQLite `SELECT`. Change this to adjust the model without touching any code.

---

### `backtest`
Runs a portfolio backtesting simulation over a date range. Calculates weighted daily returns (price + dividends), cumulative performance, and compares against an optional benchmark ticker.

```json
"backtesting_config": {
  "Source_Database": "C:/path/to/standardized.db",
  "PerShare_Table": "PerShare",
  "Financial_Statements_Table": "FinancialStatements",
  "start_date": "2020-01-01",
  "end_date": "2025-12-31",
  "portfolio": {
    "59110": { "mode": "shares", "value": 100.0 },
    "59840": { "mode": "shares", "value": 300.0 },
    "75750": { "mode": "weight", "value": 0.5 }
  },
  "benchmark_ticker": "TPX",
  "output_file": "data/backtest_results/backtest_report.txt",
  "risk_free_rate": 0.02,
  "initial_capital": 0.0
}
```

- `Source_Database` — database used for prices, dividends, and financial statement lookups.
- `PerShare_Table` — table containing per-share dividend data.
- `Financial_Statements_Table` — table used when joining dividend information via `docID`.
- `portfolio` — mapping of ticker symbol to allocation spec. Supports `weight`, `shares`, and `value` modes in the GUI and config file.
- `benchmark_ticker` — optional ticker to compare portfolio performance against. Leave blank to skip benchmark comparison.
- `output_file` — path for the text report.
- `risk_free_rate` — optional risk-free rate used in metric calculations.
- `initial_capital` — optional starting capital used for per-company cash metrics.

The backtesting engine:
- Retrieves daily prices from the `stock_prices` table.
- Looks up per-share dividends from the ratios table.
- Computes daily weighted returns with dividend adjustments.
- Calculates cumulative returns over the period.
- If a benchmark ticker is given, computes the same metrics for the benchmark and reports relative performance.

---

### `backtest_set`
Runs a batch of backtests from a CSV file containing yearly portfolio selections. For each year in the input CSV, the application runs 1-year, 2-year, 3-year, 5-year, and 10-year backtests where possible.

```json
"backtest_set_config": {
  "Source_Database": "C:/path/to/standardized.db",
  "PerShare_Table": "PerShare",
  "Financial_Statements_Table": "FinancialStatements",
  "csv_file": "C:/path/to/ols_results_summary_top10.csv",
  "benchmark_ticker": "TPX",
  "output_dir": "data/backtest_set_results",
  "risk_free_rate": 0.02,
  "initial_capital": 0.0
}
```

- `Source_Database` — database used for prices, dividends, and financial statement lookups.
- `PerShare_Table` — table containing per-share dividend data.
- `Financial_Statements_Table` — table used when joining dividend information via `docID`.
- `csv_file` — input CSV describing the yearly portfolios to test.
- `benchmark_ticker` — optional benchmark ticker.
- `output_dir` — directory where the batch reports and summaries are written.
- `risk_free_rate` — optional risk-free rate used in metric calculations.
- `initial_capital` — optional starting capital used for per-company cash metrics.
