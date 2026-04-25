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
  ...
}
```

- `enabled` — set to `true` to run the step, `false` to skip it.
- `overwrite` — when `true`, the step rebuilds or refreshes the step output. Supported by: `generate_financial_statements`, `generate_ratios`.

Steps execute in the order they appear in the `run_steps` object. In the GUI, you can reorder steps by dragging them.

## Pre-flight Validation

Before any step runs, the orchestrator checks that all required `.env` / config keys are set for every enabled step. If anything is missing, execution halts with a clear error listing the missing keys and which steps need them.

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
  "Target_Database": "C:/path/to/standardized.db",
  "Granularity_level": 3
}
```

- `Source_Database` — database containing the raw EDINET financial data.
- The source table is fixed to `financialData_full`.
- `Target_Database` — database where `FinancialStatements` and the wide taxonomy-backed `IncomeStatement`, `BalanceSheet`, `CashflowStatement`, and `ShareMetrics` tables are written.
- `Granularity_level` — maximum taxonomy level to materialize into the statement tables. `ShareMetrics` concepts are stored at level `0` so they are always included.

Runtime notes:

- `FinancialStatements` contains only filing metadata: `docID`, `edinetCode`, `docTypeCode`, `submitDateTime`, `periodStart`, `periodEnd`, and `release_id`.
- `IncomeStatement`, `BalanceSheet`, `CashflowStatement`, and `ShareMetrics` contain `docID` plus taxonomy-label columns only.
- `ShareMetrics` materializes selected share-count, dividend-per-share, and related summary concepts as flat level-`0` columns.
- The step reads family-specific contexts and applies deterministic context priority before loading each table.
- Pending filings are processed in internal pandas-backed batches of 1000 docIDs, with vectorized release resolution, release-aware concept filtering, and bulk SQLite writes per batch.

---

### `generate_ratios`
Calculates JSON-defined ratio tables for every filing. Definitions are hardcoded to `src/orchestrator/generate_ratios/ratios_definitions.json`.

Supports `overwrite`.

```json
"generate_ratios_config": {
  "Database": "C:/path/to/standardized.db",
  "batch_size": 5000
}
```

- `Database` — single database containing the source financial statement tables and the generated ratio tables.
- Ratio definitions are always loaded from `src/orchestrator/generate_ratios/ratios_definitions.json`.
- `batch_size` — accepted for compatibility; ratio generation currently runs set-based SQL against the full filing set.

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
