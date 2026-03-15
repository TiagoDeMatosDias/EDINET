# Running the Application

## Execution Modes

The application supports two execution modes:

- **GUI mode** (default): `python main.py` — launches the Flet desktop UI where you can configure steps, reorder them, and run the pipeline visually.
- **CLI mode**: `python main.py --cli` — reads `config/state/run_config.json` and executes enabled steps headlessly.

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
- `overwrite` — when `true`, the step drops and recreates its output table. Only supported by: `generate_financial_statements`, `generate_ratios`, `generate_historical_ratios`.

Steps execute in the order they appear in the `run_steps` object. In the GUI, you can reorder steps by dragging them.

## Pre-flight Validation

Before any step runs, the orchestrator checks that all required `.env` / config keys are set for every enabled step. If anything is missing, execution halts with a clear error listing the missing keys and which steps need them.

---

## Steps

### `get_documents`
Fetches the list of available filings from the EDINET API and stores document metadata in the database.

```json
"get_documents_config": {
  "startDate": "2026-02-15",
  "endDate":   "2026-02-21"
}
```

---

### `download_documents`
Downloads filings for documents already in the document list that match the filter criteria.

```json
"download_documents_config": {
  "docTypeCode": "120",
  "csvFlag":     "1",
  "secCode":     "",
  "Downloaded":  "False"
}
```

- `docTypeCode` — EDINET document type (e.g. `120` = annual report).
- `csvFlag` — `"1"` to download the XBRL-to-CSV version.
- `secCode` — filter by security code; leave blank for all.
- `Downloaded` — `"False"` to skip already-downloaded documents.

---

### `populate_company_info`
Loads the EDINET company code list from a CSV file into the database.

```json
"populate_company_info_config": {
  "csv_file": "config/reference/EdinetcodeDlInfo.csv"
}
```

---

### `import_stock_prices_csv`
Imports historical stock prices from a user-supplied CSV file into the `stock_prices` database table. Duplicate dates for the same ticker are automatically skipped.

```json
"import_stock_prices_csv_config": {
  "csv_file": "C:/path/to/prices.csv",
  "ticker": "TPX",
  "currency": "JPY",
  "date_column": "Date",
  "price_column": "Close"
}
```

- `csv_file` — absolute path to the CSV file. In the GUI, use the file picker in the config dialog.
- `ticker` — ticker symbol to assign to every imported row.
- `currency` — currency code (e.g. `JPY`, `USD`).
- `date_column` — name of the CSV column that contains dates.
- `price_column` — name of the CSV column that contains the price values (e.g. `Close`, `Open`, `High`).

Example CSV format:
```
Date,Open,High,Low,Close,Volume
2015-01-05,1400.87,1410.26,1388.37,1401.09,2044459904
2015-01-06,1377.53,1377.88,1361.14,1361.14,2684290816
```

---

### `update_stock_prices`
Fetches historical share prices from the Stooq API for all companies in the database that have financial data. No additional config required.

---

### `generate_financial_statements`
Extracts tagged XBRL values from the raw financial data table into structured per-company financial tables.

Supports `overwrite` — when enabled, the output tables are dropped and fully rebuilt.

```json
"generate_financial_statements_config": {
  "Source_Table": "financialData_full"
}
```

- `Source_Table` — the raw financial data table to read from (default: `financialData_full`).

---

### `parse_taxonomy`
Parses an EDINET XBRL taxonomy XSD file and stores element metadata (name, statement type, balance type) into the table defined by `DB_TAXONOMY_TABLE` in `.env`.

```json
"parse_taxonomy_config": {
  "xsd_file": "config/reference/jppfs_cor_2013-08-31.xsd"
}
```

---

### `generate_ratios`
Calculates per-share values and valuation ratios for every company. Ratio definitions are controlled by `config/reference/financial_ratios_config.json`.

Supports `overwrite`. In incremental mode (the default), documents already processed are skipped.

No additional run-config parameters required.

---

### `generate_historical_ratios`
Computes rolling averages, growth rates, and z-scores over the ratio tables produced by `generate_ratios`.

Supports `overwrite`.

No additional run-config parameters required.

---

### `Multivariate_Regression`
Runs a multivariate OLS regression defined entirely by a SQL query. The **first column** in the query is the dependent variable; all remaining columns are the independent variables.

```json
"Multivariate_Regression_config": {
  "Output": "data/ols_results/ols_results_summary.txt",
  "winsorize_thresholds": { "lower": 0.05, "upper": 0.95 },
  "SQL_Query": "SELECT dep_var, ind_var_1, ind_var_2 FROM Quality_Historical"
}
```

- `winsorize_thresholds` — optional; omit the key entirely to skip winsorisation.
- `SQL_Query` — any valid SQLite `SELECT`. Change this to adjust the model without touching any code.

---

### `backtest`
Runs a portfolio backtesting simulation over a date range. Calculates weighted daily returns (price + dividends), cumulative performance, and compares against an optional benchmark ticker.

```json
"backtesting_config": {
  "start_date": "2020-01-01",
  "end_date": "2025-12-31",
  "portfolio": {
    "47460": 0.1,
    "53020": 0.1,
    "71720": 0.1,
    "83660": 0.1,
    "94360": 0.1,
    "19670": 0.1,
    "61610": 0.1,
    "85950": 0.1,
    "83640": 0.1,
    "80350": 0.1
  },
  "benchmark_ticker": "TPX",
  "output_file": "data/backtest_results/backtest_report.txt"
}
```

- `portfolio` — mapping of ticker symbol → weight (values must sum to 1.0). In the GUI, a dedicated dialog lets you add/remove tickers and validates the total weight.
- `benchmark_ticker` — optional ticker to compare portfolio performance against. Leave blank to skip benchmark comparison.
- `output_file` — path for the text report.

The backtesting engine:
- Retrieves daily prices from the `stock_prices` table.
- Looks up per-share dividends from the ratios table.
- Computes daily weighted returns with dividend adjustments.
- Calculates cumulative returns over the period.
- If a benchmark ticker is given, computes the same metrics for the benchmark and reports relative performance.
