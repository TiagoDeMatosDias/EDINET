# Running the Application

All execution is controlled by `config/run_config.json`. Set a step to `true` to enable it, `false` to skip it, then run:

```
python main.py
```

Steps execute in the order listed below. Each step is independent  you can enable any subset.

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

- `docTypeCode`  EDINET document type (e.g. `120` = annual report).
- `csvFlag`  `"1"` to download the XBRL-to-CSV version.
- `secCode`  filter by security code; leave blank for all.
- `Downloaded`  `"False"` to skip already-downloaded documents.

---

### `standardize_data`
Normalises raw XBRL data into a clean, consistently named table. No additional config required.

---

### `generate_financial_ratios`
Calculates per-share values, valuation ratios, rolling averages/std, growth rates and z-scores for every company. Ratio definitions live in `config/financial_ratios_config.json`. No additional run-config parameters required.

---

### `update_stock_prices`
Fetches historical share prices from Yahoo Finance for all companies in the database. No additional config required.

---

### `parse_taxonomy`
Parses an EDINET XBRL taxonomy XSD file and stores element metadata (name, statement type, balance type) into the table defined by `DB_TAXONOMY_TABLE` in `.env`.

```json
"parse_taxonomy_config": {
  "xsd_file": "path/to/jppfs_cor_2014-03-31.xsd"
}
```

---

### `find_significant_predictors`
Runs an automated univariate OLS sweep  every eligible ratio column against every specified dependent variable  and writes a summary to a text file and full results to the database.

```json
"find_significant_predictors_config": {
  "output_file": "data/ols_results/predictor_search_results.txt",
  "winsorize_thresholds": { "lower": 0.05, "upper": 0.95 },
  "alpha": 0.05,
  "dependent_variables": [
    "Ratio_EarningsYield",
    "Ratio_PriceSales"
  ]
}
```

- `winsorize_thresholds`  quantile bounds for outlier trimming. Omit the key entirely to skip winsorisation.
- `dependent_variables`  columns to use as dependent variables. Leave empty to test all columns.

---

### `Multivariate_Regression`
Runs a multivariate OLS regression defined entirely by a SQL query. The **first column** in the query is the dependent variable; all remaining columns are the independent variables.

```json
"Multivariate_Regression_config": {
  "Output": "data/ols_results/ols_results_summary.txt",
  "winsorize_thresholds": { "lower": 0.05, "upper": 0.95 },
  "SQL_Query": "SELECT dep_var, ind_var_1, ind_var_2 FROM Standard_Data_Ratios"
}
```

- `winsorize_thresholds`  optional; omit the key entirely to skip winsorisation.
- `SQL_Query`  any valid SQLite `SELECT`. Change this to adjust the model without touching any code.
