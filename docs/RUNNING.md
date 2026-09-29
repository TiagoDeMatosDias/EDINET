# Running the Application

The current workstation includes the public homepage and pricing page, account authentication, shared company search, XBRL Filing Explorer, tags/research, arbitrary-metric company comparison, point-in-time backtesting, portfolio analytics, durable pipelines, and reproducible report ZIPs. The `download_xbrl` pipeline step is the only application path that reads `EDINET_API_TOKEN`, and it uses it solely as an outbound EDINET provider credential. See [USER_GUIDE.md](USER_GUIDE.md) for the user-facing workflow and current screenshots.

## Supported environments

- Python 3.12 or 3.13; `.venv3` is the canonical local environment.
- Node.js 22 and npm 10.
- Windows is the packaged target. Linux is supported for source development and CI.
- Base, Standardized, and Portfolio databases are selected through `config/database_paths.json`. API requests may name only the configured Base and Standardized files; any other database directory must be listed explicitly in `EDINET_ALLOWED_DATA_ROOTS`. Every other application database (auth, research, pipeline jobs, Portfolio, and the filing catalog) is refused as a request-selected database even inside an allowed root; the `DATABASES` registry in `src/orchestrator/common/db_config.py` marks which stores requests may name, so a newly registered store is private by default.

When the web server starts, it creates any missing configured database parents and files. Base and Standardized are created as empty pipeline-owned SQLite databases, Portfolio receives its versioned schema, and auth, research, pipeline-jobs, and filings receive their managed schemas and migrations.

Create the environment and install declared extras:

```powershell
py -3.13 -m venv .venv3
.\.venv3\Scripts\python.exe -m pip install -e ".[dev,build]" -c constraints.txt
```

Build the React frontend:

```powershell
Set-Location frontend-v2
npm ci
npm run build
Set-Location ..
```

Launch the local workstation:

```powershell
.\.venv3\Scripts\python.exe main.py --no-reload
```

Open `https://127.0.0.1:8000`.

### TLS certificates

The workstation always serves HTTPS. On startup it reuses the first certificate/key pair found in `data/certs/` and, when the folder holds no usable pair, generates a self-signed one there so HTTPS works immediately and later startups reuse the same certificate. Supported pair names, in lookup order: `cert.pem`+`key.pem`, `fullchain.pem`+`privkey.pem` (Let's Encrypt layout), `tls.crt`+`tls.key`, and `server.crt`+`server.key`. The directory can be moved with `EDINET_CERT_DIR`; when running the packaged Windows executable it lives in `data/certs/` next to the executable.

The generated certificate is self-signed (ten-year validity, SANs for `localhost`, `127.0.0.1`, `::1`, and the configured bind host), so browsers warn on the first visit. Accept the warning once per machine, or trust `data/certs/cert.pem` in the operating system's root store. A certificate/key pair that exists but is unreadable or mismatched stops startup with a clear error rather than being overwritten; delete or replace those files to recover.

Remote binding requires explicit opt-in, account authentication, and trusted hosts. TLS is served by the application itself; remote clients must trust the certificate in `data/certs/`, so a CA-issued certificate should be placed there for production access:

```powershell
$env:EDINET_AUTH_MODE = "accounts"
$env:EDINET_AUTH_DB = "config/state/auth.db"
$env:EDINET_TRUSTED_HOSTS = "research.example,192.0.2.10"
.\.venv3\Scripts\python.exe main.py --host 0.0.0.0 --port 8080 --allow-remote --no-reload
```

Remote `/api/*` requests require an account-issued `Authorization: Bearer <token>`. Tokens must not be placed in URLs, logs, or browser storage. `/health` remains minimal and unauthenticated. `EDINET_API_TOKEN` is reserved for outbound EDINET downloads and is never used for application authentication.

Account mode and open registration are the defaults. The first successful registration becomes the local administrator; set `EDINET_REGISTRATION_MODE=closed` (or `invite`) when additional self-service accounts should be disabled. `EDINET_REGISTRATION_MODE` is only the deployment default: once an administrator saves security settings under `/admin`, the saved registration mode, default role, access-token lifetime, and refresh idle/absolute lifetimes take precedence. Browser refresh tokens are held in an HttpOnly cookie, while access tokens remain in memory. Personal automation tokens can be created under `/api/auth/tokens` with scope `*` (full access) or `read` (GET/HEAD/OPTIONS only) and should be revoked when no longer needed; changing or resetting a password revokes all of that account's sessions and API tokens.

Refreshing prices from the provider (`/api/security/update-price`, `/api/screening/update-prices`) writes shared market data and requires the operator or admin role. Saved backtests are visible only to the account that ran them; results saved before ownership was recorded are visible to administrators only.

Responses carry `X-Content-Type-Options`, `X-Frame-Options`, `Referrer-Policy`, and a `Content-Security-Policy` for the workspace; remote deployments also send `Strict-Transport-Security`. Request size limits apply to the bytes actually received, including chunked uploads without a `Content-Length` header.

Generated artifacts and mutable state default to folders inside the project and can be relocated: `EDINET_STATE_DIR` (default `config/state`; saved screens, uploads, and job workspaces), `EDINET_BACKTEST_DIR` (default `data/Backtests`), and `EDINET_REPORT_DIR` (default `data/reports`). `EDINET_JOB_WORKSPACE_ROOT` still overrides the job workspace alone. Missing databases are created when the server starts rather than when its module is imported.

Administrators can change the minimum password length under `/admin` → Security settings. The accepted range is 15–128 characters, and the stored policy applies to registration, invitations, resets, password changes, and administrator-created credentials. The public `/pricing` page currently advertises €10 per month or €100 per year; it is informational and does not enable billing or subscription enforcement.

For frontend development, keep FastAPI serving HTTPS on port 8000 and start Vite in another terminal (the dev proxy targets `https://127.0.0.1:8000` with certificate verification disabled for the self-signed certificate):

```bash
cd frontend-v2
npm run dev
```

The primary frontend is a React/TypeScript single-page workspace in `frontend-v2/`. It communicates with the backend through `/api/*` and `/health`.

Database inputs are resolved and authorized server-side. Uploads and generated outputs live beneath per-job workspaces in `config/state/jobs/`.

### Runtime size limits

- `EDINET_MAX_UPLOAD_BYTES` defaults to 10 MiB for incoming files.
- Pipeline multipart uploads have a separate 500 MiB default ceiling. The Import Stock Prices (CSV) step also rejects files larger than 500 MiB before parsing.
- `EDINET_MAX_EXPORT_BYTES` defaults to 25 MiB for ordinary response exports.
- `EDINET_MAX_BACKTEST_ARTIFACT_BYTES` defaults to 256 MiB for server-generated backtest files. Rolling archives are built directly on disk and partial archives are removed if this limit is reached.
- `EDINET_MAX_REPORT_ARTIFACT_BYTES` defaults to 128 MiB for reproducible report ZIPs. Reports are written atomically beneath `data/reports/` and partial files are removed on failure.

Values are byte counts and are read at application startup. Increase the backtest artifact limit only when the expected archive and available disk space justify it, then restart the application.

### Filing archive storage

`Filings.db` retains each provider ZIP as a compressed `archive_content` BLOB. Extracted member bytes are not written for new ingests; the Filing Explorer extracts the requested member from the ZIP in memory when it is viewed. New catalogs retain numeric/analytical XBRL facts, contexts, units, and artifact metadata; narrative sections are reconstructed from the archive on demand instead of being materialized in the core database.

Complete Japanese-to-English translations are cached in the versioned `filing_translations` table in `Filings.db`. The viewer always preserves the Japanese source and renders English alongside it. Translation covers complete section bodies and visible report HTML; model failures or residual Japanese return a retryable error instead of a partial English result.

Existing databases created before this storage mode may still contain extracted `artifacts.content` BLOBs. Review the dry-run summary, then compact an existing database only after confirming that every archive is retained:

```powershell
\.venv3\Scripts\python.exe scripts/compact_filings_db.py
\.venv3\Scripts\python.exe scripts/compact_filings_db.py --apply
```

The compaction command refuses to clear an extracted member when its filing has no retained archive. It also requires substantial free disk space for SQLite `VACUUM`; use `--no-vacuum` only when clearing the BLOBs now and reclaiming space later is intentional.

To reclaim a large WAL left by a bulk cleanup, stop the application and workers first, inspect it, then checkpoint it through SQLite:

```powershell
\.venv3\Scripts\python.exe scripts/checkpoint_filings_db.py
\.venv3\Scripts\python.exe scripts/checkpoint_filings_db.py --apply
```

To rebuild an existing catalog with the compact numeric-only schema, use a new output path on a volume with sufficient free space. The source database is read-only and is never overwritten:

```powershell
\.venv3\Scripts\python.exe scripts/rebuild_filings_db.py --source data/databases/Filings.db --output D:\data\Filings.compact.db
\.venv3\Scripts\python.exe scripts/rebuild_filings_db.py --source data/databases/Filings.db --output D:\data\Filings.compact.db --apply
```

The rebuild omits nonnumeric/nil facts, materialized sections, and the FTS copy of narrative text while retaining the compressed ZIPs. Verify the output before switching `EDINET_FILINGS_DB` to it.

## Configuration Format

Execution configuration is supplied explicitly by the React UI, `POST /api/pipeline/run`, or `Config.from_dict(...)`. `Config` does not implicitly read `run_config.json`.

```json
{
  "steps": [
    {"name": "get_documents", "overwrite": false},
    {"name": "generate_financial_statements", "overwrite": false}
  ],
  "config": {"get_documents_config": {"startDate": "2026-02-15"}}
}
```

- `steps` order is execution order.
- `overwrite` is honored only by steps that declare support.
- Validation completes before the job is queued.

Submission returns `202` immediately. One managed worker executes jobs, while the UI polls persisted job/step status and retrieves bounded redacted output after a terminal state.

## Pre-flight Validation

Before enqueueing, the orchestrator checks required keys, field limits, embedded uploads, and allowed paths. Failure stops later steps. Cancellation is cooperative at safe checkpoints and never force-kills a Python thread.

## Bounded verification

`scripts/verify.py` runs stages sequentially with hard timeouts and terminates the stage process tree on timeout:

```powershell
.\.venv3\Scripts\python.exe -B scripts\verify.py
```

Use repeated `--stage` options for focused checks, or `--timeout-seconds N` for a stricter cap.

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

### `download_xbrl`
Downloads and indexes EDINET type-1 XBRL packages into `Filings.db`.

```json
"download_xbrl_config": {
  "mode": "all",
  "provider_token": "",
  "max_documents": 100,
  "doc_type_code": "120"
}
```

- `mode` — `explicit` downloads the IDs in `document_ids`; `backfill` discovers a bounded batch of eligible CSV-downloaded documents; `all` queries every eligible `DocumentList` row with `xbrlFlag = "1"` and `legalStatus` in `("1", "2")` that is not marked as XBRL-downloaded.
- `document_ids` — comma-separated EDINET document IDs used only in `explicit` mode.
- `max_documents` — applies only to `backfill`; `all` processes the complete matching queue without a document-count cap.
- `doc_type_code` — applies to both `backfill` and `all`; `120` is annual, `130` semi-annual, `140` quarterly, and an empty value includes all types.
- `provider_token` — optional override for `API_KEY` or `EDINET_API_TOKEN`.
- `all` adds `DocumentList.XbrlDownloaded` when needed and records `True`, `Checked_Unavailable`, or `Checked_Error` per document. It does not modify the legacy CSV `DocumentList.Downloaded` marker, and failed documents remain eligible for a later retry.
- Acquisition reuses one HTTP session, downloads at most five packages concurrently, and writes `DocumentList` status changes in batches. The document-type filter is applied before work is queued in both `backfill` and `all` modes.

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
- `csv_file` — absolute path to the CSV file. In the Pipeline workspace, use the step's file picker instead of typing a browser-local path.
- The selected CSV and the enclosing pipeline multipart request are capped at 500 MiB. Oversized files are rejected before pandas allocates a dataframe.
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
Fetches daily share prices from the JPX quote JSON endpoint for Japanese
tickers, with Stooq and Yahoo Finance chart fallbacks. The JPX stock-detail
page loads its historical table client-side from ``qjsonp.aspx``, which serves
the latest 360 trading sessions of split-adjusted closes; initial or older
backfills fall through to a provider with broader coverage rather than
silently truncating history.

Stooq and Yahoo requests use bounded retries with exponential backoff and
jitter for transient transport/HTTP failures, honor `Retry-After` when present,
and temporarily cool down a rate-limited provider so a bulk run does not repeat
blocked requests for every ticker. Yahoo also tries both chart hostnames before
the provider is marked unavailable. A failure for one ticker no longer aborts
the remaining ticker updates; the failed ticker is logged for the run.

The step supports `overwrite`: when enabled, each ticker selected for the run
is cleared and downloaded from scratch. The replacement is transactional per
ticker, so a failed or empty download restores that ticker's previous rows.

```json
"update_stock_prices_config": {
  "Target_Database": "C:/path/to/standardized.db"
}
```

- `Target_Database` — database containing the company info and financial data tables, and where stock prices will be updated.

---

### `check_tdnet_splits`
Captures Japanese stock-split and share-consolidation announcements from
TDnet, the TSE timely-disclosure service. Splits are not EDINET-reportable
events (verified against 企業内容等の開示に関する内閣府令 — no filing trigger
exists), so TDnet is the authoritative regulator-mandated venue. TDnet only
serves a rolling ~30-day window, so schedule this step daily or weekly; every
matched disclosure is stored as an event row in `Tdnet_Disclosures`
(Standardized.db) keyed by a stable disclosure id, making reruns idempotent.

Titles do not carry the split ratio or effective date, and pure split notices
have no XBRL attachment, so this step captures the event itself (ticker,
company, announcement time, title, PDF/XBRL links); ratio/date confirmation
stays with the Yahoo provider events and price heuristics in
`detect_splits`.

```json
"check_tdnet_splits_config": {
  "lookback_days": 7,
  "keywords": "株式分割,株式併合"
}
```

- `lookback_days` — calendar days to search back (default 7 for daily runs; use 30 for weekly; maximum 30).
- `keywords` — comma-separated Japanese title keywords identifying split (`株式分割`) or consolidation/reverse-split (`株式併合`) announcements.

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
  "Source_Mode": "csv",
  "Granularity_level": 3
}
```

- `Source_Mode` — `csv` (default) reads the legacy `Base.db`/`financialData_full` table; `filings` reads normalized numeric XBRL facts from `Filings.db`.
- When `Source_Mode` is `filings`, the database is taken from `EDINET_FILINGS_DB` or `config/database_paths.json` (`filings_db`).
- `Target_Database` — database where `FinancialStatements` and the wide taxonomy-backed `IncomeStatement`, `BalanceSheet`, `CashflowStatement`, and `ShareMetrics` tables are written.
- `Granularity_level` — maximum taxonomy level to materialize into the statement tables. `ShareMetrics` concepts are stored at level `0` so they are always included.

Runtime notes:

- `FinancialStatements` contains only filing metadata: `docID`, `edinetCode`, `docTypeCode`, `submitDateTime`, `periodStart`, `periodEnd`, and `release_id`.
- `IncomeStatement`, `BalanceSheet`, `CashflowStatement`, and `ShareMetrics` contain `docID` plus taxonomy-label columns only.
- `ShareMetrics` materializes selected share-count, dividend-per-share, and related summary concepts as flat level-`0` columns.
- The step prefers consolidated family contexts and falls back to matching non-consolidated contexts when a consolidated value is absent, using deterministic context priority before loading each table.
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

### `generate_rolling_metrics`
Computes rolling averages and CAGR-style growth rates for configurable metrics across selected statement tables. The columns and tables to process are declared in `src/orchestrator/generate_rolling_metrics/rolling_metrics.json`. Output tables are named `<SourceTable>_Rolling` and contain `_Average_3_Year`, `_Average_5_Year`, `_Average_10_Year`, `_Growth_3_Year`, `_Growth_5_Year`, and `_Growth_10_Year` columns for each configured metric.

Supports `overwrite`.

```json
"generate_rolling_metrics_config": {
  "Source_Database": "C:/path/to/standardized.db",
  "Target_Database": "C:/path/to/standardized.db"
}
```

- `Source_Database` — database containing the source statement and ratio tables.
- `Target_Database` — database where the rolling metric tables are written.

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
