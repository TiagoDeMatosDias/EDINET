# Running the Application

The current workstation includes the public homepage and pricing page, account authentication, shared company search, XBRL Filing Explorer, tags/research, arbitrary-metric company comparison, point-in-time backtesting, portfolio analytics, durable pipelines, and reproducible report ZIPs. The EDINET API key (the `edinet.api_key` setting) is used solely as an outbound EDINET provider credential. See [USER_GUIDE.md](USER_GUIDE.md) for the user-facing workflow and current screenshots.

## Supported environments

- Python 3.12 or 3.13; `.venv3` is the canonical local environment.
- Node.js 22 and npm 10.
- Windows is the packaged target. Linux is supported for source development and CI.

## Storage and settings

Everything the application writes lives in one data folder: `data/` beside `main.py` (or beside `ShadeResearch.exe` in a release). `EDINET_DATA_DIR` points at another folder; tests and scratch runs use it.

| Path | Holds | Rebuildable |
|---|---|---|
| `app.db` | Settings and secrets, accounts, research, pipeline jobs, portfolio, and who ran each saved backtest | No: back it up |
| `chat.db` | Chat channels and messages (kept apart because it grows with use) | No: back it up |
| `market.db` | EDINET document index (`DocumentList`), taxonomy, company info, statements, ratios, rolling metrics, prices, splits, and bonds | Yes, by the pipeline |
| `filings.db` | Provider ZIPs, XBRL facts, catalog, and translations | Yes, by the pipeline |
| `certs/` | TLS certificate and key | Generated |
| `logs/` | Rotating `server.log` | Generated |
| `artifacts/` | Backtests, reports, screening exports, job uploads, downloaded documents | Generated |

When the web server starts, it creates any missing database with its managed schema. Components that share `app.db` record their migrations per component in `schema_migrations`. The market tables are created by the pipeline; only the bond tables have a fixed schema. The server always reads its own databases; API requests cannot name one.

Settings are rows in the `settings` table of `app.db`, declared with their types, defaults, and validation in `src/settings/registry.py`. Administrators edit them under **Admin → Server settings**; from a terminal:

```powershell
.\.venv3\Scripts\python.exe main.py config list                         # every setting and its value
.\.venv3\Scripts\python.exe main.py config set edinet.api_key           # prompts, so the key stays out of shell history
.\.venv3\Scripts\python.exe main.py config set server.trusted_hosts research.example,192.0.2.10
.\.venv3\Scripts\python.exe main.py config unset jobs.retention_hours    # back to the default
```

| Setting | Default | Purpose |
|---|---|---|
| `edinet.api_key` | not set | EDINET API key for every step that downloads documents or filings. Write-only in the UI; read when a step runs, so a new key applies at once. |
| `auth.mode` | `accounts` | `disabled` lets anyone who reaches the server act as an administrator; only for loopback use. |
| `server.trusted_hosts` | none | Host names accepted with `--allow-remote`. |
| `pipeline.allowed_data_roots` | none | Extra folders pipeline steps may read input files from, besides the data folder. |
| `limits.max_upload_bytes` | 10 MiB | Largest upload other than pipeline inputs. |
| `limits.max_export_bytes` | 25 MiB | Largest screening or portfolio export. |
| `limits.max_backtest_artifact_bytes` | 256 MiB | Largest backtest archive. |
| `limits.max_report_artifact_bytes` | 128 MiB | Largest report archive. |
| `jobs.retention_hours` | 24 | Finished jobs and their uploads are removed after this long. |
| `storage.market_db_path`, `storage.filings_db_path` | data folder | Move the two large databases, for example to a bigger disk. Move the file yourself with the server stopped, then set the path. |

Every setting except the API key takes effect on the next start. How the server listens is not a setting but a launch option (`--host`, `--port`, `--allow-remote`), so a stored value can never expose the server to the network. If an administrator is locked out, `main.py config set auth.mode disabled` and a restart on loopback restore access.

### Moving from the older layout

Earlier versions kept nine databases in `data/databases/` and `config/state/databases/` (relocatable through `config/database_paths.json`), the API key in `.env`, and the chat key ring and generated files under `config/state/`. `main.py` moves such an installation into the data folder before it starts the server, or on request:

```powershell
.\.venv3\Scripts\python.exe main.py migrate --dry-run
.\.venv3\Scripts\python.exe main.py migrate
```

auth, research, pipeline-jobs, and Portfolio are merged into `app.db`; Standardized, Base, and Bonds into `market.db`; chat and Filings are moved. The API key from `.env` and the chat key ring become settings, and each saved backtest's `owner.json` and `meta.json` become a row of `saved_backtests`. Nothing is deleted: merged or imported files keep a `.migrated` suffix, and the rest is moved, which is a rename on the same disk. Every step is skipped once its target exists, so an interrupted run resumes. The migration refuses to run while any old database is open (stop the server and the pipeline first). Until it has run, database lookups refuse to create empty databases beside the old ones. The old shared `screening_history.jsonl` is kept but not imported, because its entries have no owner; history is now kept per user. Environment variables earlier versions read are reported on start with the setting that replaced them.

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

The workstation always serves HTTPS. On startup it reuses the first certificate/key pair found in `data/certs/` and, when the folder holds no usable pair, generates a self-signed one there so HTTPS works immediately and later startups reuse the same certificate. Supported pair names, in lookup order: `cert.pem`+`key.pem`, `fullchain.pem`+`privkey.pem` (Let's Encrypt layout), `tls.crt`+`tls.key`, and `server.crt`+`server.key`. When running the packaged Windows executable it lives in `data/certs/` next to the executable.

The generated certificate is self-signed (ten-year validity, SANs for `localhost`, `127.0.0.1`, `::1`, and the configured bind host), so browsers warn on the first visit. Accept the warning once per machine, or trust `data/certs/cert.pem` in the operating system's root store. A certificate/key pair that exists but is unreadable or mismatched stops startup with a clear error rather than being overwritten; delete or replace those files to recover.

Remote binding requires explicit opt-in, account authentication, and trusted hosts. TLS is served by the application itself; remote clients must trust the certificate in `data/certs/`, so a CA-issued certificate should be placed there for production access:

```powershell
.\.venv3\Scripts\python.exe main.py config set auth.mode accounts
.\.venv3\Scripts\python.exe main.py config set server.trusted_hosts research.example,192.0.2.10
.\.venv3\Scripts\python.exe main.py --host 0.0.0.0 --port 8080 --allow-remote --no-reload
```

Remote `/api/*` requests require an account-issued `Authorization: Bearer <token>`. Tokens must not be placed in URLs, logs, or browser storage. `/health` remains minimal and unauthenticated. The EDINET API key is reserved for outbound EDINET downloads and is never used for application authentication.

Account mode and open registration are the defaults. The first successful registration becomes the local administrator; close registration (or make it invitation-only) under `/admin` → Access when additional self-service accounts should be disabled. The registration mode, default role, access-token lifetime, and refresh idle/absolute lifetimes saved there are stored in `app.db`. Browser refresh tokens are held in an HttpOnly cookie, while access tokens remain in memory. Personal automation tokens can be created under `/api/auth/tokens` with scope `*` (full access) or `read` (GET/HEAD/OPTIONS only) and should be revoked when no longer needed; changing or resetting a password revokes all of that account's sessions and API tokens.

Refreshing prices from the provider (`/api/security/update-price`, `/api/screening/update-prices`) writes shared market data and requires the operator or admin role. Saved backtests are visible only to the account that ran them; results saved before ownership was recorded are visible to administrators only.

Responses carry `X-Content-Type-Options`, `X-Frame-Options`, `Referrer-Policy`, and a `Content-Security-Policy` for the workspace; remote deployments also send `Strict-Transport-Security`. Request size limits apply to the bytes actually received, including chunked uploads without a `Content-Length` header.

Generated artifacts live in `data/artifacts/`: `backtests/`, `reports/`, `exports/`, and per-job workspaces in `jobs/`. Missing databases are created when the server starts rather than when its module is imported.

Administrators can change the minimum password length under `/admin` → Security settings. The accepted range is 5–128 characters, and the stored policy applies to registration, invitations, resets, password changes, and administrator-created credentials. The public `/pricing` page currently advertises €10 per month or €100 per year; it is informational and does not enable billing or subscription enforcement.

For frontend development, keep FastAPI serving HTTPS on port 8000 and start Vite in another terminal (the dev proxy targets `https://127.0.0.1:8000` with certificate verification disabled for the self-signed certificate):

```bash
cd frontend-v2
npm run dev
```

The primary frontend is a React/TypeScript single-page workspace in `frontend-v2/`. It communicates with the backend through `/api/*` and `/health`.

Database inputs are resolved and authorized server-side. Uploads and generated outputs live beneath per-job workspaces in `data/artifacts/jobs/`.

### Runtime size limits

- `limits.max_upload_bytes` defaults to 10 MiB for incoming files.
- Pipeline multipart uploads have a separate 500 MiB default ceiling. The Import Stock Prices (CSV) step also rejects files larger than 500 MiB before parsing.
- `limits.max_export_bytes` defaults to 25 MiB for ordinary response exports.
- `limits.max_backtest_artifact_bytes` defaults to 256 MiB for server-generated backtest files. Rolling archives are built directly on disk and partial archives are removed if this limit is reached.
- `limits.max_report_artifact_bytes` defaults to 128 MiB for reproducible report ZIPs. Reports are written atomically beneath `data/artifacts/reports/` and partial files are removed on failure.

Values are byte counts, stored as settings in `app.db`, and read at application startup. Increase the backtest artifact limit only when the expected archive and available disk space justify it, then restart the application.

### Filing archive storage

`filings.db` retains each provider ZIP as a compressed `archive_content` BLOB. Extracted member bytes are not written for new ingests; the Filing Explorer extracts the requested member from the ZIP in memory when it is viewed. New catalogs retain numeric/analytical XBRL facts, contexts, units, and artifact metadata; narrative sections are reconstructed from the archive on demand instead of being materialized in the core database.

Complete Japanese-to-English translations are cached in the versioned `filing_translations` table in `filings.db`. The viewer always preserves the Japanese source and renders English alongside it. Translation covers complete section bodies and visible report HTML; model failures or residual Japanese return a retryable error instead of a partial English result.

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
\.venv3\Scripts\python.exe scripts/rebuild_filings_db.py --source data/filings.db --output D:\data\filings.compact.db
\.venv3\Scripts\python.exe scripts/rebuild_filings_db.py --source data/filings.db --output D:\data\filings.compact.db --apply
```

The rebuild omits nonnumeric/nil facts, materialized sections, and the FTS copy of narrative text while retaining the compressed ZIPs. Verify the output before pointing the `storage.filings_db_path` setting at it.

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

Before enqueueing, the orchestrator checks required settings (a step that needs the EDINET API key is refused with a message naming `edinet.api_key`), field limits, embedded uploads, and allowed paths. Failure stops later steps. Cancellation is cooperative at safe checkpoints and never force-kills a Python thread.

## Bounded verification

`scripts/verify.py` runs stages sequentially with hard timeouts and terminates the stage process tree on timeout:

```powershell
.\.venv3\Scripts\python.exe -B scripts\verify.py
```

Use repeated `--stage` options for focused checks, or `--timeout-seconds N` for a stricter cap.

## Steps

Steps always read and write the application's own databases (`market.db`, and `filings.db` for filing archives); none takes a database path. Steps marked as needing the EDINET API key are refused before they start when the `edinet.api_key` setting is empty.

### `get_documents`
Fetches the list of available filings from the EDINET API and stores document metadata in `DocumentList` in `market.db`. Needs the EDINET API key.

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

### `download_xbrl`
Downloads and indexes EDINET type-1 XBRL packages into `filings.db`. Uses the `edinet.api_key` setting.

```json
"download_xbrl_config": {
  "mode": "all",
  "max_documents": 100,
  "doc_type_code": "120"
}
```

- `mode` — `explicit` downloads the IDs in `document_ids`; `backfill` discovers a bounded batch of eligible CSV-downloaded documents; `all` queries every eligible `DocumentList` row with `xbrlFlag = "1"` and `legalStatus` in `("1", "2")` that is not marked as XBRL-downloaded.
- `document_ids` — comma-separated EDINET document IDs used only in `explicit` mode.
- `max_documents` — applies only to `backfill`; `all` processes the complete matching queue without a document-count cap.
- `doc_type_code` — applies to both `backfill` and `all`; `120` is annual, `130` semi-annual, `140` quarterly, and an empty value includes all types.
- `all` adds `DocumentList.XbrlDownloaded` when needed and records `True`, `Checked_Unavailable`, or `Checked_Error` per document. It does not modify the legacy CSV `DocumentList.Downloaded` marker, and failed documents remain eligible for a later retry.
- Acquisition reuses one HTTP session, downloads at most five packages concurrently, and writes `DocumentList` status changes in batches. The document-type filter is applied before work is queued in both `backfill` and `all` modes.

---

### `populate_company_info`
Loads the EDINET company code list into the database.

When `csv_file` is blank, the app downloads the official English EDINET code list ZIP from the EDINET code-list page, reads the CSV inside it, normalizes the column names to the existing schema, and stores the result in the company info table. When `csv_file` is provided, that local file is used instead.

```json
"populate_company_info_config": {
  "csv_file": ""
}
```

- `csv_file` — optional local CSV override. Leave blank to download the official English EDINET code list.

---

### `import_stock_prices_csv`
Imports historical stock prices from a user-supplied CSV file into the `stock_prices` database table. Duplicate dates for the same ticker are automatically skipped.

```json
"import_stock_prices_csv_config": {
  "csv_file": "C:/path/to/prices.csv",
  "default_ticker": "TPX",
  "default_currency": "JPY",
  "date_column": "Date",
  "price_column": "Price",
  "ticker_column": "Ticker",
  "currency_column": "Currency"
}
```

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
}
```

---
### `update_fx_data`
Imports ECB historical FX rates and central-bank CPI/inflation series into the `Stock_Prices` table. FX comes from the ECB `eurofxref-hist` archive; inflation covers USD, JPY, GBP, AUD, and CAD (FRED CPI series) plus EUR (ECB HICP via SDMX). All sources are free and need no API keys. Rows are deduplicated on `(Date, Ticker)` and stored as pseudo-tickers (e.g. `Inflation_USD`) so they can be used as benchmark and conversion series.

No configuration fields; it writes to the Standardized database.

---


### `check_tdnet_splits`
Captures Japanese stock-split and share-consolidation announcements from
TDnet, the TSE timely-disclosure service. Splits are not EDINET-reportable
events (verified against 企業内容等の開示に関する内閣府令 — no filing trigger
exists), so TDnet is the authoritative regulator-mandated venue. TDnet only
serves a rolling ~30-day window, so schedule this step daily or weekly; every
matched disclosure is stored as an event row in `Tdnet_Disclosures`
(`market.db`) keyed by a stable disclosure id, making reruns idempotent.

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
### `detect_splits`
Detects stock splits and consolidations from price discontinuities and confirms them against annual-report share metrics. Candidates are persisted as pending/confirmed/rejected events in the managed `Stock_Splits` table with per-ticker scan watermarks, so reruns are incremental.

```json
"detect_splits_config": {
  "mode": "incremental",
  "price_drop_threshold": 0.40
}
```

- `mode` — `incremental` scans only new price data since the last known split; `full` rescans all history for every ticker; `verify_pending` re-checks entries awaiting ShareMetrics confirmation.
  A candidate is rejected when its price did not stay moved by its ratio, when the share count stayed put at the year end, at filing, and a year on, or when it is a rise only a fractional consolidation would explain (consolidations merge a whole number of shares into one; a one-day jump of 42 % read as 17 into 12 is a rally).
- `price_drop_threshold` — minimum single-day price drop (0.0–1.0) to flag as a potential split; default `0.40` (40%).

---


### `update_bonds`
Reads corporate bonds into the bond tables of `market.db`:

1. **Bond supplements** — every shelf-registration supplement
   (発行登録追補書類, document type 100, with XBRL) listed in `DocumentList`
   and not read yet is downloaded from EDINET with the API key and kept in
   `Bond_Documents`; each bond's terms and ratings go to `Bond_Issuances`.
   Supplements EDINET no longer serves are marked unavailable.
2. **Annual-report bond schedules** — each company's latest annual report in
   `filings.db` (form 030000) is read for its bond schedule (社債明細表;
   the bonds-and-borrowings note for IFRS filers) into `Bond_Schedule_Rows`.
   Run `download_xbrl` first so new reports are in the catalog.
3. **JGB curve** — the Ministry of Finance par-yield curve (`JGB_Yields`):
   the full history on the first run or after a missed month, then the
   current month.
4. **JSDA reference prices** — the newest daily OTC reference-price files
   (公社債店頭売買参考統計値) not stored yet, into `Bond_Market_Prices`. The
   site limits request rates: files are read ten seconds apart and a refusal
   (HTTP 429) ends this part of the run; the rest are read next time.
5. **Merge** — rebuilds `Bonds`: one row per bond from the latest schedule
   plus bonds issued since, with ratings, private-placement flags, spreads at
   issue, and the matched JSDA price, yield, and spread.

Everything already read is skipped, so a daily run takes seconds after the
first (about 40 s for 561 supplements and 4,948 annual reports when stored).
**Overwrite** re-reads every stored supplement and report with the current
parser, without downloading again.

```json
"update_bonds_config": {
  "issuance_documents": true,
  "annual_reports": true,
  "jgb_curve": true,
  "market_prices": true,
  "market_days": 5,
  "max_documents": 0
}
```

- `issuance_documents` — download and read new bond supplements (needs the `edinet.api_key` setting).
- `annual_reports` — read the bond schedules of the latest annual reports.
- `jgb_curve` — refresh the government curve.
- `market_prices` / `market_days` — read up to this many of the newest JSDA files not stored yet.
- `max_documents` — cap on new filings of each kind per run; `0` reads them all.

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
  "force_reparse": "False"
}
```

- Leave `xsd_file` empty to download and parse official EDINET taxonomy releases.
- Set `xsd_file` to import a local XSD instead; `namespace_prefix`, `release_label`, `release_year`, and `taxonomy_date` are only used in that local-import mode.
- `release_selection`, `release_years`, and `namespaces` control which official releases are synced. The default `all` setting downloads the full historical set for the selected namespaces.
- `download_dir` stores downloaded taxonomy ZIP archives locally.
- `force_download` redownloads archives even if they already exist locally.
- `force_reparse` rebuilds normalized taxonomy tables even if the archive hash is unchanged.
- Each release also fills `Taxonomy_Dictionary`: every concept of every taxonomy in the archive (J-GAAP, IFRS, corporate disclosure, document information) with its XBRL item type and standard English and Japanese labels. The Filing Explorer labels statement lines from it, and screening formats percentage columns from the item types. Releases parsed before the dictionary existed are reparsed once from the cached archives in `download_dir` on the next run; no new download is needed.

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

- `Source_Mode` — `csv` (default) reads the legacy `financialData_full` table in `market.db`; `filings` reads normalized numeric XBRL facts from `filings.db`.
- `Granularity_level` — maximum taxonomy level to materialize into the statement tables. `ShareMetrics` concepts are stored at level `0` so they are always included.

Runtime notes:

- `FinancialStatements` contains only filing metadata: `docID`, `edinetCode`, `docTypeCode`, `submitDateTime`, `periodStart`, `periodEnd`, and `release_id`.
- `IncomeStatement`, `BalanceSheet`, `CashflowStatement`, and `ShareMetrics` contain `docID` plus taxonomy-label columns only.
- `ShareMetrics` materializes selected share-count, dividend-per-share, and related summary concepts as flat level-`0` columns.
- The step prefers consolidated family contexts and falls back to matching non-consolidated contexts when a consolidated value is absent, using deterministic context priority before loading each table.
- After the tables are built, the step corrects figures an issuer tagged a power of ten off, checked against the report's own other figures: a share count given in thousands (EPS and book value per share both imply a count 1,000 times larger), a P/E 100 times the year-end price over EPS, or an EPS that profit per share and the price both put a power of ten away. Only corrections two measures agree on are made; each is recorded with the filed value and the reason in `ShareMetrics_Corrections`, and the Analysis as-filed view shows the filed value.
- `ShareMetrics` per-share figures and ratios (EPS, diluted EPS, book value per share, P/E, ROE, equity ratio) are the consolidated ones: an IFRS or US GAAP filer's come from its own summary concepts (`…IFRSSummaryOfBusinessResults`, `…USGAAPSummaryOfBusinessResults`), since the Japanese GAAP concepts it also files carry the parent company's alone. A filing with any of these consolidated never takes the others from the parent company: a P/E its consolidated summary leaves out stays empty. Dividends and payout are the parent company's, as reported.
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
- Each ratio input reads one or more columns of its table with an `Aggregation`: `FirstNonNull` (the first column reported, for a line filed under several names), `sum`, or `max` (the largest, for revenue). A column can be `{"column": "...", "when": ["..."]}` to read it only where one of the `when` columns is reported. Revenue is the largest of net sales, operating revenue (a parent-only holding company or a railway), and ordinary revenue where ordinary expenses are reported (a bank or insurer, 経常収益; elsewhere that label can hold ordinary profit); gross margin stays on net sales, which cost of sales goes with. Per-share ratios divide by the year-end issued share count.

---

### `generate_rolling_metrics`
Computes rolling averages and CAGR-style growth rates for configurable metrics across selected statement tables. The columns and tables to process are declared in `src/orchestrator/generate_rolling_metrics/rolling_metrics.json`. Output tables are named `<SourceTable>_Rolling` and contain `_Average_N_Year` and `_Growth_N_Year` columns for N = 2, 3, 5, and 10 for each configured metric. Windows count fiscal years: an N-year figure needs a filing for each of the last N years, and growth compounds over the time between the two year ends. Per-share figures and share counts are put on the split-adjusted basis first, so a split inside the window does not distort them; averages are stored on their own filing's basis (views and screens apply the per-filing split factor) and growth rates need none. A company that also files reports for others (a trust bank's trusts) is measured on its own annual reports. A metric in `rolling_metrics.json` is a column name, or `{"name": "...", "columns": [...], "aggregation": "firstnonnull" | "max"}` for a line filed under several names (operating income, interest expenses) or for revenue; columns take the same `{"column", "when"}` form as ratio inputs.

Supports `overwrite`.

```json
"generate_rolling_metrics_config": {
  "Source_Database": "C:/path/to/standardized.db"
}
```

- `Source_Database` — database containing the source statement and ratio tables.

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
