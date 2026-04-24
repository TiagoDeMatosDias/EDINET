
# Python Source File Reference (Living Document)

Last updated: 2026-04-23
- Central reference for runtime/test Python modules (`src/`), Tk UI modules (`ui_tk/`), and top-level scripts.
- For each file: what it owns, available functions, input/output contract, and key dependencies/calls.
- Designed to be updated continuously as functions are added/removed/changed.

---

## How to maintain this document

When updating code, update this file in the same PR/commit:

1. Add/remove function entries for changed files.
2. Update function signatures exactly (types/defaults).
3. Update dependency lists when called functions/modules/signals change.
4. Keep sections ordered by file path.
5. Update the `Last updated` date.

Suggested per-function format:
- `def name(args) -> ReturnType`
	- Purpose: ...
	- Inputs: ...
	- Output: ...
	- Calls/Dependencies: ...

---

## Current project status

- Default interface: the Tk desktop shell launched by `python main.py` is the primary maintained UI.
- Maintained top-level views: `Home`, `Orchestrator`, `Data`, `Screening`, and `Security Analysis`.
- CLI mode remains supported through `python main.py --cli` for headless pipeline execution.
- Architecture status: `src.orchestrator` is a thin dispatcher; backend modules remain largely decoupled from `Config` and are called with explicit parameters.
- Mature user-facing workflows: ingestion, ETL, translation, ratio generation, backtesting, screening, and security analysis all have dedicated test coverage.
- Partial surface: the Data Workspace is operational for navigation/resource inspection, but it is not yet a full analytical data browser.
- Visual review workflow: screenshot capture tests save current UI images under `data/mockups/screenshots/`, and the curated README copies live under `docs/images/`.

---

## Runtime modules (`src`)

### [src/orchestrator/__init__.py](src/orchestrator/__init__.py)

Responsibility: public orchestration API and step dispatch. The orchestrator is a thin dispatcher with **no business logic**. Step execution is discovered dynamically from step packages directly under `src/orchestrator`, so new step packages can be added without modifying the orchestrator runtime.

Architecture:
- **`src/orchestrator/orchestrator.py`**: runtime entry point that owns step registries, public API validation, and execution.
- **`src/orchestrator/common/__init__.py`**: shared `StepDefinition` type plus discovery helpers that scan immediate child step packages under `src/orchestrator`.
- **`src/orchestrator/common/__init__.py`**: Step contracts and discovery helpers, including `StepDefinition`, `StepFieldDefinition`, and registry construction.
- **Discovered step packages**: each step lives in its own package such as `src/orchestrator/generate_financial_statements/` and exports `STEP_DEFINITION` only.
- **`STEP_HANDLERS`**: generated registry mapping step names and aliases to discovered handlers.

- `def run(config=None, steps=None, on_step_start=None, on_step_done=None, on_step_error=None, cancel_event=None) -> None`
	- Purpose: Run the provided pipeline config. If `config` is omitted, it loads the saved runtime config.
	- Inputs: optional `Config` or config dict, optional ordered `steps`, optional callbacks, optional `cancel_event`.
	- Output: None.
	- Calls/Dependencies: `Config`, `validate_input`, `STEP_HANDLERS`.

- `def list_available_steps() -> list[dict]`
	- Purpose: Return the discovered step catalog for the UI, including step names, config keys, overwrite support, aliases, and input field definitions.
	- Inputs: none.
	- Output: list of step metadata dicts.

- `def validate_input(config, steps=None) -> list[dict]`
	- Purpose: Validate pipeline shape plus required top-level keys, required step-config fields, and typed field values. Returns normalized enabled steps.
	- Inputs: `Config` or config dict, optional ordered `steps`.
	- Output: normalized list of step dicts (raises `RuntimeError` on invalid input).

---

### [src/backtesting.py](src/backtesting.py)

Responsibility: portfolio construction, price/dividend ingestion, return calculations, performance metrics, human-readable reports and charts. The orchestration entry point is `run_backtest()` which is invoked by `src.orchestrator`.

- `def _normalise_portfolio_entry(spec) -> tuple[str, float]`
	- Purpose: Parse a single portfolio entry (legacy float or dict) into a canonical `(mode, numeric_value)` tuple.
	- Inputs: `spec` — an `int`/`float` (legacy weight) or `dict` with keys `mode` and `value`.
	- Output: `(mode, value)` where `mode` is one of `"weight"|"shares"|"value"`.
	- Calls/Dependencies: `logger.warning`.

- `def resolve_portfolio_allocations(portfolio_config: dict, start_prices: dict[str, float], initial_capital: float = 0.0) -> tuple[dict[str, float], float, list[str]]`
	- Purpose: Resolve mixed-mode allocation specs (weights, fixed shares, fixed value) into normalised portfolio weights and an effective capital amount.
	- Inputs: `portfolio_config` (ticker → spec), `start_prices` (ticker → opening price), `initial_capital` (user-supplied or 0 to derive).
	- Output: `(portfolio_weights, effective_capital, warnings)` where `portfolio_weights` sums to 1.0 (unless empty), `effective_capital` is the capital used for allocation, and `warnings` lists issues (missing start prices, inconsistent totals).
	- Calls/Dependencies: `_normalise_portfolio_entry`, `logger.warning`.

- `def get_portfolio_prices(db_path: str, prices_table: str, tickers: list[str], start_date: str, end_date: str, *, conn: sqlite3.Connection | None = None) -> pandas.DataFrame`
	- Purpose: Query the `prices_table` for daily `Date, Ticker, Price` rows for the requested tickers and date range.
	- Inputs: `db_path`, `prices_table`, `tickers`, `start_date` (YYYY-MM-DD), `end_date` (YYYY-MM-DD), optional `conn`.
	- Output: `pd.DataFrame` (long form) with columns `Date` (datetime), `Ticker`, `Price` (numeric), ordered by `Date`.
	- Calls/Dependencies: `read_sql_query`, `to_datetime`, `to_numeric`, `sqlite3.connect`, `conn.close`.

- `def get_dividend_data(db_path: str, per_share_table: str, company_table: str, tickers: list[str], start_date: str, end_date: str, *, financial_statements_table: str = "FinancialStatements", dividend_column: str | None = None, conn: sqlite3.Connection | None = None) -> pandas.DataFrame`
	- Purpose: Load per-share dividend records for tickers and map them to `periodEnd` dates. Supports both modern (`PerShare` with `docID`) and legacy schemas (`edinetCode` + `periodEnd`).
	- Inputs: DB path, `per_share_table`, `company_table`, ticker list, date range, optional `financial_statements_table`, optional explicit `dividend_column`, optional `conn`.
	- Output: `pd.DataFrame` with columns `Ticker`, `periodEnd` (datetime), `PerShare_Dividends` (numeric). Empty DataFrame when no supported dividend column or no tickers.
	- Calls/Dependencies: `conn.execute`, `read_sql_query`, `to_datetime`, `to_numeric`, `logger.warning`, `conn.close`.

- `def calculate_portfolio_returns(prices_df: pd.DataFrame, portfolio_weights: dict[str, float], dividends_df: pd.DataFrame | None = None) -> pd.DataFrame`
	- Purpose: Compute weighted daily portfolio returns and cumulative returns. Prices are forward-filled; dividends are treated as cash (not reinvested) and added to portfolio value from their pay date onward.
	- Inputs: `prices_df` (long form with `Date`,`Ticker`,`Price`), `portfolio_weights` (ticker → weight), optional `dividends_df` (with `Ticker`,`periodEnd`,`PerShare_Dividends`).
	- Output: `pd.DataFrame` indexed by `Date` with columns `portfolio_return` (daily) and `cumulative_return` (level series).
	- Calls/Dependencies: `pivot_table`, `ffill`, `pct_change`.

- `def calculate_return_decomposition(prices_df: pd.DataFrame, portfolio_weights: dict[str, float], dividends_df: pd.DataFrame | None = None) -> dict[str, pd.DataFrame]`
	- Purpose: Produce three time series: `total` (price + dividends), `price_only`, and `dividend_only` (additive decomposition where `total = price_only + dividend_only`).
	- Inputs: same as `calculate_portfolio_returns`.
	- Output: `dict` with keys `total`, `price_only`, `dividend_only`; each value is a `DataFrame` indexed by `Date` containing daily and cumulative returns.
	- Calls/Dependencies: `calculate_portfolio_returns`.

- `def calculate_per_company_returns(prices_df: pd.DataFrame, portfolio_weights: dict[str, float], dividends_df: pd.DataFrame | None = None, initial_capital: float = 0.0) -> pd.DataFrame`
	- Purpose: Produce a per-ticker breakdown (start/end price, price return, dividend return, total return, weight and weighted contributions). When `initial_capital` > 0 includes concrete `capital_invested`, `shares_purchased`, `dividends_received` and `market_value`.
	- Inputs: `prices_df`, `portfolio_weights`, optional `dividends_df`, optional `initial_capital`.
	- Output: `pd.DataFrame` with columns including `Ticker`, `start_price`, `end_price`, `price_return`, `dividend_return`, `total_return`, `weight`, `weighted_*` and optional `capital_invested`, `shares_purchased`, `dividends_received`, `market_value`.
	- Calls/Dependencies: `groupby`, `iterrows`, `pd.DataFrame`.

- `def calculate_yearly_returns(decomposition: dict[str, pd.DataFrame]) -> pd.DataFrame`
	- Purpose: Aggregate cumulative-return series into calendar-year price/dividend/total returns.
	- Inputs: `decomposition` (output of `calculate_return_decomposition`).
	- Output: `pd.DataFrame` with `Year`, `Price Return`, `Dividend Return`, `Total Return`.

- `def calculate_dividends_by_company_year(dividends_df: pd.DataFrame | None, shares_purchased: dict[str, float] | None = None) -> pd.DataFrame`
	- Purpose: Pivot per-share dividends into a Year × Ticker table. If `shares_purchased` is provided, values become cash received (per-share × shares).
	- Inputs: `dividends_df`, optional `shares_purchased` map.
	- Output: `pd.DataFrame` indexed by `Year` with one column per ticker and a `Total` column.

- `def calculate_benchmark_returns(prices_df: pd.DataFrame, benchmark_ticker: str, dividends_df: pd.DataFrame | None = None) -> pd.DataFrame`
	- Purpose: Compute daily benchmark returns with price/dividend decomposition and cumulative series.
	- Inputs: `prices_df` (long form), `benchmark_ticker`, optional `dividends_df` for the benchmark.
	- Output: `pd.DataFrame` indexed by `Date` with columns `benchmark_return`, `cumulative_return`, `price_return`, `cum_price_return`, `dividend_return`, `cum_dividend_return`.

- `def calculate_metrics(portfolio_df: pd.DataFrame, benchmark_df: pd.DataFrame | None, start_date: str, end_date: str, risk_free_rate: float = 0.0) -> dict`
	- Purpose: Compute summary performance metrics: `total_return`, `annualized_return`, `volatility` (annualised), `sharpe_ratio`, `max_drawdown`, and benchmark equivalents when available.
	- Inputs: `portfolio_df` (from `calculate_portfolio_returns`), optional `benchmark_df` (from `calculate_benchmark_returns`), `start_date`, `end_date`, `risk_free_rate`.
	- Output: `dict` containing stated metrics plus `risk_free_rate`, and optional `benchmark_*` fields.
	- Calls/Dependencies: `pd.to_datetime`, `np.sqrt`, `cummax`.

- `def generate_report(metrics: dict, output_file: str, decomposition: dict | None = None, per_company: pd.DataFrame | None = None, benchmark_df: pd.DataFrame | None = None, yearly_returns: pd.DataFrame | None = None, dividends_by_year: pd.DataFrame | None = None) -> str`
	- Purpose: Render a human-readable textual backtest report (tables and summaries) and write it to `output_file`.
	- Inputs: `metrics` dict produced by `calculate_metrics`, optional decomposition/per-company/yearly/dividends tables.
	- Output: The textual report string (also written to disk).
	- Calls/Dependencies: `os.makedirs`, `open`, `logger.info`.

- `def generate_backtest_charts(decomposition: dict[str, pd.DataFrame], benchmark_df: pd.DataFrame | None, per_company: pd.DataFrame | None, output_dir: str, start_date: str, end_date: str, dividends_by_year: pd.DataFrame | None = None) -> list[str]`
	- Purpose: Create visualisations (PNG) for cumulative returns, drawdown, decomposition, per-company breakdown and dividends-by-year.
	- Inputs: decomposition, optional `benchmark_df`, optional `per_company`, `output_dir`, `start_date`, `end_date`, optional `dividends_by_year`.
	- Output: List of file paths created. If `matplotlib` is not installed returns an empty list.
	- Calls/Dependencies: `matplotlib.pyplot.subplots`, `fig.savefig`, `np.arange`, `os.makedirs`, `logger.info`.

- `def run_backtest(backtesting_config: dict, db_path: str, prices_table: str = "stock_prices", ratios_table: str = "PerShare", company_table: str = "companyInfo", financial_statements_table: str = "FinancialStatements") -> dict`
	- Purpose: High-level runner used by the orchestrator. Orchestrates data retrieval, allocation resolution, return calculations, metric computation, report writing and chart generation.
	- Inputs: `backtesting_config` (must include `start_date`, `end_date`, `portfolio`; may include `benchmark_ticker`, `output_file`, `risk_free_rate`, `initial_capital`), `db_path`, and optional table names.
	- Output: `metrics` dict (same shape as produced by `calculate_metrics` with additional attachments such as `per_company` list and `chart_files`).
	- Calls/Dependencies: `get_portfolio_prices`, `get_dividend_data`, `resolve_portfolio_allocations`, `calculate_portfolio_returns`, `calculate_return_decomposition`, `calculate_per_company_returns`, `calculate_yearly_returns`, `calculate_dividends_by_company_year`, `calculate_benchmark_returns`, `calculate_metrics`, `generate_report`, `generate_backtest_charts`.

- `_BACKTEST_DURATIONS: dict[str, int]`
	- Purpose: Predefined duration labels used by the backtest-set runner (e.g. `"1yr"`, `"2yr"`, ...).

- `def _generate_set_summary(all_results: list[dict], output_file: str) -> None`
	- Purpose: Produce an aggregate textual summary for a batch of backtests (mean/median stats, benchmark comparisons, per-backtest table) and write to `output_file`.
	- Inputs: `all_results` (list of result entries produced by `run_backtest_set`), `output_file` path.
	- Output: None (writes file).

- `def run_backtest_set(config: dict, db_path: str, prices_table: str = "stock_prices", ratios_table: str = "PerShare", company_table: str = "companyInfo", financial_statements_table: str = "FinancialStatements") -> list[dict]`
	- Purpose: Convenience runner that reads a CSV of yearly scored portfolios and executes a set of horizon backtests for each year (1,2,3,5,10 years by default), emitting per-run reports and an aggregate summary.
	- Inputs: `config` (must include `csv_file`, may include `benchmark_ticker`, `output_dir`, `risk_free_rate`, `initial_capital`), `db_path`, optional table names.
	- Output: List of result dicts (one per individual backtest), and writes an aggregate summary via `_generate_set_summary`.
	- Calls/Dependencies: `pd.read_csv`, `run_backtest`, `_generate_set_summary`.

---

### [src/orchestrator/processor](src/orchestrator/processor)

Responsibility: Orchestrator-owned processor mixins that provide the shared SQLite/schema helpers consumed by the extracted orchestrator services.

- `OrchestratorDataProcessor`
	- Purpose: Compose the shared base helpers plus the ratio, financial-statement, and taxonomy helper mixins used by `src/orchestrator/services/*`.
	- Notes: Instantiated directly by orchestrator step modules; preserves the historical `d.data()` test seam without importing `src/data_processing.py`.

### [src/data_processing.py](src/data_processing.py)

Responsibility: Backward-compatible facade over orchestrator-owned services and the remaining legacy ETL helpers that have not been moved yet.

`class data`
	- Purpose: Compatibility subclass of `OrchestratorDataProcessor` that keeps the legacy public API stable while delegating orchestrator-facing workflows into `src/orchestrator/services/*`.

	- `def generate_financial_statements(self, source_database, target_database, granularity_level, overwrite=False) -> None`
		- Purpose: Generate taxonomy-backed wide financial-statement tables from `financialData_full`; resumable processing in internal docID batches.
		- Inputs: `source_database`, `target_database`, `granularity_level`, optional `overwrite`.
		- Output: None (writes/updates DB tables: `FinancialStatements`, `IncomeStatement`, `BalanceSheet`, `CashflowStatement`).
		- Calls/Dependencies: `_resolve_table_name_in_schema`, `_resolve_source_col_names`, pandas batch queries, bulk SQLite temp-table inserts, Taxonomy queries, `conn.execute`, `conn.executescript`, `conn.commit`, `conn.close`, `logger.info`, `logger.warning`.

	- `def populate_business_descriptions_en(self, target_database, providers_config, table_name="FinancialStatements", docid_column="docID", source_column="DescriptionOfBusiness", target_column="DescriptionOfBusiness_EN", source_language="ja", target_language="en", overwrite=False, batch_size=25) -> None`
		- Purpose: Populate translated English business descriptions in the target statements table using an ordered fallback provider list.
		- Inputs: `target_database`, `providers_config`, optional table/column names, source/target languages, `overwrite`, `batch_size`.
		- Output: None (updates `target_column` in place, creating it when missing).
		- Calls/Dependencies: `src.description_translation.load_translation_providers`, `src.description_translation.translate_text_with_providers`, `_resolve_table_name_in_schema`, `_resolve_column_name`, `_ensure_typed_table_columns`, `conn.execute`, `conn.commit`, `conn.close`, `logger.info`, `logger.warning`.

	- `def generate_ratios(self, source_database, target_database, formulas_config, overwrite=False, batch_size=5000) -> None`
		- Purpose: Compile configured formulas into `PerShare`/`Valuation`/`Quality` tables, resolving formula dependencies and executing updates in batches.
		- Inputs: `source_database`, `target_database`, `formulas_config` (path), optional `overwrite`, `batch_size`.
		- Output: None (writes/updates `PerShare`, `Valuation`, `Quality` tables).
		- Calls/Dependencies: `_load_generate_ratios_definitions`, `_build_generate_ratios_execution_plan`, `_ensure_generate_ratios_tables`, `_ensure_table_columns`, `conn.execute`, `conn.executescript`, `conn.commit`, `conn.close`, `logger.info`, `logger.warning`.

	- `def generate_historical_ratios(self, source_database, target_database, overwrite=False, company_batch_size=200) -> None`
		- Purpose: Produce per-company historical metric tables (e.g. `Pershare_Historical`) by computing rolling/aggregate statistics and inserting in batches.
		- Inputs: `source_database`, `target_database`, optional `overwrite`, `company_batch_size`.
		- Output: None (writes/updates historical tables).
		- Calls/Dependencies: `_resolve_table_name_in_schema`, `_create_index_if_not_exists`, `_ensure_historical_table_schema`, `_build_cross_sectional_stats`, `_compute_historical_metrics`, `conn.execute`, `conn.commit`, `conn.close`, `logger.info`, `logger.warning`.

	- `def parse_edinet_taxonomy(self, xsd_file, table_name, connection=None, db_path=None) -> None`
		- Purpose: Parse an EDINET taxonomy XSD and persist relevant elements to `table_name`.
		- Inputs: `xsd_file` (path to XSD), `table_name`, optional `connection`, optional `db_path` (required when `connection` is not provided).
		- Output: None (writes taxonomy rows to DB table).
		- Calls/Dependencies: `ET.parse`, `_create_table`, `_insert_data`, `_adjust_string`, `conn.commit`, `conn.close`.

---

### [src/description_translation.py](src/description_translation.py)

Responsibility: Ordered-fallback translation provider loading, text chunking, and provider execution used by `populate_business_descriptions_en`.

- Provider types: `LibreTranslateProvider`, `MyMemoryProvider`, and `ArgosTranslateProvider`.
- `class TranslationProviderConfig`
	- Purpose: Immutable runtime configuration for a translation provider instance.

- `def split_text_chunks(text: Any, chunk_char_limit: int = 700) -> list[list[str]]`
	- Purpose: Normalize and split source text into paragraph-aware translation chunks that preserve sentence boundaries where possible.
	- Inputs: raw `text`, optional `chunk_char_limit`.
	- Output: Nested list of text chunks grouped by paragraph.

- `def load_translation_providers(config_path: str) -> tuple[list[TranslationProvider], dict[str, Any]]`
	- Purpose: Load enabled providers and shared translation settings from JSON.
	- Inputs: `config_path` to the provider configuration file.
	- Output: `(providers, settings)` where `settings` currently includes values such as `chunk_char_limit` and `row_delay_seconds`.

- `def translate_text_with_providers(text: Any, providers: list[TranslationProvider], *, source_language: str = "ja", target_language: str = "en", chunk_char_limit: int = _DEFAULT_CHUNK_CHAR_LIMIT, session: requests.Session | None = None, retire_failed_providers: bool = False, log_context: str | None = None, log_provider_activity: bool = False, slow_request_warning_seconds: float | None = 10.0) -> tuple[str, str]`
	- Purpose: Translate text using ordered provider fallback and return the translated text plus the provider name that succeeded.
	- Inputs: source `text`, active `providers`, language options, chunk size, optional shared HTTP session, and logging/runtime controls.
	- Output: `(translated_text, provider_name)`.
	- Calls/Dependencies: `_clean_text_block`, `split_text_chunks`, provider `translate()`, `_retire_provider`, `requests.Session`, `logger.info`, `logger.warning`.

---

### [src/edinet_api.py](src/edinet_api.py)

Responsibility: EDINET API wrapper, document listing, download/unzip, CSV ingestion to DB.

`class Edinet`
	- Purpose: EDINET HTTP wrapper and helpers to download, extract and ingest financial CSVs into the project DB. No Config dependency; all parameters are passed explicitly via the constructor.
	- Constructor: `Edinet(base_url, api_key, db_path, raw_docs_path=None, doc_list_table=None, company_info_table=None, taxonomy_table=None)`

	- `def get_All_documents_withMetadata(self, start_date: str = '2015-01-01', end_date: str | None = None) -> list`
		- Purpose: Iterate a date range, call the EDINET listing API and persist discovered document metadata into the configured DB table.
		- Inputs: `start_date` (YYYY-MM-DD), optional `end_date` (YYYY-MM-DD).
		- Output: List of document rows inserted/retrieved for the period.
		- Calls/Dependencies: `requests.get`, `sqlite3.connect`, `self.create_table`, `cursor.execute`, `conn.commit`, `conn.close`.

	- `def downloadDoc(self, docID: str, fileLocation: str | None = None, docTypeCode: str | None = None) -> None`
		- Purpose: Download a single EDINET document ZIP to disk.
		- Inputs: `docID`, optional `fileLocation`, optional `docTypeCode`.
		- Output: None (writes ZIP file to `fileLocation`).
		- Calls/Dependencies: `generateURL` (via `src.utils.generateURL`), `requests.get`, `open` (file write).

	- `def downloadDocs(self, input_table: str, output_table: str | None = None, filter: dict | None = None) -> None`
		- Purpose: Download and extract all not-yet-downloaded documents from the DB list, load CSVs into `output_table`, and mark as downloaded.
		- Inputs: `input_table`, optional `output_table`, optional `filter`.
		- Output: None (writes DB rows and files on disk).
		- Calls/Dependencies: `generate_filter`, `query_database_select`, `sqlite3.connect`, `create_folder`, `downloadDoc`, `list_files_in_folder`, `unzip_files`, `load_financial_data`, `query_database_setColumn`, `delete_folder`.

	- `def load_financial_data(self, financialFiles: list, table_name: str, doc: dict, connection: sqlite3.Connection | None = None) -> None`
		- Purpose: Read extracted TSV/CSV financial files into a DataFrame, attach document metadata and persist to the DB table.
		- Inputs: `financialFiles` (list of paths), `table_name`, `doc` (metadata), optional `connection`.
		- Output: None (appends rows into `table_name`).
		- Calls/Dependencies: `detect_file_encoding`, `pd.read_csv`, `self.create_table`, `df.to_sql`, `conn.commit`, `conn.close`.

	- `def store_edinetCodes(self, csv_file: str, target_database: str | None = None, table_name: str | None = None) -> None`
		- Purpose: Load EDINET company codes CSV and persist into the configured company-info table.
		- Inputs: `csv_file`, optional `target_database`, optional `table_name`.
		- Output: None (writes to DB).
		- Calls/Dependencies: `pd.read_csv`, `sqlite3.connect`, `df.to_sql`, `conn.commit`, `conn.close`.

---

### [src/stockprice_api.py](src/stockprice_api.py)

Responsibility: Importing and updating historical stock prices and persisting them to the prices table.

- `def update_all_stock_prices(db_name, Company_Table, prices_table, standardized_table=None) -> None`
	- Purpose: Iterate tickers and ensure price coverage.
	- Calls/Dependencies: `_create_prices_table`, `cursor.execute`, `load_ticker_data`, `conn.close`.

- `def load_ticker_data(ticker: str, prices_table: str, conn) -> bool`
	- Purpose: Fetch price CSV for `ticker`, append new rows, return False on provider rate-limit.
	- Calls/Dependencies: `pd.read_sql_query`, `pd.read_csv`, `to_sql`, `logger`.

- `def import_stock_prices_csv(db_name, prices_table, csv_path, ...) -> None`
	- Purpose: Import user-supplied price CSV, normalize columns and append.
	- Calls/Dependencies: `pd.read_csv`, `pd.to_datetime`, `pd.to_numeric`, `pd.read_sql_query`, `to_sql`, `conn.commit`.

---

### [src/regression_analysis.py](src/regression_analysis.py)

Responsibility: OLS regression tooling, model fitting, scoring query generation and result persistence.

- `def Run_Model(query, conn, dependent_variable_df_name, independent_variables_df_names, winsorize_limits=(0.01,0.99)) -> RegressionResultsWrapper`
	- Purpose: Run SQL query, prepare data, fit OLS via `statsmodels`, return fitted results.
	- Calls/Dependencies: `pd.read_sql_query`, `pd.to_numeric`, `sm.add_constant`, `sm.OLS`, `fit`.

- `def build_scoring_query(results, query, company_table='companyInfo') -> str`
	- Purpose: Convert coefficients into a SQL scoring expression.
	- Calls/Dependencies: `_infer_primary_source_ref`.

- `def multivariate_regression(config, db_path, company_table='companyInfo') -> None`
	- Purpose: High-level runner used by orchestration to execute regression and write results.
	- Calls/Dependencies: `pd.read_sql_query`, `Run_Model`, `write_results_to_file`.

---

### [src/utilities/__init__.py](src/utilities/__init__.py)

Responsibility: public utilities package facade and discovery entry point.

- Package structure: `src/utilities/__init__.py`, `src/utilities/utils.py`, `src/utilities/logger.py`.
- Discovery: `DISCOVERED_UTILITY_MODULES` is built from modules under `src/utilities/`.
- Backward compatibility: `src/utils.py` and `src/logger.py` remain as thin facades that forward to the new package modules.

### [src/utilities/utils.py](src/utilities/utils.py)

Responsibility: Small helpers used across modules (URL building, CSV helpers, simple CSV queries).

- `def generateURL(docID, base_url, api_key, doctype=None) -> str`
	- Purpose: Construct EDINET download URL from explicit parameters.

- `def json_list_to_csv(json_list, csv_filename) -> None`
	- Purpose: Write list-of-dicts to CSV.

- `def get_latest_submit_datetime(csv_filename) -> Optional[str]`
	- Purpose: Parse CSV and return latest `submitDateTime` as string.

---

### [src/utilities/logger.py](src/utilities/logger.py)

Responsibility: Centralized logging setup.

- `class LogSetup` / `def setup_logging(...)` — configure console/file handlers and rotate/archival behavior.
`class LogSetup`
	- Purpose: Configure application logging with file and console handlers, archive old logs.

	- `def __init__(self, log_dir: str = "logs", archive_dir: str = "logs/archive") -> None`
		- Purpose: Ensure log and archive directories exist and record paths.
		- Inputs: `log_dir`, `archive_dir`.
		- Output: None (initializes instance fields).
		- Calls/Dependencies: `Path.mkdir`.

	- `def setup_logging(self) -> tuple[logging.Logger, str]`
		- Purpose: Configure root logger, add file and console handlers, and return `(logger, log_filepath)`.
		- Inputs: none (uses instance `log_dir`/`archive_dir`).
		- Output: `(logger, log_filepath)` tuple.
		- Calls/Dependencies: `_archive_existing_logs`, `logging.getLogger`, `logging.FileHandler`, `logging.StreamHandler`, `logger.addHandler`, `logger.removeHandler`.

	- `def _archive_existing_logs(self) -> None`
		- Purpose: Move existing `run_*.log` files into the archive directory.
		- Inputs: none
		- Output: None (moves files on disk).
		- Calls/Dependencies: `self.log_dir.glob`, `shutil.move`.

`def setup_logging(log_dir: str = "logs", archive_dir: str = "logs/archive") -> tuple[logging.Logger, str]`
	- Purpose: Convenience wrapper that instantiates `LogSetup` and returns `LogSetup.setup_logging()` results.
	- Inputs: `log_dir`, `archive_dir`.
	- Output: `(logger, log_filepath)`
	- Calls/Dependencies: `LogSetup.setup_logging`.

---

## Configuration

### [config.py](../config.py)

Responsibility: Singleton configuration loader. Reads `.env` and `run_config.json` on first access.

`class Config`
	- Purpose: Singleton that loads settings from `.env` and `run_config.json`.

	- `def __new__(cls, run_config_path=None) -> Config`
		- Purpose: Standard singleton constructor; loads config from disk on first creation.

	- `def get(self, key, default=None)`
		- Purpose: Get a config value from settings dict or environment variables.

	- `@classmethod def from_dict(cls, settings: dict) -> Config`
		- Purpose: Create a Config instance from a dict **without touching disk**. Bypasses the singleton pattern. Used by the Tk UI for in-memory configuration.
		- Inputs: `settings` dict.
		- Output: New `Config` instance (not the singleton).

	- `@classmethod def reset(cls) -> None`
		- Purpose: Clear the singleton so the next `Config()` call reloads from disk.

---

## Other entry points

### [main.py](../main.py)

Responsibility: CLI / GUI entry point dispatcher.

- `def _run_cli() -> None`
	- Purpose: Headless CLI execution path — sets up logging and calls `orchestrator.run()`.
	- Calls/Dependencies: `setup_logging`, `orchestrator.run`.

- `def _run_gui() -> None`
	- Purpose: Launch the default Tk desktop GUI.
	- Calls/Dependencies: `ui_tk.run_tk_app`.

- Dispatch: `--cli` → `_run_cli()`, default → `_run_gui()`.

---

## How you can help expand this document

- Add exact function signatures (including types/defaults) when you change a function.
- Fill `Inputs`/`Output` sections with precise types and examples for frequently-changed helpers.
- Add `Calls/Dependencies` entries when introducing new inter-module calls.

This reference is intentionally concise. Expand signatures, examples, and dependency notes when you touch the corresponding modules.

---

## Tk UI modules (`ui_tk`) — default GUI

### [ui_tk/app.py](../ui_tk/app.py)

Responsibility: Tk root bootstrap, view switching, event loop, and log handler wiring.

`class App`
	- Purpose: Top-level application controller; owns the Tk root, branded top bar, log panel, view switching, and cross-view drill-ins.

	- `def __init__(self, root: tk.Tk) -> None`
		- Purpose: Initialise the root window, apply the active theme, build the shell layout, wire the log handler, and register `Ctrl+1..5` shortcuts.

	- `def switch_view(self, name: str) -> None`
		- Purpose: Switch between Home / Orchestrator / Data / Screening / Security Analysis views. Views are lazily created on first access.

	- `def show_security_analysis(self, record: dict, db_path: str | None = None) -> None`
		- Purpose: Switch to Security Analysis and open a company record selected from another view (currently the Screening results grid).

- `def run_tk_app() -> None`
	- Purpose: Public entry point — creates `Tk` root, instantiates `App`, and starts `root.mainloop()`.
	- Calls/Dependencies: `apply_theme`, `poll_events`, `QueueLogHandler`, `HomePage`, `OrchestratorPage`, `DataPage`, `ScreeningPage`, `SecurityAnalysisPage`.

---

### [ui_tk/style.py](../ui_tk/style.py)

Responsibility: Dark/light theme tokens and ttk.Style configuration for the Tk desktop shell.

- Constants: `COLORS` / `theme` (live palette dicts), typography tokens (`FONT_UI`, `FONT_UI_BOLD`, `FONT_HEADING`, `FONT_TITLE`, `FONT_SMALL`, etc.), and geometry tokens (`PAD`, `SHELL_PAD`, button radii).

- `def is_dark() -> bool`
	- Purpose: Return whether the active theme mode is dark.

- `def toggle_theme(root: tk.Tk) -> str`
	- Purpose: Switch between dark and light mode and re-apply ttk styles.

- `def apply_theme(root: tk.Tk) -> ttk.Style`
	- Purpose: Apply the active theme to the root window and all ttk widget styles (using "clam" base).
	- Inputs: `root`
	- Output: Configured `ttk.Style`.

---

### [ui_tk/utils.py](../ui_tk/utils.py)

Responsibility: Background worker utilities — thread pool, event queue, log handler.

- `executor: ThreadPoolExecutor` — shared pool (2 workers).
- `event_q: queue.Queue` — thread-safe callback queue drained by `poll_events`.

- `def run_in_background(fn, args=(), on_done=None, on_error=None)`
	- Purpose: Submit `fn` to the thread pool. Callbacks are posted to `event_q` for safe dispatch on the Tk main thread.

- `def poll_events(root) -> None`
	- Purpose: Drain `event_q` and invoke callbacks; reschedules itself every 100 ms via `root.after`.

- `class QueueLogHandler(logging.Handler)`
	- Purpose: Logging handler that posts `("log", level_name, formatted_message)` tuples onto a `queue.Queue`.

---

### [ui_tk/controllers.py](../ui_tk/controllers.py)

Responsibility: Thin adapter layer between the Tk UI and backend modules. Provides pipeline execution, setup persistence, and step configuration helpers.

- Constants: step metadata derived from `orchestrator.list_available_steps()`, plus path constants (`BASE_DIR`, `ENV_PATH`, `STATE_DIR`, `RUN_CONFIG_PATH`, `SAVED_SETUPS_DIR`, `APP_STATE_PATH`).

#### Step Field Registry

The orchestrator step definitions are the single source of truth for which configuration fields each pipeline step requires. The controller derives `STEP_CONFIG_KEY`, `STEP_DISPLAY`, `ALL_STEP_NAMES`, `STEP_FIELD_DEFINITIONS`, and `DEFAULT_STEP_CONFIGS` from `orchestrator.list_available_steps()`.

- `@dataclass StepField` — Metadata for a single step-config field.
	- `key: str` — config dict key (e.g. `"Source_Database"`).
	- `field_type: str` — widget type. One of: `"str"`, `"num"`, `"text"`, `"json"`, `"database"`, `"file"`, `"portfolio"`.
	- `default: object` — default value (empty string if omitted).
	- `label: str | None` — display label; defaults to `key` when `None`.
	- `filetypes: list[tuple[str, str]] | None` — file-dialog filters (for `"file"` type).
	- `height: int` — text-area rows (for `"text"` / `"json"` types, default 3).

- `STEP_FIELD_DEFINITIONS: dict[str, list[StepField]]` — Controller-local projection of the orchestrator step definitions. The UI reads this to render only the relevant inputs per step.

- `DEFAULT_STEP_CONFIGS: dict[str, dict]` — Derived automatically from `STEP_FIELD_DEFINITIONS` via `_build_defaults_from_fields()`. Do **not** edit this dict directly — add/change fields in the underlying orchestrator step definition instead.

#### Functions

- `def run_pipeline(steps, config_dict, on_step_start=None, on_step_done=None, on_step_error=None, cancel_event=None) -> None`
	- Purpose: Build `Config.from_dict` and delegate to `orchestrator.run` with explicit step order.
	- Calls/Dependencies: `Config.from_dict`, `orchestrator.run`.

- `def list_setups() -> list[str]` — Return sorted list of saved setup names.
- `def load_setup(name: str) -> dict` — Load named setup JSON from `saved_setups/`.
- `def save_setup(name: str, setup_data: dict) -> Path` — Save named setup JSON.
- `def save_run_config(cfg: dict)` / `def load_run_config() -> dict` — Read/write `run_config.json`.
- `def get_api_key() -> str` / `def save_api_key(key: str)` — Read/write API key in `.env`.
- `def get_default_database_path() -> str` / `def remember_database_path(db_path: str) -> None` — Resolve/persist recently used SQLite database paths for analysis views.
- `def load_app_state() -> dict` / `def save_app_state(state: dict)` — Persist UI state.
- `def build_config_dict(steps, step_configs) -> dict` — Serialize UI state into a run-config dict.
- `def build_steps_from_config(run_cfg) -> list` — Convert `run_steps` into `[[name, enabled, overwrite], ...]`.
- `def build_step_configs_from_config(run_cfg) -> dict` — Build per-step configs with defaults filled in.
- `def get_default_config_for_step(step_name) -> dict` — Return a deep copy of the default config for a step.

#### Screening Adapters

- `def screening_get_metrics(db_path: str) -> dict[str, list[str]]` — Return available screening metrics.
- `def screening_get_periods(db_path: str) -> list[str]` — Return available period years.
- `def screening_run(db_path, criteria, columns, period, sort_by, sort_order, ranking_algorithm="none", ranking_rules=None) -> pd.DataFrame` — Run a screening query with optional weighted ranking.
- `def screening_export(df, output_path) -> str` — Export results to CSV.
- `def screening_export_backtest(db_path, criteria, columns, output_path, period, max_companies, ranking_algorithm="none", ranking_rules=None, historical=False) -> str` — Export screening results in the CSV shape expected by `run_backtest_set`.
- `def screening_save(name, criteria, columns, period, ranking_algorithm="none", ranking_rules=None) -> Path` — Save screening criteria and ranking state.
- `def screening_load(name) -> dict` — Load saved screening criteria.
- `def screening_list() -> list[str]` — List saved screening names.
- `def screening_delete(name) -> None` — Delete a saved screening.
- `def screening_save_history(entry) -> None` — Append a screening history entry.
- `def screening_load_history() -> list[dict]` — Load screening run history.

#### Security Analysis Adapters

- `def security_search(db_path: str, query: str, limit: int = 25) -> list[dict]` — Search securities by company name, ticker, EDINET code, or industry.
- `def security_optimize_database(db_path: str) -> dict` — Create one-time indexes used by the Security Analysis workflow.
- `def security_get_overview(db_path: str, edinet_code: str) -> dict` — Return company, market, fundamentals, valuation, and metadata for the selected security.
- `def security_get_statements(db_path: str, edinet_code: str, periods: int = 8, statement_sources: dict[str, str] | None = None) -> dict` — Return ordered historical statement rows and period labels.
- `def security_get_ratios(db_path: str, edinet_code: str) -> dict` — Return latest valuation and quality ratios.
- `def security_get_price_history(db_path: str, ticker: str, start_date: str | None = None, end_date: str | None = None) -> list[dict]` — Return ordered daily price history rows.
- `def security_get_peers(db_path: str, edinet_code: str, industry: str | None = None, limit: int = 10) -> list[dict]` — Return deterministic peer-comparison rows.
- `def security_update_price(db_path: str, ticker: str) -> dict` — Refresh one ticker’s price history and return a structured result summary.

---

### [ui_tk/shared/widgets.py](../ui_tk/shared/widgets.py)

Responsibility: Reusable composite widgets and display helpers used across the Tk shell.

- `class RoundedButton` — Theme-aware button wrapper with `reapply_colors()` support for runtime theme toggles.
- `class SearchableCombobox` — Combobox that filters its source list while the user types; used heavily for large metric/table lists.
- `class PageHeader` — Standard page title/subtitle/actions strip used across top-level views.
- `class SectionCard` / `class StatTile` / `class EmptyState` — Reusable layout primitives for dashboards and analysis surfaces.

- `class LogPanel(ttk.Frame)` — Color-coded log output with auto-scroll, level filter, clear, and export.
	- `def append(self, level: str, text: str)` — Append a log line (thread-safe).
	- `def clear(self)` — Clear all log records and display.

- `class TabBar(ttk.Frame)` — Horizontal bracket-style text tab bar.
	- `def select(self, index: int)` — Programmatic tab selection.

- `class LabeledEntry(ttk.Frame)` — Label + `ttk.Entry` with `get()` / `set()` helpers.
- `class LabeledText(ttk.Frame)` — Label + multi-line `tk.Text` with `get()` / `set()`.
- `class FilePickerEntry(ttk.Frame)` — Entry with Browse button for file selection.
- `class DatabasePickerEntry(FilePickerEntry)` — Pre-configured for `.db` files.
- `class PortfolioGrid(ttk.Frame)` — Editable Treeview table for portfolio allocations with inline editing, add/delete rows.
	- `def get_portfolio(self) -> dict` — Return portfolio dict in `run_config.json` format.
	- `def set_portfolio(self, portfolio: dict)` — Load and display portfolio dict.

---

### [ui_tk/pages/home.py](../ui_tk/pages/home.py)

Responsibility: Landing dashboard — saved setup inventory, quick workflow entry points, and working notes.

- `class HomePage(ttk.Frame)` — Hero stats plus a saved-setups tree, quick actions into Orchestrator/Screening/Security Analysis/Data Workspace, and keyboard shortcut hints. Double-click or [Open Selected] loads a setup and switches to Orchestrator view.

---

### [ui_tk/pages/orchestrator.py](../ui_tk/pages/orchestrator.py)

Responsibility: Main pipeline builder — step list, per-step config panel, run/stop controls.

- `class OrchestratorPage(ttk.Frame)`
	- `def load_config(self, cfg: dict, name: str = "")` — Load a config dict into UI state (steps, configs, labels).
	- `def new_setup(self, name: str)` — Initialise a new empty setup.
	- `def _build_step_fields(self, parent, step_name, cfg)` — Data-driven config panel builder. Reads `ctrl.STEP_FIELD_DEFINITIONS[step_name]` and creates the appropriate widget for each declared field. No step-specific branching — all steps use this single method.
	- Keyboard shortcuts: `Alt+Up/Down` (reorder), `Delete` (remove), `Enter` (open config). Context menu for remove/disable.

---

### [ui_tk/pages/data.py](../ui_tk/pages/data.py)

Responsibility: Data Workspace — project resources, reference assets, default database context, and quick navigation.

- `class DataPage(ttk.Frame)` — Operational resource surface showing the default database, reference/output counts, stable project paths, and direct navigation into downstream workflows.

---

### [ui_tk/pages/screening.py](../ui_tk/pages/screening.py)

Responsibility: Screening workspace — filter, rank, export, and drill into candidate companies.

- `class ScreeningPage(ttk.Frame)`
	- Layout: left builder surface plus right results grid.
	- Left panel: `DatabasePickerEntry`, period selector, dynamic criteria rows, ranking rules, column selection, and run controls.
	- Right panel: sortable Treeview with alternating row colours, result summary, and empty-state handling.
	- Toolbar/actions: Load, Save, History, Export, Backtest Export, and raw/formatted value toggle.
	- `def _on_db_changed(self)` — Refresh metrics/periods when database changes.
	- `def _add_criterion(self)` / `def _remove_criterion(self, row_data)` — Manage criteria rows.
	- `def _add_ranking_rule(self)` / `def _remove_ranking_rule(self, row_data)` — Manage weighted ranking rules.
	- `def _run_screening(self)` — Collect inputs, run in background thread, populate results.
	- `def _populate_results(self, df)` — Clear and fill Treeview with formatted values.
	- `def _sort_by_column(self, col)` — Client-side sort with ascending/descending toggle.
	- `def _on_company_click(self, event)` — Open the selected result in Security Analysis when a company row can be resolved.
	- `def _save_screening(self)` / `def _load_screening(self)` — Save/load criteria dialogs.
	- `def _show_history(self)` — History dialog with re-run support.
	- `def _export_results(self)` — CSV export via file dialog.
	- `def _export_backtest_results(self)` — Export a screening result set into the backtest-set CSV shape.
	- `def reapply_colors(self)` — Theme toggle support.

---

### [ui_tk/pages/security_analysis.py](../ui_tk/pages/security_analysis.py)

Responsibility: Security-level research view — typeahead search, overview cards, statement history, charts, and peer comparison.

- `class SecurityAnalysisPage(ttk.Frame)`
	- Layout: toolbar (refresh/update), database picker + search, summary cards, and four tabs (`Overview`, `Statements`, `Charts`, `Peers`).
	- Database optimization: when a DB is selected the view can request one-time index creation for large standardized databases.
	- Search: debounced typeahead over company name, ticker, EDINET code, and industry with keyboard navigation for suggestions.
	- Overview tab: company profile, business description display, fundamentals tree, ratio tree, and metadata panel.
	- Statements tab: selectable statement/ratio source with historical period table and period-count selector.
	- Charts tab: matplotlib-backed price, statement, and peer-comparison charts with table/column/timeframe/style controls and optional peer overlays.
	- Peers tab: default industry peers plus manual peer additions and reset flows.
	- `def reapply_colors(self)` — Re-apply colours for raw Tk widgets and redraw charts after theme changes.

---

### [src/security_analysis/__init__.py](../src/security_analysis/__init__.py)

Responsibility: public package facade for the Security Analysis backend. It re-exports the main query helpers from `src/security_analysis/security_analysis.py` and exposes package discovery state for future submodules.

- Package structure: `src/security_analysis/__init__.py`, `src/security_analysis/common.py`, `src/security_analysis/security_analysis.py`.
- Discovery: `DISCOVERED_SECURITY_ANALYSIS_MODULES` is built from modules under `src/security_analysis/`.

Core implementation in `src/security_analysis/security_analysis.py`:

- `@dataclass SecuritySchema`
	- Purpose: Capture resolved table/column names for a specific SQLite database.

- `def resolve_schema(db_path: str) -> SecuritySchema`
	- Purpose: Resolve actual table and column names for `CompanyInfo`, `FinancialStatements`, `Stock_Prices`, optional statement/ratio tables, and optional `DocumentList` metadata when present, including fallback company-name fields used by the standardized database.

- `def ensure_security_analysis_indexes(db_path: str) -> dict[str, Any]`
	- Purpose: Create one-time indexes that accelerate Security Analysis search, overview, statement lookup, price history, and peer-comparison queries.

- `def search_securities(db_path: str, query: str, limit: int = 25) -> list[dict[str, Any]]`
	- Purpose: Search securities across name, ticker, EDINET code, and industry with deterministic ranking.

- `def get_security_overview(db_path: str, edinet_code: str) -> dict[str, Any]`
	- Purpose: Return company profile, market snapshot, fundamentals, valuation, quality, and metadata for the selected security.

- `def get_security_ratios(db_path: str, edinet_code: str) -> dict[str, Any]`
	- Purpose: Return latest valuation and quality ratios, including fallback calculations when direct valuation fields are missing.

- `def get_security_statements(db_path: str, edinet_code: str, periods: int = 8, statement_sources: dict[str, str] | None = None) -> dict[str, Any]`
	- Purpose: Return ordered historical statement rows for the requested financial statement and ratio sources.

- `def get_security_price_history(db_path: str, ticker: str, start_date: str | None = None, end_date: str | None = None) -> list[dict[str, Any]]`
	- Purpose: Return ordered daily price history rows for charting and change calculations.

- `def get_security_peers(db_path: str, edinet_code: str, industry: str | None = None, limit: int = 10) -> list[dict[str, Any]]`
	- Purpose: Return deterministic peer-comparison rows based on the selected company’s industry and latest snapshot.

- `def update_security_price(db_path: str, ticker: str) -> dict[str, Any]`
	- Purpose: Refresh one ticker’s price history using the existing stock-price provider module and return a structured result summary.

---

### [src/screening/__init__.py](../src/screening/__init__.py)

Responsibility: public package facade for backend screening logic. It re-exports the main screening helpers from `src/screening/screening.py` and exposes package discovery state for future screening modules.

- Package structure: `src/screening/__init__.py`, `src/screening/common.py`, `src/screening/screening.py`.
- Discovery: `DISCOVERED_SCREENING_MODULES` is built from modules under `src/screening/`.

Core implementation in `src/screening/screening.py`:

- Constants: `SCREENING_TABLES`, `OPERATOR_MAP`, `DEFAULT_COLUMNS`, `FORMAT_RULES`, ranking-related constants, and column alias helpers.

- `def get_available_metrics(db_path: str) -> dict[str, list[str]]` — Introspect DB for screening table columns.
- `def get_available_periods(db_path: str) -> list[str]` — Return distinct periodEnd years.
- `def build_screening_query(criteria: list[dict], columns: list[str], period: str | None = None, available_metrics: dict[str, list[str]] | None = None, column_aliases: dict[str, str] | None = None) -> tuple[str, list]` — Build parameterised SQL with validation.
- `def run_screening(db_path: str, criteria: list[dict], columns: list[str], period: str | None = None, sort_by: str | None = None, sort_order: str = "ASC", ranking_algorithm: str = "none", ranking_rules: list[dict] | None = None) -> pd.DataFrame` — Execute screening, apply optional ranking, and return results.
- `def export_screening_to_backtest_csv(db_path: str, criteria: list[dict], columns: list[str], output_path: str, period: str | None = None, max_companies: int = 25, ranking_algorithm: str = "none", ranking_rules: list[dict] | None = None, historical: bool = False) -> str` — Export screening results in the CSV format used by `run_backtest_set`.
- `def export_screening_to_csv(df, output_path) -> str` — Export DataFrame to CSV.
- `def format_financial_value(value, column_name: str, formatted: bool = False) -> str` — Format values for display or return the raw representation used by the UI toggle.
- `def save_screening_criteria(name: str, criteria: list[dict], columns: list[str], period: str | None, save_dir: str, ranking_algorithm: str = "none", ranking_rules: list[dict] | None = None) -> Path` — Persist criteria and ranking state as JSON.
- `def load_screening_criteria(name, save_dir) -> dict` — Load saved criteria.
- `def list_saved_screenings(save_dir) -> list[str]` — List saved screening names.
- `def delete_screening_criteria(name, save_dir) -> None` — Delete saved criteria.
- `def save_screening_history(entry, history_path) -> None` — Append to JSON-lines history.
- `def load_screening_history(history_path) -> list[dict]` — Load history (most recent first).

---

## Tests (`tests/`)

Responsibility: Unit tests covering core logic and UI helpers. Each test file targets the corresponding module:

- `[tests/test_backtesting.py](tests/test_backtesting.py)` — tests backtest data retrieval, calculations, report and chart generation, and end-to-end `run_backtest` flows.
- `[tests/test_data_processing.py](tests/test_data_processing.py)` — tests `data` ETL methods, formula compilation, historical ratio generation, and XSD parsing helpers.
- `[tests/test_description_translation.py](tests/test_description_translation.py)` — tests translation chunking, provider loading, provider fallback behaviour, and error handling.
- `[tests/test_edinet_api.py](tests/test_edinet_api.py)` — tests `Edinet` wrapper methods including download, unzip, CSV ingestion and DB interactions.
- `[tests/test_regression_analysis.py](tests/test_regression_analysis.py)` — tests OLS runner, scoring query builder, and results writer.
- `[tests/test_security_analysis.py](tests/test_security_analysis.py)` — tests schema normalization, search ranking, overview payloads, price history, peer selection, and single-ticker price updates.
- `[tests/test_stockprice_api.py](tests/test_stockprice_api.py)` — tests CSV import and stock price ingestion logic.
- `[tests/test_ui_screenshots.py](tests/test_ui_screenshots.py)` — launches the real Tk application, navigates the maintained views, and saves screenshots to `data/mockups/screenshots/` for visual review.
- `[tests/test_ui_tk_smoke.py](tests/test_ui_tk_smoke.py)` — Tk UI smoke tests: imports, theme application, widget instantiation, controller functions, QueueLogHandler, ScreeningPage, and SecurityAnalysisPage.
- `[tests/test_screening.py](tests/test_screening.py)` — Backend screening tests: query building, execution, persistence, formatting, SQL injection prevention.
- `[tests/test_orchestrator.py](tests/test_orchestrator.py)` — Orchestrator tests: `run_pipeline` basic flow, cancellation, error handling, `execute_step` dispatch, `validate_config`, `Config.from_dict` independence and singleton behaviour.
- `[tests/test_utils.py](tests/test_utils.py)` — small helper tests for URL generation and CSV export.

---

Last updated: 2026-04-23

Keep this document aligned with code changes in the same PR or commit.

