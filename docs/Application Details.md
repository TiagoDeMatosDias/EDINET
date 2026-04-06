
# Python Source File Reference (Living Document)

Last updated: 2026-04-06
- Central reference for runtime/test Python modules (`src/`) and top-level scripts.
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

## Runtime modules (`src`)

### [src/orchestrator.py](src/orchestrator.py)

Responsibility: application orchestration and step dispatch. The orchestrator is a thin dispatcher with **no business logic**. Each step is handled by a dedicated handler function that extracts configuration and calls the appropriate module with explicit parameters. No shared mutable state is carried between steps.

Architecture:
- **Step handlers** (`_step_get_documents`, `_step_download_documents`, etc.): one function per step. Each creates its own module instances with explicit params from config.
- **`STEP_HANDLERS`**: dict mapping step names to handler functions.
- **`validate_config()`**: pre-flight validation extracted into its own function.
- **No class instantiation** of `Edinet` or `data` in `run()` / `run_pipeline()` — each handler creates what it needs.

- `def execute_step(step_name: str, config, overwrite: bool = False) -> None`
	- Purpose: Dispatch to the registered handler for `step_name` via `STEP_HANDLERS`.
	- Inputs: `step_name`, `config`, `overwrite`.
	- Output: None.
	- Calls/Dependencies: `STEP_HANDLERS[step_name]`.

- `def validate_config(config, enabled_steps: list[str]) -> None`
	- Purpose: Validate required top-level keys and step-config fields for enabled steps. Raises `RuntimeError` with details on missing settings.
	- Inputs: `config`, `enabled_steps`.
	- Output: None (raises on failure).

- `def run() -> None`
	- Purpose: Load Config, determine enabled steps, validate, then execute in order.
	- Inputs: none.
	- Output: None.
	- Calls/Dependencies: `Config`, `validate_config`, `execute_step`.

- `def run_pipeline(steps: list[dict], config: Config, on_step_start=None, on_step_done=None, on_step_error=None, cancel_event=None) -> None`
	- Purpose: Execute a list of steps in order with per-step callbacks and cancellation support. Used by the Tk UI for background pipeline execution.
	- Inputs: `steps` (list of dicts with `name` and optional `overwrite`), `config`, optional callbacks, optional `cancel_event`.
	- Output: None.
	- Calls/Dependencies: `execute_step`, `threading.Event`.

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

### [src/data_processing.py](src/data_processing.py)

Responsibility: ETL and transformation of EDINET raw data into normalized financial tables and ratio series.

`class data`
	- Purpose: Stateless namespace for data-processing operations. No Config dependency; all parameters are passed explicitly by the caller (orchestrator).

	- `def generate_financial_statements(self, source_database, source_table, target_database, mappings_config, company_table=None, prices_table=None, overwrite=False, batch_size=2500) -> None`
		- Purpose: Generate normalized financial-statement tables from raw EDINET records; resumable chunked processing by `docID`.
		- Inputs: `source_database`, `source_table`, `target_database`, `mappings_config` (path), optional `company_table`, `prices_table`, `overwrite`, `batch_size`.
		- Output: None (writes/updates DB tables: `FinancialStatements`, `IncomeStatement`, `BalanceSheet`, `CashflowStatement`).
		- Calls/Dependencies: `_load_financial_statement_mappings`, `_collect_financial_statement_filters`, `_resolve_table_name_in_schema`, `_build_source_relevance_predicate`, `_create_financial_statement_tables`, `_insert_base_financial_statements`, `_insert_statement_table_rows`, `conn.execute`, `conn.executescript`, `conn.commit`, `conn.close`, `logger.info`, `logger.warning`.

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

### [src/utils.py](src/utils.py)

Responsibility: Small helpers used across modules (URL building, CSV helpers, simple CSV queries).

- `def generateURL(docID, base_url, api_key, doctype=None) -> str`
	- Purpose: Construct EDINET download URL from explicit parameters.

- `def json_list_to_csv(json_list, csv_filename) -> None`
	- Purpose: Write list-of-dicts to CSV.

- `def get_latest_submit_datetime(csv_filename) -> Optional[str]`
	- Purpose: Parse CSV and return latest `submitDateTime` as string.

---

### [src/logger.py](src/logger.py)

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
	- Purpose: Launch the **default** Tkinter terminal-style GUI.
	- Calls/Dependencies: `ui_tk.run_tk_app`.

- Dispatch: `--cli` → `_run_cli()`, default → `_run_gui()`.

---

## How you can help expand this document

- Add exact function signatures (including types/defaults) when you change a function.
- Fill `Inputs`/`Output` sections with precise types and examples for frequently-changed helpers.
- Add `Calls/Dependencies` entries when introducing new inter-module calls.

If you'd like, I can now:
- populate exact function signatures for each module by scanning the `src/` and `ui_tk/` files and inserting them here, or
- convert private helpers into abbreviated summaries and keep public API entries fully expanded.

---

## Tk UI modules (`ui_tk`) — default GUI

### [ui_tk/app.py](../ui_tk/app.py)

Responsibility: Tk root bootstrap, view switching, event loop, and log handler wiring.

`class App`
	- Purpose: Top-level application controller; owns the Tk root, tab bar, log panel, and view switching.

	- `def __init__(self, root: tk.Tk) -> None`
		- Purpose: Initialise root window, apply theme, build layout (tab bar, views, log panel), wire log handler.

	- `def switch_view(self, name: str) -> None`
		- Purpose: Switch between Home / Orchestrator / Data / Screening views. Views are lazily created on first access.

- `def run_tk_app() -> None`
	- Purpose: Public entry point — creates `Tk` root, instantiates `App`, and starts `root.mainloop()`.
	- Calls/Dependencies: `apply_theme`, `poll_events`, `QueueLogHandler`, `HomePage`, `OrchestratorPage`, `DataPage`, `ScreeningPage`.

---

### [ui_tk/style.py](../ui_tk/style.py)

Responsibility: Terminal-style dark theme tokens and ttk.Style configuration.

- Constants: `COLORS` (dict — bg, surface, border, text, text_dim, accent, success, warning, error, highlight, input_bg), `MONO_FAMILY`, `FONT_MONO`, `FONT_MONO_BOLD`, `FONT_HEADING`, `FONT_SMALL`, `PAD`.

- `def apply_theme(root: tk.Tk) -> ttk.Style`
	- Purpose: Apply the terminal dark theme to the root window and all ttk widget styles (using "clam" base).
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

- Constants: `STEP_CONFIG_KEY`, `STEP_DISPLAY`, `ALL_STEP_NAMES`, `STEPS_WITH_OVERWRITE`, path constants (`BASE_DIR`, `ENV_PATH`, `STATE_DIR`, `RUN_CONFIG_PATH`, `SAVED_SETUPS_DIR`, `APP_STATE_PATH`).

#### Step Field Registry

The step field registry (`STEP_FIELD_DEFINITIONS`) is the single source of truth for which configuration fields each pipeline step requires. Both the UI config panel and `DEFAULT_STEP_CONFIGS` are derived from it.

- `@dataclass StepField` — Metadata for a single step-config field.
	- `key: str` — config dict key (e.g. `"Source_Database"`).
	- `field_type: str` — widget type. One of: `"str"`, `"num"`, `"text"`, `"json"`, `"database"`, `"file"`, `"portfolio"`.
	- `default: object` — default value (empty string if omitted).
	- `label: str | None` — display label; defaults to `key` when `None`.
	- `filetypes: list[tuple[str, str]] | None` — file-dialog filters (for `"file"` type).
	- `height: int` — text-area rows (for `"text"` / `"json"` types, default 3).

- `STEP_FIELD_DEFINITIONS: dict[str, list[StepField]]` — Maps each step name to an ordered list of `StepField` entries. The UI reads this to render only the relevant inputs per step.

- `DEFAULT_STEP_CONFIGS: dict[str, dict]` — Derived automatically from `STEP_FIELD_DEFINITIONS` via `_build_defaults_from_fields()`. Do **not** edit this dict directly — add/change fields in `STEP_FIELD_DEFINITIONS` instead.

#### Functions

- `def run_pipeline(steps, config_dict, on_step_start=None, on_step_done=None, on_step_error=None, cancel_event=None) -> None`
	- Purpose: Build `Config.from_dict` and delegate to `orchestrator.run_pipeline`.
	- Calls/Dependencies: `Config.from_dict`, `orchestrator.run_pipeline`.

- `def list_setups() -> list[str]` — Return sorted list of saved setup names.
- `def load_setup(name: str) -> dict` — Load named setup JSON from `saved_setups/`.
- `def save_setup(name: str, setup_data: dict) -> Path` — Save named setup JSON.
- `def save_run_config(cfg: dict)` / `def load_run_config() -> dict` — Read/write `run_config.json`.
- `def get_api_key() -> str` / `def save_api_key(key: str)` — Read/write API key in `.env`.
- `def load_app_state() -> dict` / `def save_app_state(state: dict)` — Persist UI state.
- `def build_config_dict(steps, step_configs) -> dict` — Serialize UI state into a run-config dict.
- `def build_steps_from_config(run_cfg) -> list` — Convert `run_steps` into `[[name, enabled, overwrite], ...]`.
- `def build_step_configs_from_config(run_cfg) -> dict` — Build per-step configs with defaults filled in.
- `def get_default_config_for_step(step_name) -> dict` — Return a deep copy of the default config for a step.

#### Screening Adapters

- `def screening_get_metrics(db_path: str) -> dict[str, list[str]]` — Return available screening metrics.
- `def screening_get_periods(db_path: str) -> list[str]` — Return available period years.
- `def screening_run(db_path, criteria, columns, period, sort_by, sort_order) -> pd.DataFrame` — Run a screening query.
- `def screening_export(df, output_path) -> str` — Export results to CSV.
- `def screening_save(name, criteria, columns, period) -> Path` — Save screening criteria.
- `def screening_load(name) -> dict` — Load saved screening criteria.
- `def screening_list() -> list[str]` — List saved screening names.
- `def screening_delete(name) -> None` — Delete a saved screening.
- `def screening_save_history(entry) -> None` — Append a screening history entry.
- `def screening_load_history() -> list[dict]` — Load screening run history.

---

### [ui_tk/shared/widgets.py](../ui_tk/shared/widgets.py)

Responsibility: Reusable terminal-styled composite widgets.

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

Responsibility: Landing page — lists saved setups with modification dates, New/Open actions.

- `class HomePage(ttk.Frame)` — Listbox of saved setups; double-click or [Open Selected] loads a setup and switches to Orchestrator view.

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

Responsibility: Data exploration page — placeholder ("coming soon").

- `class DataPage(ttk.Frame)` — Stub frame.

---

### [ui_tk/pages/screening.py](../ui_tk/pages/screening.py)

Responsibility: Screening view — filter companies by financial criteria with sortable results.

- `class ScreeningPage(ttk.Frame)`
	- Layout: horizontal PanedWindow with left panel (criteria builder) and right panel (results Treeview).
	- Left panel: DatabasePickerEntry, period selector, dynamic criteria rows (metric/operator/value), column checkboxes, Run button.
	- Right panel: sortable Treeview with alternating row colours, status bar.
	- Toolbar: Load, Save, History, Export buttons.
	- `def _on_db_changed(self)` — Refresh metrics/periods when database changes.
	- `def _add_criterion(self)` / `def _remove_criterion(self, row_data)` — Manage criteria rows.
	- `def _run_screening(self)` — Collect inputs, run in background thread, populate results.
	- `def _populate_results(self, df)` — Clear and fill Treeview with formatted values.
	- `def _sort_by_column(self, col)` — Client-side sort with ascending/descending toggle.
	- `def _save_screening(self)` / `def _load_screening(self)` — Save/load criteria dialogs.
	- `def _show_history(self)` — History dialog with re-run support.
	- `def _export_results(self)` — CSV export via file dialog.
	- `def reapply_colors(self)` — Theme toggle support.

---

### [src/screening.py](../src/screening.py)

Responsibility: Backend screening module — query building, execution, persistence, and formatting. Contains no UI logic.

- Constants: `SCREENING_TABLES`, `OPERATOR_MAP`, `DEFAULT_COLUMNS`, `FORMAT_RULES`.

- `def get_available_metrics(db_path: str) -> dict[str, list[str]]` — Introspect DB for screening table columns.
- `def get_available_periods(db_path: str) -> list[str]` — Return distinct periodEnd years.
- `def build_screening_query(criteria, columns, period=None, available_metrics=None) -> tuple[str, list]` — Build parameterised SQL with validation.
- `def run_screening(db_path, criteria, columns, period=None, sort_by=None, sort_order="ASC") -> pd.DataFrame` — Execute screening and return results.
- `def export_screening_to_csv(df, output_path) -> str` — Export DataFrame to CSV.
- `def format_financial_value(value, column_name) -> str` — Format values for display (percent/currency/ratio).
- `def save_screening_criteria(name, criteria, columns, period, save_dir) -> Path` — Persist criteria as JSON.
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
- `[tests/test_edinet_api.py](tests/test_edinet_api.py)` — tests `Edinet` wrapper methods including download, unzip, CSV ingestion and DB interactions.
- `[tests/test_regression_analysis.py](tests/test_regression_analysis.py)` — tests OLS runner, scoring query builder, and results writer.
- `[tests/test_stockprice_api.py](tests/test_stockprice_api.py)` — tests CSV import and stock price ingestion logic.
- `[tests/test_ui_tk_smoke.py](tests/test_ui_tk_smoke.py)` — Tk UI smoke tests: imports, theme application, widget instantiation, controller functions, QueueLogHandler, ScreeningPage.
- `[tests/test_screening.py](tests/test_screening.py)` — Backend screening tests: query building, execution, persistence, formatting, SQL injection prevention.
- `[tests/test_orchestrator.py](tests/test_orchestrator.py)` — Orchestrator tests: `run_pipeline` basic flow, cancellation, error handling, `execute_step` dispatch, `validate_config`, `Config.from_dict` independence and singleton behaviour.
- `[tests/test_utils.py](tests/test_utils.py)` — small helper tests for URL generation and CSV export.

---

Last updated: 2026-04-06

If you want, I can now auto-populate parameter types and short example inputs/outputs for every function (more verbose), or keep the current concise API listings. Which do you prefer?

