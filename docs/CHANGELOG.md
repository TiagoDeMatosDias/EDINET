# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Added
- **Screening view** — new top-level view for filtering companies by financial criteria:
  - Backend package (`src/screening/`): package facade plus screening implementation for parameterised SQL query building, screening execution, CSV export, financial value formatting, and persistence for saved criteria and screening history.
  - UI page (`ui_tk/pages/screening.py`): database picker, period selector, dynamic criteria builder, column selector, sortable results Treeview with alternating row colours, and toolbar with Save/Load/History/Export actions.
  - Controller adapters in `ui_tk/controllers.py` for all screening operations.
  - Registered as fourth view in `ui_tk/app.py` with `Ctrl+4` keyboard shortcut.
  - Tests: `tests/test_screening.py` with 18 test cases covering query building, execution, persistence, formatting, and SQL injection prevention.
  - Smoke tests and screenshot capture updated to include the Screening view.
- **`generate_rolling_metrics` step** — new orchestrator step package (`src/orchestrator/generate_rolling_metrics/`) that replaces `generate_historical_ratios`:
  - Processes tables and columns declared in `rolling_metrics.json`; output tables are named `<SourceTable>_Rolling`.
  - Produces `_Average_3/5/10_Year` and `_Growth_3/5/10_Year` columns per metric using CAGR-style growth.
  - Supports `overwrite`; config keys are `Source_Database` and `Target_Database`.

### Changed
- **Orchestration layer rework** — complete rewrite of the orchestration entrypoint, now exposed through `src/orchestrator/`:
  - **Step handler pattern**: each orchestration step is handled by a dedicated function (`_step_get_documents`, `_step_download_documents`, etc.) registered in a `STEP_HANDLERS` dict, replacing the monolithic `if/elif` chain.
  - **No shared state**: `run()` and `run_pipeline()` no longer pre-create shared `Edinet` or `data` instances. Each step handler creates its own module instances with explicit parameters.
  - **`execute_step` simplified**: signature changed from `(step_name, config, edinet=None, data=None, overwrite=False)` to `(step_name, config, overwrite=False)`.
  - **`run()` simplified**: signature changed from `run(edinet=None, data=None)` to `run()`.
  - **`validate_config()` extracted**: pre-flight validation is now a standalone public function.
- **`Edinet` class decoupled from Config** — constructor now takes explicit parameters `(base_url, api_key, db_path, raw_docs_path, doc_list_table, company_info_table, taxonomy_table)` instead of reading the Config singleton.
- **`data` class decoupled from Config** — `__init__` no longer reads Config; all parameters are passed explicitly to each method by the caller.
- **`generateURL()` decoupled from Config** — signature changed from `(docID, config, doctype)` to `(docID, base_url, api_key, doctype)`.

### Removed
- **Config singleton dependency** removed from `src/edinet_api.py` and `src/data_processing.py`. Only the orchestrator reads Config.
- **`standardize_data` step** — legacy data normalisation step removed; the pipeline now reads directly from the raw `financialData_full` table.
- **`generate_financial_ratios` step** — replaced by the `generate_ratios` and `generate_rolling_metrics` steps.
- **`generate_historical_ratios` step** — replaced by `generate_rolling_metrics`.
- **`find_significant_predictors` step** — univariate OLS sweep removed.
- **`Multivariate_Regression` step** — removed from the pipeline step catalog; OLS results remain available as standalone output under `data/ols_results/`.
- **`populate_business_descriptions_en` step** — translation step removed along with its package (`src/orchestrator/populate_business_descriptions_en/`), provider config reference, and associated tests.
- **Legacy Flet UI (`ui/`)** — retired in favor of the Tk desktop UI (`ui_tk/`).
- **`--flet` startup flag** — removed from `main.py`; GUI mode now always starts Tk.
- **`tests/test_ui.py`** — removed with the legacy Flet UI modules.
- **`flet` runtime dependency** — removed from `requirements.txt`.
- **`FINANCIAL_RATIOS_CONFIG_PATH` env key** — no longer required.
- **`DB_STANDARDIZED_TABLE`, `DB_STANDARDIZED_RATIOS_TABLE`, `DB_SIGNIFICANT_PREDICTORS_TABLE` env keys** — no longer required.
- **Financial Ratios Config selector** — GUI button removed along with the associated `.env` key.

---

## [0.2.0] - 2026-03-07

### Added
- **GUI application** — `python main.py` launches the Tk desktop GUI with five workspace views, keyboard navigation, per-step configuration, setup save/load, theme toggle, and live log output
- **Backtest step** — portfolio backtesting with configurable tickers, weights, date range, dividend-adjusted returns, and optional benchmark comparison; dedicated GUI dialog with portfolio weight validation
- **Import Stock Prices (CSV) step** — load historical prices from a user-supplied CSV with configurable column mapping (Date, Price), ticker, and currency; dedicated GUI dialog with file picker
- **Per-step overwrite toggle** — `generate_financial_statements`, `generate_ratios`, and `generate_historical_ratios` support an `overwrite` flag to drop and rebuild their output table
- **Saved setups** — save and load named pipeline configurations from `config/state/saved_setups/` via the GUI
- **Pre-flight validation** — orchestrator checks that all required `.env` / config keys are present for every enabled step before execution begins
- **Progress logging** — `generate_ratios` and `generate_historical_ratios` log progress during execution
- **Database management** — GUI top-bar allows creating, opening, and switching between SQLite databases; recent databases are remembered across sessions
- **API Key dialog** — set the EDINET API key from the GUI without manually editing `.env`

### Changed
- **Config directory restructured** — configuration files moved into `config/reference/`, `config/state/`, and `config/examples/` sub-directories
- **Run config format** — steps changed from plain booleans to `{"enabled": bool, "overwrite": bool}` objects
- **Run config path** — now at `config/state/run_config.json` (was `config/run_config.json`)
- **Step order** — steps execute in the order they appear in the `run_steps` object (GUI allows drag-and-drop reordering)
- `update_stock_prices` documentation corrected to reference Stooq API (was incorrectly documented as Yahoo Finance)

### Architecture
- New `ui_tk/` module containing the Tkinter GUI application
- New `src/backtesting.py` module for portfolio backtesting logic
- `flet` removed; migrated from Flet UI to native Tkinter
- `matplotlib` added to dependencies

## [0.1.0-alpha] - 2026-02-25

### Added
- Comprehensive logging system with automatic archiving to `logs/` directory
- Timestamped log files for every run with both console and file output
- Stock price fetching from Stooq API with intelligent filtering
- Orchestrator refactored to execute steps in the order defined in configuration
- Table creation for stock prices database with pandas
- Suppression of verbose third-party library debug logs (chardet)
- Improved error handling with full exception tracebacks in logs

### Fixed
- Stock prices table now properly created before inserting data
- Chardet encoding detection debug logs no longer clutter output
- Step execution order now respects configuration file order
- Stock price API calls now only made for companies with financial data

### Changed
- Replaced print statements with proper logging throughout orchestrator and APIs
- Stock price scraping now filters to only companies in the financial data table
- Improved log messages with more context and better formatting

### Known Issues
- None reported

### Architecture
- Application follows MVC pattern with orchestrator managing step execution
- Configuration-driven design allows flexible step ordering and feature toggling
- Comprehensive test suite for core modules
