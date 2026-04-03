# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Removed
- **`standardize_data` step** — legacy data normalisation step removed; the pipeline now reads directly from the raw `financialData_full` table.
- **`generate_financial_ratios` step** — replaced by the `generate_ratios` and `generate_historical_ratios` steps.
- **`find_significant_predictors` step** — univariate OLS sweep removed; use `Multivariate_Regression` with a custom SQL query instead.
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
- **Flet GUI application** — full Material-Design desktop UI with drag-and-drop step reordering, per-step configuration dialogs, light/dark theme toggle, live log output panel, and database selector
- **CLI / GUI dual mode** — `python main.py` launches the GUI; `python main.py --cli` runs headless
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
- New `ui/` module containing the Flet application (`ui/app.py`)
- New `src/backtesting.py` module for portfolio backtesting logic
- `main.py` refactored into `_run_cli()` and `_run_gui()` entry points
- `flet` and `matplotlib` added to dependencies

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
