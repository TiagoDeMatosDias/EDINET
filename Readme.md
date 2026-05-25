# EDINET Financial Data Tool

Downloads financial filings from the Japanese securities regulator (EDINET), processes them into a structured SQLite database, and runs statistical analysis to identify relationships between financial ratios and stock valuations.

The primary interface is a web workstation (FastAPI + vanilla JavaScript) accessible at `http://127.0.0.1:8000`. It exposes six top-level views: Dashboard, Orchestrator, Screening, Backtesting, Security Analysis, and Portfolio.

Each pipeline step is configured independently, including its source or target database path where applicable.

## Current Status

- **Primary UI** – the web workstation is the default and actively maintained interface.
- **Portfolio module** – full-featured portfolio management with IBKR FlexQuery import, holdings tracking, performance analytics, interactive charts, dividend analysis, and backtest comparison.
- **Research surfaces** – Screening and Security Analysis are fully functional views for candidate discovery and single-company research.
- **Architecture** – FastAPI backend with vanilla JS frontend modules communicating via `fetchJson` calls to REST API endpoints.


## Quick Start

1. Download the latest release from [Releases](https://github.com/TiagoDeMatosDias/EDINET/releases)
2. Extract the latest release archive for your platform
3. Copy `config/examples/run_config.example.json` to `config/state/run_config.json` and configure your settings
4. Create a `.env` file with your API keys (see Setup section below)
5. Run `EDINET.exe` (Windows) or the binary for your OS — then open `http://127.0.0.1:8000` in your browser

## What it does

1. **Orchestrator** - Runs a user definable set of steps:
   1. **Fetch document list** – queries the EDINET API for available filings in a given date range.
   2. **Download documents** – downloads the XBRL/CSV filings that match the filter criteria.
   3. **Populate company info** – loads the EDINET company code list from CSV into the database.
   4. **Import stock prices (CSV)** – imports historical prices from a user-supplied CSV file with configurable column mapping.
   5. **Update stock prices** – fetches historical share prices via the Stooq API by default, with a Yahoo Finance chart fallback if Stooq is unavailable.
   6. **Parse taxonomy** – parses an EDINET XBRL taxonomy XSD file and stores element metadata in the database.
   7. **Generate financial statements** – extracts tagged values from raw XBRL data into structured per-company financial tables.
   8. **Generate ratios** – calculates per-share values, valuation ratios, and derived metrics for every company.
   9. **Generate rolling metrics** – computes rolling averages and CAGR-style growth rates for configurable metrics across selected statement tables, producing `<Table>_Rolling` output tables.
   10. **Backtest** – portfolio backtesting with weighted returns, dividend adjustment, and optional benchmark comparison.
   11. **Backtest set** – batch-runs 1/2/3/5/10-year backtests from a CSV of yearly portfolio selections.
2.  **Screening** – filter companies by financial criteria (valuation, quality, per-share metrics), apply weighted ranking rules, review sortable results, toggle raw or formatted value display, save/load criteria, inspect screening history, export CSVs, or generate backtest-set CSV inputs.
3.  **Security analysis** – inspect a single company with typeahead search, overview metric tiles, statement history with configurable column filter, interactive Chart.js charts, price refresh, and peer comparison.
4.  **Backtesting** – interactive portfolio backtesting with three input modes: manual portfolio entry with ticker autocomplete and allocation types (weight/shares/value), import from screening results, or upload a CSV of yearly portfolio selections. Features Chart.js visualizations (cumulative returns, drawdown, yearly breakdowns), per-company return decomposition, benchmark comparison, and batch set analysis with heatmap.
5.  **Portfolio management** – import IBKR FlexQuery XML transaction files, track holdings with multi-currency support and display currency selector, view transaction logs with filtering, explore interactive charts (portfolio value over time, holdings breakdown by value/currency, dividend analysis, returns/deposits heatmaps, scatter plots), compute performance metrics (total return, CAGR, Sharpe ratio, max drawdown, benchmark comparison, money-weighted returns via Modified Dietz), inspect per-company return decomposition including capital gain and dividend yield splits, and compare against model portfolios.

    Portfolio module features:
    - **Upload** – drag-and-drop XML file import with parse results, activity breakdown, and automatic price fetching for missing tickers.
    - **Holdings** – full-width table with 18+ columns including symbol, name, type, industry, quantity, price, value in native and display currency, cost basis, P&L, dividend income, total and annualized return, FX effect, and weight. Column visibility toggle, pin/unpin columns (sticky), search filter, select all/none. Summary row with totals and averages. Holding period tracking (longest hold, latest hold, number of periods).
    - **Transactions** – paginated log of all activity (trades, dividends, corporate actions, cash flows) with date range, symbol, and activity type filters.
    - **Charts** – Chart.js dashboard with holdings by value pie, portfolio by currency pie, portfolio value over time (stacked area), dividends by company and currency (stacked bars), dividend/return/deposits heatmaps, and return vs cost basis scatter. All charts support display currency selection, table toggle, and expand-to-fullscreen.
    - **Performance Metrics** – total return, CAGR, Sharpe ratio, sortino ratio, max drawdown, volatility, alpha, beta, R², and information ratio. Benchmark comparison with ticker shortcuts (SPY, IVV, VWCE), auto-detected risk-free rate, and display currency selection.

## Screenshots

Current captures from the web workstation at 1280×800:

| View | Screenshot |
|---|---|
| **Dashboard** | <img src="docs/images/web-dashboard.png" alt="EDINET Web Dashboard" width="640"> |
| **Orchestrator** | <img src="docs/images/web-orchestrator.png" alt="EDINET Web Orchestrator" width="640"> |
| **Screening** | <img src="docs/images/web-screening.png" alt="EDINET Web Screening" width="640"> |
| **Backtesting** | <img src="docs/images/web-backtesting.png" alt="EDINET Web Backtesting" width="640"> |
| **Portfolio** | *(screenshot coming soon)* |
| **Security Analysis** | <img src="docs/images/web-security-analysis.png" alt="EDINET Web Security Analysis" width="640"> |

- **Dashboard** (`/`) — job history, metrics summary, quick-launch cards into other views.
- **Orchestrator** (`/orchestrator`) — pipeline builder: step library, drag-to-order pipeline, per-step config inspector, run controls.
- **Screening** (`/screening`) — criteria and ranking builder, sortable results, formatted/raw toggle, save/load/history/export, drill-in to Security Analysis. *(Shown above: results for Transportation Equipments companies.)*
- **Backtesting** (`/backtesting`) — interactive backtesting with manual portfolio, screener import, or CSV upload. Chart.js visualizations, per-company decomposition, benchmark comparison, and batch set heatmap.
- **Security Analysis** (`/security`) — company search, overview tiles, historical data with column filter, Chart.js charts, peer comparison, price refresh. *(Shown above: TOYOTA INDUSTRIES CORPORATION.)*
- **Portfolio** (`/portfolio`) — full portfolio management with IBKR FlexQuery XML import, multi-currency holdings tracking with column/pin visibility, transaction logs, interactive Chart.js dashboard (value over time, dividends, heatmaps), performance metrics (CAGR, Sharpe, drawdown, benchmark comparison), per-company return decomposition, and model portfolio comparison.

> Screenshots captured via Playwright. Regenerate with `python tests/capture_screenshots.py`.

## Running the Application

```bash
python main.py          # starts the web server on http://127.0.0.1:8000
```

Optional flags:

```bash
python main.py --host 0.0.0.0 --port 8080 --no-reload
```

Then open your browser to the URL shown in the console output.

## Setup

### From Source

#### 1. Install dependencies

```
pip install -r requirements.txt
```

#### 2. Create a `.env` file

Copy the template below into a `.env` file in the project root and fill in your API key:

```
API_KEY=<your_edinet_api_key>
```

All other configuration (database paths, document storage paths, table names, API endpoints) is hardcoded or configured via the web UI / `config/database_paths.json`.

#### 3. Configure and run

Edit `config/state/run_config.json` to enable the steps you want, then:

```
python main.py
```

### From Release

1. Extract the release ZIP file
2. Edit the `.env` file with your configuration
3. Run the executable — it starts the web server automatically
4. Open `http://127.0.0.1:8000` in your browser

All output is logged to timestamped files in the `logs/` directory. See [LOGGING.md](LOGGING.md) for details.

## Documentation

- [BUILDING.md](docs/BUILDING.md) – How to build a distributable EXE and ZIP
- [RUNNING.md](docs/RUNNING.md) – Full description of every step and configuration options
- [LOGGING.md](docs/LOGGING.md) – Logging system documentation
- [Frontend Architecture.md](docs/Frontend%20Architecture.md) – Web frontend structure and extension guide
- [Contributing.md](docs/Contributing.md) – Contribution guidelines
- [CHANGELOG.md](docs/CHANGELOG.md) – Version history and changes

## Building an executable

Run the build script from the project root:

```bash
python scripts/build.py
```

This produces `dist/EDINET-Release.zip` containing the `.exe`, empty databases,
configuration files, and an `.env` template. See [docs/BUILDING.md](docs/BUILDING.md)
for the full step-by-step guide.

## Configuration files

| File | Purpose |
|---|---|
| `src/orchestrator/generate_ratios/ratios_definitions.json` | Ratio-table definitions used by `generate_ratios` |
| `config/database_paths.json` | Lists the databases to be used |
| `.env` | EDINET API key |

## Web Interface Features

- **Multi-page navigation** – tab bar switches between Dashboard, Orchestrator, Screening, Backtesting, Security Analysis, and Portfolio views without page reload.
- **Pipeline builder** – drag-and-drop step ordering, per-step configuration inspector, run/cancel controls with real-time job status.
- **Screening** – dynamic criteria builder with metric picker, weighted ranking rules, sortable/sort-preserving results, formatted/raw value toggle, save/load criteria, run history, CSV export, backtest-set export, and drill-in to Security Analysis.
- **Backtesting** – three input modes (manual portfolio, import from screener, CSV upload), allocation types (weight/shares/value), Chart.js visualizations (cumulative returns, drawdown, yearly breakdowns), per-company return decomposition, benchmark comparison, batch set analysis with heatmap, and portfolio comparison across multiple runs.
- **Security Analysis** – typeahead company search (by ticker, name, EDINET code, industry), overview metric tiles, unified historical data table with column filter (multi-select grouped by source table), period count selector, Chart.js charts (line/bar/area, multi-series, add/remove panels), peer comparison, and stock price refresh.
- **Portfolio** – IBKR FlexQuery XML upload with drag-and-drop, holdings table with 18+ sortable/filterable columns, column visibility and pinning (sticky), multi-currency support with display currency selector (EUR/USD/JPY etc.), FX effect column (geometric return decomposition), holding period tracking, transaction log with date/symbol/activity filters, interactive Chart.js dashboard (pie charts, stacked area, stacked bars, heatmaps), performance metrics (total return, CAGR, Sharpe, Sortino, max drawdown, volatility, alpha, beta, benchmark comparison with auto risk-free rate), per-company yearly returns (capital gain + dividend yield), money-weighted returns (Modified Dietz), and model portfolio comparison via backtest.
- **Database management** – resolve, optimize, and select database paths from the UI.
- **Session persistence** – screening criteria, results, backtesting state, security analysis context, and portfolio state survive tab switches via sessionStorage.

## Key EDINET document type codes

| Code | Document type |
|---|---|
| 120 | Securities Report (Annual Report - 有価証券報告書) |
| 140 | Quarterly Securities Report (四半期報告書) |
| 150 | Semi-Annual Securities Report (半期報告書) |
