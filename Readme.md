# EDINET Financial Data Tool

Downloads financial filings from the Japanese securities regulator (EDINET), processes them into a structured SQLite database, and runs statistical analysis to identify relationships between financial ratios and stock valuations.

The primary interface is a web workstation (FastAPI backend + React/TypeScript SPA) accessible at `http://127.0.0.1:8000`. It exposes six top-level views: Overview, Screening, Analysis, Backtesting, Portfolio, and Pipeline.

Each pipeline step is configured independently, including its source or target database path where applicable.

## Current Status

- **Primary UI** — React 19 / TypeScript / Vite SPA in `frontend-v2/`, served by FastAPI.
- **Portfolio module** — full-featured portfolio management with IBKR FlexQuery import, holdings tracking, performance analytics, interactive charts, dividend analysis, and backtest comparison.
- **Research surfaces** — Screening and Analysis are fully functional views for candidate discovery and single-company research.
- **Architecture** — FastAPI backend with React SPA frontend communicating via TanStack Query and REST API endpoints.

## Quick Start

### Prerequisites

- **Python 3.10+** with pip
- **Node.js 18+** with npm (for building the frontend)

### Setup

1. Clone the repository or download the latest release from [Releases](https://github.com/TiagoDeMatosDias/EDINET/releases)
2. Install Python dependencies:
   ```
   pip install -r requirements.txt
   ```
3. Build the React frontend:
   ```
   cd frontend-v2
   npm ci
   npm run build
   cd ..
   ```
4. Create a `.env` file in the project root with your EDINET API key:
   ```
   API_KEY=<your_edinet_api_key>
   ```
5. Run the application:
   ```
   python main.py
   ```
6. Open `http://127.0.0.1:8000` in your browser

> **Note:** The application creates default configuration and databases on first run. Pipeline steps are configured through the Pipeline view in the web UI.

Optional flags:
```
python main.py --host 0.0.0.0 --port 8080 --no-reload
```

For frontend development, keep FastAPI running on port 8000 and start the Vite dev server in another terminal:
```
cd frontend-v2
npm run dev
```

## What it does

### Views

| View | Route | Description |
|---|---|---|
| **Overview** | `/` | Job history, metrics summary, quick-launch cards |
| **Pipeline** | `/pipeline` | Pipeline builder: step library, drag-to-order pipeline, per-step config inspector, run controls |
| **Screening** | `/screen` | Filter companies by financial criteria with expression-based rules, weighted ranking, save/load, CSV export, backtest-set generation |
| **Analysis** | `/analyze` | Single-company deep dive: search, overview metrics, financial history, interactive charts, peer comparison, price refresh |
| **Backtesting** | `/backtest` | Portfolio backtesting with manual entry, screener import, or CSV upload. Charts, per-company decomposition, benchmark comparison, batch set heatmap |
| **Portfolio** | `/portfolio` | IBKR FlexQuery XML import, multi-currency holdings, transactions, interactive charts, performance metrics, model portfolio comparison |

### Pipeline Steps

1. **Fetch document list** — queries the EDINET API for available filings in a given date range.
2. **Download documents** — downloads the XBRL/CSV filings that match the filter criteria.
3. **Populate company info** — loads the EDINET company code list from CSV into the database.
4. **Import stock prices (CSV)** — imports historical prices from a user-supplied CSV file with configurable column mapping.
5. **Update stock prices** — fetches historical share prices via the Stooq API by default, with a Yahoo Finance chart fallback if Stooq is unavailable.
6. **Parse taxonomy** — parses an EDINET XBRL taxonomy XSD file and stores element metadata in the database.
7. **Generate financial statements** — extracts tagged values from raw XBRL data into structured per-company financial tables.
8. **Generate ratios** — calculates per-share values, valuation ratios, and derived metrics for every company.
9. **Generate rolling metrics** — computes rolling averages and CAGR-style growth rates for configurable metrics across selected statement tables, producing `<Table>_Rolling` output tables.
10. **Backtest** — portfolio backtesting with weighted returns, dividend adjustment, and optional benchmark comparison.
11. **Backtest set** — batch-runs 1/2/3/5/10-year backtests from a CSV of yearly portfolio selections.

## Screenshots

Current captures from the web workstation at 1280×800:

| View | Screenshot |
|---|---|
| **Overview** | <img src="docs/images/web-dashboard.png" alt="EDINET Web Overview" width="640"> |
| **Pipeline** | <img src="docs/images/web-pipeline.png" alt="EDINET Web Pipeline" width="640"> |
| **Screening** | <img src="docs/images/web-screening.png" alt="EDINET Web Screening" width="640"> |
| **Analysis** | <img src="docs/images/web-security-analysis.png" alt="EDINET Web Analysis" width="640"> |
| **Backtesting** | <img src="docs/images/web-backtesting.png" alt="EDINET Web Backtesting" width="640"> |
| **Portfolio** | <img src="docs/images/web-portfolio.png" alt="EDINET Web Portfolio" width="640"> |

> Screenshots captured via Playwright. Regenerate with `python tests/capture_screenshots.py`.

## Documentation

- [BUILDING.md](docs/BUILDING.md) — How to build a distributable EXE and ZIP
- [RUNNING.md](docs/RUNNING.md) — Full description of every step and configuration options
- [LOGGING.md](docs/LOGGING.md) — Logging system documentation
- [Frontend Architecture.md](docs/Frontend%20Architecture.md) — Web frontend structure and extension guide
- [Application Details.md](docs/Application%20Details.md) — Python source file reference
- [Contributing.md](docs/Contributing.md) — Contribution guidelines
- [CHANGELOG.md](docs/CHANGELOG.md) — Version history and changes

## Building an executable

Run the build script from the project root:

```bash
python scripts/build.py
```

This produces `dist/EDINET-Release.zip` containing the `.exe`, empty databases, configuration files, and an `.env` template. See [docs/BUILDING.md](docs/BUILDING.md) for the full step-by-step guide.

## Configuration files

| File | Purpose |
|---|---|
| `src/orchestrator/generate_ratios/ratios_definitions.json` | Ratio-table definitions used by `generate_ratios` |
| `src/orchestrator/generate_rolling_metrics/rolling_metrics.json` | Rolling-metric column specifications |
| `config/database_paths.json` | Lists the databases to be used |
| `.env` | EDINET API key |

## Web Interface Features

- **React SPA** — React 19 with TypeScript, client-side routing via React Router, lazy-loaded feature modules.
- **Server state** — TanStack Query manages all API data with caching, background refetch, and loading/error states.
- **Pipeline builder** — drag-and-drop step ordering, per-step configuration inspector, run/cancel controls with real-time job status, save/load setups.
- **Screening** — expression-based criteria builder with metric picker, weighted ranking rules, sortable results, formatted/raw value toggle, save/load criteria, run history, CSV export, backtest-set generation, drill-in to Analysis.
- **Analysis** — company search (by ticker, name, EDINET code, industry), overview metric tiles, unified historical data table with column filter, interactive Chart.js charts (line/bar/area, multi-series, add/remove panels), peer comparison, stock price refresh.
- **Backtesting** — three input modes (manual portfolio, import from screener, CSV upload), allocation types (weight/shares/value), Chart.js visualizations (cumulative returns, drawdown, yearly breakdowns), per-company return decomposition, benchmark comparison, batch set analysis with heatmap.
- **Portfolio** — IBKR FlexQuery XML upload with drag-and-drop, holdings table with sortable/filterable columns, column visibility and pinning, multi-currency support with display currency selector, FX effect column, holding period tracking, transaction log with filters, interactive Chart.js dashboard (pie charts, stacked area, stacked bars, heatmaps), performance metrics (total return, CAGR, Sharpe, Sortino, max drawdown, volatility, alpha, beta, benchmark comparison), per-company yearly returns, money-weighted returns (Modified Dietz), model portfolio comparison via backtest.
- **Database management** — resolve, optimize, and select database paths from the UI.
- **Session persistence** — screening criteria, backtesting state, and analysis context survive tab switches via localStorage/sessionStorage.

## Key EDINET document type codes

| Code | Document type |
|---|---|
| 120 | Securities Report (Annual Report - 有価証券報告書) |
| 140 | Quarterly Securities Report (四半期報告書) |
| 150 | Semi-Annual Securities Report (半期報告書) |
