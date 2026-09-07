# Shade Research

Shade Research is a local-first company research workstation: one FastAPI + React application that combines EDINET source filings and XBRL-standardized financials with company search, screening, comparison, backtesting, portfolio analysis, and private research state.

It always serves HTTPS, from `https://127.0.0.1:8000/` on the public homepage and from `/overview` once you are signed in. A self-signed certificate is generated into `data/certs/` on first start. The `/pricing` page advertises one informational plan at €10/month or €100/year; payment processing and subscription enforcement are not implemented.

## Capabilities

- **One company finder everywhere** — Analysis, Comparison, Filings, Research, and the global header search the same index of names, tickers, EDINET codes, industries, markets, and price tickers, degrading gracefully when a configured database is incomplete.
- **Company analysis** — prices, the full financial snapshot, filing and statement history, ratios, charts, tags, and a backtest handoff on a single page.
- **Company comparison** — 2–12 companies, standard metrics, any numeric `Table.Column` metric, common-size statements, and optional peer percentiles.
- **Filing Explorer** — retains compressed type-1 ZIPs, indexes compact numeric XBRL facts, reconstructs narrative content on demand, and keeps Japanese and complete English translations side by side.
- **Research and tagging** — one private tag system shared by favorites, watchlists, Analysis, and Screening, plus notes, thesis state, targets, review dates, and in-app alerts.
- **Testing ideas** — expression screening, point-in-time rolling backtests, manual and CSV backtests, IBKR FlexQuery portfolio imports, and reproducible report ZIPs.
- **Data pipeline** — 13 dynamically discovered steps, durable job state, cancellation, progress reporting, safe file uploads, XBRL `explicit`/`backfill`/`all` modes, and financial statements generated from CSV or compact filing facts.
- **Optional accounts** — registration, login, rotating sessions, personal API tokens, administrator controls, and an administrator-set 15–128-character password minimum.
- **Zero-setup databases** — missing configured databases and their managed schemas are created automatically at startup.

## Screenshots

All captures run against generated demonstration companies, filings, and portfolio activity — never operator databases. Regenerate every image with:

```powershell
.\.venv3\Scripts\python.exe tests\capture_screenshots.py
```

| View | Screenshot |
|---|---|
| Public homepage | <img src="docs/images/web-home.png" alt="Shade Research public homepage" width="640"> |
| Workspace overview (`/overview`) | <img src="docs/images/web-dashboard.png" alt="Workspace overview with data health and workflow shortcuts" width="640"> |
| Company screening (`/screen`) | <img src="docs/images/web-screening.png" alt="Expression-based company screening" width="640"> |
| Company analysis (`/analyze/:companyCode`) | <img src="docs/images/web-security-analysis.png" alt="Populated company analysis" width="640"> |
| Financial comparison (`/compare`) | <img src="docs/images/web-comparison.png" alt="Side-by-side company comparison" width="640"> |
| Filing search (`/filings`) | <img src="docs/images/web-filings.png" alt="Filing Explorer search and coverage statistics" width="640"> |
| Filing translation (`/filings/:docId`) | <img src="docs/images/web-filing-translation.png" alt="Japanese and English filing sections side by side" width="640"> |
| Tags and research (`/research`) | <img src="docs/images/web-research.png" alt="Tags, favorites, watchlists, notes, and alerts" width="640"> |
| Backtesting (`/backtest`) | <img src="docs/images/web-backtesting.png" alt="Backtesting workspace" width="640"> |
| Portfolio (`/portfolio`) | <img src="docs/images/web-portfolio.png" alt="Portfolio performance and exposure dashboard" width="640"> |
| Data pipeline (`/pipeline`) | <img src="docs/images/web-pipeline.png" alt="Data pipeline workspace" width="640"> |

See the [User Guide](docs/USER_GUIDE.md) for the complete gallery and a feature-by-feature walkthrough.

## Quick start

### Prerequisites

- Python 3.12 or 3.13
- Node.js 22 and npm 10
- Windows for packaged releases; Windows or Linux for source development

### Install and run

```powershell
py -3.13 -m venv .venv3
.\.venv3\Scripts\python.exe -m pip install -e ".[dev]"

Set-Location frontend-v2
npm ci
npm run build
Set-Location ..

.\.venv3\Scripts\python.exe main.py --no-reload
```

Open `https://127.0.0.1:8000` and accept the self-signed certificate once, or add `data/certs/cert.pem` to the operating system's trusted root store. To serve a real certificate instead, place a key pair in `data/certs/` — supported pairs are `cert.pem`+`key.pem`, `fullchain.pem`+`privkey.pem`, `tls.crt`+`tls.key`, and `server.crt`+`server.key`.

Account mode with open registration is the default; the first registered account becomes the administrator. Set `EDINET_AUTH_MODE=disabled` only for unrestricted loopback use.

`EDINET_API_TOKEN` feeds the type-1 XBRL downloader; the legacy Get Documents and Download EDINET Documents steps read `API_KEY` from pipeline configuration. When both workflows share one EDINET credential, set both values. They are outbound provider credentials only and are never accepted as application login tokens.

```dotenv
EDINET_API_TOKEN=<your-edinet-api-token>
API_KEY=<your-edinet-api-token>
```

On first start the server creates any missing configured databases (Base, Standardized, Portfolio, auth, research, pipeline-job, and filing). Populate market and filing data from the Pipeline workspace.

For frontend development, keep FastAPI serving HTTPS on port 8000 and run `npm run dev` from `frontend-v2/`; the Vite proxy targets `https://127.0.0.1:8000` without verifying the self-signed certificate. See [Running the Application](docs/RUNNING.md) for account mode, remote binding, storage, pipeline configuration, and recovery commands.

## Routes

| Route | Purpose |
|---|---|
| `/` | Public product homepage with registration, login, and pricing links |
| `/pricing` | Informational €10/month or €100/year plan |
| `/login`, `/register` | Account access and registration |
| `/overview` | Data health, recent jobs, and workflow shortcuts |
| `/screen` | Expression-based company screening and rolling-backtest handoff |
| `/analyze`, `/analyze/:companyCode` | Company search, snapshot, history, charts, tags, and filings |
| `/compare` | Multi-company financial and arbitrary-metric comparison |
| `/filings`, `/filings/:docId` | Filing search, coverage statistics, source report, sections, facts, taxonomy, and quality |
| `/research` | Tags, favorites, watchlists, notes, thesis tracking, and alerts |
| `/backtest` | Manual, CSV-set, and point-in-time rolling-screen backtests |
| `/portfolio` | IBKR import, holdings, activity, performance, and risk views |
| `/pipeline` | Pipeline recipes, uploads, run controls, progress, and job history |
| `/account`, `/admin` | Account settings and administrator controls |

`/security` and `/backtesting` remain compatibility aliases for `/analyze` and `/backtest`.

## Pipeline steps

The step library is discovered from `src/orchestrator/` and currently contains:

1. Get Documents
2. Download EDINET Documents (CSV/type-5)
3. Download XBRL Filings (type-1)
4. Populate Company Info
5. Import Stock Prices (CSV)
6. Update Stock Prices
7. Update FX Data
8. Parse Taxonomy
9. Generate Financial Statements
10. Generate Ratios
11. Generate Rolling Metrics
12. Backtest
13. Backtest Set (CSV)

The stock-price CSV step uses a file picker and accepts files up to 500 MiB. XBRL `all` mode queries eligible filings from `DocumentList`, honors the document-type filter, skips completed downloads, uses at most five concurrent downloads, batches status writes, and reuses HTTP connections. Financial statements can be generated from either the legacy CSV database or the compact numeric facts in `Filings.db`.

## Configuration and data

| File or variable | Purpose |
|---|---|
| `config/database_paths.json` | Base, Standardized, Portfolio, auth, research, job, and filing database locations |
| `.env` / `EDINET_API_TOKEN`, pipeline `API_KEY` | Outbound EDINET provider credentials for XBRL and legacy document steps |
| `EDINET_AUTH_MODE` | `disabled` for loopback compatibility or `accounts` for account authentication |
| `src/orchestrator/generate_ratios/ratios_definitions.json` | Ratio definitions |
| `src/orchestrator/generate_rolling_metrics/rolling_metrics.json` | Rolling-average and growth definitions |

`Filings.db` stores compressed provider ZIPs, compact numeric facts, catalog metadata, and versioned translation-cache rows. Narrative HTML and text are reconstructed from the retained ZIP only when requested.

## Documentation

- [User Guide](docs/USER_GUIDE.md) — user-facing workflows and complete screenshot gallery
- [Running the Application](docs/RUNNING.md) — setup, security, database storage, and every pipeline step
- [Building the Windows Release](docs/BUILDING.md) — packaged executable and ZIP workflow
- [Frontend Architecture](docs/Frontend%20Architecture.md) — routes, state, components, and extension guide
- [Python Source File Reference](docs/Application%20Details.md) — backend module responsibilities and contracts
- [Contributing](docs/Contributing.md) — development and bounded verification workflow
- [Logging and Correlation](docs/LOGGING.md) — logs, safe error envelopes, and correlation IDs
- [Changelog](docs/CHANGELOG.md) — release history and unreleased changes

## Verification and packaging

Run all bounded backend, integration, frontend, static, contract, dependency, and documentation checks:

```powershell
.\.venv3\Scripts\python.exe -B scripts\verify.py
```

Build the Windows release with:

```powershell
.\.venv3\Scripts\python.exe -B scripts\build.py
```

The release builder generates fresh configuration and empty databases; it never bundles development databases, credentials, logs, uploads, or portfolio data.

## Common EDINET document type codes

| Code | Document type |
|---|---|
| `120` | Securities Report (Annual Report / 有価証券報告書) |
| `130` | Semi-annual Securities Report (半期報告書) |
| `140` | Quarterly Securities Report (四半期報告書) |
