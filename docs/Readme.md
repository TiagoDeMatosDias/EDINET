# EDINET Financial Data Tool

Downloads financial filings from the Japanese securities regulator (EDINET), processes them into a structured SQLite database, and runs statistical analysis to identify relationships between financial ratios and stock valuations.

Each pipeline step is configured independently, including its source or target database path where applicable.

## Quick Start (Alpha Release)

1. Download the latest release from [Releases](https://github.com/TiagoDeMatosDias/EDINET/releases)
2. Extract `EDINET-0.1.0-alpha.zip`
3. Copy `config/examples/run_config.example.json` to `config/state/run_config.json` and configure your settings
4. Create a `.env` file with your API keys (see Setup section below)
5. Run `EDINET.exe` (Windows) or the binary for your OS

## What it does

1. **Fetch document list** – queries the EDINET API for available filings in a given date range.
2. **Download documents** – downloads the XBRL/CSV filings that match the filter criteria.
3. **Populate company info** – loads the EDINET company code list from CSV into the database.
4. **Import stock prices (CSV)** – imports historical prices from a user-supplied CSV file with configurable column mapping.
5. **Update stock prices** – fetches historical share prices via Stooq API (filtered to companies with financial data).
6. **Parse taxonomy** – parses an EDINET XBRL taxonomy XSD file and stores element metadata in the database.
7. **Generate financial statements** – extracts tagged values from raw XBRL data into structured per-company financial tables.
8. **Generate ratios** – calculates per-share values, valuation ratios, and derived metrics for every company.
9. **Generate historical ratios** – computes rolling averages, growth rates, and z-scores over time.
10. **Multivariate regression** – user-defined multivariate OLS regression specified as a SQL query.
11. **Backtest** – portfolio backtesting with weighted returns, dividend adjustment, and optional benchmark comparison.
12. **Backtest set** – batch-runs 1/2/3/5/10-year backtests from a CSV of yearly portfolio selections.
13. **Screening** – filter companies by financial criteria (valuation, quality, per-share metrics) with sortable results, CSV export, and saved criteria management.
14. **Security analysis** – inspect a single company with typeahead search, overview cards, statement history, charts, price refresh, and peer comparison.

## Screenshots

Current screenshots from the live Tk application. The dark theme is shown below for consistency; matching light-theme captures are also refreshed in `docs/images/`.

### Home

![EDINET Home Dark](images/ui-home-dark.png)

### Orchestrator

![EDINET Orchestrator Dark](images/ui-orchestrator-dark.png)

### Data

![EDINET Data Dark](images/ui-data-dark.png)

### Screening

![EDINET Screening Dark](images/ui-screening-dark.png)

### Security Analysis

![EDINET Security Analysis Dark](images/ui-security-analysis-dark.png)

## GUI & CLI Modes

The application can run in two modes:

- **GUI mode** (default) — Launch the Tk desktop application with `python main.py`. The GUI provides keyboard-friendly step ordering, per-step configuration panels, setup save/load, and a live log output panel.
- **Security Analysis view** — Search by company name, ticker, EDINET code, or industry and inspect one company in detail.
- **CLI mode** — Run headless from the terminal with `python main.py --cli`. This reads `config/state/run_config.json` directly and executes the enabled steps in order.

## Setup

### From Source

#### 1. Install dependencies

```
pip install -r requirements.txt
```

#### 2. Create a `.env` file

Copy the template below into a `.env` file in the project root and fill in your values:

```
API_KEY=<your_edinet_api_key>
baseURL=https://api.edinet-fsa.go.jp/api/v2/documents
doctype=5

RAW_DOCUMENTS_PATH=<your_documents_path>
DB_PATH=<your_sqlite_db_path>

DB_DOC_LIST_TABLE=DocumentList
DB_FINANCIAL_DATA_TABLE=financialData_full
DB_COMPANY_INFO_TABLE=CompanyInfo
DB_STOCK_PRICES_TABLE=Stock_Prices
DB_TAXONOMY_TABLE=TAXONOMY_JPFS_COR
```

#### 3. Configure and run

Edit `config/state/run_config.json` to enable the steps you want, then:

```
python main.py          # launches the GUI
python main.py --cli    # headless / terminal mode
```

### From Release

1. Extract the release ZIP file
2. Edit the `.env` file with your configuration
3. Run the executable

All output is logged to timestamped files in the `logs/` directory. See [LOGGING.md](LOGGING.md) for details.

## Documentation

- [RUNNING.md](RUNNING.md) – Full description of every step and configuration options
- [LOGGING.md](LOGGING.md) – Logging system documentation
- [Contributing.md](Contributing.md) – Contribution guidelines
- [CHANGELOG.md](CHANGELOG.md) – Version history and changes

## Building an executable

The project can be packaged into a single `.exe` with [PyInstaller](https://pyinstaller.org).
The exe resolves all config paths relative to the folder it lives in, so `config/`, `.env`,
and the output `data/` folder just need to sit alongside it.

### 1. Install PyInstaller

```
pip install pyinstaller
```

### 2. Build

Run from the project root:

```
pyinstaller --onefile --name EDINET main.py
```

The exe is written to `dist/EDINET.exe`.

### 3. Prepare the distribution folder

Create a deployment folder and copy the required items into it:

```
EDINET.exe                                   <- built by PyInstaller (from dist/)
.env                                         <- your API keys and DB paths
config/
    reference/
        companyinfo.csv
        jppfs_cor_2013-08-31.xsd
    state/
        run_config.json
    examples/
        run_config.example.json
data/
    ols_results/                             <- must exist for regression output steps
    backtest_results/                        <- must exist for backtest output
```

### 4. Run

Double-click `EDINET.exe` or launch it from a terminal.
It will look for `config/` and `.env` in the same folder as the exe.

> **Note:** Large dependencies (pandas, statsmodels, scipy) make the final exe around 200-300 MB.
> Build time is a few minutes on the first run.

## Configuration files

| File | Purpose |
|---|---|
| `config/state/run_config.json` | Controls which steps run, their order, and step-specific parameters |
| `config/reference/companyinfo.csv` | EDINET company code list (used by populate_company_info) |
| `config/reference/financial_statements_mappings_config.json` | Mapping rules used by `generate_financial_statements` |
| `config/reference/generate_ratios_formulas_config.json` | Formula definitions used by `generate_ratios` |
| `config/reference/jppfs_cor_2013-08-31.xsd` | XBRL taxonomy file (used by parse_taxonomy) |
| `config/examples/run_config.example.json` | Example run configuration for new users |
| `config/state/saved_setups/` | Named setup files saved from the GUI |
| `.env` | API keys, file paths, and database table names |

## GUI Features

The Tk-based GUI provides:

- **API Key dialog** – securely set the EDINET API key without editing `.env` manually.
- **Step ordering controls** – reorder pipeline steps with keyboard shortcuts (`Alt+Up` / `Alt+Down`) and contextual actions.
- **Per-step enable/disable** – check or uncheck each step.
- **Per-step configuration panel** – configure each step (including database paths and advanced options) in the side panel.
- **Overwrite toggle** – steps that support it (Generate Financial Statements, Generate Ratios, Generate Historical Ratios) show an "Overwrite" checkbox.
- **Save / Load setups** – persist and recall named configurations from `config/state/saved_setups/`.
- **Live log output** – see real-time log messages during execution in the output panel.
- **Security analysis** – search companies, inspect statements and ratios, view charts, refresh prices, compare peers, and jump directly from screening results.

## Key EDINET document type codes

| Code | Document type |
|---|---|
| 120 | Securities Report (Annual Report - 有価証券報告書) |
| 140 | Quarterly Securities Report (四半期報告書) |
| 150 | Semi-Annual Securities Report (半期報告書) |
