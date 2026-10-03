# Shade Research User Guide

Updated: 2026-09-25

Shade Research is a browser-based workspace for researching companies from source filings through standardized financial analysis, comparison, screening, backtesting, and portfolio review. EDINET and its XBRL disclosures are one supported source, not the limit of the product identity.

All screenshots in this guide were captured at 1280×720 from generated demonstration databases. Company names, filings, prices, and portfolio activity shown here are synthetic.

## Public pages and accounts

The public homepage at `/` explains the product and links directly to registration, login, and pricing. `/pricing` currently presents one informational plan: €10 per month or €100 per year when paid upfront. Billing and subscription enforcement are not implemented yet.

| Homepage | Pricing |
|---|---|
| <img src="images/web-home.png" alt="Shade Research public homepage" width="600"> | <img src="images/web-pricing.png" alt="Shade Research pricing page" width="600"> |

Account authentication is optional for loopback use. In account mode:

- the first successful registration becomes the administrator;
- subsequent registration follows the registration mode saved in Admin (open, closed, or invite-only), falling back to `EDINET_REGISTRATION_MODE` until one is saved;
- browser access tokens stay in memory and refresh tokens use an HttpOnly cookie;
- personal API tokens can be created and revoked from Account; a token with the `read` scope cannot change anything;
- changing or resetting a password signs the account out everywhere and revokes its API tokens;
- saved backtests are private to the account that ran them;
- refreshing a price from the provider is available to operators and administrators;
- an administrator can set the minimum password length from Admin → Security settings. The accepted range is 15–128 characters, and the policy applies to registration, invitations, resets, changes, and administrator-created credentials.

## Workspace overview

Open `/overview` after entering the workspace. It shows backend health, active and recent pipeline work, discovered pipeline-step count, portfolio activity, and shortcuts into the main research journeys.

<img src="images/web-dashboard.png" alt="Workspace overview" width="900">

The sidebar contains Overview, Screen, Analyze, Backtest, Portfolio, Data pipeline, Filings, Compare, and Research. Account and Admin appear when the authenticated role allows them. From the keyboard, press `G` then a letter to switch pages: `O` Overview, `S` Screen, `A` Analyze, `B` Backtest, `P` Portfolio, `D` Data pipeline (admins), `F` Filings, `C` Compare, `R` Research. Each sidebar link shows its letter, and the letters light up while `G` waits for one. `/` searches companies and `?` lists the shortcuts for the current page.

The header company finder searches across the best data currently available. It accepts company name, ticker, EDINET code, industry, and market text. The same finder is reused in Analysis, Comparison, Filings, and Research, so a ticker or company selected in one workflow resolves to the same canonical company code elsewhere. If one configured database is missing or only partly populated, search returns results from the remaining usable sources instead of failing the whole request.

## Find and analyze companies

### Screening

A screen is a list of rules, and the results fill the rest of the window below them. Each rule reads as a sentence: a metric, a comparison (`>`, `≥`, `<`, `≤`, `=`, `≠`, between, is one of, contains, is empty, has a value), and a value or a second metric. The metric picker searches every table at once and also offers formulas such as P/E, P/B, P/S, dividend yield, earnings yield, and market cap. Percentage metrics are typed as percentages (`15` means 15%).

Rules combine in two levels. The screen matches companies that pass **all** or **any** of its rules, and a **group** of rules inside it matches **any** or **all** of its own rules. Together these express both "A and (B or C)" and "(A and B) or (C and D)". Untick a rule to set it aside without deleting it. The `⋯` menu on a rule duplicates it, moves it into or out of a group, or adds arithmetic to the metric. A rule whose left side already has arithmetic or parentheses is edited as a full expression, with metrics, literals, `+ − × ÷`, tags, and validated parentheses on both sides. A rule that cannot run yet is flagged with the reason.

The `Split event` rule includes or excludes companies by split recency. By default it matches confirmed splits within the last 365 days, counted back from the as-of date (or today). Its Advanced panel exposes confirmation status and an exact cutoff date. `Stock_Splits` is linked through each filing's `Company_Code` to `CompanyInfo.Company_Ticker`, so split rules apply to the right company. Legacy raw `Stock_Splits` filters in older saved screens keep the same semantics: confirmed splits only, capped at the as-of date.

Results lead with the company name, which links to Analysis, with ticker and EDINET code beneath. Headers explain each column on hover and sort on click. `Columns` chooses output columns, adds whole column sets (overview, valuation, quality, growth, dividends, size), and defines derived columns. The default overview set covers industry, price, ROE, net margin, equity ratio, 3-year sales growth, net sales, market cap, P/E, P/B, and dividend yield.

`Screens` opens saved screens, which show their rule and column counts and when they changed, along with starter screens and a blank screen. Saving under the name of the open screen updates it. Saving under another existing name asks before replacing it. Screens can be exported to CSV, sent to Compare, or handed to a point-in-time rolling backtest.

Keyboard: `N` adds a rule and opens its metric search, `Shift+N` adds a group, `O` opens screens, `D` changes the as-of date, `C` opens columns, and `B` collapses the rules. `Ctrl+Enter` runs and `Ctrl+S` saves, even while typing. `↓` or `J` moves into the results from anywhere on the page. `J`/`K` or the arrow keys move from company to company across pages, `Enter` opens the company in Analysis, `Shift+Enter` opens it in a new tab, and `[`/`]` change pages. A company opened from the results shows its position ("3 of 984"). `Shift+J`/`Shift+K` step to the next or previous screened company, and `G S` returns to the results with the cursor on the last company viewed. `?` lists every shortcut.

<img src="images/web-screening.png" alt="Company screen builder with two rules and their results" width="900">

The as-of date (`D`) limits a screen to data that was available by a historical date. Type a date (`2023-06-30`), a month or year (`2023-06`, `2023`, read as its last day), or a distance back (`18m`, `5y`). You can also pick latest, a preset, or a recently used date. Choosing a date reruns the screen. The backtest handoff reruns the saved screening logic at each rebalance period instead of applying today's result list retroactively.

### Company Analysis

Choose a company from the global search (press `/` from anywhere, then `↑`/`↓` and `Enter`) or open `/analyze/:companyCode`. The page has three sections, reachable from the sticky section bar or with `1`, `2`, and `3`:

- **Overview** — the latest close with its one-day move, market cap, and 52-week range in the header; a split-adjusted price chart (1M to All, remembered per browser, `-`/`=` to widen or narrow) whose hover readout shows the date, close, and move since the start of the range; key statistics grouped as valuation, quality, income, and balance sheet, each with a tooltip saying how it is calculated (ROE and ROA are three-year averages); and the company profile with its business description, identifiers, research links (Filing Explorer, Comparison, Yahoo Finance, Yahoo! Finance Japan, Kabutan), and your tags (`T` adds one). Unlisted companies show statement-based statistics without an empty price panel.
- **Financial statements** — one tab per statement table with values, and rolling multi-year averages and growth rates alongside (`[`/`]` switch tabs). Lines appear in filing order with components nested under their subtotal, concepts EDINET renamed between years (for example *Capital stock* and *Share capital*) are combined into one line, and lines the company never reported stay hidden until you ask for them (`E`). Each line shows a trend sparkline, the latest year-on-year change, and its compound annual growth. Switch between reported values, year-on-year change, and common size (`V`); click lines or press `Space` on the focused line to chart up to six of them as bars or lines (`C`); `F` filters lines and **CSV** exports the lines shown with unrounded values.
- **Filings** — the retained EDINET annual reports with fiscal period, form, submission time, document ID, archive size, and parse status; `O` opens the latest. Export all downloads every retained archive with a manifest.

Press `?` on the page for the full list of keyboard shortcuts. Shortcuts pause while a field has focus. **Report** downloads a Markdown report with the snapshot and the full financial history; **Compare** (`P`) and **Backtest** (`B`) hand the company to those workspaces.

<img src="images/web-security-analysis.png" alt="Company analysis with a populated financial snapshot" width="900">

Favorites are ordinary private tags (for example one named `Favorite`); they are not stored in a separate favorites subsystem.

## Compare companies and arbitrary metrics

Comparison accepts 2–12 companies through the shared company finder. Start with the standard market, valuation, quality, income, and balance-sheet metrics, then remove anything irrelevant with the X on its metric chip.

The Add metric panel exposes searchable table and column controls. It accepts numeric statement or analytical columns as `Table.Column` references, so comparisons are not limited to a fixed metric list.

<img src="images/web-comparison-metrics.png" alt="Arbitrary comparison metric picker" width="900">

Run Compare to produce a side-by-side matrix. The result uses each company's latest available price and financial period, highlights the best value in each row, and can show peer percentiles. Common-size income and balance-sheet tables appear below the main matrix.

<img src="images/web-comparison.png" alt="Side-by-side financial comparison" width="900">

## Read EDINET filings

### Filing Explorer

`/filings` opens on the archive at a glance: how many reports and filers it holds, the submission date range, and how many reports carry data-quality notes. Report-type tabs (annual reports, semi-annual and quarterly reports, amendments, funds and trusts, foreign companies) show how many of each are retained and filter everything below them; `[` and `]` step through them. The latest filings from every filer are listed newest first with the English company name, ticker, and EDINET code where the research database knows the filer, and recently opened filings sit above the list.

Find a company (`F`) to list its reports instead, with links to its analysis (`A`) and an **Export all** download of every retained archive. The company and report type live in the URL, so the browser's back button and shared links return to the same view. `J` jumps into a list, `↑`/`↓` move between filings, and `Enter` opens one.

<img src="images/web-filings.png" alt="Filing Explorer with archive summary, report types, and the latest filings" width="900">

The filing viewer names the company and the report (form, fiscal period, filing time, size, and parse status), steps to the company's older or newer report with `[` and `]` while keeping the open tab, and links back to the company's filings (`L`) and analysis (`A`). Its four tabs (`1`–`4`) are:

- **Report** — the original EDINET documents, listed in statutory order and named by their own headings (Company overview, Consolidated cash flow statement, Auditor's report, …) with the Japanese heading beside each. Choose Japanese, side by side, or English (`T`; the choice is remembered), and `J`/`K` move between documents.
- **Sections** — the narrative text with an outline grouped by document, search across the Japanese and English text (`F`), and English alongside (`T`).
- **Statements** — every statement and note table built from the filing's own XBRL linkbases, grouped into business results, financial statements, notes, and other disclosures. Currency amounts share one unit per table, per-share amounts, share counts, and ratios keep their own, a change column compares the latest period with the prior one, and **CSV** exports the table with unrounded values.
- **Details** — data-quality notes, the parse record, the package contents by folder, and the taxonomy concepts the filing reports, summarised by taxonomy and filterable.

### Japanese and English side by side

Translation never replaces the Japanese source. Sections and report documents keep the original on the left and place a complete English result alongside it. Report documents are translated only when you ask for English; a whole-document translation that cannot be completed leaves each section translatable on its own.

<img src="images/web-filing-translation.png" alt="Japanese and English filing sections shown side by side" width="900">

Translation runs locally through Argos Translate. It translates complete section bodies and every visible report-HTML text node and user-facing label. If the model is unavailable or Japanese remains after retries, the English pane reports a retryable error rather than displaying source text or a partial translation as a successful result.

Validated translations are cached in the `filing_translations` table inside `Filings.db`. Cache rows are translator-versioned; incomplete rows from an older implementation are ignored.

## Organize research with tags

Research at `/research` stores private account-owned state:

- tags, including favorites and named watchlists;
- company-linked or general notes with revision checks;
- thesis status, target value/currency, and review date;
- in-app metric alerts.

<img src="images/web-research.png" alt="Tags, favorites, watchlists, notes, thesis, and alerts" width="900">

Create a tag once, select it, then add a company through the same shared company finder. Tags are available from Analysis and Screening as well; there is no separate favorites/watchlist database model in the current UI.

## Test an investment idea

Backtesting supports three entry paths:

- Manual portfolio — tickers with weight, share, or value allocations;
- Saved screen — point-in-time screening with monthly, quarterly, or yearly rebalancing;
- CSV set — batch portfolios supplied from a CSV file.

Configure the period, benchmark, base currency, capital, execution costs, and other assumptions before running. Completed runs expose cumulative return, drawdown, annual results, benchmark comparison, contribution data, saved artifacts, and downloads.

<img src="images/web-backtesting.png" alt="Backtesting workspace" width="900">

## Review an imported portfolio

Portfolio imports IBKR FlexQuery XML. Imported transactions are account-owned and are rebuilt into a daily ledger: every calendar day each holding is valued at its latest close (in its own currency, even when the stored quote comes from another listing, such as CSPX quoted in USD in London) and every holding and cash balance is converted at that day's ECB euro reference rate. The display currency converts the ledger without rewriting source activity.

<img src="images/web-portfolio.png" alt="Portfolio performance and exposure dashboard" width="900">

The header shows when the ledger was last valued and whether its data checks pass, with the period (YTD, 1Y, 3Y, 5Y, All), the display currency, and a benchmark (an index fund such as VWCE or CSPX). The six headline figures are the portfolio value against the money put in, the time-weighted total and annual return (with the money-weighted return beside it), volatility, maximum drawdown, and the Sharpe ratio; the info mark on each explains how it is calculated.

- **Overview** charts growth against the benchmark and consumer prices, the fall below each previous high, value against money put in, calendar-year returns, allocation by holding or currency, and the latest activity.
- **Holdings** lists every position with its shares, how long it has been held (the latest unbroken holding period: after a full sale and a later purchase, the clock restarts), its weight, value, latest close, unrealized and total P&L, and dividends; a warning mark flags a holding valued at cost or with an old quote. Enter opens a holding's details (price history against average cost, P&L, the position record); A opens it in Analysis, where Shift+J and Shift+K step through your holdings and G P returns to the same row.
- **Performance** compares returns and risk with the benchmark side by side (beta, alpha, tracking error, and capture use weekly returns, because Tokyo, Europe, and New York close hours apart), then shows monthly returns, the daily return distribution, and each holding's contribution by year.
- **Income** shows every dividend payment with its withholding tax, for the whole portfolio or the companies you choose (press `F`, type a name, `Enter`; `X` shows every payer again; or use Held now and Top 5). Switch between net, gross, and withholding, group by month, quarter, or year, and stack by company or payment currency. The payers table compares each company's dividends, withholding rate, share of income, last twelve months, latest amount per share and its growth on a year ago, yield on cost, and current yield; `Enter` shows one company and `A` adds or removes it. With one company chosen you see its dividend per share for every payment and every year, and its payment ledger (amount per share, shares paid on, gross, withheld, net); with several, their dividend per share indexed to the same start. Dividend growth is weighted by the income each company paid, and counts only complete years. A holding's details link straight to its dividends.
- **Activity** is the full imported ledger, searchable and filterable.
- **Data & method** lists what the figures rest on: each holding's quote and its age, quotes converted from another currency, weekly-only price history, unusual whole-portfolio days, the exchange-rate, cash-rate, and inflation series, and how every statistic is calculated. Sharpe and Sortino use the display currency's short-term rate (the euro area three-month AAA yield, the three-month US Treasury bill, SONIA, or the Japanese call rate), stored by the Update FX Data pipeline step; enter your own rate there when no series is stored.

**Rebuild** (R) revalues the ledger from your activity and the stored prices. **Refresh prices** (Shift+R, operators and admins) fetches the latest prices for every holding and the benchmark, replaces weekly-only histories with daily ones, corrects stored prices labelled with the wrong currency, updates exchange and interest rates, and rebuilds.

Keyboard: `1`–`6` switch sections, `-` and `=` lengthen or shorten the period, `C` and `B` choose the currency and benchmark, `F` finds a holding, record, or paying company, `I` imports a file, and `?` lists everything. In the Holdings and Activity lists, `↓` or `J` enters the list, `J`/`K` move, `Enter` opens details, and `[` `]` change pages.

Authenticated preview APIs also support tax-lot matching, option Greeks, and deterministic equity/FX scenarios; these previews return explicit assumptions and do not mutate imported activity.

## Run the data pipeline

The Pipeline workspace discovers available steps from the backend. Load a prepared recipe or assemble individual steps, order them, configure their generated fields, upload required files, and run the recipe as a durable job.

<img src="images/web-pipeline.png" alt="Data pipeline builder and step library" width="900">

Important current behavior:

- Import Stock Prices (CSV) uses a local file picker and accepts up to 500 MiB.
- Multipart uploads are attached only to the step that declares the file field, so mixed recipes can include ordinary steps and one or more upload steps.
- Download XBRL supports `explicit`, `backfill`, and `all`. `all` reads eligible `DocumentList` rows, honors the document-type filter, skips completed records, uses no more than five concurrent downloads, batches status updates, and reuses HTTP connections.
- Generate Financial Statements accepts `Source_Mode=csv` for the legacy `financialData_full` input or `Source_Mode=filings` for compact numeric facts in `Filings.db`.
- Jobs persist across browser reloads. The UI shows current step, progress, cancellation, terminal status, and bounded output.

See [Running the Application](RUNNING.md) for every field and command-line recovery tool.

## Databases and clean startup

The server creates missing configured database parents, files, managed schemas, and migrations when it starts:

- Base — EDINET document list and legacy CSV ingestion;
- Standardized — company information, statements, ratios, rolling metrics, prices, and FX data;
- Portfolio — imported activity and materialized portfolio state;
- Auth — users, credentials, sessions, tokens, policy, and audit state;
- Research — tags, notes, thesis state, alerts, screens, report recipes, and runs;
- Pipeline jobs — durable job and step state;
- Filings — retained compressed ZIPs, compact numeric XBRL index, quality/provenance metadata, and translation cache.

New filing ingests keep the compressed provider ZIP in SQLite, omit duplicate extracted member BLOBs, retain numeric/non-nil analytical facts, and reconstruct narrative sections on demand. Existing filing databases can be compacted or rebuilt with the scripts documented in [Running the Application](RUNNING.md).

## Screenshot refresh

The repository includes a deterministic screenshot environment with generated market data, filing ZIPs, translations, research state, and IBKR activity. It never reads `data/` or the operator's configured databases.

```powershell
.\.venv3\Scripts\python.exe tests\capture_screenshots.py
```

Images are written to `docs/images/`. The script starts a temporary loopback server, captures the current React views, and removes its temporary databases when complete.
