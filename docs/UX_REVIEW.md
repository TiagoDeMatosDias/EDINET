# UX and Correctness Review

Reviewer notes from a full read of the `frontend-v2` source and the auth/security
backend, plus live testing of the running app in both authentication modes
(`disabled` and `accounts`) against isolated demonstration data and two real
retained filings (a FY2026 IFRS annual report and a 2016 J-GAAP annual report,
copied read-only from the operator catalog for statement-rendering checks).

Each finding lists the symptom, the evidence, the relevant source location, and a
suggested direction. Line numbers are approximate and refer to the state of the
tree at review time.

## Verification baseline

- Frontend unit tests: 63 passing; `tsc --noEmit` clean; ESLint reports warnings
  only (React-compiler / fast-refresh advisories).
- Backend suite: 919 passing, 1 pre-existing failure unrelated to the UI
  (`tests/unit/test_stockprice_api.py::TestImportStockPricesCsv::test_recent_jpx_update_replaces_overlapping_dates`,
  a JPX overlapping-date fixture).
- Security middleware, path policy, body-size limits, and safe error envelopes
  are sound; the issues below are in application logic and client state, not the
  transport hardening.

---

## Critical

### C1 — React Query cache is not cleared on login or logout (cross-user leak in the browser)

`AuthProvider.login()` and `logout()` update the access token and `user` state but
never clear the query cache, so cached private data from the previous account
stays readable in the same browser tab until each query refetches.

- Evidence: after user A created a tag and signed out, a freshly registered user
  B saw A's tag in the Research UI in the same tab. The API itself was correct —
  `GET /api/tags` returned `{"tags": []}` for B — so this is purely a client
  cache-isolation defect, not a server authorization defect.
- Source: `frontend-v2/src/features/auth/AuthProvider.tsx` (`login`, `logout`,
  `register` in the context `value`).
- Direction: call `queryClient.clear()` on both successful login and logout (the
  provider will need access to the `QueryClient` via `useQueryClient()`).

### C2 — Screening/comparison `db_path` authorizes any database in the shared data directory

`configured_database_policy` authorizes any `*.db` located under the *parent
directory* of the configured databases. All seven databases (Base, Standardized,
Portfolio, `auth.db`, `research.db`, pipeline jobs, Filings) live in the same
`data/databases/` directory, so a client-supplied `db_path` can point at any of
them. The screening endpoints then expose that database's schema and, through
column selection, its contents.

- Evidence: a `member`-role account passed `db_path=<data dir>/auth.db` and
  `db_path=<data dir>/research.db` to `GET /api/screening/metrics` and received
  the full table/column listing of the auth database (including the credential
  and token tables) and of another user's research database. No private row
  values were extracted during review; the schema exposure plus the arbitrary
  `table.column` selection in `POST /api/screening/run` establish the read path.
- Source: `src/web_app/security.py` (`PathPolicy.authorize_database`,
  `configured_database_policy` — authorizes by directory root);
  `src/web_app/api/screening.py` (`_validate_db_path`, `get_metrics`,
  `run_screening_endpoint`).
- Direction: do not authorize by shared directory when secrets are co-located.
  Prefer ignoring the client `db_path` for screening/comparison and always using
  the server-configured Standardized database, or restrict to an explicit
  single-file allowlist. Separately, storing `auth.db`/`research.db` outside the
  analytical data root would remove the co-location entirely.

---

## High

### H1 — The dedicated `/login` and `/register` pages do not establish a session in the SPA

`LoginPage` calls `apiRequest(...)` and `setAccessToken(...)` directly and then
`navigate('/overview')`, but never updates `AuthProvider`'s `user` state. Because
the provider still sees `user === null` in `accounts` mode, `/overview` re-renders
the `AuthGate` ("Sign in"), so the user appears to have failed to sign in even
though authentication succeeded.

- Evidence: after registering on `/register`, the URL became `/overview` while the
  visible heading was still the sign-in/create-account gate. The in-app
  `AuthGate` (reached by deep-linking to a protected route) works correctly
  because it calls `onLogin(user)`, which sets provider state.
- Source: `frontend-v2/src/features/auth/LoginPage.tsx` (`submit`);
  contrast with the working `AuthGate` in
  `frontend-v2/src/features/auth/AuthProvider.tsx`.
- Direction: have `LoginPage` call `auth.login()` / `auth.register()` from context
  (both already set `user` state) instead of calling `apiRequest` + `setAccessToken`
  directly.

### H2 — Plain anchor downloads return 401 in `accounts` mode (the default)

API authentication is bearer-header only; a top-level navigation from an
`<a href>` sends no `Authorization` header, so authenticated download links fail.

- Evidence: `GET /api/filings/{doc_id}/artifact` and
  `GET /api/backtesting/download/{id}` both return 401 when opened as anchor
  navigations. The filing *company* export already avoids this by using
  `authenticatedFetch` + a blob download.
- Source: anchors in `frontend-v2/src/features/backtesting/BacktestResults.tsx`
  (both "Download" actions), the saved-list "Download" column in
  `frontend-v2/src/features/backtesting/BacktestingPage.tsx`, and the "ZIP" link
  in `frontend-v2/src/features/filings/FilingViewerPage.tsx`; auth in
  `src/web_app/security.py` (`request_security`).
- Direction: route these downloads through `authenticatedFetch` → blob (as the
  filings company export does), or issue a short-lived signed download URL for
  GET navigations.

### H3 — The filing "Statements" tab is unusable on real filings

The Statements view derives period columns by regex-scraping `context_id` and
renders every dimensional-member context as if it were a reporting period, while
routing nearly all concepts into a catch-all "Other" section. The database
already stores structured periods in `xbrl_contexts`
(`period_start` / `period_end` / `instant`), which the view ignores.

- Evidence: a real FY2026 annual report rendered ~75 period-like columns
  (`1YEARINSTANT`, `EQUITYMEMBER`, `ASSETSMEMBER`, `ORWARDMEMBER`, and other
  truncated member names) with hundreds of concepts under "Other"; a 2016 annual
  report rendered ~24. The single-context demonstration filing renders correctly,
  which is why the defect is not visible in the sample data.
- Source: `frontend-v2/src/features/filings/FilingViewerPage.tsx`
  (`extractPeriod`, `buildStatementTable`, `collectPeriodsFromRows`, `STATEMENTS`);
  available structured data in `xbrl_contexts` via
  `src/filings/api.py` and `src/filings/catalog.py`.
- Direction: pivot on `xbrl_contexts` real period start/end/instant; keep the
  consolidated series and a bounded number of recent periods; present dimensional
  members (non-consolidated, per-segment, per-share-class) as a separate
  breakdown rather than as columns. The "English" column currently repeats the
  raw concept id rather than a translated label.

---

## Medium

### M1 — Portfolio "Open analysis" navigates to a dead end

The holding drawer navigates to `/analyze?q=SYMBOL`, but the analysis page only
reads the `:companyCode` route parameter or `?ticker=`; `?q=` is ignored, so the
page shows its empty "Search for a company" state.

- Source: `onAnalyze` in
  `frontend-v2/src/features/portfolio/PortfolioWorkspace.tsx`; param reads in
  `frontend-v2/src/features/analysis/AnalysisWorkspaceUnified.tsx`.
- Direction: navigate to `/analyze?ticker=${symbol}`. Note that portfolio symbols
  are broker symbols and may still not resolve to an EDINET code, so an
  unresolved-ticker state on the analysis page is worth handling explicitly.

### M2 — Comparison "best value" highlight is direction-blind

The matrix marks the numeric maximum as "best" for every metric via `Math.max`,
so a higher P/E, P/B, P/S, EV/Sales, or Debt/Equity is highlighted as best.

- Evidence: in a two-company comparison, P/E 18.4 was flagged best over 14.7, and
  P/S and EV/Sales similarly flagged the larger value.
- Source: `bestValue` and `MetricMatrix` in
  `frontend-v2/src/features/comparison/ComparisonPage.tsx`.
- Direction: make direction explicit per metric — lower-is-better for
  valuation/leverage, higher-is-better for margins/returns, and no "best" for pure
  size metrics (revenue, assets, shares) where a maximum is not a quality signal.

### M3 — Screening results render raw, unformatted values

Result cells are `String(value)`, so price appears as `189.8463396453232` and
other numeric columns are unrounded and unitless, unlike the analysis and
comparison pages.

- Source: `buildColumns` in
  `frontend-v2/src/features/screening/ScreeningWorkspaceDense.tsx`.
- Direction: format numeric result cells (price, market cap, ratios) consistently
  with the shared number formatting used elsewhere.

### M4 — Business description can resolve to an unrelated company

The company snapshot description is a live server-side scrape of an external
provider keyed on ticker, with no provenance shown, and it ignores the filing's
own business-description field. A ticker that collides with a foreign symbol pulls
that company's text.

- Evidence: the demo company with ticker `AAA` displayed a US exchange-traded-fund
  prospectus summary. The fetch also runs synchronously during the overview
  response with multi-second provider timeouts.
- Source: `src/security_analysis/company_descriptions.py`
  (`fetch_yahoo_description`, `get_or_fetch_description`), consumed in
  `src/web_app/api/security_analysis.py`; ticker mapping in
  `src/utilities/stock_prices.py` (`_provider_symbol_for_ticker`).
- Direction: prefer the filing's own `DescriptionOfBusiness` when present, show the
  source/provenance of any external text, and guard the ticker-to-symbol mapping.

### M5 — Filing viewer overflows horizontally on mobile

At 390 px width the filing viewer scrolls horizontally (~105 px) because the report
tab uses a fixed `260px 1fr` grid and the statement tables are wide; it was the
only page that overflowed on mobile in testing.

- Source: inline grid style on the report tab in
  `frontend-v2/src/features/filings/FilingViewerPage.tsx`.
- Direction: stack the report-file sidebar above the content on narrow viewports
  and let the wide tables scroll within their own container.

### M6 — Password field's accessible name includes its hint text

The registration hint (`Minimum 15 characters. Use a passphrase…`) is a `<small>`
inside the `<label>`, so it becomes part of the field's accessible name; screen
readers announce it as the field name and label-based test queries break.

- Source: password `<label>` blocks in
  `frontend-v2/src/features/auth/LoginPage.tsx` and
  `frontend-v2/src/features/auth/AuthProvider.tsx` (`AuthGate`).
- Direction: associate the hint via `aria-describedby` instead of nesting it in the
  label.

---

## Low / polish

- **L1** — Overview "Recent work" shows backtests by opaque run id
  (`Backtest · 20260929_180717_019df714`) rather than a friendly label.
  (`frontend-v2/src/features/overview/OverviewPage.tsx`)
- **L2** — `conceptDisplay` contains no-op replacements such as
  `.replace(/Net Income/i, 'Net Income')`.
  (`frontend-v2/src/features/filings/FilingViewerPage.tsx`)
- **L3** — The pipeline page surfaces raw `<pre>` JSON dumps for step state and run
  output; acceptable for an admin tool but unpolished.
  (`frontend-v2/src/features/pipeline/PipelinePage.tsx`)
- **L4** — `AuthProvider` retries the refresh loop every 30 s on failure even while
  the user is on public marketing pages.
  (`frontend-v2/src/features/auth/AuthProvider.tsx`)
- **L5** — Backtest saved-list "Review" is a full-page `<a href>` navigation that
  discards SPA state.
  (`frontend-v2/src/features/backtesting/BacktestingPage.tsx`)

---

## Suggested order

1. C1, C2 — data isolation (client cache and cross-database read path).
2. H1, H2 — the default `accounts` mode currently blocks sign-in via the dedicated
   auth pages and blocks every anchor download.
3. H3 — the largest "correct in demo, broken on real data" gap.
4. Medium batch (M1–M6), then Low.
