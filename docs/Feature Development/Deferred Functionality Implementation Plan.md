# Deferred Functionality Implementation Plan

Status: Active implementation — authentication, research, filings, portfolio scoping, and engine primitives validated; remaining phases in progress
Created: 2026-07-23
Revised: 2026-07-25 — completed authentication default-to-accounts, per-user portfolio scoping, database path centralisation, screening per-user storage, comparison/attribution/report-builder modules, and event-driven backtesting engine primitives
Scope: Account creation and token-authenticated APIs, EDINET filing archives and XBRL viewing, metric provenance, data quality, watchlists, research notes, alerts, point-in-time backtesting, company comparison, portfolio attribution and scenarios, and reproducible research reports.
Implementation state: **Phase 1A authentication is complete**. Phase 1B research state, Phase 2A-2C filing archive/XBRL indexing, comparison foundations, execution-cost primitives, tax-lot/scenario primitives, and report-manifest primitives are implemented and validated. Remaining phase work is tracked below.

## Implementation record — 2026-07-23

- Added `config/state/auth.db` ownership for Argon2id password hashes, account roles/status, opaque access/refresh sessions, personal API-token hashes, rotation/revocation, audit events, and explicit registration mode. Web authentication no longer reads `EDINET_API_TOKEN`; account credentials are generated independently.
- Added bounded type-1 EDINET acquisition in `src/filings/`, immutable archive storage under `data/filings/archive/`, rebuildable `Filings.db` catalog/search indexes, defused XML fact/context/unit parsing, sanitized narrative sections, quality issues, and filing APIs plus a Filing Explorer route.
- Added owner-scoped `research.db` watchlists, notes, and in-app alert rules/events; point-in-time selection and execution-cost primitives; company comparison API/page; tax-lot/scenario primitives; and deterministic report manifests.
- Added bounded `/api/comparison/*` peer/snapshot/history routes, Research CRUD/template/recipe APIs, authenticated portfolio tax-lot/Greeks/scenario previews, and owner-scoped canonical report ZIP generation with manifest/download/delete endpoints.
- Verification: backend unit suite 700 passed/1 skipped in 61.8 seconds; integration suite 108 passed in 12.3 seconds; frontend TypeScript, 25 Vitest tests, lint (warnings only), and production build passed. Targeted Ruff/Mypy, requirements synchronization, documentation links, and diff checks passed. Canonical PyInstaller packaging and the frozen-app smoke test passed in 132.5 seconds under explicit command/smoke caps.

The implementation deliberately keeps compatibility backtesting and global imported Portfolio activity intact. Point-in-time filing selection, tax-lot/scenario previews, and reports are available as explicit authenticated analytical slices; full historical universe/corporate-action migration, scheduled alert workers, XLSX/PDF renderers, and legacy Portfolio owner-claim migration remain documented follow-up work rather than silently being represented as complete.

## Implementation record — 2026-07-25

### Authentication and authorisation hardening
- Changed default `auth_mode` from `disabled` to `accounts` and `registration_mode` from `closed` to `open`. The app now requires account creation on first launch; the first account becomes administrator.
- Added `token_version` column to `users` table with auto-migration. Password changes, role changes, and account disablement increment `token_version` and revoke all active sessions.
- Added `PATCH /api/auth/me`, `POST /api/auth/change-password`, `GET/DELETE /api/auth/sessions`, `GET/DELETE /api/auth/sessions/{id}` endpoints.
- Added admin router (`/api/admin/auth/*`): user list/detail, role assignment, disable/enable, audit log, invitation creation/revocation, credential reset tokens, auth-settings management.
- Added `src/auth/permissions.py` with `Permission` enum and `Role → Permission` matrix (admin/operator/member).
- Added `src/auth/dependencies.py` with FastAPI `current_user`, `require_admin`, `require_operator`, `require_permission` dependency factories.
- Added `auth_settings` singleton table (registration_mode, default_role, token lifetimes).
- Added `invitations` and `credential_resets` tables with token-based acceptance/consumption.
- Frontend: `AuthProvider` with full-screen login/register gate, session-restore via refresh cookie, background refresh loop, auth-disabled persistent warning banner.
- Frontend: `LoginPage` (`/login` route), `AccountPage` (profile, password, API tokens, sessions), `AdminPage` (user management, audit log).
- Frontend: `AppShell` auth section — user dropdown with account/admin links, "Auth disabled" badge, sign-in button.

### Research state completion
- Added `company_research` table (thesis_status, target_value, target_currency, review_on, version) with upsert API.
- Added `research_note_revisions` table for immutable revision history on note updates.
- Added `company_tags` table (owner-scoped per-company tags, replacing `Company_Tags` in Standardized.db).
- Added `saved_screens` and `screening_runs` tables in `research.db`.
- Note update now uses optimistic concurrency (`version` field); stale edits return 409 with the current version.
- Added watchlist member reorder endpoint.
- Added `GET/PATCH /api/research/companies/{code}`, `GET/PUT /api/research/tags/{code}`, `GET/POST/DELETE /api/research/screens`.

### Database path centralisation
- All database paths moved to `config/database_paths.json` with dedicated keys: `auth_db`, `research_db`, `pipeline_jobs_db`, `filings_db` (in addition to `db1`/`db2`/`db3`).
- Added `get_auth_db()`, `get_research_db()`, `get_pipeline_jobs_db()`, `get_filings_db()` to `src/orchestrator/common/db_config.py`.
- All runtime modules (`security.py`, `research/runtime.py`, `api/runtime.py`, `filings/runtime.py`) now resolve paths via `database_paths.json` instead of hardcoding.
- Database files physically moved from `config/state/` to `data/databases/` alongside existing `Base.db`/`Standardized.db`/`Portfolio.db`.

### Saved screening per-user migration
- `GET/POST/DELETE /api/screening/saved` endpoints rewritten to use per-user `research.db` storage instead of flat JSON files in `config/state/saved_screenings/`.
- Backward-compatible: list returns string array of names, load/delete accept name or screen ID.
- Screening history endpoint now requires authentication.

### Portfolio per-user scoping (major)
- Added `owner_user_id TEXT NOT NULL DEFAULT ''` column to all 5 Portfolio.db tables (`Transactions`, `Portfolio_Daily`, `Portfolio_Holdings`, `Holdings_History`, `Portfolio_Metrics`) via migration v3.
- Changed `Transactions.transaction_id` UNIQUE constraint from global to composite `(transaction_id, owner_user_id)` via migration v4 (table recreation).
- All `insert_entries`, `get_transactions`, `get_unique_symbols`, `get_date_range`, `get_activity_summary`, `delete_by_source` functions accept and filter by `owner_user_id`.
- `build_portfolio_state` scopes DELETEs/SELECTs/INSERTs to the owner.
- All `get_*` query functions in `portfolio_state.py` accept and filter by `owner_user_id`.
- All 9 chart functions in `charts.py` accept and filter by `owner_user_id`.
- `calculate_metrics` in `performance.py` accepts and propagates `owner_user_id`.
- Portfolio API layer: every endpoint derives `owner_user_id` from the bearer token via `_account(request)`, never from client input.
- All inline SQL queries in API dividend/return endpoints scoped with `WHERE owner_user_id = ?`.
- Legacy data with `owner_user_id = ''` is invisible to authenticated users.

### Filing catalog enhancements
- Added `parse_runs` table (parser version, status, fact/section/warning counts, error messages).
- Added `data_watermarks` table (source name/version, max_available_at, row count, refresh timestamp).
- Added `metric_catalog`, `observations`, `observation_sources`, `observation_dependencies` tables for provenance.
- Created `src/filings/provenance.py` with `resolve_provenance()` and `provenance_batch()`.
- Added `POST /api/filings/provenance/resolve`, `/resolve-batch`, `GET /api/filings/data-quality/summary`, `/issues`, `/coverage`.
- Expanded `quality.py` with additional rules: nil facts, negative values, extreme scale, missing units, filing-level checks.
- Added `GET /api/filings/{doc_id}/sections/{section_id}`, `/taxonomy`, `/audit-reports`, `/parse-runs`.

### Event-driven backtesting engine primitives (Phase 3 foundations)
- Created `src/backtesting/market_data.py` — versioned `Market_Prices`, `Corporate_Actions`, `Security_Aliases`, `Security_Listings` tables with query methods.
- Created `src/backtesting/calendar.py` — `TradingCalendar` with holiday-aware session determination.
- Created `src/backtesting/universe.py` — `PointInTimeUniverse` for listing/delisting eligibility checks.
- Created `src/backtesting/signals.py` — `TradingSignal` generation from as-of observations.
- Created `src/backtesting/execution.py` — order conversion with execution lag, fill simulation with adverse costs.
- Created `src/backtesting/ledger.py` — `PortfolioLedger` tracking cash, positions, transactions, dividends, splits with NAV and performance.

### Comparison, attribution, and reports modules
- Created `src/comparison/service.py` — `common_size_income()`, `common_size_balance()`, `growth_rate()`, `growth_matrix()`, `peer_percentile()`, `normalize_companies()`.
- Created `src/portfolio/attribution.py` — `holding_contribution()`, `currency_attribution()`, `industry_attribution()`, `multi_period_link()`, `contribution_reconciliation()`.
- Created `src/reports/builder.py` — `resolve_report_data()` resolving company/filing/observation/research data from real databases, `build_report_sections()`.
- Created `src/research/alerts.py` evaluators — `evaluate_price_crossing()`, `evaluate_metric_change()`, `evaluate_filing_alert()`, `evaluate_all_user_alerts()`.

### Test coverage
- 12 auth tests (up from 6): password change, profile update, admin endpoints, session management, last-admin protection, role-based access.
- 5 research tests (up from 1): thesis/target CRUD, note versioning, tag isolation, member reorder.
- 6 filing tests: archive/indexing, path traversal, duplicate rejection, provider token isolation, quality checks, Inline XBRL normalisation.
- 3 tax-lot tests, 2 portfolio analytics tests, 2 report tests, 2 as-of tests, 1 XBRL step test.
- **Total**: 33 focused unit tests (all passing), plus 11 security/integration tests (44 total).

## 1. Purpose

Turn the ideas in `Deferred Functionality Backlog.md` into an ordered delivery plan that can be implemented and reviewed in small vertical slices.

The order is intentional:

1. Establish account identity, API authentication, authorization, and ownership before creating more user-authored state.
2. Preserve user-authored research outside rebuildable market-data databases and scope it to its owner.
3. Preserve EDINET type-1 filing packages and index their Inline XBRL, instance, taxonomy, narrative, and audit-report content.
4. Establish stable company, filing, metric, and observation identities.
5. Make the source and quality of displayed values inspectable.
6. Make historical screens and backtests use only information available at the simulated time.
7. Build alerts, comparisons, portfolio analytics, and reports on those trustworthy contracts.

The roadmap must not label incomplete data as bias-free, tax-correct, or fully reproducible. Coverage gaps and assumptions are part of the result, not warnings hidden in logs.

## 2. Verified starting point

| Area | Existing capability | Confirmed gap |
|---|---|---|
| Authentication | Loopback APIs are unauthenticated. Remote protection currently (and incorrectly) reuses the provider-only `EDINET_API_TOKEN` as an inbound process-wide bearer secret. | This credential-boundary violation must be removed, not migrated. There are no accounts, password hashing, login/logout, independent token issuance or revocation, per-user identity, roles, ownership, registration policy, session UI, or authentication database. The SPA does not attach authorization headers, and streaming/export code contains direct `fetch` calls outside the shared client. |
| Filing acquisition | The document-list API records `xbrlFlag`, `csvFlag`, `legalStatus`, amendments, withdrawals, and disclosure state. The downloader requests EDINET document type `5`, extracts the converted tab-delimited CSV, loads it into `Base.db`, and deletes the ZIP and extraction directory. | Type `1` packages are not downloaded or retained. There is no per-format artifact state, immutable filing archive, resumable backfill, or way to inspect the original filing and audit report. |
| Filing data | `Base.db` retains document IDs, submission timestamps, parent document IDs, withdrawal/edit status, CSV-derived XBRL contexts, units, periods, and raw values. The current local `Base.db` is approximately 39.2 GiB and `Standardized.db` is approximately 1.44 GiB. | Standardization keeps document IDs and submission timestamps but loses much of the selected fact's context, unit, mapping rule, narrative disclosure, taxonomy relationships, and alternative candidates. Putting compressed filing packages or another denormalized fact copy into `Base.db` would worsen an existing size and maintenance problem. |
| Analysis | Company overview, history, taxonomy tree, prices, and an internal peer-selection service exist. | History responses discard document/source metadata; peer comparison is not exposed as a maintained API or workspace. |
| Screening | Historical screens and reusable expressions exist. | Historical selection currently limits `periodEnd`; it does not consistently select filings by `submitDateTime`, amendment state, or withdrawal state. |
| Tags | `Company_Tags` and tag CRUD exist; tags can participate in screens. | Tags live in the rebuildable standardized database. The Analysis `Watch` button is not wired to durable watchlist state. |
| Backtesting | Manual, CSV, single-screen, and rolling modes; currency conversion; portfolio benchmark; persisted ZIP results. | The engine uses unadjusted close data, filing-period dates for dividends, a current company universe, and buy-and-hold arithmetic without execution costs, liquidity, taxes, corporate actions, or explicit bias diagnostics. |
| Market data | `Stock_Prices` stores date, ticker, currency, and price. Providers request some event data. | Adjusted close, volume, splits, dividends, delisting events, provider lineage, and retrieval timestamps are not stored. |
| Portfolio | Transactions, holdings history, flow-adjusted returns, company contribution, benchmark metrics, option pricing, and option Greek functions exist. | There is no persistent tax-lot ledger, formal benchmark/industry/currency attribution, aggregate Greek view, or scenario engine. |
| Exports | Screening CSV and backtest ZIP/XLSX exports exist. | There is no cross-workspace report recipe, canonical manifest, source index, checksum, or rebuild contract. New report formats must not accept large result payloads back from the browser. |
| Jobs | Pipeline jobs are durable, cancellable, bounded, and restart-aware. | Scheduled alert evaluation and report generation are not modeled. The existing job implementation is pipeline-specific. |

## 3. Product and architecture decisions

These defaults keep the first release useful without overstating its capabilities.

### 3.1 Four new storage responsibilities

- `config/state/auth.db` exclusively owns non-rebuildable account and authentication state: users, password hashes, roles/status, invitations, sessions, token hashes, rate-limit state, and authentication audit events. No password or raw token is stored.
- `config/state/research.db` owns durable user-authored state: tags, watchlists, notes, alert rules/events, comparison templates, scenario definitions, and report recipes/runs.
- `data/filings/archive/` owns immutable, compressed EDINET artifacts. Type-1 ZIPs are stored on disk, never as SQLite BLOBs. A rebuildable rendered cache may live under `data/filings/cache/`.
- `data/databases/Filings.db` owns the rebuildable filing catalog and indexes: artifact manifests, parse runs, filing sections and search text, normalized XBRL facts/contexts/units, filing taxonomies, metric lineage, data-quality issues, and data watermarks. This replaces the previously proposed `Provenance.db`; the two responsibilities are too closely related to justify separate databases.
- `Base.db` remains the compatibility store for the existing type-5 CSV ingestion path.
- `Standardized.db` remains standardized statements, ratios, company data, prices, and market-data extensions.
- `Portfolio.db` remains broker transactions, reconstructed holdings, metrics, tax lots, and attribution inputs.

`auth.db` and user-authored state must never be dropped as part of rebuilding market data. Every state database uses explicit migrations and a pre-migration backup when a material existing database changes. `Filings.db` is rebuildable from the immutable archive plus document-list metadata; the archive itself is not deleted by a database rebuild.

The first implementation remains a single local application instance but may have multiple accounts. SQLite remains the default because it matches the packaged application and single-writer concurrency model. Phase 0 must measure a representative XBRL sample before the physical fact-store choice is locked. If projected fact-index size, rebuild time, or query latency exceeds the accepted local budgets, keep the SQLite catalog/search database and move the high-volume fact rows to year-partitioned Parquet queried through DuckDB. PostgreSQL is not justified unless a later plan introduces a managed multi-instance server.

### 3.2 Stable identities

- Account identity is an application-owned UUID. Normalized username/email values are mutable sign-in aliases, not foreign keys.
- Company identity is EDINET code. Tickers are dated aliases, not primary keys.
- Filing identity is EDINET document ID.
- Metric identity is a stable application-owned ID, separate from table names, labels, and taxonomy versions.
- Observation identity is deterministic from company, metric, filing/source, period, context, and calculation version.
- Sessions, tokens, watchlists, notes, alerts, templates, scenarios, reports, and jobs use UUIDs.
- API responses expose stable identifiers and never local absolute paths.

### 3.3 Availability semantics

- `period_end` describes the economic period.
- `submitted_at` describes when a filing entered the public record.
- `available_at` is the earliest timestamp at which the application permits an observation to influence a historical decision.
- Historical screening uses `available_at <= decision_time`, not `period_end <= decision_time`.
- A configured execution lag moves a signal to the next eligible market session.
- Amendments and withdrawals are resolved as of the decision time. A later amendment must not alter an earlier simulated decision.

### 3.4 Honest coverage

Strict point-in-time mode is available only when the requested run has sufficient filing, listing, price, corporate-action, and volume coverage. Otherwise the UI must either:

- run in explicitly labeled compatibility mode; or
- require the operator to acknowledge named coverage gaps.

Missing delisting or corporate-action data must never be silently treated as a zero return, a normal sale, or a still-live security.

### 3.5 Initial scope choices

- Authentication has two explicit modes: `disabled` is permitted only on a loopback bind for backward-compatible single-operator use; `accounts` protects every non-public API on loopback or remote binds. Remote binding requires `accounts`, trusted hosts, and HTTPS.
- Use random, opaque, revocable access/refresh/API tokens, not JWTs. The application is a single authority and benefits more from immediate revocation and simple key management than from self-contained tokens.
- Application authentication credentials are generated by the authentication subsystem and are completely independent from `EDINET_API_TOKEN`. The latter is an upstream provider credential used only by the EDINET download client.
- Protected API calls use `Authorization: Bearer <access-or-api-token>`. Browser refresh credentials use a rotating host-only `HttpOnly`/`SameSite=Strict` cookie and are never available to JavaScript or browser storage.
- The first account created from loopback becomes administrator. Later registration is explicitly `closed`, `invite`, or `open`; it never changes implicitly. Roles begin with `admin`, `operator`, and `member`.
- Shared filing/market data is readable according to role. Research state, portfolios, backtests, alerts, report recipes/runs, and their artifacts are owner-scoped. Pipeline/configuration mutations require `operator` or `admin`; account administration requires `admin`.
- Continue type-5 CSV ingestion while type-1 acquisition and parsing are introduced. XBRL becomes a candidate canonical source only after repeatable CSV/XBRL parity checks pass; the CSV path remains an explicit fallback during the compatibility window.
- The first filing viewer is a safe structured viewer, not a pixel-perfect clone of EDINET. It exposes filing outline, sanitized narrative, facts, statements, audit reports, metadata, and quality evidence.
- Never serve submitted HTML or arbitrary ZIP members directly. Render allowlisted derivatives, strip active content, rewrite media references through stable IDs, and isolate any HTML rendering with a restrictive sandbox and content-security policy.
- Alerts are in-app only in the first release. Email, Slack, and operating-system notifications are adapter work for a later plan.
- Alert evaluation runs while the application is running and after relevant pipeline refreshes. The UI displays the last successful evaluation watermark.
- Tax lots default to FIFO, with configurable average-cost and specific-lot support only when the source data can identify the assignment. Outputs are analytical and not tax filing advice.
- Scenarios are deterministic shocks first. Monte Carlo, optimization, and trade execution are out of scope.
- Report ZIP is the canonical reproducible artifact. XLSX and PDF are renderings of the same versioned report model.
- The current simple backtest remains available as a labeled compatibility engine until the event-driven engine passes golden-result validation.

### 3.6 Verified EDINET constraints

This revision is based on the current official [EDINET API Specification Version 2](https://disclosure2dl.edinet-fsa.go.jp/guide/static/disclosure/download/ESE140206.pdf), [submitted-document file specification](https://disclosure2dl.edinet-fsa.go.jp/guide/static/disclosure/download/ESE140104.pdf), and [EDINET FAQ](https://disclosure2dl.edinet-fsa.go.jp/guide/static/disclosure/WZEK0090_001.html):

- document type `1` returns a ZIP containing the submitted main document and audit report, including XBRL when `xbrlFlag` is `1`;
- document type `5` is EDINET's XBRL-to-CSV conversion and is separately gated by `csvFlag`;
- type-1 XBRL packages use `XBRL/PublicDoc` and `XBRL/AuditDoc` roles and can include Inline XBRL/HTML, instance XML, filing taxonomy/linkbases, manifests, and associated images;
- EDINET's own screen header and table of contents are not part of the API package, so the application must build navigation from metadata and filing content;
- a document download error can be returned as JSON even when the HTTP status is `200`, so status-only validation is incorrect;
- API acquisition is limited to the statutory viewing period and any extension period. `legalStatus` `"1"` and `"2"` are potentially retrievable; `"0"` is expired/unavailable. Withdrawn and some undisclosed filings are also unavailable;
- annual, semiannual, and eligible quarterly reports may remain available for up to the currently documented ten-year viewing-plus-extension window, but the exact result must be taken from each document's current flags rather than inferred from its date.

### 3.7 Verified authentication constraints

This revision follows [NIST SP 800-63B-4 password guidance](https://pages.nist.gov/800-63-4/sp800-63b/authenticators/), [RFC 9106 Argon2 guidance](https://www.rfc-editor.org/info/rfc9106/), [RFC 6750 bearer-token usage](https://www.rfc-editor.org/info/rfc6750/), [RFC 9700 OAuth security best current practice](https://www.rfc-editor.org/info/rfc9700/), and the OWASP [Password Storage](https://cheatsheetseries.owasp.org/cheatsheets/Password_Storage_Cheat_Sheet.html), [Authentication](https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html), and [Session Management](https://cheatsheetseries.owasp.org/cheatsheets/Session_Management_Cheat_Sheet.html) guidance:

- passwords are salted and hashed with Argon2id using measured, versioned parameters; they are never encrypted or reversibly stored;
- single-factor passwords have a minimum length of 15 characters, accept long passphrases and Unicode, allow password managers/paste, use a common/compromised-password blocklist, and do not impose arbitrary composition or periodic-change rules;
- login and recovery responses are generic, timing is normalized where practical, and persisted throttling limits account/IP guessing without permanent attacker-triggered lockout;
- bearer tokens are sent in the `Authorization` header, never URLs, logs, local storage, or session storage, and remote token use requires TLS;
- access tokens are short-lived and scope-limited; refresh tokens rotate, and reuse of an old refresh token revokes its token family;
- browser refresh cookies are host-only, `HttpOnly`, `SameSite=Strict`, `Secure` whenever HTTPS is used, and paired with origin/CSRF checks on cookie-authenticated endpoints;
- password, role, account-status, and compromise-recovery changes revoke existing sessions and tokens.

## 4. Priority and release map

Sizes are relative engineering scope, not calendar commitments.

| Phase | Outcome | Priority | Size | Depends on |
|---|---|---:|---:|---|
| 0 | Contracts, threat model, XBRL/storage pilot, data-source gates, fixtures, and baselines | P0 | M | None |
| 1A | Accounts, token authentication, authorization, and ownership foundation | P0 | L | Phase 0 |
| 1B | Durable owner-scoped research state, watchlists, notes, and tag migration | P0 | M | Phase 1A |
| 2 | Filing archive, XBRL viewer, metric provenance, and data quality | P0 | XL | Phases 0 and 1A |
| 3 | Realistic point-in-time backtesting and bias diagnostics | P0 | XL | Phase 2 and market-data gate |
| 4 | Filing, screen, price, and metric alerts | P1 | L | Phases 1A-2 |
| 5 | Multi-company comparison workspace | P1 | M | Phases 1B and 2 |
| 6 | Portfolio attribution, tax lots, forecasts, Greeks, and scenarios | P1 | XL | Phases 1A-1B; uses Phase 2 where available |
| 7 | Versioned reproducible research reports | P1 | L | Phases 1A-6 contracts stable |
| 8 | Cross-feature migration, performance, packaging, and release validation | P0 | M | All selected release phases |

Recommended release slices:

- Release A0 — Identity foundation: Phases 0 and 1A.
- Release A1 — Research state: Phase 1B.
- Release A2 — Filing archive and viewer: Phase 2A-2C.
- Release A3 — Trust layer: Phase 2D-2E.
- Release B — Historical correctness: Phase 3.
- Release C — Monitoring and comparison: Phases 4-5.
- Release D — Portfolio decision tools: Phase 6.
- Release E — Portable research: Phases 7-8.

Do not implement every phase as one change set. Each phase has separate schema, service/API, frontend, and validation review points.

## 5. Phase 0 — Contracts, authentication threat model, XBRL/storage pilot, data gates, and safety net

### 5.1 Define contracts before schema

Add typed domain contracts for:

- `UserRef`
- `Principal`
- `AuthTokenMetadata`
- `Permission`
- `OwnedResourceRef`
- `CompanyRef`
- `FilingRef`
- `FilingArtifact`
- `FilingSection`
- `XbrlFactRef`
- `MetricDefinition`
- `ObservationRef`
- `ProvenanceTrace`
- `QualityIssue`
- `AsOfPolicy`
- `BacktestAssumptions`
- `DataCoverage`
- `ResearchArtifactManifest`

Use Pydantic models at the API boundary and matching TypeScript interfaces in `frontend-v2/src/api/types.ts`. Domain services may use dataclasses where validation is not required.

### 5.2 Build deterministic fixtures

Create small fixture databases containing:

- users in active, disabled, and locked states; administrator/operator/member roles; duplicate normalized aliases; expired/revoked/rotated/reused tokens; invitations; throttled login attempts; and audit events;
- owned and shared resources belonging to different users, plus unclaimed legacy records;
- an original filing, an amendment, and a later withdrawal;
- a minimal type-1 archive with `XBRL/PublicDoc`, `XBRL/AuditDoc`, a manifest, Inline XBRL, an instance document, a company taxonomy, linkbases, and an image;
- malformed archives for path traversal, absolute paths, excessive expansion, too many members, unsupported media, DTD/entity declarations, invalid continuation chains, and broken internal links;
- two facts for the same concept with different contexts and units;
- an unambiguous mapped fact and an ambiguous mapping;
- a missing reporting period and a large scale change;
- a ticker alias change, split, cash dividend, delisting, and missing-volume interval;
- a screen whose membership changes after a new filing;
- buys, partial sales, a full close, dividends, withholding tax, FX changes, and an option position.

No test may depend on operator databases or live providers. Authentication fixtures use generated credentials and must never reuse an operator password/token. Filing fixtures must be synthetic or distributable under an explicitly recorded license; do not silently add a real company's full filing to the repository.

### 5.3 Approve the authentication threat model and ownership matrix

Record the protected deployment modes and named threats before schema work:

- untrusted local processes/users when loopback authentication is disabled;
- password guessing, credential stuffing, username discovery, timing discrepancies, and denial-of-service through lockout;
- database theft, log/trace/backup leakage, token theft, replay, session fixation, refresh-token reuse, and stale role/account state;
- XSS access to browser memory, CSRF against cookie-authenticated refresh/logout endpoints, malicious links, caching, and cross-origin requests;
- remote cleartext transport, untrusted proxy headers, host-header attacks, and incorrect TLS termination;
- concurrent login/refresh/revoke races and application restart during token rotation;
- lost administrator credentials, compromised accounts, and restoration from an old `auth.db` backup.

Approve:

- exact public endpoints and fail-closed protection for every other `/api/*` route;
- `disabled` versus `accounts` deployment behavior;
- registration modes, first-administrator bootstrap, invitations, and administrator recovery;
- access, refresh, and personal API-token lifetimes/scopes;
- role-to-permission and route-to-permission matrices;
- resource classification as shared, owner-scoped, or administrator/operator-only;
- how existing tags, saved screens, jobs, portfolios, backtests, and artifacts are claimed by the first administrator;
- remote HTTPS/proxy requirements and trusted proxy/host configuration;
- audit retention and privacy policy.

### 5.4 Run the XBRL storage and parser pilot

Before freezing the physical schema, run a bounded, resumable pilot over a representative sample of annual, amended annual, quarterly/semiannual, and audit-report packages across several taxonomy years and company sizes. Start with a small operator-approved date/company range rather than a historical backfill.

For each package record:

- compressed and declared/uncompressed member bytes;
- member, Inline XBRL document, context, unit, fact, concept, relationship, section, image, and text counts;
- download, validation, parse, index, search, and rebuild durations;
- database and full-text-index growth;
- peak process memory and temporary-disk use;
- CSV/XBRL overlap, disagreements, and facts available only in one representation;
- parse errors grouped by taxonomy year, form, and filing structure.

Extrapolate from both package count and observed distributions; do not estimate the archive from a single large or small filing. The pilot decision record must choose:

1. a single normalized `Filings.db`; or
2. a SQLite catalog/search database plus year-partitioned Parquet fact storage.

The default is option 1. Choose option 2 only when measured full-history projections or query/rebuild benchmarks exceed documented operator budgets. Keep the repository/service API independent of that physical decision.

### 5.5 Data-source decision gates

Record EDINET acquisition policy before Phase 2:

- which document types and filing years are in scope;
- eligibility from `xbrlFlag`, `legalStatus`, withdrawal, and disclosure status;
- type-1 retention policy and whether type-2 PDF/type-3 attachments/type-4 English files are later optional artifacts;
- request concurrency, retry/backoff, daily request budget, and backfill schedule;
- archive quota, per-package compressed and expanded limits, minimum free-space floor, and operator-visible cleanup policy;
- whether historical gaps outside EDINET's viewing/extension period are accepted or supplied by operator-owned archives.

Before Phase 3 implementation, record an approved source and licensing/retention policy for:

- historical listing and delisting intervals;
- adjusted and unadjusted prices;
- trading volume;
- splits, cash dividends, and other supported corporate actions;
- benchmark constituents if formal allocation/selection attribution is required;
- option volatility when market implied volatility is unavailable.

The ingestion layer must record provider, source identifier, retrieval time, and coverage range. Operator-supplied CSV imports remain a supported fallback.

### 5.6 Characterize current behavior

Add tests that prove:

- current loopback `/api/*` calls need no credentials;
- current remote protection incorrectly treats the provider-only `EDINET_API_TOKEN` as an application bearer credential;
- the replacement authentication subsystem never reads, imports, hashes, compares, persists, or accepts `EDINET_API_TOKEN`;
- the SPA shared client, stream helper, and direct export request do not consistently attach bearer tokens;
- the current downloader defaults to EDINET type `5`, uses one `Downloaded` marker, extracts the ZIP, and removes the source artifact;
- the download client rejects a `200` JSON error payload rather than treating it as a ZIP;
- current screening uses period end in its as-of selection;
- current price storage has no action/volume semantics;
- current backtest results are reproducible in compatibility mode;
- existing tag, backtest, portfolio, and export contracts remain readable.

Capture performance baselines for representative fixture and local-database operations. Future budgets should be relative to these measurements rather than invented absolute targets.

### 5.7 Phase 0 exit criteria

- Contracts and fixtures are reviewed.
- The authentication threat model, public-route allowlist, registration/bootstrap policy, role/permission matrix, ownership matrix, token policy, TLS policy, and recovery procedure are approved.
- The representative XBRL pilot is reproducible, bounded, and has an approved physical storage decision.
- Archive quota, eligibility, backfill, and retention policies are recorded.
- Provider/licensing decisions required for strict simulation are recorded.
- Existing behavior has regression tests.
- No user database has been migrated.
- No UI advertises unfinished functionality.

## 6. Phase 1A-1B — Accounts, token authentication, and research state

Authentication is implemented and reviewed before owner-scoped research or other new user data.

### 6.1 Phase 1A — Authentication package and dependencies

Create `src/auth/` with focused modules:

- `models.py` — user, principal, token, session, role, and permission contracts;
- `schema.py` — versioned `auth.db` schema, migrations, integrity checks, and backups;
- `passwords.py` — Argon2id hashing, verification, parameter calibration, and rehash detection;
- `tokens.py` — cryptographically random token creation, hashing, rotation, and constant-time verification;
- `repository.py` — transactional account/session/token storage;
- `registration.py` — first-admin, closed, invite, and open-registration policy;
- `rate_limit.py` — persisted account/network throttling with bounded cleanup;
- `permissions.py` — centralized role/scope checks and ownership helpers;
- `service.py` — account lifecycle and authentication orchestration;
- `dependencies.py` — current-principal and permission dependencies;
- `api.py` — `/api/auth/*` and `/api/admin/auth/*`;
- `audit.py` — structured, privacy-limited authentication audit events.

Add a directly pinned Argon2 implementation such as `argon2-cffi` after a package/build spike. Do not write a password-hashing algorithm or token random-number generator. Keep functions below 80 lines by separating validation, hashing, SQL, policy, and route mapping.

### 6.2 Dedicated authentication database

Add `auth_db` configuration, defaulting to `config/state/auth.db`. It is never accepted from a request and is not attached to market-data queries.

Initial tables:

- `users(user_id, username, username_normalized, email, email_normalized, password_hash, password_changed_at, role, status, token_version, created_at, updated_at, last_login_at, disabled_at)`;
- `sessions(session_id, user_id, token_family_id, created_at, last_seen_at, idle_expires_at, absolute_expires_at, revoked_at, revoke_reason, client_label)`;
- `access_tokens(token_id, session_id, user_id, token_hash, token_prefix, scopes_json, issued_at, expires_at, last_seen_at, revoked_at)`;
- `refresh_tokens(token_id, session_id, token_family_id, token_hash, token_prefix, issued_at, expires_at, rotated_to_token_id, consumed_at, revoked_at)`;
- `api_tokens(token_id, user_id, name, token_hash, token_prefix, scopes_json, created_at, expires_at, last_used_at, revoked_at)`;
- `invitations(invitation_id, token_hash, email_normalized, role, created_by, created_at, expires_at, accepted_by, accepted_at, revoked_at)`;
- `credential_reset_tokens(reset_id, user_id, token_hash, created_by, created_at, expires_at, consumed_at, revoked_at)`;
- `auth_rate_limits(bucket_key_hash, action, window_started_at, attempt_count, blocked_until, updated_at)`;
- `auth_audit_events(event_id, occurred_at, actor_user_id, subject_user_id, event_type, outcome, session_id, token_id, remote_address_hash, user_agent_hash, correlation_id, details_json)`;
- `auth_settings(singleton_id, registration_mode, default_role, access_token_seconds, refresh_idle_seconds, refresh_absolute_seconds, updated_at, updated_by)`;
- `schema_migrations(version, applied_at)`.

Constraints enforce normalized alias uniqueness, valid role/status values, token expiry ordering, one-time invitation/reset use, and token-family relationships. Store only token hashes and a non-secret display prefix. Password hashes include algorithm/parameter metadata in the encoded Argon2 value. Authentication events never contain passwords, complete tokens, request bodies, raw IP addresses, or sensitive domain data.

`auth.db` is non-rebuildable. Use restrictive filesystem ACLs where supported, a pre-migration backup, documented backup/restore, and an integrity check before startup migration. Full-disk or volume encryption is recommended for at-rest protection; transparent database encryption is a separate dependency decision.

### 6.3 Account creation and password policy

- Preserve the display username and compare a Unicode-normalized/case-folded alias. Never normalize, case-fold, or trim the password itself.
- Require at least 15 Unicode characters for password-only login, support at least 64 characters, and apply a generous byte ceiling before Argon2 work to prevent resource exhaustion.
- Allow spaces, Unicode, paste, and password managers. Do not require character classes, security questions, scheduled password changes, or password hints.
- Reject common/compromised passwords using a locally available, licensed blocklist. A network breach-check service is optional later and must not receive the full password or hash.
- Calibrate Argon2id on the packaged target within an approved latency/memory budget, record the parameters, and rehash after successful login when policy strengthens.
- Return one generic login failure for unknown, disabled, locked, or bad-password accounts. Perform a dummy Argon2 verification for unknown aliases and keep response/status behavior equivalent where practical.
- Persist throttling by normalized account bucket and coarse network bucket. Use bounded exponential delay/temporary throttling rather than permanent attacker-triggered lockout.
- With an empty database, first-administrator creation is allowed only from loopback or the offline administration command. Afterward, registration follows the stored `closed`, `invite`, or `open` mode.
- Open registration creates `member` accounts only. Role promotion, disablement, credential reset, and registration-policy changes require an administrator and revoke affected sessions where appropriate.
- Do not hard-delete the final active administrator. Initial deletion support is disable/anonymize after owned-data handling is defined.

Email is optional identity metadata in the first release. Email verification and self-service email recovery are not claimed without a configured delivery service. Provide an offline, host-local administrator recovery command that resets credentials, invalidates every account token, and emits an audit record.

### 6.4 Opaque access, refresh, and API tokens

- Generate at least 256 bits of randomness and use distinguishable prefixes such as `edn_at_`, `edn_rt_`, and `edn_pat_`.
- Store a SHA-256 digest of the random high-entropy secret and locate it through a non-secret token ID/prefix; compare candidate digests in constant time. Fast hashing is appropriate for unguessable 256-bit tokens, not for passwords.
- Return a short-lived access token and explicit expiry from login/refresh. The SPA keeps it in memory only and sends it as `Authorization: Bearer`.
- Put the browser refresh token only in a host-only `HttpOnly`, `SameSite=Strict` cookie. Mark it `Secure` under HTTPS; remote accounts mode refuses cleartext transport.
- Rotate the refresh token on every use in one transaction. Reuse of a consumed token revokes its entire family and requires login.
- Bind access tokens to a session, user, token version, scopes, expiry, and revocation state. Password/role/status changes increment `token_version` and revoke active sessions.
- Personal API tokens are created only after reauthentication, shown once, optionally expire, have user-selected names and least-privilege scopes, and can never exceed the issuing user's permissions.
- Never accept access, refresh, invitation, reset, or API tokens in a query string.

Default lifetimes are configurable and recorded in the threat-model decision. Start evaluation with roughly 15-minute access tokens, a bounded refresh idle period, and a longer absolute refresh limit; approve final values from the intended local/remote usage rather than scattering constants through code.

### 6.5 Authentication and account APIs

The exact bearer-exempt allowlist is:

- `GET /health` — minimal liveness only;
- `GET /api/auth/status` — mode and whether registration/login is available, without user counts or aliases;
- `POST /api/auth/register`;
- `POST /api/auth/login`;
- `POST /api/auth/refresh`;
- `POST /api/auth/logout`.

Registration is still policy- and origin-gated; refresh/logout require a valid refresh token cookie, exact allowed origin, CSRF token/header, and fetch-metadata checks. Every other `/api/*` route fails closed without a valid bearer access/API token. Static SPA assets remain public so the login page can load.

Add:

- `GET /api/auth/me`;
- `PATCH /api/auth/me`;
- `POST /api/auth/change-password`;
- `GET/DELETE /api/auth/sessions` and `/api/auth/sessions/{session_id}`;
- `GET/POST /api/auth/api-tokens`;
- `DELETE /api/auth/api-tokens/{token_id}`;
- administrator user list/detail, role/status, invitation, reset, registration-policy, session-revocation, and audit endpoints under `/api/admin/auth/*`.

Authentication middleware validates the bearer token, attaches a typed immutable principal to request state, and emits the RFC-compatible `WWW-Authenticate` response. Route dependencies enforce permission and ownership; missing/invalid credentials return `401`, while a valid principal lacking permission returns `403`. OpenAPI declares one bearer security scheme and explicitly marks only the allowlist public.

Do not rely on developers remembering to add a dependency to each router. Add a startup/OpenAPI test that fails when a new `/api/*` operation is neither in the exact public allowlist nor covered by authentication metadata.

### 6.6 Frontend authentication flow

Create:

- `AuthProvider` and an in-memory access-token store;
- `/login`, conditional `/register`, account settings, active sessions, and personal API-token pages;
- administrator account/registration/invitation/audit pages;
- protected-route and permission-aware navigation components;
- a single-flight refresh coordinator so concurrent expiry does not rotate one refresh token multiple times.

Refactor `apiRequest`, `apiStream`, screening export, artifact downloads, and every future request through one authenticated transport layer. It attaches the bearer token, refreshes before known expiry, handles abort/timeouts, and clears identity/query caches on logout.

Do not store access or refresh tokens in `localStorage`, `sessionStorage`, IndexedDB, URLs, React Query persistence, logs, error objects, or service-worker caches. Authenticated responses use `Cache-Control: no-store`; logout revokes the server session, expires cookies, clears in-memory/query state, and returns `Clear-Site-Data` where safe.

Avoid blindly replaying non-idempotent mutations after a `401`. Refresh before sending when possible; automatically retry only safe/idempotent operations or requests protected by an idempotency key. Streams authenticate when opened, and download links fetch authenticated blobs rather than embedding tokens in URLs.

### 6.7 Authorization, ownership, and privacy

Initial policy:

| Resource | Read | Mutate |
|---|---|---|
| Company, filing, taxonomy, standardized market data | authenticated member | operator/admin pipeline only |
| Research notes, watchlists, alerts, comparisons, backtests, reports, artifacts | owner; explicit audited admin support access only | owner |
| Portfolio transactions, holdings, lots, and artifacts | owner only | owner |
| Pipeline definitions, jobs, provider refresh, and global configuration | operator/admin | operator/admin |
| Users, roles, registration, invitations, token revocation, auth audit | admin | admin |

Every owner-scoped table and artifact manifest stores `owner_user_id`. API queries derive it from the principal; clients cannot select a different owner through request fields. Stable user UUIDs may be copied into other databases, but cross-database foreign keys are not possible, so service-layer ownership validation and orphan checks are mandatory. Migrate `pipeline_jobs.db` with `owner_user_id`, `created_by_user_id`, and an explicit visibility/operation class before accounts mode exposes job history.

Existing global user-authored data is not silently exposed to all new accounts. On migration it becomes `legacy-unclaimed`; the first administrator can review and atomically claim it. Shared market/filing databases remain global. Jobs record both owner and executing principal; administrator support access is explicit and audited.

### 6.8 Provider-credential separation and deployment migration

- `disabled` mode preserves current unauthenticated behavior only on loopback and shows a persistent UI warning.
- `accounts` mode uses only account-issued tokens for ordinary APIs.
- Remote binding requires `accounts`, trusted hosts, an approved HTTPS endpoint, and explicit trusted-proxy configuration when TLS terminates upstream.
- `EDINET_API_TOKEN` is exclusively the outbound EDINET data-download credential. Authentication code, bootstrap commands, inbound middleware, account APIs, `auth.db`, and application bearer-token comparisons must never read or use it.
- Remove the current `AppSettings.api_token`/remote-middleware wiring to `EDINET_API_TOKEN`; do not rename, copy, import, transform, or preserve that value as an application credential.
- First-administrator bootstrap uses only the loopback registration flow or offline host-local administration command. It does not require or accept a shared environment bearer secret.
- Startup fails closed if remote mode is requested with no account database/administrator, cleartext public configuration, broad trusted hosts, or ambiguous proxy settings.
- Authentication can be rolled back to disabled mode only on loopback. Rollback never drops `auth.db` or token/audit history.

### 6.9 Authentication tests and Phase 1A exit criteria

- Fresh schema, all migrations, pre-migration backup, corruption handling, rollback, restrictive path policy, and restore from backup.
- Username/email normalization collisions, password length/Unicode/boundary/blocklist behavior, Argon2 calibration/rehash, and no plaintext credentials.
- Generic/timing-aware failures, persisted per-account/network throttling, concurrency, restart behavior, and no permanent lockout denial-of-service.
- First-admin loopback/offline bootstrap; closed/invite/open registration; invitation expiry/reuse; last-admin protection; disable/reset/role-change revocation.
- Access expiry, scope, revoke, constant-time verification, refresh rotation/races/reuse-family revocation, session idle/absolute expiry, API-token display-once and least privilege.
- Exact public allowlist; every other API returns `401` without a token and `403` for insufficient permission; health/auth status leak no sensitive state.
- Setting, changing, or knowing `EDINET_API_TOKEN` never authenticates an inbound request, creates an account/session/token, or changes authorization behavior.
- CSRF/origin/fetch-metadata, cookie flags, TLS/proxy/trusted-host, cache, CORS, host-header, and token redaction boundaries.
- Cross-account IDOR tests for every owner-scoped API, artifact, stream, job, and download.
- Frontend login/register/logout/refresh races, reload recovery, disabled account, expired session, route guard, permission navigation, no web-storage token, authenticated stream/export/download, and cache clearing.
- Packaged Windows smoke tests cover first-account creation, login, one protected API, refresh, logout/revocation, and remote configuration fail-closed.

Phase 1A exits only when all non-public APIs are protected in accounts mode, token/credential leakage checks pass, legacy static-token migration is documented, and owner/permission helpers are available to later phases.

### 6.10 Phase 1B — Research backend package

Create `src/research/` with focused modules:

- `models.py` — typed research contracts.
- `schema.py` — versioned `research.db` migrations and backups.
- `store.py` — owner-scoped transactional CRUD and pagination.
- `watchlists.py` — membership rules and ordering.
- `notes.py` — notes, research status, target values, and revisions.
- `api.py` — `/api/research/*` router.

Keep functions below 80 lines by separating validation, SQL, model mapping, authorization, and route concerns.

### 6.11 Research schema

Initial tables:

- `company_tags(owner_user_id, company_code, tag, created_at)`
- `watchlists(watchlist_id, owner_user_id, name, description, color, position, archived_at, created_at, updated_at, version)`
- `watchlist_members(watchlist_id, company_code, position, added_at, note)`
- `company_research(owner_user_id, company_code, thesis_status, target_value, target_currency, review_on, updated_at, version)`
- `research_notes(note_id, owner_user_id, company_code, watchlist_id, title, body_markdown, created_at, updated_at, version, archived_at)`
- `research_note_revisions(note_id, version, title, body_markdown, saved_at)`
- `saved_screens(screen_id, owner_user_id, name, definition_json, created_at, updated_at, version)`
- `screening_runs(run_id, owner_user_id, screen_id, requested_at, as_of, summary_json, artifact_relpath)`
- `schema_migrations(version, applied_at)`

Use ownership-aware uniqueness constraints for watchlist names, tags, company research, and memberships. Deletes are soft for notes/watchlists unless the owner explicitly purges archived content.

### 6.12 Tag compatibility and migration

- Copy existing `Standardized.db.Company_Tags` rows into a reviewed `legacy-unclaimed` migration set, then let the first administrator claim them into `research.db.company_tags`.
- Stage the flat `config/state/saved_screenings/*.json` files and `screening_history.jsonl` as legacy-unclaimed records, then import/claim them into owner-scoped tables without deleting the originals until validation succeeds.
- Keep `/api/tags/*` as an authenticated compatibility facade backed by the current user's `research.db` rows.
- Preserve saved screening rules that reference `Company_Tags.tag` while ensuring they resolve against the current owner only.
- Teach screening metric discovery and query building to treat `Company_Tags` as a synthetic owner-scoped research-state source attached read-only for the request.
- Do not maintain two writable tag sources.
- If a claimed `Watchlist` tag exists, offer to seed that owner's default watchlist from it; do not reinterpret every tag as a watchlist.

### 6.13 Research API

Add authenticated typed endpoints:

- `GET/POST /api/research/watchlists`
- `GET/PATCH/DELETE /api/research/watchlists/{watchlist_id}`
- `POST/DELETE /api/research/watchlists/{watchlist_id}/members/{company_code}`
- `PATCH /api/research/watchlists/{watchlist_id}/members/reorder`
- `GET/PATCH /api/research/companies/{company_code}`
- `GET/POST /api/research/companies/{company_code}/notes`
- `GET/PATCH/DELETE /api/research/notes/{note_id}`

Use optimistic concurrency through the `version` field. A stale edit returns `409` with the current version instead of overwriting newer text. A resource owned by another user returns the approved non-enumerating response.

### 6.14 Research frontend

Add a top-level `/research` workspace with Watchlists and Notes tabs.

- Wire the Analysis `Watch` button to the signed-in user's default watchlist.
- Show that user's watchlist membership in global company search, screening rows, and company analysis.
- Add watchlist creation, rename, archive, drag ordering, member add/remove, and analysis handoff.
- Add structured thesis status, target value/currency, next review date, and Markdown notes.
- Render Markdown without raw HTML.
- Add due-review and recently updated research cards to Overview.

### 6.15 Research tests

- Fresh schema and every migration path.
- Backup on material migration.
- Legacy tag staging/claim is explicit, authorized, and idempotent.
- Saved `Company_Tags` screens still run for their owner.
- Cross-account list/read/write/delete isolation and guessed-ID behavior.
- CRUD, pagination, ordering, optimistic conflict, archive, and restore.
- Markdown/XSS boundary tests.
- React tests for Watch, member changes, note conflicts, due-review state, and account switching.
- OpenAPI uniqueness, authentication, permission, and typed-contract tests.

### 6.16 Phase 1B exit criteria

- Each user's research survives a full Standardized database rebuild and is isolated from other accounts.
- Existing tags and saved tag-based screens remain compatible after explicit ownership claim.
- The Watch button is functional, authenticated, owner-scoped, and reload-safe.
- Conflicting note edits cannot silently lose data.
- Focused backend/frontend checks and the full bounded milestone suite pass.

## 7. Phase 2 — Filing archive, XBRL viewer, provenance, and data quality

Phase 2 is delivered in independent vertical slices. The archive and filing list can ship before XBRL becomes a standardized-data source.

### 7.1 Phase 2A — Storage and filing catalog

Add semantic configuration keys without breaking `db1`/`db2`/`db3`:

- `filings_db`, default `data/databases/Filings.db`;
- `filing_archive`, default `data/filings/archive`;
- `filing_cache`, default `data/filings/cache`;
- compressed-package, declared-expanded, member-count, member-size, cache, total-archive, and minimum-free-space limits.

Add matching server-side getters and authorize every path under configured data roots. No API accepts an operator filesystem path.

Create `src/filings/`:

- `schema.py` — versioned, rebuildable schema;
- `models.py` — filing/artifact/fact/section contracts;
- `repository.py` — catalog queries and transactions;
- `edinet_client.py` — bounded EDINET metadata and artifact requests;
- `archive.py` — immutable writes, hashing, manifests, and safe reads;
- `jobs.py` — incremental download, parse, reindex, and backfill operations;
- `instance.py` — contexts, units, and instance facts;
- `inline.py` — Inline XBRL facts, continuations, footnotes, and narrative;
- `taxonomy.py` — per-filing extension taxonomy and linkbases;
- `sections.py` — outline, text, sanitized rendering, and search;
- `provenance.py`, `quality.py`, and `service.py`.

Initial catalog tables:

- `filings(doc_id, company_code, sec_code, filer_name, ordinance_code, form_code, doc_type, description, period_start, period_end, submitted_at, available_at, parent_doc_id, operation_at, withdrawal_status, edit_status, disclosure_status, legal_status, xbrl_flag, csv_flag, taxonomy_release_id, metadata_seen_at)`;
- `filing_artifacts(artifact_id, doc_id, edinet_type, artifact_kind, availability_status, download_status, content_sha256, compressed_bytes, declared_expanded_bytes, member_count, archive_relpath, retrieved_at, attempts, last_error_code, last_error_at)`;
- `archive_members(member_id, artifact_id, member_path, folder_role, media_type, compressed_bytes, expanded_bytes, crc32, content_sha256, validation_status)`;
- `parse_runs(parse_run_id, artifact_id, parser_version, schema_version, status, started_at, completed_at, warning_count, error_code, error_details_json)`;
- `data_watermarks(source_name, source_version, max_available_at, row_count, refreshed_at)`.

Use a uniqueness constraint on `(doc_id, edinet_type, content_sha256)` so the same content is idempotent while a changed package remains an auditable version. Keep `DocumentList.Downloaded` as the legacy type-5 CSV marker; do not overload it for XBRL. Backfill per-format state into `filing_artifacts`.

Archive layout:

```text
data/filings/archive/
  <doc-id>/
    type-1-<sha256>.zip
```

Validate `doc_id` before path construction. Write through a same-directory `.partial` file, stream while hashing and enforcing the compressed limit, flush/close, validate the package, and atomically rename. A database row becomes `ready` only after the final file exists. Never extract the entire ZIP into the permanent archive.

### 7.2 Bounded and secure EDINET acquisition

Refactor the current downloader so metadata discovery, artifact download, archive validation, and type-5 CSV ingestion are separate services. Do not extend the existing monolithic `downloadDocs` loop.

The artifact client must:

- request type `1` for the submitted main document, audit report, and available XBRL files;
- obtain its provider credential through the EDINET acquisition package, the only subsystem allowed to read `EDINET_API_TOKEN`; that package uses it solely for outbound EDINET metadata/document requests;
- build query parameters through the HTTP client and log only document ID and requested type, never the subscription key or the rendered request URL;
- use explicit connect/read timeouts, a job deadline, cooperative cancellation, and a small bounded retry count;
- retry transient network errors, `429`, and eligible `5xx` responses with capped exponential backoff and jitter;
- validate both status and `Content-Type`, because EDINET can return an HTTP `200` JSON error body;
- require a valid ZIP signature and central directory before publication;
- record a stable redacted error code rather than provider payloads or secrets.

Download eligibility is derived from current document metadata:

- `xbrlFlag == "1"`;
- `legalStatus` is `"1"` (viewing period) or `"2"` (extension period);
- the document is not withdrawn and is not currently wholly undisclosed;
- document type is included by operator policy.

If a package cannot be acquired, record `not_available`, `expired`, `withdrawn`, `undisclosed`, or `failed`; do not repeatedly retry permanent conditions. EDINET only exposes documents during its viewing/extension period, so a historical archive will have explicit gaps unless the operator supplies packages from another lawful source.

Before reading a member, reject:

- absolute, UNC, drive-qualified, empty, or `..` paths;
- duplicate/conflicting normalized paths;
- symlinks and non-regular members;
- excessive member count, per-member size, total declared expansion, or compression ratio;
- encrypted members and unsupported compression methods;
- DTD/entity-bearing XML and any parser configuration that permits network retrieval.

Parse members directly from `ZipFile.open()` or a job-owned bounded temporary workspace. Replace the current unrestricted `extractall` use for every path touched by the new pipeline before enabling type-1 downloads.

### 7.3 Phase 2B — Incremental acquisition and backfill

Add durable, cancellable pipeline operations:

- `sync_filing_catalog`;
- `download_filing_packages`;
- `index_filing_packages`;
- `reindex_changed_parser_versions`;
- `scan_filing_quality`.

Each operation stores a cursor/watermark and can resume without re-downloading a verified hash. Process work in bounded batches with a default EDINET download concurrency of one. Surface progress as discovered, eligible, downloaded, skipped, indexed, failed, bytes stored, and estimated remaining items.

Roll out acquisition in this order:

1. new eligible annual and amended annual filings;
2. on-demand filings requested from Company Analysis;
3. a bounded recent-history window;
4. quarterly/semiannual filings if approved;
5. older available history in operator-selected date slices.

Never start a full-history backfill implicitly during application startup. Before each batch, check archive quota and free-space floor. When the limit would be crossed, finish the current atomic item, pause the job with an actionable state, and preserve its cursor.

Type-5 CSV and type-1 package outcomes are independent. One can succeed while the other remains missing or failed. The existing CSV ingestion continues to populate `Base.db` until the parity gate in Phase 2D is approved.

### 7.4 Phase 2C — XBRL, taxonomy, narrative, and search index

Use the package manifest and folder roles before filename heuristics. Index `XBRL/PublicDoc` and `XBRL/AuditDoc` separately and preserve source member identity.

Machine facts:

- Prefer the included XBRL instance for normalized fact extraction when present.
- Parse contexts, entity identifiers, instant/duration periods, explicit and typed dimensions, units including divide units, decimals, precision, nil state, language, and footnotes.
- Preserve the lexical value and separately store a normalized numeric/date/boolean/text value.
- For Inline XBRL, resolve `ix:nonNumeric`, `ix:nonFraction`, continuation chains, transformations, scale, sign, escape state, exclusions, and tuple/order semantics where used.
- Cross-check instance and Inline XBRL representations and record unexplained disagreements.

Taxonomy:

- Parse schema references, company extension concepts, labels by language/role, presentation, calculation, definition, reference, and footnote relationships.
- Identify the base EDINET taxonomy release and deduplicate reusable concept/label material by taxonomy/package hash.
- Keep filing-specific extension relationships scoped to the filing; do not merge same-looking company concepts across issuers without a stable namespace identity.
- Reuse proven low-level taxonomy helpers where their contracts fit, but do not force the existing central-taxonomy table to own filing-specific extensions.

Narrative and audit content:

- Build a stable document and section order from the manifest, entry points, headings, anchors, and EDINET-style table-of-contents markers.
- Preserve Japanese text, tables, lists, notes, tagged text blocks, audit-report sections, and fact-to-section links.
- Index plain section text in FTS5 when available. If the packaged SQLite build lacks FTS5, fail the Phase 0 gate or provide an explicitly benchmarked search fallback.
- Store searchable text and section metadata in `Filings.db`; store sanitized HTML derivatives and decoded media in the rebuildable cache, not as database BLOBs.

Index tables:

- `xbrl_documents(document_id, artifact_id, folder_role, member_id, document_kind, language, target_namespace, entrypoint_order)`;
- `xbrl_contexts(context_key, document_id, source_context_id, entity_scheme, entity_identifier, period_kind, period_start, period_end, instant, dimensions_json, context_hash)`;
- `xbrl_units(unit_key, document_id, source_unit_id, numerator_json, denominator_json, unit_hash)`;
- `taxonomy_concepts(concept_key, namespace_uri, local_name, data_type, substitution_group, period_type, balance, abstract, nillable)`;
- `taxonomy_labels(concept_key, language, role, label, source_hash)`;
- `taxonomy_relationships(relationship_id, artifact_id, arcrole, linkrole, from_concept_key, to_concept_key, order_value, weight, preferred_label, closed, usable)`;
- `xbrl_facts(fact_id, document_id, concept_key, context_key, unit_key, source_fact_id, fact_order, lexical_value, normalized_numeric, normalized_text, decimals, precision, scale, sign, nil, language, transformation, source_member_id, source_locator, fact_hash)`;
- `xbrl_footnotes(footnote_id, document_id, language, role, text)` and `xbrl_fact_footnotes(fact_id, footnote_id, arcrole)`;
- `filing_sections(section_id, document_id, parent_section_id, section_order, level, heading, plain_text, sanitized_cache_key, source_member_id, source_anchor, section_hash)`;
- `filing_section_facts(section_id, fact_id, occurrence_order)`;
- `filing_text_fts`, content-linked to `filing_sections` when FTS5 is selected.

Use compact integer internal keys for joins but expose only deterministic public IDs. Add indexes after pilot query plans for company/date lists, filing sections, concept/context fact lookups, source resolution, and parser-version reindexing.

### 7.5 Filing APIs

Add a dedicated router with cursor pagination and strict response limits:

- `GET /api/filings?company_code=&doc_type=&from=&to=&status=&cursor=&limit=`;
- `GET /api/filings/coverage`;
- `GET /api/filings/{doc_id}`;
- `GET /api/filings/{doc_id}/documents`;
- `GET /api/filings/{doc_id}/outline`;
- `GET /api/filings/{doc_id}/sections/{section_id}`;
- `GET /api/filings/{doc_id}/facts?concept=&context=&section_id=&cursor=&limit=`;
- `GET /api/filings/{doc_id}/statements`;
- `GET /api/filings/{doc_id}/taxonomy?role=&depth=`;
- `GET /api/filings/{doc_id}/audit-reports`;
- `GET /api/filings/{doc_id}/media/{media_id}`;
- `POST /api/filings/{doc_id}/acquisition` to enqueue an eligible on-demand download;
- `GET /api/filings/{doc_id}/artifact` for an authorized raw-package download by stable ID.

The filing detail response includes artifact/parse state, amendments, disclosure status, source metadata, taxonomy release, counts, warnings, and public EDINET link. Narrative, facts, and taxonomy are fetched lazily. Do not return a full annual report, all facts, or absolute local paths in a list/detail response.

Raw-package download uses a server-generated filename, checksum, content length, attachment disposition, and configured response policy. Media lookup resolves a database ID to a validated archive member/cache file and allowlists MIME types; it never accepts a member path.

Authenticated members may read indexed filings. On-demand acquisition, reindex, raw-package download, and archive/cache administration use explicit operator/admin permissions; a public document does not imply arbitrary filesystem or pipeline access.

### 7.6 Filing Explorer and Company Analysis UI

Add routes:

- `/filings` — searchable Filing Explorer;
- `/filings/:docId` — filing workspace.

Add a `Filings` navigation item only after the coverage endpoint and empty/unavailable states are implemented.

Company Analysis gains a Filing History section containing submission time, covered period, form/document type, original/amendment relationship, XBRL/CSV availability, archive/parse status, audit-report presence, and quality badge. The list includes annual, amended annual, and any later approved report types; it does not mislabel every type-1 package as an annual report.

The filing workspace uses:

- a left document/section outline;
- a central structured, sanitized report view with Japanese text and tables;
- a right inspector for the selected fact, context, unit, dimensions, labels, footnotes, source member, and quality issues;
- tabs for Document, Statements, Facts, Audit, Taxonomy, and Metadata/Quality;
- in-filing text search and filters for period, consolidated state, concept, unit, and language;
- links back to Company Analysis and to the public EDINET document page.

Initial viewer acceptance is semantic completeness and safe navigation, not exact EDINET typography. A later rendering iteration may improve layout fidelity after representative filings pass accessibility, performance, and security checks.

### 7.7 Rendering and content-security boundary

Treat every filing member as hostile input even though it originated from EDINET:

- parse with external entities, DTDs, and network access disabled;
- allowlist structural HTML and safe style properties; remove scripts, forms, frames, objects, embeds, event handlers, active URLs, remote fonts, and external navigation;
- rewrite internal anchors and images to application-owned stable IDs;
- validate image decoders, MIME signature, dimensions, and decoded-pixel limits;
- render sanitized fragments through a sandbox that cannot execute scripts or access same-origin application state;
- keep plain-text fallback available when sanitization or rendering fails;
- version the sanitizer/parser and rebuild derivatives when either version changes.

Do not offer a “trust original HTML” switch. The raw package remains downloadable for expert offline inspection.

### 7.8 Phase 2D — CSV/XBRL parity and canonical-source gate

Run both sources for the pilot and a maintained reconciliation sample. Match by document, concept QName, period, entity, dimensions, unit, and fact semantics rather than row order or translated label.

Classify differences as:

- expected representation difference;
- CSV conversion omission;
- parser omission;
- transformation/scale/sign error;
- context/dimension mismatch;
- duplicate or conflicting filing fact;
- unresolved taxonomy/label issue.

Publish parity metrics by taxonomy release and document type. No standardized metric source switches merely because aggregate counts are close. Approve canonical XBRL sourcing only when critical statement facts, signs/scales, contexts, units, and amendments meet documented thresholds and every unexplained material difference is resolved.

After approval:

- standardization reads normalized XBRL facts through a repository interface;
- type-5 CSV remains a selectable compatibility/fallback source;
- every standardized run records source kind, artifact hash, parser version, mapping version, and selected fact ID;
- source changes are migration/version events and never silently rewrite historical as-of results.

### 7.9 Phase 2E — Metric provenance and quality

Add:

- `metric_catalog(metric_id, display_name, value_kind, unit_family, source_table, source_column, concept_qname, formula_json, formula_version, valid_from, valid_to)`;
- `observations(observation_id, company_code, metric_id, value_numeric, value_text, unit, currency, period_start, period_end, available_at, doc_id, source_kind, calculation_version)`;
- `observation_sources(observation_id, fact_id, source_fact_hash, extraction_rule, selection_reason, confidence)`;
- `observation_dependencies(observation_id, input_observation_id, role, transform_json)`;
- `quality_issues(issue_id, scope_kind, scope_ref, rule_code, rule_version, severity, detected_at, data_watermark, details_json, resolved_at)`.

Use deterministic hashes for raw fact references; do not depend on SQLite `rowid`. Generate provenance beside standardization so the exact selected source fact and extraction rule are known. Ratio and rolling-metric observations store dependency edges and formula versions.

Implement explicit, versioned rules:

- archive missing, invalid, changed, or no longer downloadable;
- incomplete/unparseable document, manifest, instance, Inline XBRL, taxonomy, audit section, or media;
- instance/Inline XBRL or CSV/XBRL disagreement;
- invalid continuation, transformation, scale, sign, context, dimension, decimals, precision, unit, or calculation relationship;
- missing expected period;
- stale or missing market price;
- conflicting values for equivalent contexts;
- unexpected unit or currency;
- scale change inconsistent with history and unit;
- ambiguous or fallback metric mapping;
- missing derived-metric input;
- amendment/restatement relative to an earlier filing;
- withdrawn or undisclosed filing selected;
- taxonomy release fallback;
- incomplete price, action, volume, listing, filing-archive, or parser coverage.

Return rule codes, evidence, severity, and version. Do not expose an unexplained confidence score. If a summary score is later added, its weights and formula must be visible.

Add:

- `POST /api/provenance/resolve`;
- `GET /api/data-quality/summary`;
- `GET /api/data-quality/issues`;
- `GET /api/data-quality/coverage`.

`POST /resolve` accepts stable company/metric/period or observation identifiers and supports batching. It returns the selected value, filing and artifact hash, source fact/context/unit, calculation dependencies, parser/extraction versions, amendments, and quality issues.

Extend Analysis history period descriptors with `doc_id`, `period_end`, `submitted_at`, and `available_at`. Keep the existing `periods: string[]` field during a compatibility window. Analysis and Screening cells open a reusable source-inspection drawer; derived formulas show a dependency tree.

### 7.10 Operations, recovery, and retention

- The archive is immutable and operator-owned. No automatic retention process deletes the only copy of a filing.
- The rendered cache is disposable and may use least-recently-used cleanup under a separate quota.
- `Filings.db` supports integrity checks, schema migrations, and full rebuild from document metadata plus archived packages.
- Reindex jobs create new parse runs and swap active indexes per filing only after success. A failed parser upgrade leaves the previous version readable.
- Backups include `auth.db` and `research.db`; `Filings.db` may be rebuilt, but archive manifests/checksums and operator-supplied packages must be included in the documented backup set.
- Coverage reports distinguish archive coverage, parse coverage, searchable-text coverage, normalized-fact coverage, and provenance coverage.
- Health/status reports show database bytes, archive bytes, cache bytes, free space, queued/backoff items, last successful metadata sync, parser version distribution, and permanent acquisition gaps.

### 7.11 Tests

- EDINET client timeout, cancellation, retry cap, `429`, `5xx`, `200` JSON error, truncated stream, wrong content type, and secret redaction.
- Atomic partial-file cleanup, restart reconciliation, duplicate hash, changed hash, quota, and free-space behavior.
- ZIP traversal, drive/UNC path, symlink, duplicate name, member count, compression ratio, expanded-byte, CRC, and unsupported-media boundaries.
- XML entity/DTD/network denial and bounded parser memory on adversarial fixtures.
- Instance and Inline XBRL parity for numeric, non-numeric, nil, language, scale, sign, transformation, continuation, footnote, typed dimension, divide unit, precision, and decimals cases.
- PublicDoc/AuditDoc separation, manifest ordering, extension taxonomy, all linkbase roles, section hierarchy, Japanese search, table rendering, image rewriting, sanitizer, CSP, and plain-text fallback.
- Original/amended/withdrawn/undisclosed filing resolution at multiple as-of times.
- Deterministic filing, section, fact, and observation IDs across rebuilds.
- CSV/XBRL reconciliation and canonical-source gate fixtures.
- Formula dependency graphs and cycle rejection.
- Every quality rule with positive and negative fixtures.
- Incremental download/rebuild idempotency, cancellation, resume, and parser-version reindex.
- API redaction, authorization, pagination, cursor stability, batching, response limits, raw-artifact disposition, and invalid media IDs.
- Filing Explorer, Company Analysis filing list, viewer tabs, fact inspector, quality drawer, empty state, unavailable state, and large-filing lazy loading.
- Measured query plans and bounded performance tests at pilot scale; no test reads the operator's 40 GiB `Base.db` or calls live EDINET.

### 7.12 Rollout and exit criteria

Rollout flags:

- `filing_archive_enabled`;
- `filing_viewer_enabled`;
- `xbrl_standardization_source` with `csv`, `shadow`, and `xbrl` states.

Start with `csv`; acquire and index in `shadow`; switch to `xbrl` only after the Phase 2D review. Rollback changes the source flag and active parser/index version; it does not delete archives.

Phase 2A-2C exit:

- New and on-demand eligible type-1 packages download through bounded durable jobs and survive restart.
- The Filing Explorer and Company Analysis list accurately show archived, unavailable, expired, failed, and unparsed states.
- A representative annual filing and audit report can be searched and safely inspected without serving original active content.
- Archive/database/cache sizes and free-space state are visible.

Phase 2D-2E exit:

- Approved source classes meet the documented CSV/XBRL parity gate.
- A displayed filing or derived value can be traced to an immutable artifact, source fact, context/unit, and parser/extraction/calculation version.
- Amendments do not rewrite earlier as-of traces.
- Quality issues are evidence-backed and filterable.
- Incremental refresh avoids full raw-table scans.
- Existing screens and analysis views remain usable if `Filings.db` has not yet been built, but clearly display archive/provenance as unavailable.

## 8. Phase 3 — Realistic point-in-time backtesting

### 8.1 Market and universe data

Add versioned market tables without immediately breaking `Stock_Prices` consumers:

- `Market_Prices(date, ticker, currency, open, high, low, close, adjusted_close, volume, provider, source_id, retrieved_at)`
- `Corporate_Actions(ticker, action_date, action_type, value, currency, provider, source_id, retrieved_at)`
- `Security_Aliases(company_code, ticker, valid_from, valid_to, exchange, source)`
- `Security_Listings(company_code, ticker, valid_from, valid_to, status, delisting_cash_value, source)`

The new engine uses these tables. Compatibility code may continue reading `Stock_Prices` until migrated.

### 8.2 As-of selection service

Create one shared selector used by historical screening and backtesting:

```text
decision timestamp
  -> filings with available_at <= decision timestamp
  -> apply withdrawal/amendment state visible at that timestamp
  -> choose latest valid filing per company/reporting period
  -> compute metrics using only eligible observations
  -> produce signal
  -> apply configured lag
  -> execute on next eligible market session
```

Historical screens must include an `as_of_policy_version` and data watermark in their result metadata.

### 8.3 Event-driven engine

Create `src/backtesting/simulation/`:

- `models.py` — versioned assumptions and result contracts.
- `calendar.py` — eligible sessions and execution lag.
- `universe.py` — listing/delisting and symbol aliases.
- `signals.py` — point-in-time screen output.
- `orders.py` — target-to-order conversion.
- `execution.py` — fill price, commissions, spread/slippage, and liquidity.
- `ledger.py` — cash, positions, lots, dividends, taxes, and corporate actions.
- `metrics.py` — gross/net performance, turnover, exposures, and costs.
- `diagnostics.py` — coverage, look-ahead, survivorship, and truncation checks.
- `engine.py` — orchestration only.

Assumptions include:

- signal timestamp and filing lag;
- next-open, next-close, or volume-weighted execution policy where data supports it;
- rebalance cadence;
- fractional-share policy;
- commission and minimum fee;
- spread/slippage model;
- maximum volume participation and minimum average daily value;
- dividend handling and reinvestment;
- withholding/capital-gain tax assumptions;
- cash yield;
- maximum position weight and uninvested-cash handling.

### 8.4 Correctness rules

- Do not use a same-day close for a signal created after that close.
- Apply splits to quantity and cost basis.
- Use actual dividend event dates rather than financial period end.
- Keep gross return, net return, transaction cost, tax, and slippage separately reconcilable.
- Calculate turnover from executed trades.
- A delisted security follows a recorded delisting action. If none exists, mark the run incomplete rather than inventing proceeds.
- Missing price/volume/action intervals produce named coverage diagnostics.
- Every result stores assumptions, engine version, data watermarks, and a deterministic configuration hash.

### 8.5 Compatibility and API

- Add `simulation_mode: "compatibility" | "point_in_time"` to typed requests.
- Keep existing saved requests readable by defaulting them to `compatibility`.
- Add `GET /api/backtesting/coverage`.
- Add `POST /api/backtesting/validate-assumptions`.
- Persist assumptions and diagnostics in result JSON and ZIP manifests.
- Store `owner_user_id` on saved definitions, runs, jobs, and artifacts; list/read/download/delete operations resolve ownership from the authenticated principal.
- Move large exports to server-owned result IDs; do not post completed rolling-result payloads back to export endpoints.

### 8.6 Frontend

- Add an Assumptions panel with presets and advanced controls.
- Show data coverage before Run.
- Show gross versus net results, cost/tax decomposition, turnover, fills, rejected orders, and cash.
- Add a Bias & Coverage result tab.
- Label compatibility results as simplified.
- Prevent a strict-mode run when mandatory data is missing unless the selected policy explicitly permits degraded coverage.

### 8.7 Tests

Golden toy-market cases:

- filing submitted after period end;
- amendment visible only to later decisions;
- signal-to-next-session lag;
- split and dividend;
- delisting;
- commission/minimum fee;
- slippage and volume cap;
- partial fill and cash remainder;
- tax and lot accounting;
- ticker alias change;
- missing corporate action;
- deterministic rerun from identical assumptions/watermarks.

Property/invariant tests:

- cash plus marked positions equals net asset value;
- gross minus costs/taxes reconciles to net;
- position quantity changes only through fills/actions;
- no observation has `available_at` after its decision timestamp;
- run hashes change when assumptions or data watermarks change.

### 8.8 Phase 3 exit criteria

- Strict runs contain no known look-ahead path in fixtures.
- Survivorship and action coverage are measured and displayed.
- Gross/net/cost/tax ledgers reconcile.
- Compatibility results remain readable.
- Point-in-time mode is not made the default until golden and representative regression results are reviewed.

## 9. Phase 4 — Alerts

### 9.1 Alert model

Add to `research.db`:

- `alert_rules(alert_id, owner_user_id, name, kind, scope_kind, scope_ref, condition_json, schedule_json, enabled, cooldown_seconds, created_at, updated_at, version)`
- `alert_rule_state(alert_id, owner_user_id, last_watermark, state_json, last_evaluated_at, last_error)`
- `alert_events(event_id, owner_user_id, alert_id, dedupe_key, company_code, effective_at, observed_at, title, details_json, status, created_at)`
- `alert_evaluation_runs(run_id, owner_user_id, started_at, completed_at, watermark, status, counts_json, error_message)`

Supported first-release kinds:

- new filing;
- saved-screen entry or exit;
- price threshold crossing;
- material metric change.

### 9.2 Evaluation behavior

- New filing uses `available_at` and filing identity.
- Screen transition compares the current member set with the last successfully evaluated snapshot.
- Price alerts trigger on a crossing, not on every observation above/below the threshold.
- Metric change compares eligible observations and identifies the source filings.
- Dedupe keys make retries idempotent.
- Cooldown suppresses repeated events without deleting evidence.
- Failed evaluation retains its prior watermark so the next run can retry safely.

Implement evaluators as pure functions plus a cancellable `evaluate_alerts` pipeline step. Add optional app-start and interval scheduling that submits owner-scoped work through the durable job manager under an explicit service principal. Record `origin="scheduler"` and owner on scheduled jobs; do not build a second untracked worker.

### 9.3 API and UI

- CRUD under `/api/research/alerts`.
- `GET /api/research/alert-events` with status/type/company pagination.
- `PATCH /api/research/alert-events/{event_id}` for read/dismissed state.
- `POST /api/research/alerts/evaluate` for a manual durable run.
- Add Alerts to `/research`, an unread count in the shell, and an Overview card.
- Event links open the filing, screen, price chart, or metric provenance drawer that caused the event.
- Display “evaluated through” and state clearly that the local application does not evaluate while stopped.

### 9.4 Tests and exit criteria

- Frozen-time tests for all alert types.
- Retry/dedupe/cooldown/watermark tests.
- Amendment and stale-price cases.
- App restart and interrupted evaluation.
- Watchlist-scoped rules.
- No duplicate event after a failed response or worker restart.
- Cross-account rule, event, watermark, manual evaluation, and scheduler isolation.
- No network notification channel is implied by the UI.

## 10. Phase 5 — Company comparison workspace

### 10.1 Backend

Create `src/comparison/`:

- `models.py`
- `service.py`
- `normalization.py`
- `percentiles.py`
- `formulas.py`
- `api.py`

Expose the existing deterministic peer service as a typed endpoint, then add:

- `GET /api/comparison/peers/{company_code}`
- `POST /api/comparison/snapshot`
- `POST /api/comparison/history`
- owner-scoped CRUD for reusable comparison templates under `/api/research/comparison-templates`

Requests specify company codes, as-of time, reporting-period basis, metrics/formulas, currency, scaling, and peer universe.

### 10.2 Calculations

- Income-statement common size uses revenue as denominator.
- Balance-sheet common size uses total assets.
- Cash-flow common size uses revenue only when available and labels the basis.
- Growth bridges show starting value, named drivers where calculable, and residual; do not fabricate driver attribution from totals alone.
- Margin bridges use consistent periods and units.
- Peer percentile calculation records universe, count, null policy, and tie method.
- Reusable formulas use the validated screening expression grammar but reference stable metric IDs.
- Currency conversion records the rate date and source.
- Mixed fiscal periods or incomplete company data remain visible.

### 10.3 Frontend

Add `/compare` with:

- multi-company search and reorder;
- optional suggested peers;
- summary metric matrix;
- common-size statements;
- growth/margin charts;
- valuation matrix and peer percentiles;
- reusable formula/template editor;
- provenance drawer from every source value.

Limit the first release to a reviewed maximum company count and metric count. Use virtualization or paged sections rather than returning an unbounded matrix.

### 10.4 Tests and exit criteria

- Common-size denominator edge cases.
- Currency, scale, fiscal-period, null, tie, and percentile cases.
- Formula validation and division by zero.
- As-of amendment behavior.
- Deterministic peer order.
- Template migration and compatibility.
- No comparison silently mixes unavailable periods or units.

## 11. Phase 6 — Portfolio attribution, tax lots, forecasts, Greeks, and scenarios

### 11.1 Tax-lot ledger

Add versioned Portfolio migrations:

- add `owner_user_id` to imported transactions, derived holdings, daily values, metrics, import manifests, and artifacts before adding analytics;
- `Tax_Lots(lot_id, owner_user_id, symbol, opened_at, source_transaction_id, original_quantity, remaining_quantity, cost_native, cost_base, currency, method_version)`
- `Tax_Lot_Disposals(disposal_id, owner_user_id, source_transaction_id, lot_id, quantity, proceeds_native, proceeds_base, cost_native, cost_base, realized_gain_native, realized_gain_base, fx_gain_base, matched_at)`
- `Tax_Lot_Assignments(owner_user_id, source_transaction_id, lot_id, quantity, created_at)` for explicit owner choices.

Stage existing global portfolio rows as `legacy-unclaimed` and require an explicit first-administrator claim before they appear in an account. Rebuild lots deterministically from the authenticated owner's normalized transactions. Allocate commissions/taxes consistently, handle cancellations and corporate actions, and reconcile lot quantity to holdings after every event.

The API supports preview before replacing derived lot tables. Material migrations and rebuilds take a backup and run as cancellable jobs.

### 11.2 Attribution

Implement layers in order:

1. Holding contribution using beginning weights and daily local/FX returns.
2. Currency attribution separating local return, FX return, and interaction.
3. Industry aggregation using dated classification where available.
4. Benchmark-relative allocation/selection/interaction only when benchmark constituent weights exist.

Use a documented multi-period linking method. If benchmark constituents are unavailable, label the result “return contribution,” not formal Brinson attribution.

### 11.3 Dividend forecasts

- Prefer announced dividend events with ex/payment dates.
- Otherwise provide a labeled run-rate estimate from recent distributions.
- Show gross, expected withholding, currency, and confidence basis.
- Never present a filing-period dividend value as a known payment date.

### 11.4 Greeks and scenarios

Reuse `src/portfolio/option_pricing.py` and surface:

- position and portfolio delta, gamma, theta, vega, and rho;
- quantity and contract multiplier;
- volatility source or explicit fallback assumption;
- time-to-expiry and risk-free-rate source.

Add deterministic scenarios:

- ticker and industry equity shocks;
- parallel and per-currency FX shocks;
- rate shocks where an instrument model supports them;
- volatility shocks for options;
- combined named scenarios.

Store owner-scoped scenario definitions in `research.db`. Return per-position P&L, grouped contribution, total impact, and assumptions. Monte Carlo and automatic hedging recommendations remain out of scope.

### 11.5 API and frontend

Typed endpoints:

- `/api/portfolio/tax-lots`
- `/api/portfolio/tax-lots/rebuild`
- `/api/portfolio/realized-pnl`
- `/api/portfolio/unrealized-pnl`
- `/api/portfolio/attribution`
- `/api/portfolio/dividend-forecast`
- `/api/portfolio/greeks`
- `/api/portfolio/scenarios/evaluate`
- research-state CRUD for scenario definitions.

Add Portfolio tabs for Attribution, Lots & P&L, and Scenarios. Every result states base currency, date range, data watermark, and assumptions.

Every Portfolio route derives the owner from the bearer principal. Uploads, rebuilds, transaction identifiers, charts, exports, and artifacts must pass cross-account IDOR tests; no request parameter may select another user.

### 11.6 Tests and exit criteria

- FIFO, average cost, explicit assignment, partial disposal, cancellation, split, option multiplier, commission/tax, and FX cases.
- Lot quantity/cost reconciliation.
- Daily contribution sums to portfolio return within tolerance.
- Multi-period linking and benchmark attribution fixtures.
- Finite-difference checks for Greeks.
- Scenario aggregation and currency conversion.
- Legacy portfolio claim and cross-account transaction, holding, analytics, rebuild, upload, and artifact isolation.
- Existing portfolio rebuild, performance, and import tests remain green.

## 12. Phase 7 — Reproducible research reports

### 12.1 Canonical report model

Create `src/reports/`:

- `models.py` — versioned `ReportDocument` and section contracts.
- `builder.py` — resolves server-owned IDs and freezes data.
- `manifest.py` — canonical JSON and SHA-256 inventory.
- `render_zip.py`
- `render_xlsx.py`
- `render_pdf.py`
- `store.py`
- `api.py`

The frontend sends a recipe referencing saved screens, companies, watchlists, backtest IDs, comparison templates, portfolio snapshots, and selected sections. It must not upload complete result objects for the server to echo into an export.

### 12.2 Artifact contents

Canonical ZIP:

```text
manifest.json
report.json
report.html
data/
  screen-definition.json
  selected-companies.csv
  observations.csv
  quality-issues.json
  assumptions.json
  backtest-summary.json
sources/
  filings.json
  provenance.json
charts/
  ...
```

The manifest includes:

- report schema version;
- application version;
- creation and as-of timestamps;
- recipe/configuration hash;
- database watermarks;
- engine/formula/rule versions;
- source document IDs;
- relative file paths, media types, byte sizes, and SHA-256 hashes;
- named omissions and coverage gaps.

Do not bundle raw licensed provider files unless the approved source policy permits redistribution. Document IDs, source metadata, selected observations, and hashes remain included.

### 12.3 Renderers

- ZIP is implemented first and is authoritative.
- XLSX uses a directly pinned dependency and escapes spreadsheet formula injection in user/provider text.
- PDF and HTML render from the same section model. Complete a Windows/PyInstaller and Japanese-font spike before selecting the PDF renderer.
- Charts are rendered from frozen report data, not re-queried independently.

### 12.4 Durable generation and storage

Add to `research.db`:

- `report_recipes(report_recipe_id, owner_user_id, name, definition_json, created_at, updated_at, version)`
- `report_runs(report_run_id, owner_user_id, report_recipe_id, job_id, status, as_of, config_hash, artifact_relpath, size_bytes, sha256, created_at, completed_at, error_message)`

Generate reports through a cancellable durable job. Write to a size-limited `.partial` artifact, validate the manifest, and atomically rename. Remove partial output on failure. Reuse the hardened path policy and introduce a separate configurable report-artifact limit.

### 12.5 API and frontend

- CRUD report recipes.
- `POST /api/reports/runs` returns `202` plus job/run IDs.
- Status, list, manifest, download, and delete endpoints use stable IDs.
- Add a report builder with section ordering, as-of preview, coverage summary, format selection, and artifact history.
- A “rebuild” action compares current watermarks with the frozen run and creates a new run; it never mutates the old artifact.

### 12.6 Tests and exit criteria

- Canonical JSON and deterministic hash.
- Every manifest checksum matches its file.
- Rebuild with identical frozen inputs produces equivalent data content.
- Missing source, changed watermark, and unsupported section behavior.
- Path traversal, ZIP member, HTML, Markdown, CSV, and XLSX formula-injection boundaries.
- Artifact limit and partial cleanup.
- Cross-account recipe, source-ID, run, manifest, download, rebuild, and delete isolation.
- XLSX opens, PDF renders required fonts, and the packaged Windows app can generate/download every selected format.

## 13. Phase 8 — Cross-feature release validation

### 13.1 Migration and rollback

- Test empty, current, and every supported prior schema version.
- Back up `auth.db`, `research.db`, `Portfolio.db`, and other material user databases before migration.
- Make `Filings.db` indexes, provenance, and lot tables rebuildable without deleting immutable filing archives.
- Never auto-delete accounts, credentials, token/audit history, notes, watchlists, alerts, scenarios, recipes, or prior report artifacts during migration.
- Document how to restore authentication safely, invalidate sessions after restoring an old backup, rescan filing archives, rebuild derived databases, and roll back an active parser/source version.

### 13.2 Security and privacy

- Enforce the exact authentication allowlist, bearer validation, permission matrix, owner scoping, TLS/host/proxy policy, and OpenAPI security contract for every router.
- Apply request, response, upload, and artifact limits.
- Validate all stable IDs and resolve every artifact path under configured roots.
- Redact passwords, all token classes, API keys, local paths, note contents, account identifiers not required for the event, and provider payloads from errors and logs.
- Treat ZIP members, XML/XBRL, submitted HTML/CSS, images, taxonomy labels, and filing links as untrusted input.
- Treat notes and imported labels as untrusted text in HTML, Markdown, CSV, XLSX, and PDF.

### 13.3 Performance

- All list endpoints paginate.
- Authentication token lookups and revocation checks use measured indexed queries; cleanup is bounded and never scans all audit history on a request.
- Provenance and alert refreshes use data watermarks.
- Filing downloads stream to bounded disk, parsers work in bounded batches, and list/detail pages never parse a package synchronously.
- Filing text, facts, taxonomy, and media load lazily; full-text and fact queries have measured indexes and result caps.
- Screening responses resolve provenance lazily.
- Comparison requests have company/metric limits.
- Backtests reuse loaded market, FX, action, and filing data across rolling periods.
- Report generation streams to bounded disk artifacts.
- Add indexes based on measured query plans, not speculative full-database indexes.

### 13.4 Bounded verification

Every command uses the existing timeout/process-tree controls.

| Check | Initial hard cap |
|---|---:|
| Focused backend test invocation | 60 seconds |
| Full Python unit stage | 120 seconds |
| Integration stage | 60 seconds |
| Frontend tests | 60 seconds |
| Frontend lint | 60 seconds |
| Frontend production build | 60 seconds |
| Static/docs/contracts stage | 60 seconds |
| Windows package and smoke test | 180 seconds |

If a stage reaches its cap, terminate its process tree, inspect the narrow cause, and reduce or isolate the work. Do not repeatedly raise the timeout without evidence.

### 13.5 Release exit criteria

- Focused, full unit, integration, frontend, static, documentation, migration, and contract stages pass.
- A packaged Windows smoke test creates/logs into an account and exercises at least one authenticated endpoint and page from every included feature.
- Every non-public API has an OpenAPI bearer requirement and negative missing/invalid/insufficient-scope tests.
- Cross-account isolation passes for every user-owned database row, job, stream, upload, and artifact.
- No test workspace, partial artifact, application process, or build child remains.
- Documentation covers data sources, assumptions, limitations, migrations, backups, and recovery.
- `docs/Application Details.md`, `docs/Frontend Architecture.md`, `docs/RUNNING.md`, and `docs/CHANGELOG.md` match the implemented release.

## 14. Review boundaries for each phase

Implement each phase in four operator-reviewable batches:

1. Schema, migrations, domain models, and fixtures.
2. Services, pipeline work, and API contracts.
3. Frontend vertical slice.
4. Integration, performance, docs, and package validation.

Do not commit changes. Stop at each phase boundary for operator review. A later phase may begin only when its dependency's stored and API contracts are accepted.

## 15. Recommended first implementation batch

After this revision is approved, implement the identity foundation before adding another protected API or user-owned feature:

1. Approve the Phase 0 authentication threat model, exact public allowlist, role/permission matrix, ownership matrix, registration/bootstrap behavior, token lifetimes, and remote TLS policy.
2. Add generated auth/ownership fixtures, pin and package-spike Argon2, then add the versioned `auth.db` schema, backup, and recovery command.
3. Implement account creation/login, Argon2 verification/rehash, throttling, opaque access/refresh tokens, rotation/reuse detection, logout/revocation, and authentication audit events.
4. Replace the remote-only static middleware with fail-closed account authentication and permission/ownership dependencies; update OpenAPI and migrate every frontend request, stream, export, and download to the authenticated client.
5. Add login/register/account/admin UI and cross-account/API security tests, then run the bounded milestone suite and packaged login smoke test.
6. Only after Phase 1A acceptance, run the separate bounded XBRL feasibility gate and begin owner-scoped research and Phase 2A filing work.

Do not combine authentication, ownership migrations, historical XBRL backfill, parser work, and the filing viewer into one change set.

## 16. Explicitly deferred beyond this plan

- Shared/team workspaces, collaborative editing, organizations/groups, and row-level sharing between accounts.
- MFA/passkeys, SSO/OIDC/SAML, social login, external identity providers, and delegated OAuth authorization.
- Email verification, email-based self-service recovery, and security notification delivery until an outbound provider is explicitly configured.
- SQLCipher or another transparent `auth.db` encryption layer; rely on hashed credentials/tokens, restrictive ACLs, backups, and volume encryption in the initial release.
- Cloud-hosted schedulers and guaranteed alerts while the local app is stopped.
- Email, Slack, Teams, mobile push, or SMS delivery.
- Automated trading or broker order submission.
- Tax-return preparation or jurisdiction-specific filing guarantees.
- Monte Carlo simulation, portfolio optimization, and AI-generated investment recommendations.
- Redistribution of provider data without an approved license.
- Routine archival of EDINET type-2 PDF, type-3 attachments, or type-4 English packages beyond an approved follow-up scope.
- Pixel-perfect replication of EDINET's viewer, execution of submitted active content, or automatic translation/summarization of filing narratives.
- A claim of complete survivorship-bias removal before historical universe coverage is demonstrated.
