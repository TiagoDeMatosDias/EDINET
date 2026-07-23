# Deferred Functionality Backlog

Status: Planning
Recorded: 2026-07-22
Planning trigger: Revisit after the Project Hardening Plan reaches final acceptance.
Implementation plan: [Deferred Functionality Implementation Plan.md](Deferred%20Functionality%20Implementation%20Plan.md)

## Purpose

Preserve the original product ideas from the project review. Detailed sequencing, design, and acceptance criteria now live in the linked implementation plan. This backlog remains the source list; the plan controls implementation.

## Deferred ideas

### Account creation and token-authenticated APIs

- Add configurable account authentication with registration, login, logout, session management, and administrator account controls.
- Keep users, Argon2id password hashes, roles, invitations, token hashes, throttling, and authentication audit events in a dedicated non-rebuildable `config/state/auth.db`.
- Authenticate every non-public API with revocable opaque bearer access or personal API tokens; use rotating refresh tokens for browser sessions.
- Generate application authentication tokens independently. `EDINET_API_TOKEN` remains exclusively an outbound EDINET download credential and is never accepted by account/bootstrap/inbound authentication code.
- Allow unauthenticated loopback compatibility only when authentication is explicitly disabled. Require account mode, trusted hosts, and HTTPS for remote binding.
- Scope research, portfolios, backtests, alerts, reports, jobs, and artifacts to their owner, with explicit operator/administrator permissions for shared pipeline and account administration.

### XBRL filing archive and viewer

- Download and retain eligible EDINET type-1 packages alongside the existing type-5 CSV ingestion.
- Index Inline XBRL, XBRL instances, filing-specific taxonomies, narrative sections, images, and audit reports.
- Add a dedicated Filing Explorer and filing workspace, with Company Analysis listing each company's available reports.
- Keep immutable ZIP packages on disk and use a separate rebuildable filing/index database; select SQLite-only versus a SQLite/partitioned-fact hybrid from a measured pilot.
- Introduce XBRL as a standardized-data source only after versioned CSV/XBRL parity checks pass.

### Metric provenance and data quality

- Trace displayed values to filing, document ID, taxonomy concept, context, unit, period, extraction rule, and restatement lineage.
- Flag missing periods, stale prices, scale anomalies, conflicting contexts, and low-confidence mappings.
- Provide a source inspection view from screening and company analysis.

### Watchlists, research notes, and alerts

- Build saved watchlists around the existing company-tag capability.
- Add thesis notes, target values, review dates, and structured research status.
- Alert on new filings, screen entry/exit, price thresholds, and material metric changes.

### More realistic point-in-time backtesting

- Model filing-availability lag, delisted securities, corporate actions, transaction costs, slippage, liquidity limits, taxes, and turnover.
- Add explicit survivorship-bias and look-ahead-bias diagnostics.
- Version assumptions alongside results.

### Company comparison workspace

- Compare multiple companies with common-size statements, growth and margin bridges, valuation matrices, peer percentiles, and reusable formulas.

### Portfolio attribution and scenarios

- Add holding, currency, industry, and benchmark return attribution.
- Add tax lots and realized/unrealized P&L views.
- Add dividend forecasts, options Greeks, and FX/equity/rate stress scenarios.

### Reproducible research reports

- Export versioned research packages containing the screen definition, as-of date, source filings, selected companies, charts, assumptions, and backtest results.
- Consider Excel, PDF, and machine-readable ZIP formats.

## Future planning requirements

When hardening is complete, evaluate each idea against:

- User value and frequency of use.
- Data availability and licensing.
- Financial-correctness and audit requirements.
- Storage and performance impact.
- API/frontend complexity.
- Test and maintenance cost.
- Whether the idea builds on the provenance and job infrastructure established by hardening.
- Authentication threat model, password/token policy, registration/bootstrap mode, TLS deployment, permission matrix, ownership migration, account recovery, and cross-account isolation.
- For XBRL work, measured compressed/expanded storage, parser coverage, EDINET viewing-period gaps, safe rendering, and CSV parity.

Review and approve the separate feature plan before implementation. Do not implement directly from this backlog.
