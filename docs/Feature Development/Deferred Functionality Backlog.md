# Deferred Functionality Backlog

Status: Core vertical slices implemented; follow-up depth remains
Recorded: 2026-07-22
Updated: 2026-07-30
Implementation plan: [Deferred Functionality Implementation Plan.md](Deferred%20Functionality%20Implementation%20Plan.md)

## Purpose

This file preserves the original deferred-product themes and records their current disposition. The implementation plan remains the design and sequencing record; current runtime behavior is documented in [../USER_GUIDE.md](../USER_GUIDE.md), [../RUNNING.md](../RUNNING.md), and [../Application Details.md](../Application%20Details.md).

## Implemented core slices

### Account creation and token-authenticated APIs

- Dedicated `auth.db` with Argon2id password hashes, users, roles, rotating sessions, personal API tokens, throttling, invitations, resets, and audit state.
- Public login/register and authenticated Account/Admin views.
- Owner-scoped research, portfolio, backtest, report, job, and artifact contracts where implemented.
- Administrator-controlled 15–128 character minimum password policy.
- `EDINET_API_TOKEN` remains an outbound provider credential and is never accepted as an application credential.

### XBRL filing archive and viewer

- Type-1 acquisition with `explicit`, `backfill`, and `all` modes, document-type filtering, five-download concurrency cap, HTTP reuse, and batched status writes.
- Compact `Filings.db` storage: compressed source ZIPs, artifact metadata, contexts/units, numeric non-nil facts, quality issues, and on-demand narrative reconstruction.
- Filing Explorer landing coverage, shared company search, dedicated filing viewer, and Company Analysis filing links.
- Sanitized source HTML plus Japanese/complete-English side-by-side report and section views. Validated Argos translations are versioned and cached in `Filings.db`.
- `generate_financial_statements` can use compact filing facts instead of the legacy CSV source.

### Watchlists, research notes, and alerts

- Favorites and named watchlists are ordinary private tags shared by Analysis, Research, and Screening.
- Owner-scoped notes, revisions, thesis status, targets, review dates, and in-app alert rules/events are stored in `research.db`.
- Analysis, Comparison, Filings, Research, and the global header share one best-effort company finder.

### Company comparison workspace

- Bounded 2–12 company comparison using Company Analysis data contracts.
- Standard market, valuation, quality, income, and balance-sheet metrics.
- Searchable arbitrary numeric `Table.Column` metrics with per-metric removal.
- Common-size income/balance-sheet rows and optional selected-peer percentiles.

### Backtesting, portfolio previews, and reports

- Point-in-time observation and execution-cost primitives plus rolling saved-screen backtests with explicit assumptions, progress, cancellation, and artifacts.
- Owner-scoped tax-lot, Greeks, and deterministic scenario preview APIs.
- Owner-scoped reproducible report recipes/runs with canonical manifests, bounded atomic ZIP artifacts, checksums, and downloads.

## Partially implemented; follow-up remains

### Metric provenance and data quality

Implemented foundations include filing/fact metadata, normalized observations, source/dependency tables, parser quality issues, data watermarks, and filing audit views. Remaining work is consistent value-level provenance drill-down from every Analysis, Screening, Comparison, Backtest, and report cell, plus broader anomaly/confidence rules.

### More realistic point-in-time backtesting

Availability dates, execution costs, rolling screens, and bias-aware primitives exist. Remaining work includes measured historical-universe/delisting coverage, fuller corporate-action handling, liquidity/tax models, and explicit survivorship diagnostics across every supported source.

### Portfolio attribution and scenarios

Tax-lot, Greeks, and deterministic shock engines exist as authenticated previews. Remaining work is durable scenario CRUD, complete attribution and lot UIs, richer dividend forecasts, and reconciliation against imported broker statements.

### Reproducible research reports

Canonical ZIP reports are implemented. XLSX/PDF renderers, richer chart freezing, packaged Japanese-font validation, and the complete frontend report-builder workflow remain follow-up work.

## Still deferred beyond the current product

- Shared/team workspaces, organizations, collaborative editing, and row-level sharing.
- MFA/passkeys, SSO/OIDC/SAML, social login, and external identity providers.
- Email-based verification/recovery and security notifications until an outbound provider is configured.
- Cloud scheduling and guaranteed alerts while the local application is stopped.
- Email, Slack, Teams, mobile push, and SMS alert delivery.
- Automated trading or broker order submission.
- Tax-return preparation or jurisdiction-specific filing guarantees.
- Monte Carlo simulation, portfolio optimization, and AI-generated investment recommendations.
- Provider-data redistribution without an approved license.
- A claim of complete survivorship-bias removal before historical-universe coverage is demonstrated.

## Review criteria for remaining work

Evaluate each follow-up against user value, source availability/licensing, financial correctness, storage/performance, migration and recovery behavior, ownership/security, test cost, and packaged-Windows support. Implement from an approved current plan, not directly from this backlog.
