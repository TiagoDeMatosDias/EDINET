# Deferred Functionality Backlog

Status: Deferred  
Recorded: 2026-07-22  
Planning trigger: Revisit after the Project Hardening Plan reaches final acceptance.

## Purpose

Preserve promising product ideas from the project review without expanding the current implementation scope. These entries are reminders, not approved designs, estimates, or commitments.

## Deferred ideas

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

Create a separate approved feature plan before implementation. Do not implement directly from this backlog.
