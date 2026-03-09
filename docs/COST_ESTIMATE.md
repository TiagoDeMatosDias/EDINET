# EDINET Project – Development Cost Estimate

This document provides a comprehensive estimate of the effort and cost required for a development team to build the EDINET financial data tool from scratch.

---

## Project Overview

EDINET is a Python-based desktop application that:
- Integrates with the Japanese EDINET securities API to download XBRL filings
- Parses and normalises XBRL financial data into a structured SQLite database
- Calculates financial ratios, per-share values, and statistical z-scores
- Performs univariate and multivariate OLS regression analysis to identify valuation predictors
- Supports portfolio backtesting with dividend adjustments and benchmark comparison
- Provides both a Flet Material Design GUI and a headless CLI execution mode

**Codebase summary (v0.2.0)**

| Component            | Lines of code |
|----------------------|---------------|
| Core source (`src/`) | 3,894         |
| GUI (`ui/`)          | 1,223         |
| Tests (`tests/`)     | 1,891         |
| Config / utilities   | ~125          |
| **Total**            | **~7,133**    |

---

## Assumed Team Composition

| Role                                 | Billing rate (USD/hr) | Time commitment  |
|--------------------------------------|-----------------------|------------------|
| Senior Python / Data Engineer        | $150–$175             | Full-time (1.0×) |
| Financial Domain Expert (consultant) | $175–$225             | Part-time (0.3×) |
| UI Developer (Flet / Material)       | $100–$130             | Part-time (0.5×) |
| QA / Test Engineer                   | $80–$110              | Part-time (0.4×) |

Rates reflect mid-market US contractor rates as of 2025–2026. Adjust for geography and seniority as needed.

---

## Development Phases

### Phase 1 – Project Setup & Architecture (1 week)
- Repository structure, CI/CD pipeline, linting & formatting rules
- Configuration system (singleton JSON loader, `.env` integration)
- Logging infrastructure (timestamped files, auto-archiving, GUI streaming)
- Pipeline orchestration skeleton

**Effort:** 1 senior engineer × 1 week = **~40 hrs**

---

### Phase 2 – EDINET API Integration (2.5 weeks)
- Authenticate and query the EDINET document list endpoint
- Bulk-download XBRL/CSV financial filings
- Parse and populate EDINET company code reference data
- Taxonomy XSD parsing for XBRL element definitions
- Retry logic, rate limiting, error handling

**Effort:** 1 senior engineer × 2.5 weeks = **~100 hrs**

---

### Phase 3 – XBRL Data Processing & Financial Ratios (4 weeks)
- Normalise raw XBRL elements into a clean relational schema
- Calculate per-share values (EPS, BPS, DPS, CFPS)
- Compute valuation ratios (P/E, P/B, EV/EBITDA, dividend yield, etc.)
- Growth rate and trailing-twelve-month aggregations
- Cross-sectional z-score normalisation for regression readiness
- Financial domain knowledge required throughout; domain expert involvement ~30%

**Effort:** 1 senior engineer × 4 weeks + 0.3× domain expert × 4 weeks  
= **~160 hrs (engineer) + ~48 hrs (domain expert) = ~208 hrs**

---

### Phase 4 – Stock Price API & CSV Import (1.5 weeks)
- Stooq API integration (fetch historical prices, handle missing data)
- User-supplied CSV import with flexible column mapping
- Upsert logic to keep the database current without duplication

**Effort:** 1 senior engineer × 1.5 weeks = **~60 hrs**

---

### Phase 5 – Statistical Regression Analysis (3 weeks)
- Univariate OLS sweep across all financial ratio predictors
- Multivariate OLS model with user-defined variable selection
- Winsorisation to reduce outlier influence
- Result formatting, significance filtering, and output export

**Effort:** 1 senior engineer × 3 weeks = **~120 hrs**  
Domain expert review: 0.3× × 1 week = **~12 hrs**

---

### Phase 6 – Portfolio Backtesting (4 weeks)
- Percentile-ranked stock selection from regression scores
- Weighted portfolio construction and rebalancing logic
- Dividend-adjusted total-return calculations
- Benchmark (index) comparison with relative performance metrics
- Turnover tracking and transaction-cost modelling

**Effort:** 1 senior engineer × 4 weeks = **~160 hrs**  
Domain expert review: 0.3× × 2 weeks = **~24 hrs**

---

### Phase 7 – Flet GUI Application (3 weeks)
- Material Design desktop shell (dark/light theme toggle)
- Database selector and connection management
- Drag-and-drop pipeline step ordering
- Per-step configuration dialogs (date ranges, thresholds, model parameters)
- Saved configuration presets
- Real-time log streaming panel

**Effort:** 0.5× UI developer × 3 weeks + 0.5× senior engineer × 3 weeks  
= **~60 hrs (UI dev) + ~60 hrs (engineer) = ~120 hrs**

---

### Phase 8 – Testing (3 weeks, integrated throughout)
- Unit tests for all core modules (backtesting, data processing, regression, API, utils)
- Mock-based API tests to avoid live network calls
- Edge-case coverage: missing data, encoding issues, empty result sets
- Target: ≥50% test-to-code ratio (the current codebase already achieves ~49%)

**Effort:** 0.4× QA engineer × 3 weeks = **~48 hrs**  
Senior engineer contribution (writing unit tests alongside features): **~60 hrs** (included in phases above)

---

### Phase 9 – Documentation & Packaging (1.5 weeks)
- User-facing docs: `Readme.md`, `RUNNING.md`, `LOGGING.md`, `CHANGELOG.md`, `Contributing.md`
- Google-style docstrings across all modules
- PyInstaller packaging for Windows executables (~200–300 MB bundles)
- `config/examples/` and `config/reference/` reference files

**Effort:** 0.5× senior engineer × 1.5 weeks = **~30 hrs**

---

### Phase 10 – Integration, Bug Fixes & Buffer (2 weeks)
- End-to-end pipeline runs against real EDINET data
- Performance profiling and database query optimisation
- Bug triage from QA and domain expert review
- Buffer for unforeseen scope (recommended 10–15% of total effort)

**Effort:** 1 senior engineer × 2 weeks + 0.4× QA × 1 week  
= **~80 hrs (engineer) + ~16 hrs (QA) = ~96 hrs**

---

## Summary of Effort

| Phase                                      | Senior Eng (hrs) | Domain Expert (hrs) | UI Dev (hrs) | QA (hrs) |
|--------------------------------------------|-----------------|---------------------|--------------|----------|
| 1 – Setup & Architecture                   | 40              | –                   | –            | –        |
| 2 – EDINET API Integration                 | 100             | –                   | –            | –        |
| 3 – XBRL Processing & Financial Ratios     | 160             | 48                  | –            | –        |
| 4 – Stock Price API & CSV Import           | 60              | –                   | –            | –        |
| 5 – Regression Analysis                    | 120             | 12                  | –            | –        |
| 6 – Backtesting                            | 160             | 24                  | –            | –        |
| 7 – GUI Application                        | 60              | –                   | 60           | –        |
| 8 – Testing                                | (included)      | –                   | –            | 48       |
| 9 – Documentation & Packaging              | 30              | –                   | –            | –        |
| 10 – Integration, Bug Fixes & Buffer       | 80              | –                   | –            | 16       |
| **Total**                                  | **810**         | **84**              | **60**       | **64**   |
| **Grand total across all roles**           |                 |                     |              | **1,018 hrs** |

---

## Cost Breakdown

### Low estimate (lower billing rates, lean team)

| Role             | Hours | Rate (USD/hr) | Cost        |
|------------------|-------|---------------|-------------|
| Senior Eng       | 810   | $150          | $121,500    |
| Domain Expert    | 84    | $175          | $14,700     |
| UI Developer     | 60    | $100          | $6,000      |
| QA Engineer      | 64    | $80           | $5,120      |
| **Total**        |       |               | **$147,320** |

### High estimate (higher billing rates, US market)

| Role             | Hours | Rate (USD/hr) | Cost        |
|------------------|-------|---------------|-------------|
| Senior Eng       | 810   | $175          | $141,750    |
| Domain Expert    | 84    | $225          | $18,900     |
| UI Developer     | 60    | $130          | $7,800      |
| QA Engineer      | 64    | $110          | $7,040      |
| **Total**        |       |               | **$175,490** |

### Estimated total range

> **$147,000 – $175,500 USD**  
> *(approximately 25–26 calendar weeks / 6 months with a 3-person team)*

---

## Timeline

Assuming parallel work across roles and standard 40-hour work weeks:

```
Month 1:  Setup, EDINET API, start XBRL processing
Month 2:  XBRL processing, stock price APIs, start regression
Month 3:  Regression analysis, start backtesting, GUI begins
Month 4:  Backtesting, GUI, testing in parallel
Month 5:  Integration, documentation, packaging
Month 6:  Bug fixes, buffer, release preparation
```

---

## Key Assumptions

1. **Domain knowledge**: Japanese financial reporting (XBRL / JPPFS taxonomy) requires specialist input. Without an experienced domain consultant, Phase 3 effort could increase by 30–50%.
2. **EDINET API stability**: The official EDINET API is government-operated. Downtime or schema changes could add unplanned work.
3. **Single-developer scenario**: A solo senior developer could complete the project in approximately 5–7 months calendar time (28–35 weeks) at comparable total hours.
4. **Infrastructure costs**: This estimate covers labour only. Hosting (if any future web deployment is added), API access, and software licences are not included.
5. **Ongoing maintenance**: Post-release support and feature additions are not included. Budget 10–20% of initial build cost per year for maintenance.
6. **No requirement for a web backend**: The current architecture is a local desktop application. A server-side or SaaS rewrite would significantly increase cost.

---

## Risk Factors

| Risk                                          | Likelihood | Impact | Mitigation                                         |
|-----------------------------------------------|------------|--------|-----------------------------------------------------|
| XBRL/JPPFS taxonomy complexity underestimated | Medium     | High   | Engage domain expert early; prototype Phase 3 first |
| EDINET API changes or downtime                | Low–Medium | Medium | Implement robust retry logic; mock API for dev/test |
| Backtesting edge cases (corporate actions)    | Medium     | Medium | Comprehensive test suite; validate against known data|
| GUI scope creep                               | Medium     | Medium | Define UI spec before development; time-box Phase 7 |
| Solo developer burnout / turnover             | Low        | High   | Maintain thorough documentation; code review culture |

---

*Estimate prepared: March 2026. All figures are indicative and should be validated with team leads and stakeholders before project initiation.*
