# Knowledge Index

Updated: 2026-05-02

## Documentation structure
- `docs/Application Details.md` — Per-file API reference for `src/`, `ui_tk/`, tests, and config. Now includes mermaid architecture diagram.
- `docs/Frontend Architecture.md` — Web workstation frontend structure, screen modules, API routes. Now includes mermaid dataflow diagram.
- `docs/RUNNING.md` — How to run the app, all pipeline steps with config examples.
- `docs/LOGGING.md` — Logging setup and conventions.
- `docs/CHANGELOG.md` — Version history.
- `docs/Contributing.md` — Contribution guidelines.
- `docs/Feature Development/` — Historical implementation plans (marked COMPLETED).

## Current architecture state (2026-05-02)
- `src/orchestrator/` — Thin dispatcher with dynamically discovered step packages (no business logic).
- `src/orchestrator/common/edinet.py` — EDINET API wrapper (was `src/edinet_api.py`).
- `src/orchestrator/common/backtesting.py` — Backtesting logic (was `src/backtesting.py`).
- `src/orchestrator/common/db_config.py` — DB path resolution (`get_db2()`).
- `src/orchestrator/common/validation.py` — Pipeline validation.
- `src/web_app/` — FastAPI + vanilla JS frontend. Screening is fully functional; Security Analysis is a stub.
- `src/web_app/api/screening.py` — Dedicated screening API routes at `/api/screening/*`.
- `src/screening/` — Screening backend package.
- `src/security_analysis/` — Security analysis backend package.
- `ui_tk/` — Tk desktop GUI (primary UI).
- `src/utilities/` — Shared utilities (logging, stock prices, URL building).

## Removed/archived modules
- `src/data_processing.py` — Removed. Functionality in `src/orchestrator/` step services.
- `src/edinet_api.py` — Removed. Replaced by `src/orchestrator/common/edinet.py`.
- `src/backtesting.py` — Removed. Replaced by `src/orchestrator/common/backtesting.py`.
- `src/orchestrator/populate_business_descriptions_en/` — Removed.
- `ui/` (Flet) — Removed in favor of `ui_tk/`.
