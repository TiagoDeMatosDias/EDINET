# Knowledge Index

Updated: 2026-05-03

## Documentation structure
- `docs/Application Details.md` — Per-file API reference for `src/`, `src/web_app/`, tests, and config. Now includes mermaid architecture diagram.
- `docs/Frontend Architecture.md` — Web workstation frontend structure, screen modules, API routes. Now includes mermaid dataflow diagram.
- `docs/RUNNING.md` — How to run the app, all pipeline steps with config examples.
- `docs/LOGGING.md` — Logging setup and conventions.
- `docs/CHANGELOG.md` — Version history.
- `docs/Contributing.md` — Contribution guidelines.
- `docs/Feature Development/` — Historical implementation plans (marked COMPLETED).

## Current architecture state (2026-05-03)
- `src/orchestrator/` — Thin dispatcher with dynamically discovered step packages (no business logic).
- `src/orchestrator/common/edinet.py` — EDINET API wrapper (was `src/edinet_api.py`).
- `src/orchestrator/common/backtesting.py` — Backtesting logic (was `src/backtesting.py`).
- `src/orchestrator/common/db_config.py` — DB path resolution (`get_db2()`).
- `src/orchestrator/common/validation.py` — Pipeline validation.
- `src/web_app/` — FastAPI + vanilla JS frontend. All views (Dashboard, Orchestrator, Screening, Security Analysis) are fully functional.
- `src/web_app/api/screening.py` — Dedicated screening API routes at `/api/screening/*`.
- `src/web_app/api/security_analysis.py` — Security analysis API routes at `/api/security/*`.
- `src/screening/` — Screening backend package.
- `src/security_analysis/` — Security analysis backend package.
- `src/utilities/` — Shared utilities (logging, stock prices, URL building).

## Removed/archived modules
- `src/data_processing.py` — Removed. Functionality in `src/orchestrator/` step services.
- `src/edinet_api.py` — Removed. Replaced by `src/orchestrator/common/edinet.py`.
- `src/backtesting.py` — Removed. Replaced by `src/orchestrator/common/backtesting.py`.
- `src/orchestrator/populate_business_descriptions_en/` — Removed.
- `ui/` (Flet) — Removed in favor of `ui_tk/`.
- `ui_tk/` (Tkinter desktop UI) — Removed 2026-05-03 in favor of the web workstation (`src/web_app/`).
- `tests/test_ui_tk_smoke.py` — Removed with `ui_tk/`.
- `tests/test_ui_screenshots.py` — Removed with `ui_tk/`.
- `docs/Feature Development/Update User Interface.md` — Removed (historical Tk implementation plan).
- `docs/images/` — Tk screenshots removed; web screenshots forthcoming.
- `customtkinter`, `pillow` — Removed from `requirements.txt` (only used by tk UI).
