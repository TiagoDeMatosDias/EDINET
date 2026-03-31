## Update User Interface — Implementation-ready Plan

Purpose
-------
Make the existing UI implementation-ready for an incremental migration from the current Flet-based `ui/` to a new, maintainable Tkinter-based `ui_tk/` package. This document is prescriptive: it converts the high-level decision into a prioritized, testable, code-level checklist so the team can start implementation immediately.

Scope & Goal
------------
- Create a parallel `ui_tk/` package (Tkinter + `ttk` + `matplotlib`) and keep `ui/` operational until parity.
- Decouple UI from business logic via thin controllers that call existing backend modules (`orchestrator.py`, `backtesting.py`, `edinet_api.py`).
- Provide a background-worker pattern that keeps the Tk mainloop responsive and safe.
- Deliver per-page, testable feature parity starting with the Pipeline page.

Immediate deliverables
----------------------
1. `docs/ui-component-inventory.md` — component inventory and prioritised feature list.
2. `ui_tk/` package scaffold with `run_tk_app()` entrypoint.
3. Pipeline page prototype (UI + background runner + chart placeholder).
4. Controller adapters for the pipeline page and unit tests for controllers.
5. Packaging updates and a QA checklist to verify builds.

Actionable implementation steps (developer checklist)
--------------------------------------------------
1) Discovery & inventory (owner: developer)
   - Produce `docs/ui-component-inventory.md` with rows: `page | widget | data source | triggers | long-running? | notes` and add screenshots. Prioritise by user impact.
   - Deliverable: inventory file + a short list of must-have vs nice-to-have features.

2) Architecture & CLI integration (owner: lead)
   - Add `--ui` CLI flag to `main.py`. Example:

```python
import argparse
from ui import run as run_flet
from ui_tk import run_tk_app

parser = argparse.ArgumentParser()
parser.add_argument('--ui', choices=('flet','tk'), default='flet')
args = parser.parse_args()
if args.ui == 'tk':
    run_tk_app(config)
else:
    run_flet(config)
```

3) Scaffolding (owner: developer)
   - Create `ui_tk/` with files:
     - `ui_tk/__init__.py` (exports `run_tk_app`) 
     - `ui_tk/app.py` (bootstrap: Tk root + main frame)
     - `ui_tk/controllers.py` (adapters to backend)
     - `ui_tk/utils.py` (background runner helpers)
     - `ui_tk/shared/widgets.py` (common widgets)
     - `ui_tk/pages/pipeline.py`, `settings.py`, `results.py`
   - Add `tests/test_ui_tk_smoke.py` with an import/short-launch smoke test that uses mocked controllers.

4) Pipeline prototype (owner: developer)
   - Build `ui_tk/pages/pipeline.py` with:
     - Inputs (`ttk.Entry`/`ttk.Combobox`), `Start`/`Stop` buttons, progress label, and a `matplotlib` placeholder canvas.
     - Wire button callbacks to controllers via `ui_tk/controllers.py` using the background runner.
   - Deliverable: runnable prototype that starts, displays progress, updates chart area with mock data.

5) Background worker pattern (owner: developer)
   - Implement `ui_tk/utils.py` with these primitives:
     - `ThreadPoolExecutor`-based runner
     - `queue.Queue()` to transport results/events to main thread
     - `poll_events(root)` called via `root.after(100, poll_events, root)`
   - Minimal pattern (pseudo):

```python
import queue, threading
from concurrent.futures import ThreadPoolExecutor

executor = ThreadPoolExecutor(max_workers=2)
event_q = queue.Queue()

def run_in_background(fn, args=(), on_done=None):
    fut = executor.submit(fn, *args)
    def cb(f):
        try:
            res = f.result()
            event_q.put(('done', on_done, res))
        except Exception as e:
            event_q.put(('error', e))
    fut.add_done_callback(cb)

def poll_events(root):
    try:
        while True:
            typ, data, payload = event_q.get_nowait()
            if typ == 'done' and data:
                data(payload)  # safe main-thread callback
    except queue.Empty:
        pass
    root.after(100, poll_events, root)
```

6) Controllers & testing (owner: developer)
   - Controllers expose small, testable functions like `start_backtest(params) -> job_id` and `fetch_results(job_id) -> ResultObject`.
   - Unit tests (e.g., `tests/test_ui_tk_controllers.py`) should mock `orchestrator` and `backtesting` calls and validate mapping, error handling and result normalization.

7) Per-page migration checklist (repeatable template)
   For each page (Pipeline, Settings, Backtesting Runner, Results, Screener/Research):
   - [ ] Create prototype layout (`ui_tk/pages/<page>.py`).
   - [ ] Implement controller adapters (`ui_tk/controllers.py`).
   - [ ] Add unit tests for controllers (`tests/test_<page>_controllers.py`).
   - [ ] Add integration smoke test that calls a small backend run.
   - [ ] Polish UX: keyboard navigation, tooltips, error messages.

8) Charts & tables
   - Use `matplotlib` + `FigureCanvasTkAgg` for static charts. For tables use `ttk.Treeview` with helper methods for sorting and exporting CSV.
   - If later interactivity is required (pan/zoom/hover), evaluate `plotly + webview` but note packaging complexity.

9) Tests, QA & CI
   - Tests to add:
     - `tests/test_ui_tk_smoke.py` — import + short-run with mocks
     - `tests/test_ui_tk_controllers.py` — unit tests for adapters
   - CI: run unit tests and smoke test in CI. Keep smoke tests short (use small mocked dataset).

10) Packaging & release (PyInstaller)
    - Update `requirements.txt` to include at least: `matplotlib`, `pillow`.
    - EDINET.spec notes: include `ui_tk` package and ensure `matplotlib.backends.backend_tkagg` is available. Example additions:

```python
# in EDINET.spec (conceptual)
hiddenimports = ['tkinter', 'matplotlib.backends.backend_tkagg']
datas = [('ui_tk/icons', 'ui_tk/icons'), ('config/examples', 'config/examples')]
```

    - Build and smoke the EXE on the target platforms. Watch for missing-data errors and matplotlib backends.

Acceptance criteria (ready-for-implementation)
-------------------------------------------
- The document `docs/ui-component-inventory.md` exists and lists all UI pages and long-running operations.
- `ui_tk/` scaffold is created and `python main.py --ui tk` starts the Tk app (bootstrap) without raising import errors.
- Pipeline prototype can start a short background job, update progress in UI and render a simple chart.
- Controllers have unit tests that mock backend calls and verify behavior.
- PyInstaller build includes `ui_tk` and produces a runnable EXE on a developer workstation.

Dependencies (suggested minimal versions)
----------------------------------------
- matplotlib >= 3.6
- pillow >= 9.0
- pytest >= 7.0 (for tests)

QA checklist (manual)
--------------------
1. Launch: `python main.py --ui tk` (optionally add `-c config/run_config.json`).
2. Open Pipeline page; enter parameters; click `Start`.
3. Confirm UI remains responsive and status/progress updates appear.
4. Confirm chart area updates with mock or real data.
5. Export results (CSV/PDF) and open the file.

Open decisions (record here and resolve before wide rollout)
-----------------------------------------------------------
- Theme: standard `ttk` vs `ttkbootstrap` (trade-off: extra dependency vs modern look).
- Charts: keep `matplotlib` or adopt interactive `plotly` (packaging trade-offs).
- Scope: full feature parity before removing `ui/` or keep both long-term with one canonical UI.

Next immediate steps (this sprint)
---------------------------------
1. Produce `docs/ui-component-inventory.md` (run a quick audit of `ui/`).
2. Scaffold `ui_tk/` package with the file list above and a minimal `run_tk_app()` that creates a root and shows an empty frame.
3. Implement Pipeline prototype and unit tests for its controller.

## Mockup (ASCII)
Below is an updated ASCII mockup that matches the screenshot and your requested layout changes:
- Log output moved to the bottom (full-width).
- Step configuration appears in a right-side panel when editing a step.
- Top tabs (`Home` / `Orchestrator` / `Data`) act as "views" that change the main area.
- A simple `Home` screen allows selecting and switching views.

Orchestrator view (default) — step config panel closed:

```
 ___________________________________________________________________________
| SHADE Research                 Home  |  Orchestrator  |  Data    [API Key] |
| [hex logo]                                                    (theme) (⚙)  |
|---------------------------------------------------------------------------|
|  +--------------------------+  +---------------------------------------+  |
|  | Pipeline Steps           |  |             Main Area (Orchestrator) |  |
|  | ------------------------ |  |  - shows selected step details or     |  |
|  | [≡] [■] Get Documents   (⚙) |  |    an overview of the pipeline        |  |
|  | [≡] [■] Download Docs   (⚙) |  |  - drag to reorder, click to select   |  |
|  | [≡] [■] Populate Company (⚙)|  |  - when not configuring a step,       |  |
|  | [≡] [■] Parse Taxonomy  (⚙) |  |    the right config panel is hidden   |  |
|  | [≡] [■] Update Prices   (⚙) |  |                                       |  |
|  | [≡] [■] Import Prices   (⚙) |  |                                       |  |
|  | [≡] [■] Generate FStmts (⚙) |  |                                       |  |
|  | [≡] [■] Generate Ratios [ ] |  |                                       |  |
|  | [≡] [✔] Multivar Regr  (⚙)  |  |                                       |  |
|  | [≡] [■] Backtest Port   (⚙) |  |                                       |  |
|  |                          |  |                                       |  |
|  | [ Save Setup ] [ Load ]  |  |                                       |  |
|  +--------------------------+  +---------------------------------------+  |
|                                                                           |
|  -----------------------------------------------------------------------  |
|  |                            Log Output (bottom)                       |  |
|  |  [timestamp] Started step: Get Documents                              |  |
|  |  [timestamp] Downloaded 123 files                                     |  |
|  |  ...                                                                  |  |
|  |                                                                       |  |
|  -----------------------------------------------------------------------  |
|                                  [ ◀ Stop ]         [ ▶ Run ]            |
|___________________________________________________________________________|
```

Orchestrator view — step config panel open (select step -> Enter):

```
 ___________________________________________________________________________
| SHADE Research                 Home  |  Orchestrator  |  Data    [API Key] |
|---------------------------------------------------------------------------|
|  +--------------------------+  +---------------------------+ +-----------+  |
|  | Pipeline Steps           |  |     Main Area (summary)   | | Config    |  |
|  | ------------------------ |  |                           | | Panel     |  |
|  | [≡] [■] Get Documents   (⚙)|  |  - selected: Get Documents| |  Title    |  |
|  | [≡] [■] Download Docs   (⚙)|  |  - last run: 00:12       | |  Field A  |  |
|  | [≡] [■] Populate Co.    (⚙)|  |  - status: idle          | |  Field B  |  |
|  | ...                      |  |                           | |  Checkbox |  |
|  |                          |  |                           | |  [Save]   |  |
|  +--------------------------+  +---------------------------+ +-----------+  |
|                                                                           |
|  -----------------------------------------------------------------------  |
|  |                            Log Output (bottom)                       |  |
|  |  [timestamp] User opened config for: Get Documents                    |  |
|  |  [timestamp] Config saved                                              |  |
|  -----------------------------------------------------------------------  |
|                                  [ ◀ Stop ]         [ ▶ Run ]            |
|___________________________________________________________________________|
```

Home view — simple selector to pick a view:

```
 ___________________________________________________________________________
| SHADE Research                 Home  |  Orchestrator  |  Data    [API Key] |
|---------------------------------------------------------------------------|
|   Welcome — choose a view:                                                   |
|                                                                           |
|   [ Orchestrator ]   [ Data ]   [ Reports ]   [ Settings ]                |
|                                                                           |
|   Recent setups:                                                          |
|   - My default pipeline                                                     |
|   - Quick import + ratios                                                   |
|                                                                           |
|   [Open last setup]     [New setup]                                       |
|---------------------------------------------------------------------------|
|  -----------------------------------------------------------------------  |
|  |                            Log Output (bottom)                       |  |
|  |  (global application log / messages)                                 |  |
|  -----------------------------------------------------------------------  |
|                                  [ Quit ]                                |
|___________________________________________________________________________|
```

Behavior details (acceptance & interactions)
- Log Output: always shown at the bottom across all views; scrollable, time-stamped, and can be cleared/exported.
- Config Panel: opens on the right when the user selects a step and presses Enter (or clicks gear). While open:
    - Focus remains in the config panel; `Tab` cycles through fields; `Tab` back out to the pipeline list to pick next step (as requested).
    - Saving updates the underlying setup and appends a log entry.
    - Panel can be closed with `Esc` or a close button.
- Views:
    - `Home` shows view selector and recent setups.
    - `Orchestrator` shows pipeline and step-level interaction.
    - `Data` (placeholder) can show data import/status pages (design later).
    - Users can switch views at any time; switching does not lose unsaved step config if the config panel is open (prompt to save).

Acceptance criteria (mockup updates)
- Log area is moved to bottom, visible in all views.
- Step configuration appears in a right-hand panel and is keyboard-accessible (Enter to open, Tab navigation).
- Top tabs behave as view selectors; `Home` is implemented as an initial screen and is reachable from all views.
- Mockup in the docs matches the new layout and clearly communicates interaction flow for implementers.

## Design Review — Developer Guidance

Purpose
-------
Provide clear implementation guidance, rationale, and concrete patterns so future developers can preserve the intended UX while implementing the `ui_tk` migration. This section captures visual, interaction, and engineering rules to reduce rework and maintain consistency.

Key decisions (summary)
-----------------------
- Keep `ui_tk/` parallel to the existing `ui/` until feature parity is reached.
- Use a persistent right-side slide-over for step configuration (keyboard-openable with `Enter`).
- Use a bottom, global log area visible across all views (collapsible, filterable).
- Views (`Home` / `Orchestrator` / `Data`) are top-level selectors that change the main workspace.

Visual & style guidelines
-------------------------
- Spacing & grid: follow an 8px baseline rhythm for padding, margins and gaps.
- Elevation: use subtle elevation or borders to separate panels (central workspace > left step list > right config panel).
- Palette: document token names in `ui_tk/style.py`: `color.primary`, `color.surface`, `color.text`, `color.success`, `color.warning`, `color.error`.
- Typography: prefer the platform UI font (e.g., Segoe UI on Windows). Use a consistent scale: body (14–16px), headings (18–22px).
- Buttons & affordances: primary action (Run) is filled with the primary accent; secondary actions are outline/ghost.
- Iconography: use a single icon style and keep text labels alongside icons for clarity.

Layout & interaction rules
--------------------------
- Left column: compact list of pipeline steps with a visible drag handle, icon, primary label, small status chip and a single configure affordance (gear or Enter).
- Main area: shows either a detailed overview of the selected step or an overall pipeline canvas depending on the active view.
- Right config panel: slide-over panel that is keyboard accessible. Open rules:
    - Focus: select step -> press `Enter` (or click gear) opens panel and focuses first input.
    - Tab behavior: `Tab` moves through fields inside the panel. Once at the end, `Tab` returns to pipeline list (config panel should not trap Tab permanently).
    - Close: `Esc` or click Close. Unsaved changes: prompt or auto-save policy determined per-field (decide in implementation).
- Bottom log: persistent, resizable height, with controls for filtering (Info/Warnings/Errors), search, and auto-scroll toggle. Auto-scroll only when the user is at the bottom; otherwise show a 'New messages' indicator.

Component & engineering patterns
--------------------------------
- Architectural contract: UI pages (views) call adapter functions in `ui_tk/controllers.py` which translate UI inputs to backend calls (no business logic in UI code).
- Controller contract examples:

```python
def start_pipeline_step(step_id: str, params: dict) -> str:
        """Start an asynchronous step; return a job_id immediately."""

def get_step_status(job_id: str) -> dict:
        """Return normalized status: {state: 'running'|'done'|'error', progress: 0.0, details: {...}}"""
```

- Background / threading model (required):
    - Keep Tkinter mainloop on the main thread.
    - Use `concurrent.futures.ThreadPoolExecutor` for IO-bound background tasks.
    - All results and UI updates must be marshalled through an event queue polled from the main thread (use `root.after(...)`).

Background helper API (recommended)
----------------------------------
Implement and export a small helper in `ui_tk/utils.py`:

```python
from typing import Callable

def run_in_background(fn: Callable, args: tuple=(), on_done: Callable=None, on_error: Callable=None) -> concurrent.futures.Future:
        """Run fn in a thread; call on_done(result) on main thread via the event queue when finished."""

# Event queue is processed via root.after(100, poll_events, root)
```

File layout & naming conventions
-------------------------------
- `ui_tk/app.py`: bootstrap, root creation, view selection and global wiring (event queue, executor).
- `ui_tk/pages/<view>.py`: each view exposes a `Frame`/`Container` factory (e.g., `build_pipeline_frame(root, controllers)`).
- `ui_tk/controllers.py`: pure adapter functions that import and call `orchestrator.py`, `backtesting.py`, and `edinet_api.py`.
- `ui_tk/shared/widgets.py`: reusable widgets (status chip, primary toolbar, labeled-entry, small utility wrappers).
- `ui_tk/utils.py`: executor, queue, poller, helpers for scheduling tasks and main-thread callbacks.

Testing guidance
----------------
- Unit tests: focus on `controllers.py` behavior; mock out `orchestrator` and `backtesting` using `unittest.mock` or `pytest` fixtures.
- Smoke tests: `tests/test_ui_tk_smoke.py` should import pages and create instances of frames to ensure no import-time side effects.
- CI: run unit tests and the smoke import tests. Keep smoke tests lightweight and avoid running interactive loops on CI.

Accessibility checklist
-----------------------
- Keyboard navigation: full keyboard accessibility for the pipeline list, config panel, and Run/Stop.
- Visible focus indicators for all interactive controls.
- Color contrast: ensure color tokens meet WCAG AA contrast ratios.
- Screen reader: add accessible names/labels for controls where possible (platform-specific guidance).

Theming, icons & assets
------------------------
- Centralize theme tokens in `ui_tk/style.py` (colors, sizes, fonts) so changing theme affects the whole app.
- Reuse icons from `ui/generate_icons.py`; copy into `assets/icons/` or `ui_tk/assets/icons/` and load via `PIL.ImageTk.PhotoImage`. Remember to keep references to PhotoImage objects to prevent garbage collection.

Packaging notes
---------------
- Update `requirements.txt` to include `matplotlib` and `pillow` and optionally `ttkbootstrap` if chosen.
- Update [`EDINET.spec`](EDINET.spec) to include `ui_tk` files and `matplotlib` backend hooks: ensure `matplotlib.backends.backend_tkagg` and `PIL` data are included in `hiddenimports` / `datas`.

Performance & long-running work
--------------------------------
- Avoid blocking the main thread; offload CPU heavy tasks to separate processes if thread limits cause contention.
- Limit `ThreadPoolExecutor` `max_workers` to a small number by default (2–4) and make it configurable.

Developer workflow & PR checklist
-------------------------------
- PR must include:
    - Screenshots of the UI (or a short GIF) for UI changes.
    - Unit tests for controller behavior when business logic changes.
    - Updated `EDINET.spec` and `requirements.txt` for any new packaging-relevant dependency.
    - A short note in the changelog or `docs/` describing the UX decision if it affects other flows.

Quick links (where to look)
---------------------------
- [ui/app.py](ui/app.py)
- [ui/__init__.py](ui/__init__.py)
- [main.py](main.py)
- [orchestrator.py](orchestrator.py)
- [ui/generate_icons.py](ui/generate_icons.py)
- [EDINET.spec](EDINET.spec)
- [requirements.txt](requirements.txt)

Implementation priorities (short)
-------------------------------
1. Home view and top-level view selector.
2. Persistent right config panel with keyboard behavior implemented.
3. Move log to bottom and add filtering/search.
4. Prototype the Pipeline page with background runner.

PR ready acceptance criteria (developer-facing)
---------------------------------------------
- A single PR adding the `ui_tk` scaffold must include a smoke test and documentation that references this design guidance section.
- The right config panel must open with `Enter`, be navigable with `Tab`, and persist unsaved changes handling must be described in the PR.
- The log must be present at the bottom and have an `auto-scroll` toggle.




