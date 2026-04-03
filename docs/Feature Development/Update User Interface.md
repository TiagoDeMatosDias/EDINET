## Update User Interface — Implementation-ready Plan

Status
------
Completed and retained as historical implementation notes. The legacy `ui/` package and `--flet` entry path have been removed.

Purpose
-------
Replace the existing Flet-based `ui/` with a new Tkinter-based `ui_tk/` package styled to look like a modern terminal application. This document is prescriptive: it defines everything needed for implementation.

Scope & Goal
------------
- Replace the Flet-based `ui/` with `ui_tk/` (Tkinter + custom terminal-style theming). Deprecate and remove `ui/` once migration is verified.
- Adopt a dark, terminal-inspired visual design: monospace fonts, dark backgrounds, green/cyan/amber accents, minimal chrome.
- Decouple UI from business logic via thin controllers that call existing backend modules (`orchestrator.py`, `backtesting.py`, `edinet_api.py`).
- Refactor `orchestrator.py` to expose per-step execution with cancellation and callback hooks so the UI can run steps in background threads with real-time log streaming.
- Allow users to dynamically build pipelines: create new setups, add/remove/reorder steps from the available step catalogue.
- No charting or visualization features. The UI is a pipeline configuration and execution tool. Backtesting reports remain text-file-based and are unchanged.

What is NOT in scope
--------------------
- No matplotlib, plotly, or any charting/graphing. Those libraries are not used in the current UI and are not needed here. The `matplotlib` dependency in `requirements.txt` exists for backtesting internals only and must not be pulled into the UI layer.
- No changes to backtesting report generation (those write to text files and remain as-is).
- No new data visualization pages. A "Data" tab may be added later but is a placeholder only.

---

## Current State (reference for implementers)

The existing Flet UI (`ui/`) has a single page: the **Orchestrator/Pipeline page** with:
- A left panel: scrollable step list with drag-and-drop reorder, enable/disable checkboxes, gear icons for config dialogs, overwrite toggles.
- A right panel: read-only log output (`ft.TextField`).
- Bottom row: Save Setup, Load Setup, progress bar, Run button.
- Modal dialogs for per-step configuration (each step has a specific dialog with fields matching `run_config.json`).
- A portfolio grid editor for the `backtest` step (editable table with clipboard support).
- App bar: logo, Orchestrator/Data tab pills, API Key button, dark mode toggle.

**Current orchestrator (`src/orchestrator.py`):**
- `run()` iterates through `run_steps` in `run_config.json`, calling `_execute_step()` for each enabled step sequentially.
- `_execute_step()` is private and handles all 12 step types via if/elif.
- No per-step progress callbacks, no cancellation, no event emission.
- `Config` is a singleton loaded from disk; the UI writes `run_config.json` before calling `run()`.

**Current threading model:**
- Flet's `page.run_thread()` spawns a single background thread for the entire `orchestrator.run()` call.
- A custom `_UILogHandler` buffers log records and flushes them to the UI text field every 0.15 seconds.
- No cancellation support. No per-step job tracking.

**Current persistence:**
- `config/state/run_config.json`: active pipeline config (steps + per-step configs).
- `config/state/app_state.json`: recent databases list.
- `config/state/saved_setups/*.json`: named setup files (same format as `run_config.json`).
- `.env`: API key.

---

## Phase 0: Orchestrator Refactoring (prerequisite)

The current orchestrator must be modified before the new UI can deliver its background execution and modularity goals. These changes are backward-compatible — the CLI path (`python main.py --cli`) will continue to work via `orchestrator.run()`.

### 0a) Make `_execute_step` public

Rename `_execute_step` to `execute_step` and give it a clean public signature:

```python
def execute_step(step_name: str, config: Config, overwrite: bool = False,
                 edinet=None, data=None) -> None:
    """Execute a single pipeline step. Raises on failure."""
```

The existing `run()` function calls this internally; no behavior change.

### 0b) Add a `run_pipeline` function with per-step callbacks

Create a new public function alongside `run()` that the UI will call:

```python
def run_pipeline(
    steps: list[dict],          # [{"name": "get_documents", "overwrite": False}, ...]
    config: Config,
    on_step_start: Callable[[str], None] | None = None,
    on_step_done: Callable[[str], None] | None = None,
    on_step_error: Callable[[str, Exception], None] | None = None,
    cancel_event: threading.Event | None = None,
) -> None:
    """
    Execute a list of steps in order. Before each step, check cancel_event.
    Call on_step_start/done/error callbacks at the appropriate points.
    """
    for step in steps:
        if cancel_event and cancel_event.is_set():
            logger.info("Pipeline cancelled by user.")
            return
        name = step["name"]
        overwrite = step.get("overwrite", False)
        if on_step_start:
            on_step_start(name)
        try:
            execute_step(name, config, overwrite=overwrite)
            if on_step_done:
                on_step_done(name)
        except Exception as e:
            if on_step_error:
                on_step_error(name, e)
            raise
```

### 0c) Decouple from Config singleton for UI use

The UI must be able to pass a config dict without writing to disk first. Add an alternative constructor or a `from_dict` method to `Config`:

```python
@classmethod
def from_dict(cls, settings: dict) -> "Config":
    """Create a Config instance from a dict without touching disk."""
    instance = object.__new__(cls)
    instance.settings = settings
    instance.run_config_path = None
    return instance
```

The existing `run()` path and CLI remain unchanged.

### 0d) Log streaming (already works)

The existing `logging` infrastructure already supports adding custom handlers. The UI will attach a handler that routes log records to a `queue.Queue`, which the Tk main thread polls. No orchestrator changes needed for this — it works the same way the current Flet `_UILogHandler` does.

---

## Phase 1: Scaffolding & Architecture

### 1a) Update `main.py`

```python
# main.py
if '--cli' in sys.argv:
    _run_cli()
else:
    from ui_tk import run_tk_app
    run_tk_app()
```

A temporary `--flet` flag may be added during transition; remove it with `ui/` retirement.

### 1b) Create `ui_tk/` package structure

```
ui_tk/
  __init__.py          # exports run_tk_app()
  app.py               # Tk root bootstrap, view switching, event queue setup
  style.py             # terminal theme: colors, fonts, widget configuration
  controllers.py       # thin adapters to orchestrator/backtesting/edinet_api
  utils.py             # background runner (ThreadPoolExecutor + Queue + polling)
  shared/
    widgets.py         # reusable components: TerminalText, StepListItem, LabeledEntry, StatusChip
  pages/
    home.py            # landing page with setup selection
    orchestrator.py    # pipeline builder + step config + run controls
    data.py            # placeholder for future data views
```

### 1c) Smoke test

Add `tests/test_ui_tk_smoke.py`: imports all modules, instantiates key widgets with a hidden `Tk()` root, verifies no import-time errors or missing dependencies.

---

## Phase 2: Terminal-Style Visual Design

The application must look like a modern terminal UI (think lazygit, btop, k9s) — not a standard Windows desktop app.

### Design tokens (`ui_tk/style.py`)

```python
COLORS = {
    "bg":           "#0d1117",   # deep dark background
    "surface":      "#161b22",   # panel backgrounds
    "border":       "#30363d",   # panel borders
    "text":         "#c9d1d9",   # primary text
    "text_dim":     "#8b949e",   # secondary/muted text
    "accent":       "#58a6ff",   # interactive elements, selected tabs
    "success":      "#3fb950",   # completed steps, success messages
    "warning":      "#d29922",   # warnings
    "error":        "#f85149",   # errors, stop button
    "highlight":    "#1f6feb",   # focused/active row
}

FONTS = {
    "mono":         ("Cascadia Mono", "Consolas", "Courier New"),  # fallback chain
    "mono_size":    11,
    "heading_size": 14,
}
```

### Visual rules

- **Background**: all panels use `bg` or `surface` colors. No white or light backgrounds.
- **Typography**: monospace fonts everywhere. Data, labels, buttons, logs — all monospace.
- **Borders**: 1px solid `border` color around panels. No rounded corners, no shadows, no gradients.
- **Buttons**: text-only or minimal outline. Primary action (Run ▶) uses `accent` color. Stop uses `error` color. All buttons monospace.
- **Log area**: black background, monospace text, colored by log level (INFO=`text`, WARNING=`warning`, ERROR=`error`, DEBUG=`text_dim`). Must look like an actual terminal.
- **Step list items**: single-line rows with `[ ]`/`[✓]` text checkboxes, step name, and status indicator. Highlight selected row with `highlight` color. No icons/images — use text characters.
- **Tabs**: text labels with underline or bracket indicators for active tab, e.g. `[ Orchestrator ]` vs `  Data  `.
- **Inputs**: dark background fields with monospace text and a subtle border. No platform-native input styling.
- **Scrollbars**: thin, dark. Use ttk styling or custom to match the terminal look.

### Tk configuration approach

Use `tk.configure()` and `ttk.Style()` to override all default widget appearances. Do NOT use `ttkbootstrap` — it doesn't produce the terminal aesthetic we want. Build custom styling directly.

---

## Phase 3: Orchestrator Page (primary page)

This is the main page of the application. It replaces the existing Flet pipeline page.

### Layout

The page has three zones: **step list** (left), **main area** (center, with optional config panel on right), and **log output** (bottom, full width).

### 3a) Step List (left panel)

- Displays the steps currently in the user's pipeline setup.
- Each row shows: `[drag handle ≡] [✓/☐] step_name [⚙]`
- The step list is NOT fixed. Users can:
  - **Add steps**: an `[+ Add Step]` button at the bottom opens a dropdown/menu of available step types (the 12 types from the orchestrator). Selecting one appends it to the pipeline.
  - **Remove steps**: right-click or a delete button on each step row.
  - **Reorder steps**: drag-and-drop (via mouse bindings on the `≡` handle) or keyboard shortcuts (Alt+Up/Down).
  - **Enable/disable steps**: click the checkbox. Disabled steps are skipped during a run.
- Below the step list: `[ Save Setup ]  [ Load Setup ]  [ New Setup ]`
- **New Setup**: clears the step list, prompts for a name, starts with an empty pipeline.
- **Load Setup**: dropdown of saved setups from `config/state/saved_setups/`.
- **Save Setup**: saves current pipeline (step list + all step configs) to a named JSON file.

### 3b) Config Panel (right side, shown on demand)

- Opens when the user selects a step and presses Enter or clicks the gear icon.
- Shows the configuration fields for that specific step (matching the current modal dialogs: database paths, date ranges, file pickers, SQL queries, portfolio grid, etc.).
- All existing step config dialogs must be ported as inline panel contents (not modals).
- The backtest portfolio grid editor (editable table with clipboard support) must be preserved.
- `Esc` closes the panel. `Tab` cycles through fields. On close, config is auto-saved to the in-memory setup.

### 3c) Log Output (bottom panel, full width)

- Always visible across all views.
- Monospace text on dark background — styled as a real terminal.
- Color-coded by log level: INFO=green, WARNING=amber, ERROR=red, DEBUG=dim gray.
- Controls: `[Clear]  [Export]  [Auto-scroll ✓]  Filter: [All ▾]`
- Auto-scroll: enabled by default; scrolls to bottom on new messages. Disabled if the user scrolls up manually. Re-enabled when user scrolls back to bottom.
- Resizable height (drag the border between main area and log area).
- Backed by a `queue.Queue` fed by a custom logging handler on the background thread.

### 3d) Run Controls

- Bottom bar (below log area, or integrated with log controls): `[ ◀ Stop ]  [ ▶ Run ]`
- **Run**: saves config, spawns a background thread via `run_pipeline()` (Phase 0b), streams logs in real-time. All enabled steps execute in order. Updates step status chips (idle → running → done/error) as each step starts/finishes via callbacks.
- **Stop**: sets the `cancel_event` (threading.Event). Pipeline stops before the next step. The currently running step completes (cannot be interrupted mid-step without orchestrator-level changes).
- During a run: Run button is disabled, Stop button is enabled. Step checkboxes and config are locked.
- On completion: show a summary line in the log ("Pipeline completed in X seconds, N steps succeeded, M failed").

---

## Phase 4: Home Page

Simple landing view shown on app start.

```
┌──────────────────────────────────────────────────────────────────────────┐
│  SHADE Research                  [ Orchestrator ]  [ Data ]  [API Key]  │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  SHADE Research - EDINET Pipeline Manager                                │
│                                                                          │
│  Recent Setups:                                                          │
│    > simple                    2026-02-26                                 │
│    > backtest                  2026-02-26                                 │
│    > signpredictor             2026-02-26                                 │
│                                                                          │
│  [ New Setup ]    [ Open Setup ]    [ Orchestrator ]                     │
│                                                                          │
├──────────────────────────────────────────────────────────────────────────┤
│  [Log Output]                                                            │
│  > Application started                                                   │
│  > Loaded 7 saved setups                                                 │
└──────────────────────────────────────────────────────────────────────────┘
```

- Clicking a recent setup loads it and switches to the Orchestrator view.
- `[ New Setup ]` creates an empty pipeline and switches to Orchestrator.
- Views are switched via the top tab bar (always visible). Active tab is indicated with brackets or underline.

---

## Phase 5: Background Worker Pattern

### `ui_tk/utils.py`

```python
import queue
import threading
from concurrent.futures import ThreadPoolExecutor

executor = ThreadPoolExecutor(max_workers=2)
event_q: queue.Queue = queue.Queue()

def run_in_background(fn, args=(), on_done=None, on_error=None):
    """Submit fn to thread pool. Callbacks are dispatched on the main thread."""
    def _callback(fut):
        try:
            result = fut.result()
            if on_done:
                event_q.put(("done", on_done, result))
        except Exception as e:
            if on_error:
                event_q.put(("error", on_error, e))
    fut = executor.submit(fn, *args)
    fut.add_done_callback(_callback)
    return fut

def poll_events(root):
    """Drain the event queue and dispatch callbacks on the main thread."""
    try:
        while True:
            kind, callback, payload = event_q.get_nowait()
            callback(payload)
    except queue.Empty:
        pass
    root.after(100, poll_events, root)
```

### UI Log Handler

```python
import logging

class QueueLogHandler(logging.Handler):
    """Logging handler that puts formatted records onto a queue for the UI to consume."""
    def __init__(self, log_queue: queue.Queue):
        super().__init__()
        self.log_queue = log_queue

    def emit(self, record):
        self.log_queue.put(("log", record.levelname, self.format(record)))
```

The Tk main thread polls this queue alongside `event_q` and appends text to the log widget with appropriate color tags.

---

## Phase 6: Controllers

`ui_tk/controllers.py` — thin adapter layer. No business logic.

```python
import threading
from config import Config
from src import orchestrator

def run_pipeline(steps: list[dict], config_dict: dict,
                 on_step_start=None, on_step_done=None,
                 on_step_error=None, cancel_event=None):
    """Adapter: build Config from dict, call orchestrator.run_pipeline()."""
    config = Config.from_dict(config_dict)
    orchestrator.run_pipeline(
        steps=steps,
        config=config,
        on_step_start=on_step_start,
        on_step_done=on_step_done,
        on_step_error=on_step_error,
        cancel_event=cancel_event,
    )

def save_setup(name: str, setup_data: dict) -> None:
    """Save a named setup to config/state/saved_setups/{name}.json."""

def load_setup(name: str) -> dict:
    """Load a named setup from config/state/saved_setups/{name}.json."""

def list_setups() -> list[str]:
    """List available saved setup names."""

def save_api_key(key: str) -> None:
    """Write API_KEY to .env."""

def get_api_key() -> str:
    """Read API_KEY from .env."""
```

---

## Mockups (ASCII, terminal style)

### Orchestrator — config panel closed

```
┌──────────────────────────────────────────────────────────────────────────┐
│  SHADE Research            [ Home ]  [*Orchestrator*]  [ Data ] [•API]  │
├────────────────────────┬─────────────────────────────────────────────────┤
│  Pipeline: simple      │                                                │
│ ───────────────────    │  Setup: simple                                 │
│  ≡ [✓] get_documents   │  Steps: 5 enabled / 8 total                   │
│  ≡ [✓] download_docs   │  Last run: 2026-03-28 14:22 (OK)              │
│  ≡ [ ] populate_co     │                                                │
│  ≡ [✓] parse_taxonomy  │  Select a step and press Enter to configure.  │
│  ≡ [✓] update_prices   │  Drag ≡ to reorder. [+] to add steps.        │
│  ≡ [ ] import_prices   │                                                │
│  ≡ [✓] gen_fstmts      │                                                │
│  ≡ [ ] gen_ratios      │                                                │
│                        │                                                │
│  [+ Add Step]          │                                                │
│ ───────────────────    │                                                │
│  [Save] [Load] [New]  │                                                │
├────────────────────────┴─────────────────────────────────────────────────┤
│  LOG ─────────────────────── [Clear] [Export] [Auto-scroll ✓] [All ▾]  │
│  14:22:01 INFO  Pipeline started                                        │
│  14:22:01 INFO  Step: get_documents                                     │
│  14:22:05 INFO  Retrieved 847 documents                                 │
│  14:22:05 INFO  Step: download_docs                                     │
│  14:22:18 WARN  3 documents failed to download                          │
│  14:22:18 INFO  Pipeline completed (5 steps, 17.2s)                     │
├──────────────────────────────────────────────────────────────────────────┤
│                                   [ ◀ Stop ]              [ ▶ Run ]     │
└──────────────────────────────────────────────────────────────────────────┘
```

### Orchestrator — config panel open (Enter on get_documents)

```
┌──────────────────────────────────────────────────────────────────────────┐
│  SHADE Research            [ Home ]  [*Orchestrator*]  [ Data ] [•API]  │
├──────────────────┬──────────────────────────┬────────────────────────────┤
│  Pipeline        │  Step: get_documents     │  CONFIG                    │
│ ──────────────── │  Status: idle            │ ─────────────────────────  │
│  ≡ [✓]►get_docs  │  Last run: 14:22 (OK)   │  Start Date               │
│  ≡ [✓] dl_docs   │                          │  [2026-02-15         ]    │
│  ≡ [ ] pop_co    │                          │  End Date                 │
│  ≡ [✓] parse_tx  │                          │  [2026-02-27         ]    │
│  ≡ [✓] upd_pr    │                          │  Target Database          │
│  ...             │                          │  [Browse...          ]    │
│                  │                          │                           │
│  [+ Add Step]    │                          │  [Save Config]            │
├──────────────────┴──────────────────────────┴────────────────────────────┤
│  LOG ─────────────────────── [Clear] [Export] [Auto-scroll ✓] [All ▾]  │
│  14:25:00 INFO  Opened config for: get_documents                        │
├──────────────────────────────────────────────────────────────────────────┤
│                                   [ ◀ Stop ]              [ ▶ Run ]     │
└──────────────────────────────────────────────────────────────────────────┘
```

### Home view

```
┌──────────────────────────────────────────────────────────────────────────┐
│  SHADE Research            [*Home*]  [ Orchestrator ]  [ Data ] [•API]  │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  EDINET Pipeline Manager                                                 │
│                                                                          │
│  Saved Setups:                                                           │
│   ──────────────────────────────────────────                             │
│    simple ················ 2026-02-26                                    │
│    backtest ·············· 2026-02-26                                    │
│    signpredictor ········· 2026-02-26                                    │
│    2026-02-26 ············ 2026-02-26                                    │
│    2026-02-26-2 ·········· 2026-02-26                                    │
│   ──────────────────────────────────────────                             │
│                                                                          │
│  [ New Setup ]          [ Open Selected ]                                │
│                                                                          │
├──────────────────────────────────────────────────────────────────────────┤
│  LOG ─────────────────────── [Clear] [Export] [Auto-scroll ✓] [All ▾]  │
│  > Application started                                                   │
│  > Loaded 7 saved setups                                                 │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## Behavior Details

### Log Output
- Always visible at the bottom across all views. Full-width. Resizable height.
- Monospace, color-coded by log level. Timestamps on every line.
- Auto-scroll by default; pauses when user scrolls up; shows "↓ New messages" indicator.
- Filter dropdown: All / Info / Warning / Error.
- Clear button empties the display (does not affect the log file on disk).
- Export button saves current log buffer to a text file.

### Step List & Pipeline Building
- The available step types are the 12 defined in the orchestrator: `get_documents`, `download_documents`, `populate_company_info`, `parse_taxonomy`, `update_stock_prices`, `import_stock_prices_csv`, `generate_financial_statements`, `generate_ratios`, `generate_historical_ratios`, `Multivariate_Regression`, `backtest`, `backtest_set`.
- `[+ Add Step]` shows a menu/dropdown of these 12 types. Selecting one appends it to the current pipeline with default config.
- Steps can be added multiple times (e.g., two `get_documents` steps with different date ranges) if desired.
- Remove a step via right-click context menu or a delete key.
- Reorder via drag-and-drop on the `≡` handle.
- Each step has `enabled` and `overwrite` flags (overwrite shown in the config panel, not on the step row).

### Config Panel
- Opens right-side panel for the selected step. Not a modal — inline panel.
- Fields are populated from the step's config dict (same structure as `run_config.json`).
- File/database pickers use `tkinter.filedialog`.
- The backtest portfolio grid must be ported as an inline editable table with the same clipboard (Ctrl+C/V/A) and add/delete row support.
- `Tab` navigates through fields. `Esc` closes panel. Changes auto-save to in-memory config on field blur.

### Views
- Top tab bar: `Home`, `Orchestrator`, `Data`. Always visible.
- Switching views does not discard unsaved config.
- `Data` is a placeholder page for now (shows "Coming soon" or similar).

### Setup Persistence
- Same format as current `run_config.json` (backward-compatible).
- `Save Setup`: prompts for name, writes to `config/state/saved_setups/{name}.json`.
- `Load Setup`: lists files in `saved_setups/`, loads the selected one, populates step list and configs.
- `New Setup`: clears step list to empty, prompts for name.
- On `Run`, the current config is also written to `config/state/run_config.json` (so CLI mode stays compatible).

---

## Implementation Steps (ordered checklist)

### Step 1: Orchestrator refactoring
- [ ] Rename `_execute_step` → `execute_step`.
- [ ] Add `run_pipeline()` with per-step callbacks and `cancel_event` (Phase 0b).
- [ ] Add `Config.from_dict()` class method (Phase 0c).
- [ ] Verify `python main.py --cli` still works with no regressions.
- [ ] Add tests for `run_pipeline()` with mocked steps.

### Step 2: Scaffold `ui_tk/`
- [ ] Create all files listed in Phase 1b.
- [ ] Implement `ui_tk/style.py` with terminal theme tokens.
- [ ] Implement `ui_tk/app.py`: Tk root, apply theme, view switching frame, event queue setup.
- [ ] Implement `ui_tk/utils.py`: background runner + `QueueLogHandler` + polling.
- [ ] Update `main.py` to launch `ui_tk` by default.
- [ ] Add `tests/test_ui_tk_smoke.py`.

### Step 3: Home page
- [ ] Implement `ui_tk/pages/home.py`: setup list, New/Open buttons.
- [ ] Wire to `controllers.list_setups()` and `controllers.load_setup()`.

### Step 4: Orchestrator page — step list
- [ ] Implement step list widget (left panel).
- [ ] Add Step button with step type dropdown.
- [ ] Remove step (right-click or delete key).
- [ ] Drag-and-drop reorder.
- [ ] Enable/disable checkboxes.
- [ ] Save/Load/New Setup buttons.

### Step 5: Orchestrator page — config panel
- [ ] Port each step's config dialog as an inline panel (not a modal).
- [ ] Port the backtest portfolio grid editor.
- [ ] File/database picker integration via `tkinter.filedialog`.
- [ ] Keyboard navigation (Enter to open, Tab through fields, Esc to close).

### Step 6: Orchestrator page — log output & run controls
- [ ] Implement log output widget (bottom panel, full width).
- [ ] Color-coded log lines by level.
- [ ] Auto-scroll toggle, Clear, Export, Filter controls.
- [ ] Run button → `controllers.run_pipeline()` via background thread.
- [ ] Stop button → sets `cancel_event`.
- [ ] Step status updates (idle → running → done/error) via callbacks.

### Step 7: API Key management
- [ ] API Key button in the top bar opens a dialog to view/set the key.
- [ ] Writes to `.env` via `controllers.save_api_key()`.

### Step 8: Testing
- [ ] `tests/test_ui_tk_smoke.py` — import + widget instantiation.
- [ ] `tests/test_ui_tk_controllers.py` — mock orchestrator, test `run_pipeline`, setup save/load.
- [ ] `tests/test_orchestrator.py` — test `run_pipeline()` with callbacks and cancellation.

### Step 9: Packaging
- [ ] Update `requirements.txt`: remove `flet`, ensure `pillow` is present. Do NOT add `matplotlib` to UI dependencies.
- [ ] Update `EDINET.spec`: include `ui_tk` package, remove `flet` hooks.
- [ ] Build and test the PyInstaller executable.

### Step 10: Deprecate and remove `ui/`
- [ ] Remove `ui/` directory.
- [ ] Remove `flet` from `requirements.txt`.
- [ ] Remove `--flet` flag from `main.py` if added.
- [ ] Update docs.

---

## Acceptance Criteria

- `python main.py` launches the Tk UI with terminal-style dark theme.
- `python main.py --cli` continues to work unchanged.
- Home page shows saved setups and allows creating/opening them.
- Orchestrator page allows building a pipeline from scratch (add/remove/reorder steps).
- Step config panel opens inline (not modal) with all fields from the current Flet dialogs.
- Run executes steps in a background thread with real-time log streaming.
- Stop cancels the pipeline between steps.
- Log output is full-width, color-coded, with auto-scroll, filter, clear, and export.
- All config is backward-compatible with existing `run_config.json` / saved setup format.
- PyInstaller build produces a working executable.

## Dependencies

- `pillow >= 9.0` (for icon loading via `PIL.ImageTk.PhotoImage`)
- `pytest >= 7.0` (for tests)
- Tkinter (bundled with Python)
- No new UI-layer dependencies. No `matplotlib`, `ttkbootstrap`, or `plotly`.

## File References

- [main.py](main.py) — entry point, CLI/GUI switching
- [src/orchestrator.py](src/orchestrator.py) — pipeline execution (must be refactored)
- [config.py](config.py) — Config singleton (must add `from_dict`)
- [config/state/run_config.json](config/state/run_config.json) — pipeline config format
- [config/state/saved_setups/](config/state/saved_setups/) — saved setup files
- [config/state/app_state.json](config/state/app_state.json) — app state
- [ui/pages/pipeline/step_dialogs.py](ui/pages/pipeline/step_dialogs.py) — step config dialogs to port
- [ui/pages/pipeline/persistence.py](ui/pages/pipeline/persistence.py) — defaults and persistence logic to port
- [ui/pages/pipeline/run_controls.py](ui/pages/pipeline/run_controls.py) — current run/log logic (reference)
- [src/logger.py](src/logger.py) — logging setup
- [EDINET.spec](EDINET.spec) — PyInstaller spec
- [requirements.txt](requirements.txt) — dependencies


