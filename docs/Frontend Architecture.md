# Frontend Architecture

This document describes the structure of the web workstation's frontend and explains how to extend it with new screens.

---

## Directory layout

```
src/web_app/
├── server.py                   ← FastAPI server (Python)
├── api/
│   └── __init__.py             ← API façade (Python)
└── frontend/                   ← All browser-side assets
    ├── common/                 ← Shared utilities used by every screen
    │   ├── styles.css          ← Global stylesheet (design tokens + all component classes)
    │   ├── state.js            ← Shared application state, DOM element cache, callbacks
    │   ├── utils.js            ← Pure DOM/data helpers (el, $, fetchJson, …)
    │   ├── console.js          ← Console log panel (log, renderConsole, exportConsole)
    │   └── topbar.js           ← Topbar wiring: health check, console toggle
    ├── main/
    │   ├── main.html           ← Main dashboard page (served at /)
    │   └── main.js             ← Main page entry point
    ├── orchestrator/
    │   ├── orchestrator.html   ← Orchestrator page (served at /orchestrator)
    │   ├── orchestrator.js     ← Orchestrator page entry point
    │   └── index.js            ← Pipeline builder, step inspector, run/stop logic
    ├── screening/
    │   ├── screening.html      ← Screening page (served at /screening)
    │   ├── screening.js        ← Screening page entry point
    │   └── index.js            ← Screening screen logic (stub)
    └── security_analysis/
        ├── security.html       ← Security Analysis page (served at /security)
        ├── security.js         ← Security Analysis page entry point
        └── index.js            ← Security Analysis screen logic (stub)
```

The entire frontend is vanilla ES-module JavaScript — no build step, no bundler.
Each screen is a separate HTML page served by its own FastAPI route.
Tabs in the topbar use inline `onclick="location.href='/...'"` to navigate between pages.

---

## Python layer

### `server.py`

The thin FastAPI application.  Responsibilities:

- Mounts `frontend/` as the static-file root (`/assets/*`).
- Mounts `assets/` (root level) as brand assets (`/brand-assets/*`).
- Registers all `/api/*` and `/health` routes from `src/web_app/api/`.
- Serves each page at its own route:
  - `/` → `frontend/main/main.html`
  - `/orchestrator` → `frontend/orchestrator/orchestrator.html`
  - `/screening` → `frontend/screening/screening.html`
  - `/security` → `frontend/security_analysis/security.html`

### `api/__init__.py`

A façade that re-exports the `router_app` FastAPI sub-application and `cleanup_completed_jobs` from `src.api.router`.  
Adding new API endpoints for a specific screen should be done by creating a new route module in this package and importing it here, rather than modifying `src.api.router` directly.

---

## Frontend layer

### Page HTML files

Each page (`main/main.html`, `orchestrator/orchestrator.html`, etc.) is a self-contained HTML document sharing the same layout:

- A `<header class="topbar">` with brand, tabs, and action buttons.
- A `<main class="workspace">` with the page's primary content.
- A `<footer class="console">` for the terminal-style event stream.

Tab buttons use inline `onclick` handlers for page navigation:

```html
<button class="tab is-active" onclick="void 0">Main</button>
<button class="tab" onclick="location.href='/orchestrator'">Orchestrator</button>
<button class="tab" onclick="location.href='/screening'">Screening</button>
<button class="tab" onclick="location.href='/security'">Security Analysis</button>
```

Each page loads its own `<script type="module">` entry point (e.g. `/assets/main/main.js`).

### Page entry points (e.g. `main/main.js`, `orchestrator/orchestrator.js`)

Each page has its own bootstrap script that:

1. Populates the `els` DOM element cache for elements on that page.
2. Registers cross-module callbacks (`refreshJobs`) so screen modules can trigger data refreshes without importing the page script.
3. Wires up topbar events via `wireTopbarEvents()` from `common/topbar.js`.
4. Runs initial API fetches (health, steps, jobs).
5. Hands off to screen-specific render functions.

### `frontend/common/state.js`

Exports three shared objects:

| Export | Purpose |
|--------|---------|
| `STATE` | Single mutable object holding all application state (view, pipeline, jobs, setups, …). Every module that reads or writes application state imports `STATE` from here. |
| `els` | Empty object populated by each page's bootstrap script with references to every named DOM element. Avoids repeated `document.querySelector` calls and keeps element lookups in one place. |
| `callbacks` | Object with `setView` and `refreshJobs` slots. Populated by page scripts at startup so screen modules can trigger navigation or data refreshes without a circular import. |

Also exports the browser-storage helpers: `loadLocalSetups`, `persistLocalSetups`, `saveLastSetupName`.

### `frontend/common/utils.js`

Pure utility functions with **no imports**.  Key exports:

| Function | Description |
|----------|-------------|
| `el(tag, attrs, ...children)` | Minimal virtual-DOM builder.  Supports `class`, `dataset`, `text`, `html`, and `on*` event listener shorthands as attribute keys. |
| `$(sel, root?)` | `document.querySelector` shorthand. |
| `$all(sel, root?)` | `document.querySelectorAll` → plain Array. |
| `fetchJson(url, options?)` | `fetch` wrapper that parses JSON and throws a descriptive `Error` on non-2xx responses. |
| `deepClone(value)` | `JSON.parse(JSON.stringify(…))` clone. |
| `formatDate(value)` | Locale-formatted date/time string. |
| `metric(label, value, tone)` | Creates a dashboard metric tile node. |
| `kvLine(label, value)` | Creates a two-column label/value inspector row. |
| `section(title, subtitle, blurb, body)` | Creates a bordered section card used in the inspector. |

### `frontend/common/console.js`

Manages the bottom console panel.

| Export | Description |
|--------|-------------|
| `log(level, message)` | Appends an entry to `STATE.logs` and re-renders the panel. Levels: `info`, `warn`, `error`, `debug`. |
| `renderConsole()` | Re-renders the console from `STATE.logs`, respecting the current filter and auto-scroll setting. |
| `exportConsole()` | Downloads all current log entries as a `.log` text file. |

### `frontend/common/topbar.js`

Shared topbar helpers: wires console toggle, refresh button, and health check. Does **not** handle tab navigation (tabs use inline `onclick` in HTML).

---

## Screen modules

### `orchestrator/index.js`

Contains all logic for the Orchestrator screen and the Main dashboard (which surfaces a summary of orchestrator state).

**Render entry-points** (called by page scripts):

| Function | Called when |
|----------|------------|
| `renderOrchestrator()` | User navigates to the Orchestrator tab. |
| `renderMain()` | User navigates to the Main tab, or any state change that affects the dashboard. |
| `renderAll()` | After bulk state changes (e.g. step metadata loaded, setup hydrated). |
| `renderStepLibrary()` | After the search query changes or step metadata is refreshed. |
| `renderPipelineList()` | After the pipeline array changes or a step's status changes. |
| `renderInspector()` | After the selected step or inspector tab changes. |
| `renderRecentJobs()` | After the jobs list is refreshed. |

**Pipeline mutations** (wired to UI events in page scripts):

| Function | Description |
|----------|-------------|
| `addStep(name)` | Adds a step to the pipeline (or re-enables it if already present). |
| `removeStepById(id)` | Removes a step and adjusts the selection. |
| `moveStep(index, direction)` | Moves a step up (`-1`) or down (`+1`). |
| `selectStep(stepId)` | Sets the inspector's focused step. |
| `runPipeline()` | POSTs the current pipeline to `/api/pipeline/run` and streams step status back. |
| `stopPipeline()` | Aborts the in-flight fetch request. |

**Setup management**:

| Function | Description |
|----------|-------------|
| `hydrateSetup(payload)` | Loads a saved setup object into `STATE` and re-renders everything. |
| `saveSetup()` | Serialises the current pipeline + config to `localStorage`. |
| `newSetup()` | Resets the pipeline and config to defaults. |
| `showLoadMenu(anchor)` | Shows the floating "Load setup" popup anchored to a button. |
| `closeLoadMenu()` | Dismisses the popup. |
| `initializeSetup()` | Called once at bootstrap to restore the last setup name and seed defaults. |

### `screening/index.js`

Stub screen.  Exports a single `render()` function that is called by `screening/screening.js` when the Screening page loads.  Add all screening-specific state, API calls, and DOM rendering here.

### `security_analysis/index.js`

Stub screen.  Same contract as the Screening module.

---

## Adding a new screen

1. **Create the folder and files**

   ```
   src/web_app/frontend/my_screen/
   ├── my_screen.html    ← Page HTML
   ├── my_screen.js      ← Page entry point
   └── index.js          ← Screen logic module
   ```

   `index.js` must export at minimum:

   ```js
   // frontend/my_screen/index.js
   export function render() {
     // Build or update the DOM inside the view panel.
   }
   ```

   Copy the HTML structure from an existing page (topbar, workspace, console footer),
   update the tab buttons with the correct `onclick` handlers, and point the
   `<script type="module">` tag to your entry point.

2. **Add the route** in `src/web_app/server.py`:

   ```python
   @app.get("/my_screen")
   def page_my_screen() -> FileResponse:
       return FileResponse(FRONTEND_DIR / "my_screen" / "my_screen.html")
   ```

3. **Add the tab button** to every page's `<nav class="tabs">` block:

   ```html
   <button class="tab" onclick="location.href='/my_screen'">My Screen</button>
   ```

4. **Add API routes** (if needed) in `src/web_app/api/` and import them in `src/web_app/api/__init__.py`.

No build step, no config changes — the browser loads the new module automatically via native ES module resolution.

---

## Design conventions

- **No build tooling.** All JavaScript is written as native ES modules loaded directly by the browser.  Import paths use relative URLs.
- **Shared state via `STATE`.** All screens read and write the same `STATE` object.  Do not keep per-module global variables for data that other screens might need.
- **DOM element cache via `els`.** Populate `els` in each page's bootstrap script.  Screen modules that need to access an element that isn't in `els` yet should add it there rather than calling `querySelector` at render time.
- **Cross-module callbacks via `callbacks`.** When a screen needs to trigger navigation or a data refresh it should call `callbacks.setView(...)` or `callbacks.refreshJobs()`.
- **`el()` over `innerHTML`.** Use the `el()` helper from `common/utils.js` to build DOM nodes programmatically.  This avoids XSS risks and keeps the code auditable.
- **`log()` for all user-visible events.**  Use `log('info' | 'warn' | 'error' | 'debug', message)` from `common/console.js` to surface status messages to the operator in the bottom console panel.
- **Tab navigation via inline `onclick`.** Tabs use `onclick="location.href='/...'"` — simple, no JS wiring needed.
