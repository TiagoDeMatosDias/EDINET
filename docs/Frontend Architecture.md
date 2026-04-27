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
    ├── index.html              ← SPA shell (single HTML file for all screens)
    ├── main.js                 ← Application entry point
    ├── common/                 ← Shared utilities used by every screen
    │   ├── styles.css          ← Global stylesheet (design tokens + all component classes)
    │   ├── state.js            ← Shared application state, DOM element cache, callbacks
    │   ├── utils.js            ← Pure DOM/data helpers (el, $, fetchJson, …)
    │   └── console.js          ← Console log panel (log, renderConsole, exportConsole)
    ├── orchestrator/
    │   └── index.js            ← Pipeline builder, step inspector, run/stop logic
    ├── screening/
    │   └── index.js            ← Screening screen (stub — add logic here)
    └── security_analysis/
        └── index.js            ← Security Analysis screen (stub — add logic here)
```

The entire frontend is vanilla ES-module JavaScript — no build step, no bundler.  
The FastAPI server mounts the `frontend/` directory under `/assets` and serves `index.html` for every non-API route.

---

## Python layer

### `server.py`

The thin FastAPI application.  Responsibilities:

- Mounts `frontend/` as the static-file root (`/assets/*`).
- Registers all `/api/*` and `/health` routes from `src/web_app/api/`.
- Serves `frontend/index.html` for `/` and every unknown path (SPA fallback).

### `api/__init__.py`

A façade that re-exports the `router_app` FastAPI sub-application and `cleanup_completed_jobs` from `src.api.router`.  
Adding new API endpoints for a specific screen should be done by creating a new route module in this package and importing it here, rather than modifying `src.api.router` directly.

---

## Frontend layer

### `frontend/index.html`

The single HTML file that hosts all screen panels.  Each screen is a `<section>` with a `data-view-panel` attribute that matches the tab's `data-view` attribute:

```html
<!-- tab button -->
<button class="tab" data-view="screening">Screening</button>

<!-- matching panel -->
<section class="view" data-view-panel="screening"> … </section>
```

Only the active panel has the `is-active` class; all others are `display:none`.  
Adding a new screen requires:

1. A new tab `<button>` in the `<nav class="tabs">` block.
2. A new `<section class="view" data-view-panel="…">` panel in `<main class="workspace">`.

### `frontend/main.js`

The application entry point.  On load it:

1. Populates the `els` DOM element cache (imported from `common/state.js`).
2. Registers the two cross-module callbacks (`setView`, `refreshJobs`) so screen modules can trigger navigation and data refreshes without importing `main.js` back (which would create a circular dependency).
3. Calls `initializeSetup()` and `attachEvents()`.
4. Runs the initial API fetches (`refreshHealth`, `refreshSteps`, `refreshJobs`) in parallel.
5. Hands off to `renderAll()` once data is available.

`setView(view)` is the central navigation function.  It:

- Updates `STATE.view` and `location.hash`.
- Toggles `is-active` on the tab buttons and view panels.
- Calls the appropriate per-screen render entry-point.

To wire a new screen into navigation, add a branch to `setView` and import the screen's render function at the top of `main.js`.

### `frontend/common/state.js`

Exports three shared objects:

| Export | Purpose |
|--------|---------|
| `STATE` | Single mutable object holding all application state (view, pipeline, jobs, setups, …). Every module that reads or writes application state imports `STATE` from here. |
| `els` | Empty object populated by `main.js` during bootstrap with references to every named DOM element. Avoids repeated `document.querySelector` calls and keeps element lookups in one place. |
| `callbacks` | Object with `setView` and `refreshJobs` slots. Populated by `main.js` at startup so screen modules can trigger navigation or data refreshes without a circular import. |

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

---

## Screen modules

### `orchestrator/index.js`

Contains all logic for the Orchestrator screen and the Main dashboard (which surfaces a summary of orchestrator state).

**Render entry-points** (called by `main.js`):

| Function | Called when |
|----------|------------|
| `renderOrchestrator()` | User navigates to the Orchestrator tab. |
| `renderMain()` | User navigates to the Main tab, or any state change that affects the dashboard. |
| `renderAll()` | After bulk state changes (e.g. step metadata loaded, setup hydrated). |
| `renderStepLibrary()` | After the search query changes or step metadata is refreshed. |
| `renderPipelineList()` | After the pipeline array changes or a step's status changes. |
| `renderInspector()` | After the selected step or inspector tab changes. |
| `renderRecentJobs()` | After the jobs list is refreshed. |

**Pipeline mutations** (wired to UI events in `main.js`):

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

Stub screen.  Exports a single `render()` function that is called by `main.js` when the Screening tab is activated.  Add all screening-specific state, API calls, and DOM rendering here.

### `security_analysis/index.js`

Stub screen.  Same contract as the Screening module.

---

## Adding a new screen

1. **Create the folder and module**

   ```
   src/web_app/frontend/my_screen/
   └── index.js
   ```

   `index.js` must export at minimum:

   ```js
   // frontend/my_screen/index.js
   export function render() {
     // Build or update the DOM inside the view panel.
   }
   ```

2. **Add the HTML panel** in `frontend/index.html`:

   ```html
   <!-- Tab button -->
   <button class="tab" data-view="my_screen">My Screen</button>

   <!-- View panel -->
   <section class="view" data-view-panel="my_screen">
     <!-- screen content here -->
   </section>
   ```

3. **Wire navigation** in `frontend/main.js`:

   ```js
   import { render as renderMyScreen } from './my_screen/index.js';

   // Inside setView():
   } else if (view === 'my_screen') {
     renderMyScreen();
   }
   ```

4. **Add API routes** (if needed) in `src/web_app/api/` and import them in `src/web_app/api/__init__.py`.

No build step, no config changes — the browser loads the new module automatically via native ES module resolution.

---

## Design conventions

- **No build tooling.** All JavaScript is written as native ES modules loaded directly by the browser.  Import paths use relative URLs.
- **Shared state via `STATE`.** All screens read and write the same `STATE` object.  Do not keep per-module global variables for data that other screens might need.
- **DOM element cache via `els`.** Populate `els` in `main.js` during bootstrap.  Screen modules that need to access an element that isn't in `els` yet should add it there rather than calling `querySelector` at render time.
- **Cross-module callbacks via `callbacks`.** When a screen needs to trigger navigation or a data refresh it should call `callbacks.setView(...)` or `callbacks.refreshJobs()` rather than importing `main.js`.
- **`el()` over `innerHTML`.** Use the `el()` helper from `common/utils.js` to build DOM nodes programmatically.  This avoids XSS risks and keeps the code auditable.
- **`log()` for all user-visible events.**  Use `log('info' | 'warn' | 'error' | 'debug', message)` from `common/console.js` to surface status messages to the operator in the bottom console panel.
