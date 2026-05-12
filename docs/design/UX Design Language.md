# Shade Research — UX Design Language & Refinement Plan

**Version:** 2.0  
**Date:** 2026-05-12  
**Status:** Draft for review  

> **Revision 2.0:** Added §5.2 Component API signatures, §5.3 `.badge` specification,
> §6.6 before/after migration example, §6.7 `data-*` identifier schema,
> expanded §7.5 Backtesting redesign with all sub-screens (Manual, CSV, Results,
> Rolling), §9 Navigation & State Architecture, §10 Error/Empty/Loading states,
> §11 Keyboard & Accessibility with ARIA table, §12 Animation & Motion,
> §14 Testing Strategy with checklists, and Appendix C Deferred Items.
> Fleshed out 14 identified gaps from v1.0 review.

---

## Table of Contents

1. [Problem Inventory](#1-problem-inventory)
2. [Design Principles](#2-design-principles)
3. [Theme & Design Tokens](#3-theme--design-tokens)
4. [Layout System](#4-layout-system)
5. [Component Library](#5-component-library)
6. [Input Integrity & DOM Patching](#6-input-integrity--dom-patching)
7. [Screen-by-Screen Redesign](#7-screen-by-screen-redesign)
8. [Responsive Strategy](#8-responsive-strategy)
9. [Navigation & State Architecture](#9-navigation--state-architecture)
10. [Error, Empty & Loading States](#10-error-empty--loading-states)
11. [Keyboard & Accessibility](#11-keyboard--accessibility)
12. [Animation & Motion](#12-animation--motion)
13. [Implementation Roadmap](#13-implementation-roadmap)
14. [Testing Strategy](#14-testing-strategy)

---

## Visual Reference

> **Note:** Current-state screenshots with problem callouts and target-state wireframes
> should be placed in `docs/design/screenshots/` and linked here. Until those are
> captured, refer to the ASCII layout diagrams in §7.

---

## 1. Problem Inventory

These are the specific, verifiable issues observed in the current v1 UI. Each
problem has been traced to its code-level root cause.

### 1.1 Scroll Jumping
- **Root cause:** `replaceChildren()` is called on many containers (`els.inspectorBody`, `els.pipelineList`, `els.stepLibrary`, `#setup-list`, `#catalog-preview`) on every state mutation. This destroys and recreates DOM nodes even when the content hasn't structurally changed.
- **Effect:** Any click in a scrollable region that triggers a render cascade (e.g., clicking a pipeline step, toggling a checkbox, changing an input value) resets scroll position to the top of every rebuilt container.
- **Affected areas:** Orchestrator pipeline list, Orchestrator inspector, Main dashboard setup list, Main dashboard step catalog preview.

### 1.2 Inconsistent Information Density
- **Main dashboard hero-strip:** Consumes 107px of vertical space for a decorative heading (`"Pipeline dashboard and setup launcher"`) and a paragraph that add zero actionable information. The 4 metric tiles below it float in an otherwise empty row.
- **Main dashboard side-stack:** The "Local Setups" panel is fixed at 220px height regardless of content, forcing empty space when 1-2 setups exist.
- **Orchestrator:** Information density is appropriate. The 4-column grid (sidebar 260px, main 1fr, resizer 8px, inspector 340px) is well-proportioned.
- **Screening:** Config area takes variable height (`max-height: 50%`) leaving the results table to fight for space. The expression bar section is padded but functional.
- **Security Analysis:** The toolbar/header/tabbar stack uses fixed-height bands competently. Metrics grid uses a clean tile layout.

### 1.3 Non-Functional Labels
- **Main hero-strip** contains the entirely decorative:
  - `.kicker` — `"Main screen"`
  - `h1` — `"Pipeline dashboard and setup launcher"`
  - `p` — `"Review recent runs, jump into the orchestrator, and keep the workspace compact and fast."`
- **Panel subtitles** repeat what the panel title already communicates:
  - `"Latest pipeline executions returned by the API."`
  - `"Saved in this browser."`
  - `"Discovered from /api/steps."`
  - `"Selected steps in execution order."`
  - `"Select a step to edit its configuration."`
- **Security Analysis empty state** has `"Search for a company to begin"` + `"Type a ticker, EDINET code, or company name above."` — two lines where one behavioral hint would suffice.

### 1.4 Text Input Cursor Reset
- **Root cause:** The `createFieldInput` function in `orchestrator/index.js` uses `addEventListener('change', ...)` — which fires on blur. But the commit callback calls `syncPipelineState()` → which may trigger a full `renderInspector()` if `selectStep()` was called from elsewhere, destroying the focused input.
- **Secondary cause:** In the Screening criteria builder, clicking a token to edit creates a new `<input>` inline. After editing, the criteria are rebuilt, replacing the focused input.
- **Affected areas:** Step config fields in the Orchestrator inspector, screening expression tokens.

### 1.5 Missing Responsive Design
- **Single breakpoint at 1320px** that stacks all columns into a single column. Below 1320px, the sidebar, main, and inspector become a vertical stack with no scroll management or viewport awareness.
- **No fluid scaling** — all widths are in fixed `px` (sidebar 260px, inspector 340px, console 200px). No `vw`, `%`, `fr`-based proportional scaling for the workspace.
- **Fixed `height: 100vh`** on `.app-shell` means any viewport under ~700px height will clip content. At mobile sizes (<768px), the 4-column orchestration layout is unusable.
- **Console is always 200px** with no collapse-to-44px option on smaller screens (despite `body.console-collapsed` existing).

### 1.6 DOM Rebuild Cascades
- **Current pattern:** Every state mutation triggers a full `replaceChildren()` of one or more containers. The call chain is:
  `user action → commit to STATE → syncPipelineState() → renderPipelineList() + renderMain() + renderInspector()`
- **Effect:** Even a single checkbox toggle rebuilds the entire pipeline list (all steps), the main dashboard metrics, and the inspector — destroying scroll positions and any in-progress interactions.
- **Waste:** In most cases, only the affected step's row needs its `is-selected` / `is-done` class toggled, or a single badge text needs updating.

---

## 2. Design Principles

These principles govern all design decisions going forward. Every proposed change will be measured against them.

### 2.1 Every Pixel Earns Its Place
No decorative text, no "welcome" headings, no self-describing UI. If a label doesn't communicate actionable information right now, it's not needed. Empty space is wasted unless it serves a deliberate grouping/separation function.

### 2.2 Information Density by Screen Purpose
| Screen | Purpose | Density Target |
|--------|---------|----------------|
| Main Dashboard | At-a-glance system status + quick-launch | High — show everything in one viewport, no scrolling |
| Orchestrator | Configuration authoring | Medium-high — fit pipeline + inspector side by side, allow scrolling in lists |
| Screening | Interactive data exploration | Variable — config compact, results table fills remaining space |
| Security Analysis | Single-company deep dive | Medium — metrics header compact, data tables scrollable |
| Backtesting | Parameter entry + results review | Medium — config compact, results fill remaining space |

### 2.3 DOM Stability
- Never destroy an element while the user is interacting with it.
- Replace children only when the *structure* of a list changes (items added/removed/reordered).
- For *state* changes (highlight, enable/disable, status), update classes and text content on existing DOM nodes.
- Input values must be synced to STATE via `input` events (not just `change`/blur) but rendering must never replace a focused input.

### 2.4 Consistent Component Language
All screens use the same building blocks: panels, tables, form rows, buttons, badges, metric tiles, section cards. No per-screen CSS that reinvents an existing pattern.

### 2.5 Layout Adapts, Content Scales
- The layout must work from 1920px down to 1024px without horizontal scrolling.
- Below 1024px, sidebars collapse to overlays or tabs.
- Column widths use `fr` units with `minmax()` constraints, not fixed pixels.
- All numeric/date inputs and text areas must respect `min-width` and `max-width` constraints.

---

## 3. Theme & Design Tokens

### 3.1 Color Palette (Refined)

The existing dark palette is strong. The refinement focuses on semantic naming and filling gaps.

```css
:root {
  /* Background hierarchy (darkest → lightest) */
  --color-bg-root:      #0a0f18;   /* Page background */
  --color-bg-surface:   #101826;   /* Panel backgrounds */
  --color-bg-elevated:  #162032;   /* Elevated panels, modals, popovers */
  --color-bg-input:     #0b1320;   /* Form inputs */
  --color-bg-hover:     rgba(255,255,255,0.04);

  /* Borders */
  --color-border:       #243244;   /* Default panel/row borders */
  --color-border-strong:#31435a;   /* Hover/active borders */
  --color-border-accent:#58a6ff;   /* Focus/highlight borders */

  /* Text */
  --color-text:         #d9e2f2;   /* Primary body text */
  --color-text-muted:   #8ea0b8;   /* Secondary, labels, hints */
  --color-text-dim:     #5a6a80;   /* Disabled, placeholder */

  /* Semantic */
  --color-accent:       #58a6ff;
  --color-accent-soft:  rgba(88,166,255,0.14);
  --color-success:      #44d17b;
  --color-warning:      #e0af4f;
  --color-danger:       #ff6b6b;

  /* Shadows */
  --shadow-panel:       0 2px 8px rgba(0,0,0,0.3);
  --shadow-popup:       0 8px 24px rgba(0,0,0,0.5);
  --shadow-modal:       0 16px 48px rgba(0,0,0,0.6);
}
```

### 3.2 Typography

```css
:root {
  --font-mono:    'IBM Plex Mono', 'Consolas', 'Courier New', monospace;
  
  /* Scale — all sizes in rem for accessibility */
  --text-xs:      0.6875rem;   /* 11px — badges, hints, micro labels */
  --text-sm:      0.75rem;     /* 12px — table cells, secondary labels */
  --text-base:    0.8125rem;   /* 13px — body, panel titles, list items */
  --text-md:      0.875rem;    /* 14px — section headings, brand */
  --text-lg:      1.125rem;    /* 18px — metric values */
  --text-xl:      1.25rem;     /* 20px — major headings (used sparingly) */

  --leading-tight: 1.2;
  --leading-base:  1.45;
  --weight-normal: 400;
  --weight-medium: 500;
  --weight-semibold: 600;
  --weight-bold:   700;
}
```

### 3.3 Spacing Scale

```css
:root {
  --space-1: 4px;
  --space-2: 6px;
  --space-3: 8px;
  --space-4: 10px;
  --space-5: 12px;
  --space-6: 16px;
  --space-8: 24px;
  --space-10: 32px;
}
```

**Usage rules:**
- `--space-1` / `--space-2`: internal gaps within a component (badge padding, icon-text gap).
- `--space-3` / `--space-4`: standard panel padding, inter-component gaps.
- `--space-5` / `--space-6`: section separation, panel header padding.
- `--space-8` / `--space-10`: workspace edge padding, major content block separation.
- Never mix px and spacing-scale variables in the same container.

### 3.4 Radius & Visual Weight

```css
:root {
  --radius-none: 0;          /* Panels, tables (current convention — keep) */
  --radius-sm:   2px;        /* Buttons, inputs, badges */
  --radius-md:   4px;        /* Chips, status tags */
}
```

The current `--radius: 0` convention gives the UI its terminal/research-console character. We preserve this as the default but allow `--radius-sm` for interactive elements that benefit from a softer touch target.

### 3.5 Sizing Constraints

```css
:root {
  /* Layout columns — use fr units, these are minimums */
  --sidebar-min:    200px;
  --sidebar-max:    320px;
  --inspector-min:  280px;
  --inspector-max:  520px;
  
  /* Console */
  --console-open:   180px;
  --console-closed: 44px;
  
  /* Inputs */
  --input-height:   32px;
  --input-min-width: 80px;
}
```

---

## 4. Layout System

### 4.1 The Shell (Unchanged Skeleton)

The existing `.app-shell` grid with `52px header / 1fr workspace / console` is correct. All screens share this skeleton. The refinement is in how each workspace fills its `1fr`.

### 4.2 Workspace Layout Patterns

Define **three canonical layout patterns** that every screen uses:

#### Pattern A: Two-Column (Main Dashboard)
```
+------------------------------------------+-----------+
| Primary content (tables, lists)          | Sidebar   |
| fills available space                    | (metrics, |
|                                          | quicknav) |
+------------------------------------------+-----------+
```
CSS: `grid-template-columns: minmax(0, 1fr) var(--sidebar-min);`
- Sidebar column collapses to `360px` and uses `minmax(280px, 360px)`.
- Primary column takes remaining space with `overflow: auto`.

#### Pattern B: Three-Column (Orchestrator)
```
+----------+-----------+---+--------------+
| Sidebar  | Main      | R | Inspector    |
| (steps)  | (pipeline)|   | (config)     |
+----------+-----------+---+--------------+
```
CSS: `grid-template-columns: minmax(var(--sidebar-min), var(--sidebar-max)) minmax(0, 1fr) 8px minmax(var(--inspector-min), var(--inspector-max));`
- The resizer column is 8px with `cursor: col-resize`.
- Resizer drag adjusts both sidebar and inspector simultaneously, clamping to their min/max.

#### Pattern C: Stacked Config + Results (Screening, Backtesting)
```
+------------------------------------------+
| Config panel (compact, collapsible)      |
+------------------------------------------+
| Results (fills remaining height)         |
+------------------------------------------+
```
CSS: `display: flex; flex-direction: column; height: 100%;`
- Config panel uses `flex-shrink: 0` and optional `max-height`.
- Results area uses `flex: 1; min-height: 0; overflow: auto`.

### 4.3 Gap & Padding Conventions

- **Panel internal padding:** `--space-3` (8px) top/bottom, `--space-4` (10px) left/right.
- **Inter-panel gap:** `--space-3` (8px).
- **Workspace edge padding:** `--space-4` (10px) on all sides.
- **No panel-level margin** — spacing is entirely handled by the parent grid/flex gap.

### 4.4 Scrolling

- `overflow: hidden` on `html`, `body`, and `.app-shell` (prevent double scrollbars).
- Each scrollable region (`.panel-body`, `.data-table` wrapper, `.list`) independently sets `overflow: auto`.
- Never set `overflow: auto` on a grid parent — put it on the child that actually needs to scroll.
- All scrollable regions must restore their `scrollTop` after a DOM update that doesn't change their content structure.
- **Critical:** `min-height: 0` on all flex/grid children that contain scrollable regions. Without it, `overflow: auto` won't work — the child will expand to fit content instead of scrolling.

---

## 5. Component Library

Every UI element across all screens must be built from this catalog. No ad-hoc `el('div', { style: '...' })` elements that replicate an existing component.

### 5.1 Core Components

#### 5.1.1 Panel
A bordered container with an optional header.
```
┌──────────────────────────────────┐
│ Panel Title                 [act]│ ← .panel-head
├──────────────────────────────────┤
│ content                          │ ← .panel-body (scrollable)
└──────────────────────────────────┘
```
- **Class:** `.panel`
- **Header:** `.panel-head` with `.panel-title` (required) and optional `.panel-actions` slot
- **Body:** `.panel-body` — scrollable, fills remaining height
- **Removes:** `.panel-subtitle` — eliminated. Panel titles are self-explanatory.
- **Removes:** `.panel-head-meta` — replaced by `.panel-actions` for actionable elements.

#### 5.1.2 DataTable
A dense, sortable data table with sticky headers.
- **Class:** `.data-table` (renamed from `.dense-table` for clarity)
- **Features:** Sticky header row, row hover highlight, optional sortable columns, zebra-striping off (dense mode)
- **Empty state:** Single row with colspan + muted text
- **Loading state:** Placeholder rows with shimmer animation

#### 5.1.3 MetricTile
A compact KPI display.
```
┌──────────────┐
│ LABEL        │ ← .metric-label (11px, muted, all-caps)
│ 1,234.56     │ ← .metric-value (18px, bold)
│ +12.3%       │ ← .metric-sub (11px, semantic color)
└──────────────┘
```
- **Tones:** `up` (green), `down` (red), `neutral` (muted) — replaces the current string-based tone.
- **Grid:** Container uses `display: grid; grid-template-columns: repeat(auto-fill, minmax(160px, 1fr)); gap: var(--space-3);`

#### 5.1.4 FormField
A labeled input row.
```
Label (required indicator)
[________________________]
  hint text
```
- **Components:** `<label>`, `<input>/<select>/<textarea>`, optional `<span class="hint">`
- **Layout:** `grid; gap: 4px`
- **Required fields:** Asterisk on label, not a separate badge
- **Path fields:** Input + Browse button side by side (existing `.field-with-picker` pattern)

#### 5.1.5 Button (Standardized Variants)
| Variant | Class | Use |
|---------|-------|-----|
| Primary | `.btn-primary` | Run, Submit, Save |
| Secondary | `.btn-secondary` | Cancel, Clear, Reload |
| Ghost | `.btn-ghost` | Misc actions, Browse |
| Danger | `.btn-danger` | Stop, Delete, Remove |
| Icon | `.btn-icon` | Small icon-only actions (↑↓×) |

All buttons share: `font: inherit; padding: 5px 12px; border: 1px solid; cursor: pointer; transition: 0.12s;`
- Replace the current ad-hoc classes: `.ghost-btn`, `.accent-btn`, `.danger-btn`, `.tiny-btn`, `#run-btn`, `#stop-btn`
- Buttons in a row use: `.btn-row { display: flex; gap: var(--space-2); }`

#### 5.1.6 Badge / StatusTag
Inline status indicator.
```
[ enabled ] [ done ] [ overwrite ]
```
- **Class:** `.badge`
- **Variants:** `.badge-success`, `.badge-warning`, `.badge-danger`, `.badge-accent`, `.badge-muted`
- **Unified:** Merge `.chip` and `.status-tag` into a single `.badge` component

#### 5.1.7 ListItem (Interactive Row)
For selectable items in sidebars (setups, steps).
```
┌───
│ Title                    meta info
│ One-line summary text...
└───
```
- **Class:** `.list-item`
- **States:** `.is-selected`, `.is-highlight`, `.is-disabled`
- **Replaces:** `.setup-row`, `.catalog-item`, `.step-item`

#### 5.1.8 SectionCard
For grouping related config fields in the inspector.
```
┌── Section Title ──────────────────────┐
│                                        │
│  (kv-grid of form fields)              │
│                                        │
└────────────────────────────────────────┘
```
- **Class:** `.section-card` (renamed from `.section`)
- **Header remains:** `.section-head` with `.section-title`
- **No subtitle** — if the title isn't clear enough, fix the title.

#### 5.1.9 Popup / Dropdown
For load menus, column pickers, criteria builders.
- **Class:** `.popup`
- **Features:** Fixed positioning, z-index 1000+, shadow, border, close-on-click-outside
- **Replaces:** `.popup-menu`, `.scr-pop`, `.scr-pop-menu`, inline-positioned divs

#### 5.1.10 ProgressBar
For pipeline step progress.
- **Class:** `.progress-bar`
- **Replaces:** text-only progress in recent jobs table
- **Structure:** Outer container + inner fill div with `width: N%` and `transition: width 0.3s`

### 5.2 Component API Signatures

Every factory function returns a live DOM node. Functions accept objects for
readability at the call site.

```javascript
// Panel
Panel({ heading: 'Recent Jobs', actions: [reloadBtn], body: tableNode })

// DataTable
DataTable({
  columns: [{ key: 'job_id', label: 'Job ID', className: 'col-code' }],
  rows: jobs,
  rowKey: row => row.job_id,          // stable identity for patching
  renderCell: (row, col) => formatCell(row[col.key], col),
  emptyText: 'No jobs yet',
  sortable: true,
})

// MetricTile
MetricTile({ label: 'API', value: 'Online', tone: 'up' })

// MetricGrid — container for MetricTile children
MetricGrid({ children: [tile1, tile2, tile3, tile4] })

// FormField
FormField({
  label: 'Target Database',
  required: true,
  hint: 'Absolute path or filename relative to project root',
  input: inputElement,                // <input>, <select>, or <textarea>
  after: browseButton,               // optional suffix element
})

// Button
Button({ variant: 'primary', label: 'Run', disabled: false, onClick: run })
Button({ variant: 'danger', label: 'Stop', disabled: true })
Button({ variant: 'ghost', size: 'sm', label: 'Browse', onClick: pickFile })
Button({ variant: 'icon', label: '↑', title: 'Move up', onClick: moveUp })

// Badge
Badge({ text: 'enabled', tone: 'success' })
Badge({ text: 'running', tone: 'warning' })
Badge({ text: 'overwrite', tone: 'accent' })

// ListItem
ListItem({
  title: 'Q1 2026 Analysis',
  meta: '3 active steps',
  badges: [Badge({ text: 'modified', tone: 'warning' })],
  selected: false,
  highlighted: false,
  onClick: () => loadSetup('Q1 2026 Analysis'),
})

// ListSection — a panel section containing ListItems
ListSection({
  title: 'Local Setups',
  items: setupListItems,
  emptyText: 'No setups saved yet',
})

// SectionCard
SectionCard({
  title: 'Step Configuration',
  body: formGridNode,
})

// Popup
Popup({
  anchor: buttonElement,   // positioned relative to this element
  children: menuItems,
  onClose: closeMenu,
})

// ProgressBar
ProgressBar({ value: 45, max: 100, label: '45%' })

// FieldWithPicker — the input+browse combo pattern (promoted from ad-hoc to named)
FieldWithPicker({
  field: fieldDef,         // metadata from steps API
  value: currentValue,
  onChange: commitValue,
})

// Console — manages the footer log panel
Console({ collapsed: false })
```

### 5.3 `.badge` Specification

The unified badge replaces both `.chip` and `.status-tag`. It has exactly these
variants, and the factory function enforces them:

| Tone | CSS Class | Text Color | Border Color | Use |
|------|-----------|------------|--------------|-----|
| `success` | `.badge-success` | `--color-success` | `rgba(68,209,123,0.45)` | done, enabled, online |
| `warning` | `.badge-warning` | `--color-warning` | `rgba(224,175,79,0.45)` | running, modified, pending |
| `danger` | `.badge-danger` | `--color-danger` | `rgba(255,107,107,0.45)` | error, failed, offline |
| `accent` | `.badge-accent` | `--color-accent` | `rgba(88,166,255,0.4)` | overwrite, selected, info |
| `muted` | `.badge-muted` | `--color-text-muted` | `--color-border` | disabled, default, idle |

Badge styles:
```css
.badge {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  border: 1px solid var(--color-border);
  padding: 2px 6px;
  font-size: var(--text-xs);
  line-height: 1.4;
  white-space: nowrap;
  user-select: none;
}
```
Badges are **never interactive**. If a badge needs a click action, it should be
a Button instead.

### 5.4 Component Usage Matrix

| Component | Main | Orchestrator | Screening | Security | Backtesting |
|-----------|------|-------------|-----------|----------|-------------|
| Panel | ✓ | ✓ | ✓ | ✓ | ✓ |
| DataTable | ✓ (jobs) | — | ✓ (results) | ✓ (tables) | ✓ (results) |
| MetricTile | ✓ | — | — | ✓ (header) | ✓ (results) |
| MetricGrid | ✓ | — | — | ✓ (header) | ✓ (results) |
| FormField | — | ✓ (inspector, global) | ✓ (config) | — | ✓ (config) |
| Button | ✓ | ✓ | ✓ | ✓ | ✓ |
| Badge | ✓ (status) | ✓ (step states) | — | — | — |
| ListItem | ✓ (setups, catalog) | ✓ (step library) | — | — | ✓ (saved) |
| ListSection | ✓ | ✓ | — | — | ✓ |
| SectionCard | — | ✓ (inspector) | — | — | — |
| Popup | — | ✓ (load menu) | ✓ (pickers) | ✓ (filters) | ✓ (export) |
| ProgressBar | ✓ (jobs) | — | — | — | — |
| FieldWithPicker | — | ✓ (inspector, global) | — | — | ✓ (config) |
| Console | ✓ | ✓ | — | — | — |

---

## 6. Input Integrity & DOM Patching

This is the most impactful single change: **stop destroying DOM nodes on every state update.**

### 6.1 Current Anti-Pattern

```javascript
// BAD: destroys all children, rebuilds from scratch
function renderPipelineList() {
  els.pipelineList.replaceChildren();
  STATE.pipeline.forEach(step => {
    const row = buildStepRow(step);  // creates fresh DOM
    els.pipelineList.append(row);
  });
}
```

### 6.2 Target Pattern: Patch by ID

```javascript
// GOOD: only updates what changed
function renderPipelineList() {
  const existing = new Map();
  els.pipelineList.querySelectorAll('[data-step-id]').forEach(el => {
    existing.set(el.dataset.stepId, el);
  });

  STATE.pipeline.forEach((step, index) => {
    let row = existing.get(step.id);
    if (!row) {
      // New step — create and insert
      row = buildStepRow(step);
      els.pipelineList.append(row);
    } else {
      // Existing step — patch classes and content
      existing.delete(step.id);
      updateStepRow(row, step, index);
    }
  });

  // Remove steps no longer in pipeline
  for (const [id, el] of existing) el.remove();
}
```

### 6.3 Rules for Patching

1. **Every element that can be updated must have a stable `data-*` identifier.** Pipeline steps use `data-step-id`. Jobs use `data-job-id`. Criteria use `data-crit-id`. Setups use `data-setup-name`.

2. **Class toggling replaces element replacement.** Instead of `replaceChildren()` and rebuilding, toggle `.is-selected`, `.is-done`, `.is-running`, `.is-error` on existing nodes.

3. **Text content updates use `textContent`.** For labels, summaries, and count displays, set `el.querySelector('.step-summary').textContent = newSummary` rather than rebuilding the entire row.

4. **Inputs are NEVER replaced while focused.** Before patching, check `document.activeElement`. If it's inside a container being patched:
   - Skip that container's `replaceChildren()` entirely.
   - Defer the update to the `blur` event of the focused element.
   - Or: keep the input node alive and only update its parent's class list.

5. **Scroll position is preserved.** After a patch operation, if the scrollable element's `scrollTop` was at a non-zero value and the number of children hasn't changed, restore `scrollTop`.

### 6.4 Input Event Model

| Event | Fires | Syncs STATE | Triggers render |
|-------|-------|-------------|-----------------|
| `input` | Every keystroke | Yes (throttled 150ms) | No (just updates STATE) |
| `change` | On blur or Enter | Yes | Yes (deferred to next frame) |
| `focus` | On focus | No | No |
| `blur` | On blur | Yes (final sync) | Yes |

This avoids the current problem where `change` fires on blur and triggers a full render that destroys the input. Instead:
- `input` events sync the value to STATE without rendering.
- `change` events validate, format, and then render (but by then the input has already lost focus).
- The inspector never replaces a `kv-grid` while any input inside it has focus.

### 6.5 Render Scheduling

Use `requestAnimationFrame` batching to avoid redundant renders:

```javascript
let _renderScheduled = false;
function scheduleRender() {
  if (_renderScheduled) return;
  _renderScheduled = true;
  requestAnimationFrame(() => {
    _renderScheduled = false;
    performRender();
  });
}
```

Multiple state mutations within the same frame (e.g., toggling 5 checkboxes) produce exactly one render.

### 6.6 Migrating `createFieldInput` — Before / After

The current `createFieldInput` in `orchestrator/index.js` wires `change` events
that trigger render cascades. Here's the concrete migration:

**Before (current):**
```javascript
function createFieldInput(field, current, commit) {
  const input = el('input', { type: 'text' });
  input.value = String(current ?? '');
  input.addEventListener('change', () => {
    commit(readValueByType(field.field_type, input.value, current));
    // ↑ commit() calls syncPipelineState() + renderPipelineList()
    //   which calls replaceChildren(), destroying this input
  });
  return input;
}
```

**After (target):**
```javascript
function createFieldInput(field, current, commit) {
  const input = el('input', { type: 'text' });
  input.value = String(current ?? '');

  // Sync state silently on every keystroke (throttled, no render)
  input.addEventListener('input', throttle(() => {
    const parsed = readValueByType(field.field_type, input.value, current);
    cfg[field.key] = parsed;  // write to config object directly — no render
  }, 150));

  // Validate + render only on blur (input is no longer focused, safe to rebuild)
  input.addEventListener('change', () => {
    const parsed = readValueByType(field.field_type, input.value, current);
    commit(parsed);
  });

  // Guard: if input is focused when parent tries to replaceChildren,
  // skip the replace. The input keeps its value and cursor position.
  input.addEventListener('focus', () => { input.dataset.focused = '1'; });
  input.addEventListener('blur',  () => { delete input.dataset.focused; });

  return input;
}

// Simple throttle helper
function throttle(fn, ms) {
  let timer = null;
  return (...args) => {
    if (timer) return;
    timer = setTimeout(() => { timer = null; fn(...args); }, ms);
  };
}
```

Key changes:
1. `input` event syncs `cfg[key]` without rendering.
2. `change` event fires on blur — the input has already lost focus, so rebuilding its container is safe.
3. `data-focused` attribute lets the render system skip `replaceChildren()` on any container that holds a focused element.

### 6.7 The `data-*` Identifier Schema

Every element that participates in DOM patching must carry a stable identifier.
Naming convention: `data-{domain}-id`.

| Domain | Attribute | Value | Example |
|--------|-----------|-------|---------|
| Pipeline steps | `data-step-id` | `STATE.pipeline[i].id` (UUID) | `data-step-id="a1b2c3d4"` |
| Jobs | `data-job-id` | Job ID from API | `data-job-id="job_20260512_001"` |
| Screening criteria | `data-crit-id` | `ST._nextId` counter | `data-crit-id="5"` |
| Screening columns | `data-col-id` | Column ID from unified array | `data-col-id="7"` |
| Setups | `data-setup-name` | Setup name string | `data-setup-name="Q1 Analysis"` |
| Backtest results | `data-bt-id` | Result entry ID | `data-bt-id="bt_001"` |
| Saved backtests | `data-bt-saved-id` | Saved entry ID | `data-bt-saved-id="saved_001"` |
| Form fields | `data-field-key` | Config key name | `data-field-key="Target_Database"` |
| Inspector sections | `data-section-id` | Section title slug | `data-section-id="step-config"` |

Rules:
- IDs must be stable for the lifetime of the item. Pipeline step IDs survive reordering and renames.
- IDs survive serialization/deserialization (the UUID survives `deepClone()` round-trips).
- When an item is removed and re-added, it gets a new ID (even if the name is the same).

---

## 7. Screen-by-Screen Redesign

### 7.1 Main Dashboard (`/`)

#### Current Problems
- Hero strip wastes 107px on decorative text.
- 4 metric tiles with excessive padding and empty space.
- "Local Setups" panel has fixed 220px height, wasted when <3 setups.
- "Step Catalog" panel shows only 6 items with empty space below.

#### Redesign

```
┌─ Topbar ───────────────────────────────────────────┐
├────────────────────────────────────────────────────┤
│ ┌─ Recent Jobs ──────────────────────┐┌─ QuickNav ┐│
│ │ Job ID   Status  Step  Progress    ││ Metrics   ││
│ │ abc123…  done    ETL   100%        ││ API: OK   ││
│ │ def456…  done    FS    100%        ││ Pipeline: ││
│ │ ghi789…  error   Ratio 45%         ││  3/7 actv ││
│ │ …                                  ││ Required: ││
│ │                                    ││  5/5 keys ││
│ │                                    ││ Jobs: 12  ││
│ │                                    │├───────────┤│
│ │                                    ││ Setups    ││
│ │                                    ││ • Q1 2026 ││
│ │                                    ││ • Backtest││
│ │                                    ││ • Default ││
│ │                                    │├───────────┤│
│ │                                    ││ Quick Add ││
│ │                                    ││ • get_doc ││
│ │                                    ││ • parse…  ││
│ │                                    ││ • gen_fs  ││
│ └────────────────────────────────────┘└───────────┘│
└────────────────────────────────────────────────────┘
```

**Changes:**
1. **Remove hero-strip entirely.** The topbar already says "Shade Research — Main" via the active tab.
2. **Metric tiles move into the sidebar as a compact 2×2 grid** at the top. Each tile: 9px padding, 11px label, 16px value, no tone text.
3. **"Recent Jobs" table fills the left column** with `flex: 1` and no fixed height constraint. Auto-resizes to fit the viewport.
4. **Sidebar is a single scrollable column** with three sections stacked vertically: Metrics → Setups → Quick Add Catalog. Each section uses a `.panel` with a compact header.
5. **Setup items are compact** — one line each: name + date + step count as a badge. Click navigates to Orchestrator.
6. **Quick Add catalog shows all steps** (not just 6) with a search filter at the top of that section.

### 7.2 Orchestrator (`/orchestrator`)

#### Current Problems
- DOM rebuilds on every interaction (see §6.1).
- Inspector rebuilds entirely on step selection, losing scroll position.
- Setup name input loses cursor position on `change`.

#### Redesign

The 4-column layout is fundamentally correct. Fixes are behavioral, not structural:

1. **Apply DOM patching** (see §6) to all three panels (sidebar, pipeline, inspector).
2. **Sidebar reorder:**
   - Setup name + New/Load/Save buttons first.
   - Run/Stop as prominent buttons below, full-width.
   - Workspace Config as a collapsible section.
   - Step search + library below.
3. **Remove subtitle text** from all panel heads — titles alone are sufficient.
4. **Pipeline list:** Remove `replaceChildren()`. Use `data-step-id` patching. Drag-and-drop preserves DOM nodes.
5. **Inspector:** The `buildStepEditor` outputs stay in the DOM. Only rebuild when `selectedStepId` changes to a *different* step. Re-selecting the same step does nothing.
6. **Input `change` → `input` for text/number fields.** The `change` handler persists to STATE. The `input` handler updates the `cfg` object without triggering renders.

### 7.3 Screening (`/screening`)

#### Current Problems
- Criteria expression bar is functional but the builder popups are complex DOM built inline.
- Clicking a token to edit creates a temporary input that gets destroyed on commit.
- Columns section is at the bottom of config, far from criteria.

#### Redesign

```
┌─ Config (collapsible) ────────────────────────────┐
│ [Run] [Save] [Load]          Date: [____] Status  │
│                                                    │
│ ▶ Screening Details                               │
│                                                    │
│ Criteria:  [ P/E < 15 ] [ ROE > 0.10 ]  [+ Add]  │
│ Columns:   [Ticker] [Company] [P/E] [ROE]         │
│            [+ Add Col] [+ Add Computed]            │
│                                                    │
│ [Update Prices] [Export CSV] [Export BT] [→BT]    │
├────────────────────────────────────────────────────┤
│ ┌─ Results (1,234 rows) ────────────────────────┐ │
│ │ Ticker │ Company    │ P/E  │ ROE   │ …        │ │
│ │ 7203   │ Toyota     │ 8.2  │ 0.12  │          │ │
│ │ 6758   │ Sony       │ 14.1 │ 0.09  │          │ │
│ │ …                                             │ │
│ └────────────────────────────────────────────────┘ │
│ ▶ SQL                                             │
└────────────────────────────────────────────────────┘
```

**Changes:**
1. **Compact config toolbar** — buttons, date, status all in one row. Criteria and columns below as condensed expression bars.
2. **Expression bar editing** — when a token is clicked for editing, the existing token element receives a `contenteditable` span or inline input. On commit, the token text updates in-place — the bar is never destroyed.
3. **Column pickers** use the standardized `Popup` component with search filter.
4. **Results table** is the dominant element — `flex: 1` takes all remaining space. Config panel has `max-height: 40%` with overflow scroll.
5. **SQL display** is a collapsible `details` below results, not inside them.

### 7.4 Security Analysis (`/security`)

#### Current Problems
- The table/metrics layout works well. Minor refinements needed.
- `"Search for a company to begin"` + subtitle is redundant.

#### Redesign

1. **Empty state:** Replace two-line message with one line: `"Search by ticker, code, or company name"` — this is already in the search input placeholder.
2. **Metrics tiles in company header:** Reduce padding to match `MetricTile` component specs. Grid of up to 6 tiles, auto-wrapping.
3. **Tables:** Use standardized `DataTable` with sticky headers and sort indicators.
4. **Filter dropdowns:** Use standardized `Popup` component.
5. **Remove the status indicator text** from the toolbar — the backend status pill in the topbar already handles this.

### 7.5 Backtesting (`/backtesting`)

#### Current Problems
- 3393-line monolith mixing state, rendering, chart management, and event handling.
- Config area is fixed `max-width: 780px` — wastes 30%+ of the workspace on 1440px+ screens.
- Portfolio editor is an ad-hoc table with inline `$('[data-role=ticker]')` queries.
- CSV import drop zone has no drag-over visual feedback beyond a class toggle.
- Chart export buttons are invisible until hover (discoverability problem).
- Multi-result tabs work but have no keyboard navigation.
- The heatmap for rolling backtests has no legend or color scale explanation.
- Saved backtests list has no search/filter and grows unbounded.
- Error and warning banners use inconsistent classes (defined twice in the CSS with different styles).

#### Redesign: Mode Selection

```
┌─ Mode Tabs ──────────────────────────────────────┐
│ [Manual Portfolio]  [From Screener]  [From CSV]  │
├──────────────────────────────────────────────────┤
│  (mode-specific config panel below)              │
└──────────────────────────────────────────────────┘
```

Mode tabs remain structurally unchanged. Add `role="tablist"` and `aria-selected`
for accessibility (§11).

#### Redesign: Manual Portfolio Config

```
┌─ Config ────────────────────────────────────────────────────┐
│ DATES:  [2020-01-01]  to  [2025-12-31]    Presets: 1Y 3Y 5Y│
│                                                              │
│ BENCHMARK TICKER:  [TPX____]    RISK-FREE RATE:  [0.02]    │
│ INITIAL CAPITAL:   [0_______]                                │
│                                                              │
│ PORTFOLIO:                         [+ Add Row]  [Clear All]  │
│ ┌──────┬────────┬─────────┬──────┐                          │
│ │Ticker│ Mode   │ Value   │  ×   │                          │
│ ├──────┼────────┼─────────┼──────┤                          │
│ │59110 │ shares │ 100     │  ×   │                          │
│ │59840 │ shares │ 300     │  ×   │                          │
│ │75750 │ weight │ 0.50    │  ×   │                          │
│ └──────┴────────┴─────────┴──────┘                          │
│                                                              │
│ DURATIONS:  ☑ 1yr  ☑ 2yr  ☑ 3yr  ☑ 5yr  ☑ 10yr            │
│                                                              │
│ [Run]  [Cancel]    Status: Ready                            │
└──────────────────────────────────────────────────────────────┘
```

**Changes:**
1. **Config uses `max-width: 960px`** — still centered, but uses more screen space.
2. **Date inputs side-by-side** with preset chips (1Y, 3Y, 5Y, 10Y) that auto-fill end date.
3. **Benchmark, risk-free rate, and initial capital** in a compact row, not stacked.
4. **Portfolio table uses standardized `DataTable`** — editable cells, not ad-hoc querySelector reads.
5. **Duration chips** remain as-is — functional and clear.
6. **Run/Cancel buttons** are `btn-primary` and `btn-danger`, full-width below config.

#### Redesign: CSV Import

```
┌─ CSV Upload ────────────────────────────────────────┐
│                                                      │
│          ┌──────────────────────────┐               │
│          │  📂  Drop CSV file here  │               │
│          │  or click to browse      │               │
│          └──────────────────────────┘               │
│                                                      │
│  File selected: ols_results_summary_top10.csv       │
│                                                      │
│  Preview (first 5 rows):                            │
│  ┌────────┬──────────┬──────────┬──────┐           │
│  │ Year   │ Ticker1  │ Ticker2  │ ...  │           │
│  │ 2020   │ 7203     │ 6758     │      │           │
│  │ 2021   │ 7203     │ 8306     │      │           │
│  └────────┴──────────┴──────────┴──────┘           │
│                                                      │
│  [Clear File]  [Run]                                 │
└──────────────────────────────────────────────────────┘
```

**Changes:**
1. **Drop zone has three visual states**: idle (dashed border), dragover (solid accent border + tinted background), has-file (solid border + filename shown).
2. **Preview table** appears immediately on file selection, not after a separate parse step.
3. **Clear File button** lets user reset without reloading the page.

#### Redesign: Results

```
├─ Results Tabs ──────────────────────────────────────────┤
│ [Run 1: Manual 3-stock] [Run 2: Top 10 OLS] [× Clear]  │
├─ Parameter Summary ─────────────────────────────────────┤
│ Dates: 2020-01-01 → 2025-12-31 │ Benchmark: TPX │ ...  │
├─────────────────────────────────────────────────────────┤
│ Duration: [1yr] [2yr] [3yr] [5yr] [10yr]               │
├─ Metric Tiles ─────────────────────────────────────────┤
│ Total Return  │ Sharpe  │ Max DD  │ Win Rate           │
│ +142.3%       │ 0.87    │ -23.1%  │ 58.2%              │
├─────────────────────────────────────────────────────────┤
│ ┌─ Cumulative Returns Chart ─────────────────────┐ [⤓]│
│ │  (Chart.js canvas — aspect-ratio: 16/9)        │     │
│ └─────────────────────────────────────────────────┘     │
│                                                         │
│ ┌── Drawdown Chart ───┐ ┌── Annual Returns ──────────┐ │
│ │                     │ │                            │ │
│ └─────────────────────┘ └────────────────────────────┘ │
│                                                         │
│ Per-Company Breakdown  ▾                                │
│ ┌────────┬────────┬─────────┬────────┬──────┐         │
│ │ Ticker │ Return │ Contrib │ Sharpe │ …    │         │
│ │ 59110  │ +56.2% │ 18.7%   │ 0.92   │      │         │
│ │ 59840  │ +32.1% │ 10.7%   │ 0.54   │      │         │
│ └────────┴────────┴─────────┴────────┴──────┘         │
│                                                         │
├─ Saved Results ─────────────────────────────────────────┤
│ • Run 1: Manual 3-stock    +142.3% total   [Load] [Del] │
│ • OLS Top 10 2024          +89.7% total    [Load] [Del] │
└─────────────────────────────────────────────────────────┘
```

**Changes:**
1. **Result tabs** show parameter summary below them for context.
2. **Duration tabs** remain between summary and metrics — functional, no change needed.
3. **Chart export button** (⤓) is always visible, not hidden-until-hover. Positioned in the chart container header.
4. **Per-company table** uses standardized `DataTable` with sortable columns (current sort-by-click behavior is preserved but standardized).
5. **Saved results list** gains a `max-height: 320px` with scroll. Items use `ListItem` with delete confirmation.
6. **Heatmap legend:** Add a color scale bar below the heatmap explaining the green→red gradient (positive→negative returns).

#### Redesign: Rolling Backtest

```
┌─ Rolling Screening Backtest ─────────────────────────────┐
│ Cadence: [Monthly ▾]  Weighting: ☑ Equal  ☑ Value       │
│ Max Companies: [25]  Benchmark: [TPX____]                │
│ Start Period: [2020-01]  End Period: [2025-12]           │
│                                                           │
│ Criteria from screening: P/E < 15  AND  ROE > 0.10      │
│                                                           │
│ [Run Rolling Backtest]  [Cancel]  Progress: 45 / 72      │
│ ████████████████░░░░░░░░░░░░░░░░░░ 62%                   │
├───────────────────────────────────────────────────────────┤
│ Period: [2020-Q1 ▾]  Weighting: [equal ▾]  Duration: 1yr │
├─ Heatmap ────────────────────────────────────────────────┤
│  (Chart.js canvas — full-width)                          │
│                                                           │
│ Legend:  ████ -50%  ████ 0%  ████ +50%  ████ +100%+    │
├─ Drill-down Table ───────────────────────────────────────┤
│ Ticker │ Company  │ Return │ Weight │ …                  │
└───────────────────────────────────────────────────────────┘
```

**Changes:**
1. **Show the criteria being used** — currently the rolling backtest inherits criteria silently from screening. Display them.
2. **Progress bar** during execution (replaces text-only progress).
3. **Heatmap legend** — always visible, not just in documentation.
4. **Drill-down controls** (Period, Weighting, Duration) are dropdowns with clear labels, not hidden state.

#### Code Architecture Note

DO NOT refactor `backtesting/index.js` into multiple files as part of the UX
refresh. The monolith is large but self-contained and well-tested. Splitting it
introduces import/export bugs without improving user experience. Revisit file
organization after Phase 3 (DOM patching) is stable across all screens.

---

## 8. Responsive Strategy

### 8.1 Breakpoints

| Name | Width | Behavior |
|------|-------|----------|
| Full | ≥1400px | All columns visible, sidebars at max width |
| Standard | 1200-1399px | Sidebars at min width, inspector may collapse |
| Compact | 1024-1199px | Orchestrator inspector becomes overlay/tab |
| Narrow | 768-1023px | Two-column becomes single column, side-by-side panels stack |
| Mobile | <768px | Full vertical stack, tabs become hamburger menu |

### 8.2 Per-Screen Breakpoint Behavior

#### Main Dashboard
- **≥1024px:** Two columns (table + sidebar).
- **<1024px:** Single column. Sidebar sections (metrics, setups, catalog) stack below the jobs table.

#### Orchestrator
- **≥1200px:** Three columns (sidebar + pipeline + inspector) with resizer.
- **1024-1199px:** Sidebar collapses to an overlay triggered by a "Steps" button. Pipeline and inspector remain side-by-side.
- **<1024px:** Single column. Inspector becomes a bottom panel or overlay.

#### Screening & Backtesting
- **All widths:** The stacked config+results pattern works at any width since it's vertical.
- **<768px:** Date inputs and button rows wrap.

#### Security Analysis
- **All widths:** Vertical stack (search → header → tabs → table) works naturally.
- **<768px:** Metric tiles in header go from 6-column to 3-column to 2-column grid.

### 8.3 Console Behavior

- **≥1024px:** Console is 180px when open, 44px when collapsed.
- **<1024px:** Console is always collapsed to 44px. Expand on click to an overlay panel that covers the bottom 40% of the viewport.
- **<768px:** Console hidden entirely. A "Show Console" button in the topbar reveals it as a full-width overlay.

### 8.4 Touch Targets

All interactive elements must have a minimum touch target of 32×32px at breakpoints <1024px. This means:
- Buttons get `min-height: 32px; min-width: 32px;`
- Checkbox inputs get `width: 18px; height: 18px;`
- Table rows get `min-height: 36px;`
- Drag handles get `min-width: 28px;`

---

## 9. Navigation & State Architecture

### 9.1 Navigation Model

The application uses **multi-page navigation** (full page loads via `location.href`).
This is deliberate — it keeps each screen's JS bundle self-contained and avoids
the complexity of a client-side router.

**Do not introduce SPA-style navigation.** The current approach is:
- Tabs use inline `onclick="location.href='/...'"` — simple, no JS wiring.
- Each page loads its own `<script type="module">` entry point.
- The `callbacks.setView` function exists for programmatic navigation (e.g.,
  "Load this setup in the Orchestrator" from the Main dashboard) and falls
  back to `window.location.href`.

### 9.2 State Persistence Contract

Multiple persistence mechanisms coexist. Each has a defined scope and lifetime:

| Mechanism | Scope | Lifetime | Used By |
|-----------|-------|----------|---------|
| `STATE` (JS module) | Current page | Page unload | All screens (via `common/state.js`) |
| `localStorage` (`edinet.web.setups`) | Browser, cross-page | Until cleared | Orchestrator setups |
| `localStorage` (`edinet.web.lastSetup`) | Browser, cross-page | Until cleared | Last active setup name |
| `localStorage` (`edinet.backtesting.saved`) | Browser, cross-page | Until cleared | Saved backtest results |
| `sessionStorage` (`screening_state`) | Browser tab | Tab close | Screening criteria + results |
| `sessionStorage` (`sa.state`) | Browser tab | Tab close | Security Analysis view state |
| Server API (`/api/screening/saved`) | Server | Until deleted | Saved screening criteria |

Rules:
1. **`STATE` is ephemeral** — it resets on every page load. Never assume data
   from a previous page survives in `STATE`.
2. **`localStorage` is cross-page** — setups saved in the Orchestrator are
   visible on the Main dashboard. Backtest results persist across sessions.
3. **`sessionStorage` is tab-scoped** — screening state survives page
   navigations within the same tab (e.g., Main → Screening → Backtesting →
   back to Screening) so the user doesn't lose their work. Cleared on tab close.
4. **Server-side persistence** (saved screening criteria) is the source of
   truth for shared/collaborative state. `localStorage` is personal scratchpad.

### 9.3 Cross-Screen Data Flow

```
Main Dashboard
  │
  ├─ "Load Setup" → navigates to Orchestrator with setup pre-loaded
  │                   (via localStorage read on Orchestrator bootstrap)
  │
  ├─ "Quick Add Step" → navigates to Orchestrator and adds the step
  │
  └─ Recent Jobs → read-only view, no navigation triggers

Orchestrator
  │
  └─ Run Pipeline → jobs appear in Main dashboard's Recent Jobs table
                     (fetched fresh via /api/jobs on next page load)

Screening
  │
  ├─ "Export Backtest" / "Backtest →" → navigates to Backtesting
  │    with criteria/columns passed via sessionStorage
  │
  └─ Results → persisted to sessionStorage on beforeunload
                restored on next Screening page load

Backtesting
  │
  ├─ "From Screener" mode → reads screening state from sessionStorage
  │
  └─ Results → saved to localStorage (optional, user triggers)
                loaded on next Backtesting page load

Security Analysis
  │
  └─ Search results → persisted to sessionStorage
       view state (active tab, hidden columns) → persisted to sessionStorage
```

### 9.4 Console State Across Pages

The console (`STATE.logs`) is per-page. Logs do not carry over between page
navigations. This is intentional — each page has its own operational context.

If cross-page log persistence is desired in the future, implement it via
`sessionStorage` with a maximum of 200 entries.

---

## 10. Error, Empty & Loading States

Every screen must handle these three states consistently. The current
implementation is inconsistent — some screens have empty states, others show
blank panels.

### 10.1 Empty State Pattern

An empty state appears when a list, table, or results area has no data to
display. It must communicate **what's missing** and **how to fix it**.

```
┌──────────────────────────────────────────┐
│                                          │
│              (icon — 24px)               │
│                                          │
│         No setups saved yet              │
│    Save a pipeline in the Orchestrator   │
│         to see it appear here            │
│                                          │
└──────────────────────────────────────────┘
```

**Class:** `.empty-state`
**Structure:** icon (optional) + title (required) + subtitle (optional, actionable)

Screen-by-screen:

| Screen | Empty Condition | Title | Subtitle |
|--------|----------------|-------|----------|
| Main — Jobs | No jobs returned | `No pipeline runs yet` | `Run a pipeline in the Orchestrator to see results here` |
| Main — Setups | No localStorage setups | `No setups saved` | `Save a pipeline setup in the Orchestrator` |
| Orchestrator — Pipeline | No steps added | `Pipeline is empty` | `Click a step in the library to add it` |
| Orchestrator — Step library | Search filters all | `No matching steps` | `Try a different search term` |
| Screening — Results | No results from query | `No companies match` | `Adjust your criteria and try again` |
| Security — Company | No company selected | `Search for a company` | `Type a ticker, code, or name above` |
| Backtesting — Results | No results yet | `Run a backtest to see results` | `Configure parameters above and click Run` |
| Backtesting — Saved | No saved results | `No saved backtests` | `Run a backtest and save the results` |

### 10.2 Loading State Pattern

Loading indicators appear when data is being fetched. Two variants:

**Inline loading (for lists/tables):**
- Show 3-5 placeholder rows with a shimmer/skeleton animation.
- Each placeholder row is a `div` with `class="skeleton"` — a gray bar that pulses.
- Do not show a spinner overlay that blocks interaction.

**Overlay loading (for blocking operations like pipeline run):**
- Show a centered spinner with status text in the panel being blocked.
- The overlay uses `position: absolute; inset: 0; background: rgba(10,15,24,0.85);`
- Status text updates as the operation progresses.

**Skeleton CSS:**
```css
.skeleton {
  height: 14px;
  background: linear-gradient(90deg, var(--color-bg-surface) 25%, var(--color-bg-elevated) 50%, var(--color-bg-surface) 75%);
  background-size: 200% 100%;
  animation: skeleton-shimmer 1.5s ease-in-out infinite;
  border-radius: var(--radius-sm);
}
@keyframes skeleton-shimmer {
  0% { background-position: 200% 0; }
  100% { background-position: -200% 0; }
}
```

### 10.3 Error State Pattern

Errors appear inline, not as modal dialogs. The affected panel shows an error
banner at its top.

```
┌─ Panel Title ───────────────────────────────────────┐
│ ⚠ Failed to load steps: Connection refused           │
│                                            [Retry]  │
├──────────────────────────────────────────────────────┤
│  (panel content — may be empty or show stale data)   │
└──────────────────────────────────────────────────────┘
```

**Class:** `.error-banner`
**Structure:** Error icon (⚠), message text, optional Retry button.
**Behavior:**
- The banner replaces the panel's content area until dismissed or retried.
- The Retry button re-invokes the failed operation.
- Errors never use `alert()` or `confirm()`.
- Pipeline step errors appear in the pipeline list as `is-error` state on the
  step row, with the error message in a tooltip or expanded detail.

**Error message format:**
```
⚠ [operation] failed: [short reason]
```
Example: `⚠ Jobs reload failed: Network error`

### 10.4 Warning State Pattern

Warnings are non-blocking. They appear as a dismissable banner above the
relevant content.

```
┌──────────────────────────────────────────────────────┐
│ ⚡ Portfolio total weight is 0.50, not 1.00    [×]   │
└──────────────────────────────────────────────────────┘
```

**Class:** `.warning-banner`
**Behavior:** Dismissable with the × button. Reappears if the condition persists on next render.

---

## 11. Keyboard & Accessibility

### 11.1 Keyboard Shortcuts (Global)

All shortcuts use `Ctrl` (or `Cmd` on macOS). They work regardless of which
page is active.

| Shortcut | Action | Scope |
|----------|--------|-------|
| `Ctrl+Enter` | Run pipeline / Execute screening | Orchestrator, Screening, Backtesting |
| `Ctrl+S` | Save current setup | Orchestrator |
| `Ctrl+N` | New setup | Orchestrator |
| `Alt+↑` | Move selected step up | Orchestrator pipeline |
| `Alt+↓` | Move selected step down | Orchestrator pipeline |
| `Delete` | Remove selected step | Orchestrator pipeline |
| `Escape` | Close any open popup/dropdown | Global |
| `Ctrl+/` | Show keyboard shortcut help | Global |

**Rules:**
- Shortcuts only fire when no input/textarea/select is focused (except
  `Ctrl+Enter` which always fires — submit the form).
- `Escape` has priority: it always closes popups first, then clears selection.

### 11.2 Focus Management

- **Page load:** Focus moves to the first interactive element in the main
  content area (skip the topbar). For the Main dashboard, focus the jobs table.
  For the Orchestrator, focus the setup name input.
- **Popup open:** Focus moves to the first focusable element inside the popup.
  On close, focus returns to the trigger element.
- **Step selection:** Clicking a pipeline step does NOT move focus to the
  inspector (avoid scroll-jumping the pipeline list). User tabs to inspector
  manually or clicks into it.
- **Tab order:** Topbar tabs → workspace content (left to right, top to
  bottom) → console.

### 11.3 ARIA Attributes

| Element | ARIA | Notes |
|---------|------|-------|
| Topbar nav | `role="navigation" aria-label="Primary navigation"` | Already present |
| Active tab | `aria-current="page"` | Set by JS on page load |
| Panel | `role="region" aria-label="Panel Title"` | Matches `.panel-title` text |
| DataTable | `role="table"` | Along with `role="rowgroup"`, `role="row"`, `role="columnheader"`, `role="cell"` |
| Sortable column | `aria-sort="ascending|descending|none"` | Updates on sort click |
| Pipeline step | `role="listitem"` (inside `role="list"` container) | |
| Step enable checkbox | `aria-label="Enable step: get_documents"` | Descriptive label |
| Popup | `role="dialog" aria-modal="true" aria-label="..."` | |
| Badge | `aria-label="Status: enabled"` | For screen readers |
| Console log | `aria-live="polite" aria-label="Event log"` | Already present |
| Expandable details | `<details>` element (native) | Already used in some places |
| Progress bar | `role="progressbar" aria-valuenow="45" aria-valuemin="0" aria-valuemax="100"` | |
| Error banner | `role="alert"` | Screen reader announces immediately |
| Warning banner | `role="status"` | Screen reader announces at next opportunity |

### 11.4 Reduced Motion

Respect `prefers-reduced-motion`:

```css
@media (prefers-reduced-motion: reduce) {
  *, *::before, *::after {
    animation-duration: 0.01ms !important;
    transition-duration: 0.01ms !important;
  }
}
```

When reduced motion is preferred:
- Skeleton shimmer becomes a static gray bar.
- Progress bar fill snaps to position without transition.
- Popup open/close is instant.

---

## 12. Animation & Motion

Motion is sparse — this is a data-dense research tool, not a marketing site.
Animations should be functional, not decorative.

### 12.1 Transition Timing

All interactive transitions use `0.12s ease` — fast enough to feel instant,
slow enough to register.

```css
:root {
  --transition-fast: 0.10s ease;
  --transition-base: 0.12s ease;
  --transition-slow: 0.18s ease;
}
```

### 12.2 Where To Animate

| Element | Property | Timing | Purpose |
|---------|----------|--------|---------|
| Button hover | `background`, `border-color`, `color` | `--transition-base` | Feedback |
| Input focus | `border-color`, `background` | `--transition-base` | Focus indication |
| Badge state change | `color`, `border-color` | `--transition-base` | Status update |
| Panel border highlight | `border-color` | `--transition-base` | Selection feedback |
| Popup open | `opacity`, `transform: translateY(-4px)` | `--transition-fast` | Spatial context |
| Progress bar fill | `width` | `--transition-slow` | Progress tracking |
| Skeleton shimmer | `background-position` | `1.5s ease-in-out infinite` | Loading indication |

### 12.3 Where NOT To Animate

- **Table row insertion/removal** — causes layout thrashing. Use instant swaps.
- **Pipeline step reordering** — drag-and-drop provides its own visual feedback.
- **Text content updates** — instant text swaps are expected in a terminal-style UI.
- **Scroll position restoration** — must be synchronous, no animation.
- **Tab switching** — instant content swap. No cross-fade.

### 12.4 Pipeline Run Lifecycle Visual States

During a pipeline run (which may take 2-10 minutes), each step transitions
through states. The visual treatment must be clear but not distracting:

```
idle ──→ running ──→ done
  │          │          │
  │          │          └── green left border + "done" badge
  │          │
  │          └── blue left border + spinner icon + "running" badge
  │
  └── default border + no badge (or "idle" muted badge)

Any state ──→ error: red left border + error message expanded below the step row
```

**Running state animation:** A subtle pulse on the left border (`@keyframes pulse-border`)
to indicate activity without being distracting.

```css
@keyframes pulse-border {
  0%, 100% { border-left-color: var(--color-accent); }
  50% { border-left-color: rgba(88, 166, 255, 0.4); }
}
.pipeline-step.is-running {
  border-left-color: var(--color-accent);
  animation: pulse-border 1.8s ease-in-out infinite;
}
@media (prefers-reduced-motion: reduce) {
  .pipeline-step.is-running { animation: none; }
}
```

### 12.5 Console Log Animation

New log entries slide in subtly:

```css
.log-line {
  animation: log-enter 0.15s ease-out;
}
@keyframes log-enter {
  from { opacity: 0; transform: translateY(-6px); }
  to { opacity: 1; transform: translateY(0); }
}
```

Only the most recent 5 entries animate (avoid re-animating the entire log on render).

---

## 13. Implementation Roadmap

### Phase 0: Design Tokens (No behavior change)
**File:** `frontend/common/styles.css` — redefine CSS custom properties.

1. Replace current `:root` variables with the new token set (§3).
2. Map old variable names to new ones where possible to avoid sweeping class changes.
3. Verify all screens render identically with the new tokens.

**Risk:** Low. Pure CSS variable remapping.
**Regression check:** Open all 4 screens, verify colors and spacing unchanged.

### Phase 1: UI Cleanup (Visual-only)
**Files:** `main/main.html`, `orchestrator/orchestrator.html`, `orchestrator/index.js`, `main/main.js`, `screening/index.js`, `security_analysis/index.js`

1. **Remove decorative text:**
   - Delete hero-strip from `main.html` (or replace with compact metrics-only row).
   - Remove all `.panel-subtitle` text content from panel heads.
   - Remove `.kicker` elements.
   - Simplify security analysis empty state to one line.
2. **Replace ad-hoc buttons with standardized classes:** `.btn-primary`, `.btn-secondary`, `.btn-ghost`, `.btn-danger`, `.btn-icon`.
3. **Consolidate** `.chip` + `.status-tag` → `.badge`.
4. **Standardize panel padding and gaps** to the spacing scale (§3.3).

**Risk:** Low-Medium. CSS class changes may cascade. Verify each screen.
**Regression check:** Open all screens, verify layout and interactivity.

### Phase 2: Component Extraction (No behavior change)
**Files:** New file `frontend/common/components.js` — export factory functions.

1. Extract `Panel(heading, body)` — creates panel with standardized header.
2. Extract `DataTable(columns, rows, opts)` — creates table with sticky headers.
3. Extract `MetricTile(label, value, tone)` — replaces current `metric()`.
4. Extract `FormField(label, input, hint)` — replaces `createFieldRow`.
5. Extract `Badge(text, variant)` — replaces inline chip/status-tag creation.
6. Extract `ListItem(item)` and `ListSection(title, items)`.
7. Migrate screens one at a time to use the new components.
8. Verify visual parity.

**Risk:** Medium. Each screen migration is independent. Test incrementally.

### Phase 3: DOM Patching (Behavioral)
**Files:** `orchestrator/index.js` (primary), `screening/index.js`, all render functions.

1. **Pipeline list patching** — use `data-step-id` to update in-place instead of `replaceChildren()`.
2. **Inspector patching** — only rebuild when `selectedStepId` changes to a different step.
3. **Main dashboard** — patch jobs table rows by `data-job-id`, not full rebuild.
4. **Screening criteria** — update expression bar tokens in-place.
5. **Implement render batching** via `requestAnimationFrame` scheduler.
6. **Implement input safety** — never destroy a focused input's container.

**Risk:** High. This is the core behavioral change. Needs careful testing of all interaction flows.
**Regression check:** 
- Click pipeline steps, verify scroll position preserved.
- Edit text fields, verify cursor position preserved.
- Toggle checkboxes rapidly, verify no flicker.
- Drag to reorder steps, verify smooth.

### Phase 4: Responsive Layout
**Files:** `frontend/common/styles.css` — add media queries. Minor HTML adjustments.

1. Add breakpoint media queries with the rules from §8.
2. Add console collapse behavior for narrow screens.
3. Add inspector overlay for orchestrator at <1200px.
4. Test at 1920px, 1366px, 1200px, 1024px, 768px, 480px widths.

**Risk:** Medium. Layout changes may break existing screen-specific styles.
**Regression check:** Resize browser window through all breakpoints on each screen.

### Phase 5: Information Density Tuning
**Files:** All screen files — fine-tuning after the above phases are stable.

1. Adjust panel heights from fixed `px` to `fr` or `auto`.
2. Reduce MetricTile padding from 10px/12px to 8px/10px.
3. Reduce table cell padding from 9px/10px to 6px/10px.
4. Remove redundant row gaps where content naturally groups.
5. User acceptance testing — is the density right?

**Risk:** Low. Tuning values only. Easy to revert.

---

## 14. Testing Strategy

### 14.1 Visual Regression Testing

After each phase, manually verify these scenarios on every screen:

**Checklist (printable, run after every phase):**

```
□ Main dashboard loads without console errors
□ Main dashboard — jobs table renders rows correctly
□ Main dashboard — setup list click navigates to Orchestrator
□ Main dashboard — step catalog click navigates to Orchestrator + adds step
□ Orchestrator — step library populates from API
□ Orchestrator — add step to pipeline, verify it appears
□ Orchestrator — reorder steps via drag, verify order persists
□ Orchestrator — reorder steps via arrow buttons, verify correct
□ Orchestrator — select step, verify inspector shows correct config
□ Orchestrator — edit text field, verify cursor position preserved
□ Orchestrator — edit text field, verify value persists on blur
□ Orchestrator — toggle step enabled, verify badge updates
□ Orchestrator — toggle overwrite, verify badge appears/disappears
□ Orchestrator — remove step, verify pipeline list updates
□ Orchestrator — Run/Stop buttons toggle correctly
□ Orchestrator — Ctrl+Enter runs pipeline
□ Orchestrator — Ctrl+S saves setup
□ Orchestrator — Escape closes popup
□ Orchestrator — console toggle, clear, export, filter, autoscroll work
□ Screening — metrics load from API
□ Screening — date picker works
□ Screening — add criteria, verify expression bar renders
□ Screening — edit criteria token, verify inline edit works
□ Screening — add column, verify column bar appears
□ Screening — Run produces results table
□ Screening — Export CSV works
□ Security Analysis — search returns results
□ Security Analysis — select company, verify header + tabs render
□ Security Analysis — switch tabs, verify tables render
□ Security Analysis — toggle millions formatting
□ Security Analysis — filter columns
□ Backtesting — mode tabs switch correctly
□ Backtesting — Manual portfolio: add/remove rows, edit values
□ Backtesting — CSV: drop file, verify preview
□ Backtesting — Run produces results with charts
□ Backtesting — duration tabs filter results
□ Backtesting — save/load results
□ Backtesting — rolling backtest config renders criteria from screening
□ Responsive: resize to 1920, 1366, 1200, 1024, 768, 480
□ Responsive: verify no horizontal scrollbar at any width ≥1024px
```

### 14.2 DOM Patching Verification (Phase 3 specific)

These tests verify that DOM patching works correctly — the riskiest change.

```
□ Click pipeline step 1 → scroll down → click pipeline step 5
  → Verify scroll position is preserved (did not jump to top)

□ Focus a text input in the inspector → type "hello" → press Tab
  → Verify value "hello" persisted, cursor was at end during typing

□ Focus a text input → click a different pipeline step
  → Verify input value was committed before inspector rebuilt

□ Toggle 3 checkboxes rapidly
  → Verify only one render occurred (no flicker)

□ Drag step from position 1 to position 5
  → Verify drop was smooth, no duplicate rows appeared

□ Add step, remove step, add same step again
  → Verify new step gets new data-step-id (not recycled)

□ Open console → verify log lines appear with slide animation
  → Only most recent 5 lines animate

□ Run pipeline → verify step states transition: idle → running → done
  → Verify left border animation on running step
  → Verify done badge appears on completion
```

### 14.3 Browser Compatibility

The application targets modern browsers (Chrome 120+, Firefox 120+, Edge 120+,
Safari 17+). Features used:

| Feature | Support |
|---------|---------|
| CSS Grid | Full support |
| CSS Custom Properties | Full support |
| ES Modules (`import`/`export`) | Full support |
| `replaceChildren()` | Full support |
| `crypto.randomUUID()` | Full support |
| `File System Access API` | Chrome/Edge only; Firefox/Safari fallback to `<input type="file">` |
| `<details>` element | Full support |
| `AbortController` | Full support |
| `requestAnimationFrame` | Full support |
| `prefers-reduced-motion` | Full support |

No polyfills needed. The File System Access API already has a fallback path
(`pickPathLikeValue` falls back to a hidden `<input type="file">`).

### 14.4 Performance Budget

| Metric | Target |
|--------|--------|
| First contentful paint | < 300ms (all screens are server-rendered HTML) |
| Time to interactive | < 500ms (JS bundles are small, < 50KB per page) |
| Render after state change | < 16ms (within one frame) |
| Memory (idle) | < 30MB heap (no large data in JS, data is in backend DB) |
| Longest JS file | `backtesting/index.js` (3393 lines) — acceptable as-is, split later |

---

## Appendix A: Current State → Target State Mapping

| Current Element | Problem | Target | Phase |
|----------------|---------|--------|-------|
| `.hero-strip` | 107px decorative waste | Removed; metrics moved to sidebar top | 1 |
| `.panel-subtitle` | Redundant text | Removed | 1 |
| `.kicker` | Decorative only | Removed | 1 |
| `metric()` | String-based tone, inconsistent | `MetricTile(label, value, tone: up|down|neutral)` | 2 |
| `.dense-table` | Name unclear | `.data-table` | 2 |
| `.chip`, `.status-tag` | Two components, same purpose | `.badge` | 2 |
| `.ghost-btn`, `.accent-btn`, etc. | Ad-hoc naming | `.btn-primary`, `.btn-secondary`, etc. | 1 |
| `#run-btn`, `#stop-btn` | ID-based styling | `.btn-primary.btn-run`, `.btn-danger.btn-stop` | 1 |
| `replaceChildren()` cascades | Scroll jump, input reset | DOM patching by `data-*` IDs | 3 |
| `kvLine()` + inline divs | Inconsistent inspector | `FormField(label, input, hint)` | 2 |
| `section()` | Creates subtitle that gets removed | `SectionCard(title, body)` | 2 |
| `.popup-menu`, `.scr-pop` | Inconsistent popups | `.popup` base + variants | 2 |
| `.setup-row` | Not a reusable pattern | `.list-item` + `ListItem()` factory | 2 |
| `.catalog-item` | Duplicates `.step-item` pattern | `.list-item.is-highlight` | 2 |
| `.workspace-grid` fixed px | Poor responsive behavior | `minmax()` columns + breakpoints | 4 |
| `--sidebar: 260px` | Named for one use | `--sidebar-min`, `--sidebar-max` | 0 |
| `--inspector: 340px` | Named for one use | `--inspector-min`, `--inspector-max` | 0 |
| `--console: 200px` | No collapsed state | `--console-open: 180px`, `--console-closed: 44px` | 0 |

## Appendix B: CSS Class Rename Plan

| Old Class | New Class | Notes |
|-----------|-----------|-------|
| `.dense-table` | `.data-table` | Same styles, clearer name |
| `.chip` | `.badge` | Merged with `.status-tag` |
| `.status-tag` | `.badge` | Merged with `.chip` |
| `.ghost-btn` | `.btn-ghost` | Standardized prefix |
| `.accent-btn` | `.btn-primary` | Semantic name |
| `.danger-btn` | `.btn-danger` | Same style, new class |
| `.tiny-btn` | `.btn-ghost.btn-sm` | Modifier for small variant |
| `.metric` | `.metric-tile` | Avoids clash with `metric()` function |
| `.section` | `.section-card` | Differentiates from `<section>` element |
| `.kv-row` | `.form-field` | Used for labeled inputs now |
| `.kv-grid` | `.form-grid` | Container for form fields |
| `.panel-head` | (unchanged) | Keep |
| `.panel-title` | (unchanged) | Keep |
| `.panel-subtitle` | (removed) | No replacement |
| `.panel-head-meta` | (removed) | Use `.panel-actions` with Badge/Button children |
| `.workspace-grid` | (unchanged) | Keep, update column defs |
| `.main-grid` | (unchanged) | Keep, adjust proportions |
| `.hero-strip` | (removed) | No replacement |
| `.side-stack` | (unchanged) | Keep, update sizing |
| `.compact-list` | `.list` | Generic list container |
| `.setup-row` | `.list-item` | Standardized |
| `.catalog-item` | `.list-item` | Merged with above |
| `.catalog-preview` | `.list` | Generic list container |
| `.step-library` | (unchanged) | Keep, use `.list-item` inside |
| `.step-item` | `.list-item` | Standardized |
| `.pipeline-list` | (unchanged) | Keep |
| `.pipeline-step` | (unchanged) | Keep, add `data-step-id` |
| `.button-row` | `.btn-row` | Standardized prefix |
| `.panel-empty` | `.empty-state` | Standardized empty state |
| `.field-with-picker` | (unchanged) | Promoted to named component `FieldWithPicker()` |

## Appendix C: Deferred Items

These items are explicitly out of scope for this design refresh. They are
recorded here so future work can reference them.

### C.1 Dark/Light Theme Toggle
**Decision:** Dark theme only. The app is a research console — light mode adds
maintenance burden with no user benefit. If demand arises, implement as CSS
custom property swap (not separate stylesheets).

### C.2 Internationalization (i18n)
**Decision:** English only. The app targets Japanese financial data but the
interface language is English. Adding i18n would require string extraction,
translation files, and RTL layout support — out of scope.

### C.3 Mobile-First Redesign
**Decision:** Support down to 1024px. Below that, the app is "best effort" —
it won't be broken but won't be optimized. The target user is on a desktop
monitor doing quantitative research. A proper mobile layout would require a
fundamentally different information architecture.

### C.4 Real-Time Collaboration
**Decision:** Not planned. The app is single-user. Adding WebSocket sync for
shared state would require backend changes beyond the scope of a UI refresh.

### C.5 Custom Chart Component
**Decision:** Keep Chart.js. It handles the current needs (line charts, bar
charts, heatmaps) adequately. A custom canvas renderer would be a 2-4 week
project with no functional gain.

### C.6 Backtesting File Split
**Decision:** Defer. The 3393-line `backtesting/index.js` is large but
well-structured internally. Splitting it risks introducing circular import
bugs. Revisit after Phase 3 (DOM patching) proves stable across all screens.
