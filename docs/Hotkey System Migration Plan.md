# Hotkey System Migration Plan

Status: **Implemented (2026-10-08).** Phases 0–5 are done; see `frontend-v2/src/hotkeys/README.md` for the standard as built. Deviations from this plan:

- Scope files hold metadata only; a screen passes handlers keyed by hotkey id to `useHotkeyScope(scope, handlers)` instead of `action: ctx => …` entries, which keeps each screen's live state in its component and needs no per-screen context type.
- A hotkey may have several default keys (`['j', 'ArrowDown']`); the server stores a list per id (at most four).
- Scopes declare a `parent`; conflicts are checked along parent chains and against the global scope, so sibling tabs may reuse `j`/`k`.
- Keys handled inside controls (focused lists, text boxes) are declared `fixed`: listed and documented, not dispatched or rebindable (open question 2 below). Chords that must work while typing (`Ctrl+Enter`, `Ctrl+S`) are regular hotkeys with `whileTyping` and stay rebindable to other chords.
- Arbitration keeps one `window` listener per mounted scope, as before (inner components mount first and win); `catalog.test.ts` guarantees no default depends on that order.
- Settings live in an Account-page section (open question 1); there is no per-key disable (question 3); both proposed alignments were applied (question 4).
Scope: `frontend-v2/` (the React 19 + TypeScript + Vite workstation UI).
Goal: replace the current "hotkeys scattered across ~30 files" system with a single, centralized, standardized hotkey system that is generic, scalable, and simple.

This document is a plan. It records the current state, the problems, the proposed architecture, the standard for adding hotkeys, the user-facing settings surface, a phased migration, a testing strategy, and the risks. It is written so that implementation can start from it without re-deriving any of the decisions.

---

## 1. Goals and requirements

The migration must deliver four properties:

1. **One centralized place in the user panel** where every hotkey on every screen can be viewed, set up, and changed.
2. **A consistent default hotkey setup** that is the same everywhere out of the box.
3. **A pre-established standard** for how to add a hotkey to a new or existing screen, so a developer never invents a new pattern.
4. **Far less "code all over the place"** — a single small change (add a key, change a key, change what a key does) should touch one file, not many.

Everything must be done **generically and scalably**: adding a screen or a hotkey should be a small, mechanical step, and the settings UI and the in-app help should update automatically without extra code.

### Non-goals (explicit)

- No new global key-chord system (e.g. a leader key) beyond the existing `G`-then-letter navigation, which is kept.
- No per-key enable/disable or per-key delay tuning in v1 (see §11).

### Decisions already made (per review)

- **Persistence is server-side.** Hotkey preferences are stored against the user's account on the backend, not in `localStorage`. See §7.
- **Aligning existing keys to the standard is required**, not an optional follow-up. It is a numbered migration phase (§6, Phase 4) with a concrete proposed mapping (§4.4).

---

## 2. Current state

### 2.1 The pieces that exist today

| Piece | File | Role |
|---|---|---|
| `useHotkeys` hook | `frontend-v2/src/hooks/useHotkeys.ts` | Single-key page shortcuts. `useHotkeys(bindings, enabled)`. Listens on `window` `keydown`; skips typing targets, skips when a modifier is held, skips when `event.defaultPrevented`. **Single keys only — no modifier support.** |
| `isTypingTarget` | `frontend-v2/src/hooks/useHotkeys.ts` | Shared helper: is the focused element a text field? |
| `GlobalHotkeys` | `frontend-v2/src/components/GlobalHotkeys.tsx` | Global `G`-then-letter navigation, `?` help, and `Shift+Tab` to leave a field. Mounted once in `AppShell`. |
| `pageShortcuts` | `frontend-v2/src/components/pageShortcuts.ts` | The `G`-then-letter destinations (12 pages). Centralized. |
| `ShortcutsDialog` | `frontend-v2/src/components/ShortcutsDialog.tsx` | Shared modal that renders a list of `ShortcutGroup`s. |
| Per-page `SHORTCUTS` constants | 12 feature files | Hand-written documentation of each page's keys, duplicated next to the bindings. |
| `researchShortcuts.ts`, `chatShortcuts.ts` | research / chat features | Partial centralization attempts: docs-only, still separate from the bindings. |

### 2.2 How a key works today

1. A screen calls `useHotkeys({ key: handler, ... }, enabled)` with an inline object of handlers.
2. The hook adds a `window` `keydown` listener. On a matching single key (no modifier, not typing, not already handled), it calls the handler.
3. Arbitration between overlapping scopes is by `event.defaultPrevented` — the first listener to fire and call `preventDefault` wins.
4. Separately, each screen hand-writes a `SHORTCUTS: ShortcutGroup[]` constant and renders it in a `ShortcutsDialog`, opened by a `?` binding that each screen also hand-wires.
5. `GlobalHotkeys` handles `?` with a `setTimeout` trick: it opens the *global* help only if no page claimed `?` during the same dispatch. This is fragile and order-dependent.

### 2.3 Full inventory of hotkey call sites

There are **35 `useHotkeys` call sites** across **30 files**, plus the global component. The keys below are the current defaults.

**Global / shell**

| File | Keys | Purpose |
|---|---|---|
| `components/GlobalHotkeys.tsx` | `G`+letter, `?`, `Shift+Tab` | Navigate, help, leave field |
| `components/GlobalCompanySearch.tsx` | `/` | Focus the global company search |

**Analysis**

| File | Keys | Purpose |
|---|---|---|
| `features/analysis/AnalysisWorkspaceUnified.tsx` | `? 1 2 3 4 d t n p b` | Help, jump to 4 sections, focus discussion/tag/note, compare, backtest |
| `features/analysis/FinancialHistoryWorkspace.tsx` | `[ ] v e f c x` | Cycle table, cycle view, toggle empty, filter, chart type, clear slots |
| `features/analysis/FilingsPanel.tsx` | `o` | Open first filing |
| `features/analysis/PricePanel.tsx` | `- + =` | Step price range |

**Auth / account**

| File | Keys | Purpose |
|---|---|---|
| `features/auth/AccountPage.tsx` | `1-7 v ?` | Jump to 7 sections, view public profile, help |
| `features/auth/AdminPage.tsx` | `f i ?` | Filter, invite, help |

**Backtesting**

| File | Keys | Purpose |
|---|---|---|
| `features/backtesting/BacktestingPage.tsx` | `1 2 3 r x e s a b l [ ] d D w` | Modes, run, cancel, setup, add holding, benchmark, saved list, cycle tab/duration/weighting |

**Comparison**

| File | Keys | Purpose |
|---|---|---|
| `features/comparison/ComparisonPage.tsx` | `? 1 2 3 4 a m p P j k l h s x X [ ] e r` | Help, jump sections, add, metric, peers, move row/col, sort, hide metric, remove company, move company, toggle empty/ranks |

**Filings**

| File | Keys | Purpose |
|---|---|---|
| `features/filings/FilingsPage.tsx` | `f [ ] j x a ?` | Finder, cycle category, focus table, clear company, analyze, help |
| `features/filings/FilingViewerPage.tsx` | `1 2 3 4 [ ] a l ?` | Tabs, older/newer filing, analyze, filings list, help |
| `features/filings/viewer/ReportTab.tsx` | `j k t` | Next/prev report, cycle language |
| `features/filings/viewer/SectionsTab.tsx` | `f t j k` | Search, side-by-side, next/prev section |
| `features/filings/viewer/StatementsTab.tsx` | `j k f` | Next/prev statement, filter |

**Overview**

| File | Keys | Purpose |
|---|---|---|
| `features/overview/OverviewPage.tsx` | `j k ] [ n t u ?` | Move focus, next/prev card, go to screen/backtest/chat, help |

**Pipeline**

| File | Keys | Purpose |
|---|---|---|
| `features/pipeline/PipelinePage.tsx` | `f d s l R c ?` | Filter, daily, save, setup, run, cancel, help |

**Portfolio**

| File | Keys | Purpose |
|---|---|---|
| `features/portfolio/PortfolioWorkspace.tsx` | `1-5 - + = c b f x r R ?` | Tabs, step range, currency, benchmark, find, clear income, rebuild, refresh, help |
| `features/portfolio/PortfolioTable.tsx` | `j k ↓ ↑ [ ]` | Enter list, page |
| `features/portfolio/PortfolioActivity.tsx` | `Delete A n` | Delete selected, toggle all, add |
| `features/portfolio/PortfolioTrailNav.tsx` | `J K` | Step trail |

**Research**

| File | Keys | Purpose |
|---|---|---|
| `features/research/ResearchPage.tsx` | `? 1-5` | Help, tabs |
| `features/research/BookView.tsx` | `j k o a f [ ] s e t n c d` | List nav, open, add, filter, cycle tag, status, thesis, tag, note, compare, download |
| `features/research/NotesView.tsx` | `j k n f e x d` | List nav, note, filter, edit, delete, download |
| `features/research/AlertsView.tsx` | `j k n t x` | List nav, new, triggered-only, delete |
| `features/research/pricing/OptionsView.tsx` | `a m [ ] v t i s l r` | Company, manual, strike, vol source, days, market, strategy, ladder, reset |
| `features/research/pricing/BondsView.tsx` | `a m [ ] c y p d f r` | Company, manual, maturity, coupon, yield, price, fee, reset |

**Screening**

| File | Keys | Purpose |
|---|---|---|
| `features/screening/ScreeningWorkspace.tsx` | `n N o d c b r ?` | Add rule, add group, screens, date, columns, collapse, run, help |
| `features/screening/ResultsTable.tsx` | `j k ↓ ↑ [ ]` | Enter list, page |
| `features/screening/ScreenTrailNav.tsx` | `J K` | Step trail |

**Chat**

| File | Keys | Purpose |
|---|---|---|
| `features/chat/ChatPage.tsx` | `j k Enter Esc [ ] u a t f n N r o c p d x b i l ?` | Message nav, composer, clear, entries, unread, feed, switcher, filter, dm/group, reply, company, place, profile, dm, delete, block, details, focus list, help |
| `features/chat/ProfilePage.tsx` | `m b e Esc` | Message, block, edit own, disarm |

### 2.4 The problems

1. **Scattered.** 35 call sites in 30 files. Changing one behavior (e.g. what `?` does, or the typing-target rule) means touching many files.
2. **Duplicated documentation.** 12 hand-written `SHORTCUTS` constants mirror the bindings and drift from them. `researchShortcuts.ts` and `chatShortcuts.ts` are partial fixes that still live apart from the bindings.
3. **No user customization.** Keys are hard-coded; there is no way to rebind, and no place to see them all.
4. **No single "show me every hotkey" surface.** The `?` dialog only shows the current page's keys plus the global ones.
5. **No persistence** of any user preference for keys.
6. **Fragile `?` arbitration.** `GlobalHotkeys` uses a `setTimeout` race to decide whether to open global help vs. the page's help.
7. **No modifier support.** `useHotkeys` is single-key only; `Ctrl+Enter`, `Ctrl+S`, etc. are handled ad hoc in `onKeyDown` handlers inside forms.
8. **No standard.** Each developer invents the binding, the docs constant, the `?` wiring, and the dialog state independently.
9. **Inconsistent defaults.** The same key means different things across screens (`d` = download / details / date / duration; `r` = run / refresh / reset; `c` = columns / chart / compare / coupon / place). Within a screen this is fine, but there is no documented convention.

---

## 3. Proposed architecture

Introduce one new module, `frontend-v2/src/hotkeys/`, that owns everything. Screens stop calling `useHotkeys` with inline objects and instead **declare a scope** (metadata + actions) and mount it with one hook. The registry aggregates all scopes; the settings UI and the help dialog are generated from the registry, so they update automatically.

### 3.1 Core concepts

- **`KeySpec`** — a normalized key: `{ key: string; shift?: boolean; ctrl?: boolean; alt?: boolean; meta?: boolean }`. This replaces the bare-string key and adds modifier support. `N` (shift+n) becomes `{ key: 'n', shift: true }`.
- **`Hotkey`** — one binding: `{ id, defaultKey: KeySpec, label, group?, description?, action: (ctx) => (event) => void }`. `id` is a stable, scope-unique string (e.g. `screening.add-rule`).
- **`Scope`** — a named set of hotkeys for one screen/panel: `{ id, label, hotkeys: Hotkey[] }`. A scope is *active* while its component is mounted.
- **Registry** — a module-level store of all scopes plus the set of currently-active scopes. Provides lookup, conflict detection, and the "active scope" used by the help dialog.
- **Store** — persists user overrides (a map of `hotkeyId -> KeySpec`) **server-side against the user's account** (§7), exposed to React through a TanStack Query hook.
- **`useHotkeyScope(scope, ctx, options)`** — the single hook screens call. It registers the scope as active, listens for keys, resolves each key against the scope's hotkeys (applying user overrides), and dispatches to the action.
- **`HotkeySettingsPanel`** — the user-facing surface in the Account page. Lists every scope and hotkey, shows the current key (default or override), and lets the user rebind or reset.

### 3.2 Data model (types)

```ts
// frontend-v2/src/hotkeys/types.ts
export interface KeySpec {
  key: string            // 'n', 'Enter', 'Escape', '[', 'Delete', …
  shift?: boolean
  ctrl?: boolean
  alt?: boolean
  meta?: boolean
}

export type HotkeyAction<Ctx> = (ctx: Ctx) => (event: KeyboardEvent) => void

export interface Hotkey<Ctx> {
  id: string                       // scope-unique, stable
  defaultKey: KeySpec
  label: string
  group?: string                   // e.g. 'Rules', 'Navigation'
  description?: string
  action: HotkeyAction<Ctx>
}

export interface Scope<Ctx> {
  id: string                       // e.g. 'screening', 'analysis', 'chat'
  label: string                    // e.g. 'Screen'
  hotkeys: Hotkey<Ctx>[]
}
```

Because actions need live component state, a scope is **generic over its context type**. Each screen defines a small context interface (the functions the hotkeys call) and passes a live instance to the hook. The registry and settings UI only ever touch the *metadata* (id, key, label, group) — never the actions — so they stay decoupled from React and from each screen's implementation.

### 3.3 Key normalization and matching

```ts
// frontend-v2/src/hotkeys/keys.ts
export function keyFromEvent(event: KeyboardEvent): KeySpec {
  return {
    key: normalizeKey(event.key),   // ' ' -> 'Space', lowercase letters, etc.
    shift: event.shiftKey, ctrl: event.ctrlKey, alt: event.altKey, meta: event.metaKey,
  }
}
export function keysMatch(a: KeySpec, b: KeySpec): boolean { /* key + all modifiers equal */ }
export function formatKey(spec: KeySpec): string { /* 'Shift+N', 'Ctrl+Enter', 'n' */ }
```

`normalizeKey` lowercases single letters (so `N` and `n` are the same base key, distinguished by `shift`), maps `' '` to `'Space'`, and leaves named keys (`Enter`, `Escape`, `Delete`, `ArrowDown`, …) as-is. This makes shift-based chords first-class and removes the current ad-hoc `N`/`n` duality.

### 3.4 Dispatch and arbitration

`useHotkeyScope` installs one `window` `keydown` listener per active scope, exactly like today, so the existing arbitration model is preserved:

- Skip if `event.defaultPrevented` (another scope already claimed it) or `event.isComposing`.
- Skip if the target is a typing target (`isTypingTarget`, moved into `hotkeys/`).
- Build the `KeySpec` from the event; find the first hotkey in the scope whose effective key (default or user override) matches.
- If found: `event.preventDefault()` and run the action.

**Priority.** Scopes are checked in **reverse mount order** (most recently mounted first), so an inner component (a tab, a panel) takes precedence over the page that contains it. This matches the current intent (a tab's `j`/`k` list navigation wins over the page) and is deterministic. The registry maintains an ordered stack of active scopes; `useHotkeyScope` pushes on mount and pops on unmount.

**Global scope.** A reserved `global` scope holds the always-on keys: `/` (search), `?` (help), `Shift+Tab` (leave field), and the `G`-then-letter sequence. It is mounted once in `AppShell` and is always at the *lowest* priority (checked last), so page keys win over globals except where a page does not bind the key. The `G` sequence and `Shift+Tab` keep their current special handling in `GlobalHotkeys`; only the `?` help is moved onto the registry (see §3.6).

### 3.5 The registry

```ts
// frontend-v2/src/hotkeys/registry.ts
export function defineScope<Ctx>(scope: Scope<Ctx>): Scope<Ctx> {
  validate(scope)                 // unique ids, no duplicate effective keys within the scope
  register(scope)                 // add to the all-scopes map (once, at module load)
  return scope
}
export function allScopes(): Scope<unknown>[]          // for the settings UI
export function activeScopes(): Scope<unknown>[]       // currently mounted, priority order
export function topScope(): Scope<unknown> | null      // for the help dialog
export function findConflict(scopeId: string, key: KeySpec, exceptId?: string): Hotkey<unknown> | null
```

`defineScope` runs at module load (each screen's `*Hotkeys.ts` file calls it once). `validate` throws in development on duplicate ids or duplicate keys within a scope, so mistakes fail fast. `register` is idempotent.

### 3.6 The help dialog, unified

Today every screen wires `?` to its own dialog state, and `GlobalHotkeys` races to open a global one. After the migration:

- `?` is a **global** hotkey. It opens one shared `ShortcutsDialog` (kept, but now registry-driven).
- The dialog shows the **top active scope's** hotkeys (grouped) plus the global hotkeys. Because it reads from the registry, it is always correct and there is no per-screen `SHORTCUTS` constant or `?` wiring to maintain.
- The `setTimeout` race in `GlobalHotkeys` is deleted.

### 3.7 File structure

```
frontend-v2/src/hotkeys/
├── index.ts                    # public API: defineScope, useHotkeyScope, HotkeySettingsPanel, store
├── types.ts                    # KeySpec, Hotkey, Scope
├── keys.ts                     # keyFromEvent, keysMatch, formatKey, normalizeKey
├── registry.ts                 # defineScope, allScopes, activeScopes, topScope, findConflict
├── store.ts                    # HotkeyStore: server-backed overrides via TanStack Query (get/save/reset)
├── useHotkeyScope.ts           # the hook screens call
├── isTypingTarget.ts           # moved from hooks/useHotkeys.ts
├── HotkeySettingsPanel.tsx     # the Account-page settings surface
└── HotkeyCapture.tsx           # the "press a key" capture input for rebinding

# one file per screen, next to the feature:
features/screening/screeningHotkeys.ts
features/analysis/analysisHotkeys.ts
features/portfolio/portfolioHotkeys.ts
features/filings/filingsHotkeys.ts
features/research/researchHotkeys.ts
features/chat/chatHotkeys.ts
features/backtesting/backtestHotkeys.ts
features/comparison/comparisonHotkeys.ts
features/pipeline/pipelineHotkeys.ts
features/overview/overviewHotkeys.ts
features/auth/accountHotkeys.ts
features/auth/adminHotkeys.ts
```

### 3.8 Code sketches

**A screen declares its scope once** (`features/screening/screeningHotkeys.ts`):

```ts
import { defineScope } from '../../hotkeys'

export interface ScreeningCtx {
  addRule: (opts?: { match?: string }) => void
  runScreen: () => void
  showScreens: () => void
  showDate: () => void
  showColumns: () => void
  toggleCollapsed: () => void
}

export const screeningScope = defineScope<ScreeningCtx>({
  id: 'screening',
  label: 'Screen',
  hotkeys: [
    { id: 'add-rule',  defaultKey: { key: 'n' }, label: 'Add a rule',        group: 'Rules',    action: ctx => () => ctx.addRule() },
    { id: 'add-group', defaultKey: { key: 'n', shift: true }, label: 'Add a group', group: 'Rules', action: ctx => () => ctx.addRule({ match: 'any' }) },
    { id: 'screens',   defaultKey: { key: 'o' }, label: 'Open saved screens', group: 'Panels',  action: ctx => () => ctx.showScreens() },
    { id: 'date',      defaultKey: { key: 'd' }, label: 'Change the as-of date', group: 'Panels', action: ctx => () => ctx.showDate() },
    { id: 'columns',   defaultKey: { key: 'c' }, label: 'Open columns',       group: 'Panels',  action: ctx => () => ctx.showColumns() },
    { id: 'collapse',  defaultKey: { key: 'b' }, label: 'Collapse the rules', group: 'Panels',  action: ctx => () => ctx.toggleCollapsed() },
    { id: 'run',       defaultKey: { key: 'r' }, label: 'Run the screen',     group: 'Actions', action: ctx => () => ctx.runScreen() },
  ],
})
```

**The screen mounts it** (`features/screening/ScreeningWorkspace.tsx`):

```tsx
const ctx: ScreeningCtx = useMemo(() => ({
  addRule, runScreen,
  showScreens: () => setShowScreens(true),
  showDate: () => setShowDate(true),
  showColumns: () => setShowColumns(true),
  toggleCollapsed: () => setCollapsed(c => !c),
}), [addRule, runScreen, setCollapsed])

useHotkeyScope(screeningScope, ctx, { enabled: !overlayOpen && Boolean(metrics.data) })
```

The old `useHotkeys({ n: …, N: …, o: …, … }, …)` block and the `SHORTCUTS` constant and the `?`/dialog state are all deleted.

**The settings store** (`frontend-v2/src/hotkeys/store.ts`) is **server-backed** (see §7 for the backend design). It exposes a TanStack Query hook and a mutation, not a localStorage layer:

```ts
// frontend-v2/src/hotkeys/store.ts
export const hotkeySettingsKey = ['settings', 'hotkeys'] as const

// Reactive read: fetches GET /api/settings/hotkeys for the current user.
export function useHotkeyOverrides(): UseQueryResult<{ overrides: Record<string, KeySpec> }>

// Rebind: PUTs the full overrides map (small, atomic). Invalidates the query on success.
export function useSaveHotkeyOverrides(): UseMutationResult<...>

// Reset all: DELETE /api/settings/hotkeys. Invalidates the query on success.
export function useResetHotkeyOverrides(): UseMutationResult<...>
```

Only **overrides** are stored server-side (never defaults), so a fresh account gets the standard defaults and the payload stays tiny. There is **no `localStorage`** for hotkeys — the in-memory TanStack Query cache is the only client-side cache, and it is refilled from the server on load.

---

## 4. The standard (how to add a hotkey)

This is requirement 3. Once the module exists, the process is mechanical and documented in `frontend-v2/src/hotkeys/README.md`:

### 4.1 Add a hotkey to an existing screen

1. Open the screen's `*Hotkeys.ts` file.
2. Add one entry to `hotkeys`: a unique `id`, a `defaultKey`, a `label`, an optional `group`, and an `action` that calls a function on the context.
3. If the action needs a new function, add it to the screen's context interface and provide it in the `ctx` object in the component.
4. Done. The settings panel and the `?` help pick it up automatically. No other file changes.

### 4.2 Add hotkeys to a new screen

1. Create `features/<screen>/<screen>Hotkeys.ts` with a `defineScope` call (copy an existing one as the template).
2. Define the screen's context interface and pass a live `ctx` to `useHotkeyScope` in the screen component.
3. Use the **standard key roles** (§4.3) for standard actions so the new screen feels consistent.
4. Done.

### 4.3 Standard key roles (the consistent defaults)

These are the conventions the registry's defaults follow and that new screens should use. They are the "consistent default setup" (requirement 2).

**Global (always active, lowest priority):**

| Key | Role |
|---|---|
| `/` | Search companies |
| `G` + letter | Go to a page |
| `?` | Show shortcuts for the current screen |
| `Shift+Tab` | Leave the field you are typing in |
| `Esc` | Close a menu / leave a field |

**Per-screen roles (use these keys for these jobs):**

| Key | Role |
|---|---|
| `1`–`9` | Jump to tab / section N |
| `j` / `k` (and `↓` / `↑`) | Next / previous in a list |
| `Enter` | Open / confirm the current item |
| `f` | Focus the filter / search field |
| `n` | New / create |
| `a` | Add |
| `o` | Open |
| `x` | Delete / remove (confirm by pressing twice where destructive) |
| `d` | Download / details |
| `[` / `]` | Previous / next (page, tab, step, cycle) |
| `r` | Run / refresh / reset (context-dependent, documented per screen) |
| `Esc` | Close / cancel |

**Rules for choosing a key**

- Prefer a single unmodified letter for a primary action; use a modifier (`Shift`, `Ctrl`) only for a secondary variant of the same action (e.g. `n` add rule, `Shift+N` add group).
- Never bind a key that the browser or OS reserves with a modifier (`Ctrl+C`, `Ctrl+V`, `Ctrl+W`, …). The dispatcher already ignores modifier-held keys except for explicitly declared chords.
- Keep `?`, `/`, `G`, and `Esc` reserved for their global roles.
- If a standard role is already taken in a scope for a different job, keep the existing key (behavior-preserving) and note the deviation in the scope's `description`. Do not silently reassign.

### 4.4 Aligning existing keys to the standard (required)

Alignment is a **required** migration phase (§6, Phase 4), not an optional follow-up. Its purpose is to make the out-of-the-box defaults consistent with the standard roles in §4.3, so that a user who has never touched the settings gets a coherent keyboard across every screen.

**How it works.** Each scope's `defaultKey` values are reviewed against the standard roles. Where a current key already matches the standard role for that action, it is kept. Where it deviates, it is changed to the standard key. Screen-specific actions (e.g. `coupon`, `yield`, `strike` in the calculators) are not standard roles; they keep a sensible key (usually the first letter of the action) and are documented in the scope.

**Why it is safe.** User overrides are stored separately from defaults (§7). Changing a `defaultKey` only affects users who have *not* rebound that key; anyone who already customized a binding keeps their override. The change is therefore low-risk and reversible (a user can always rebind).

**Proposed alignment.** The current system already follows the standard roles in most places (the developers converged on similar conventions). The concrete changes are small; the table below lists every screen, the keys that change, and the keys that are kept-but-documented as screen-specific. Keys not listed are unchanged and already standard.

| Screen | Change (current → aligned) | Rationale |
|---|---|---|
| Pipeline | `R` (run) → `r` | Standard role: `r` = run. No in-scope conflict. |
| Portfolio Activity | add `x` as an alias for `Delete` (keep `Delete`) | Standard role: `x` = delete. `Delete` key kept for power users. |

All other screens require **no key changes**; their keys already match the standard roles or are documented screen-specific keys. The alignment phase therefore consists of:

1. Applying the two key changes above.
2. Adding a `description` (or group label) to every screen-specific key so the settings UI and help dialog explain it (e.g. `d` = "as-of date" on Screen, "maturity date" on Bonds).
3. Adding a registry validation rule (§3.5) that warns when a standard-role key is used for a non-standard action, so future drift is caught at load.
4. A changelog entry listing the changed keys.

**Screen-specific keys to document** (kept, not changed): `d` (date on Screen, maturity on Bonds), `c` (columns on Screen, chart on Financial History, compare on Research, coupon on Bonds, place on Chat), `b` (backtest on Analysis, collapse on Screen, benchmark on Portfolio, block on Chat), `t` (tag on Analysis, language on Report, side-by-side on Sections, triggered on Alerts, days on Options, backtest on Overview), `v` (view on Account, cycle view on Financial History, vol source on Options), `p` (compare on Analysis, peers on Comparison, profile on Chat, price on Bonds), `l` (filings list on Filing Viewer, saved list on Backtest, ladder on Options, focus list on Chat), `s` (status on Research, sort on Comparison, save on Pipeline, strategy on Options), `e` (empty on Financial History, edit on Notes/Profile, setup on Backtest), `m` (manual on Options/Bonds, message on Profile), `y` (yield on Bonds), `i` (invite on Admin, market on Options, details on Chat), `u` (chat on Overview, unread on Chat, unblock on Account), `w` (weighting on Backtest), `h` (column-left on Comparison).

These are kept because they are the natural first-letter key for a screen-specific action and do not conflict with any standard role *within their scope*. The standard only constrains the shared roles (list nav, tabs, filter, new, add, open, delete, download, prev/next, run, close); it does not force global uniqueness of every letter.

---

## 5. The centralized user panel (requirement 1)

Add a **"Keyboard"** section to the Account page (`features/auth/AccountPage.tsx`), the existing user panel. It renders `HotkeySettingsPanel`.

### 5.1 Layout

- One row per hotkey, grouped by screen (scope) and then by `group`.
- Each row shows: the label, the current key (rendered as `<kbd>` chips, e.g. `Shift` `N`), and whether it is a default or a user override.
- A **Rebind** control per row: clicking it enters capture mode (`HotkeyCapture`); the next key pressed becomes the new binding. `Esc` cancels capture.
- A **Reset** control per row (restore the default) and a **Reset all** button at the top.
- A conflict warning appears live if the captured key is already used by another hotkey in the same scope; the save is blocked until resolved.

### 5.2 Behavior

- Changes persist immediately to the server (`PUT /api/settings/hotkeys`, §7) and take effect immediately (the query cache updates; active scopes re-read overrides).
- The panel lists **every** scope and hotkey in the app, so it is the single place to see and change all hotkeys on every screen — including screens the user has not visited.
- It works whether or not the user is signed in: signed-in users store settings on their account; in the auth-disabled "local workspace" mode they are stored under the shared `local` user (§7.2).

### 5.3 Why the Account page

The Account page is already the user panel, already has a numbered-section layout with its own hotkeys, and is reachable from the sidebar. Adding a section there satisfies "a single centralized point in the user panel" without a new route. (A dedicated `/keyboard` route is a possible later refinement, not required.)

---

## 6. Migration plan (phased)

The migration is behavior-preserving and incremental. Each phase ends with the app fully working and tests green, so it can be paused or shipped at any phase boundary.

### Phase 0 — Infrastructure (no behavior change)

1. **Backend:** add the `user_settings` table and `get/set/delete_user_setting` methods to `AuthStore` (§7); add the `/api/settings/hotkeys` router (`GET`/`PUT`/`DELETE`) and register it in `src/web_app/api/__init__.py`; add API tests.
2. **Frontend:** create `frontend-v2/src/hotkeys/` with `types.ts`, `keys.ts`, `registry.ts`, `store.ts` (server-backed), `useHotkeyScope.ts`, `isTypingTarget.ts` (moved), and unit tests for key normalization, matching, conflict detection, and the store.
3. Keep `useHotkeys` and all existing call sites untouched. The new module coexists.
4. Add `hotkeys/README.md` documenting the standard (§4).

**Exit criteria:** backend settings API has passing tests; frontend module has passing unit tests; no existing behavior changed.

### Phase 1 — Migrate screens one at a time

For each screen, in this order (simplest first, to build confidence):

1. `AccountPage` (auth) — small, self-contained.
2. `AdminPage` (auth) — small.
3. `OverviewPage` — small.
4. `PipelinePage` — small.
5. `FilingsPage` + `FilingViewerPage` + the three viewer tabs — a scope per tab.
6. `ScreeningWorkspace` + `ResultsTable` + `ScreenTrailNav`.
7. `AnalysisWorkspaceUnified` + `FinancialHistoryWorkspace` + `FilingsPanel` + `PricePanel`.
8. `ComparisonPage`.
9. `BacktestingPage`.
10. `PortfolioWorkspace` + `PortfolioTable` + `PortfolioActivity` + `PortfolioTrailNav`.
11. `ResearchPage` + `BookView` + `NotesView` + `AlertsView` + `OptionsView` + `BondsView`.
12. `ChatPage` + `ProfilePage` — largest scope; do last.

Per-screen steps:

1. Create `<screen>Hotkeys.ts` with a `defineScope` that reproduces the screen's current keys, labels, and actions exactly.
2. Replace the `useHotkeys({...})` call with `useHotkeyScope(scope, ctx, { enabled })`.
3. Delete the screen's `SHORTCUTS` constant and its `?`/dialog state; the unified help dialog now covers it.
4. Run the screen's existing tests; add a test that presses each key and asserts the action fired.
5. Verify the key set is identical to before (the inventory in §2.3 is the reference).

**Exit criteria per screen:** behavior identical, tests green, no `useHotkeys` call remains in that screen.

### Phase 2 — Unify the help dialog

1. Move `?` to the global scope; make `ShortcutsDialog` registry-driven (shows the top active scope + globals).
2. Delete the `setTimeout` race in `GlobalHotkeys` and the per-screen `?` bindings (already removed in Phase 1).
3. Keep `G`-then-letter and `Shift+Tab` in `GlobalHotkeys`.

**Exit criteria:** one help dialog everywhere; `GlobalHotkeys` no longer races on `?`.

### Phase 3 — Settings UI (server-backed)

1. Build `HotkeySettingsPanel` + `HotkeyCapture`.
2. Add the "Keyboard" section to `AccountPage`.
3. Wire the store to the `/api/settings/hotkeys` endpoints so rebinding saves server-side, takes effect immediately, and persists across devices.

**Exit criteria:** a user can view and rebind every hotkey from the Account page; changes are stored server-side and survive reload and device change.

### Phase 4 — Align defaults to the standard (required)

Apply the alignment from §4.4: make the two key changes (Pipeline `R`→`r`; Portfolio Activity `x` alias for `Delete`), add `description`/group labels to every screen-specific key, enable the registry standard-role validation, and ship a changelog entry. Because the settings UI now exists (Phase 3), any user who dislikes an aligned default can rebind it server-side.

**Exit criteria:** all scope `defaultKey` values match the standard roles (§4.3) or are documented screen-specific keys; changelog published.

### Phase 5 — Remove legacy and update docs

1. Delete `hooks/useHotkeys.ts` (and `isTypingTarget` if fully moved) once no call sites remain.
2. Delete any remaining per-screen `SHORTCUTS` constants and `researchShortcuts.ts` / `chatShortcuts.ts` (their content now lives in the scopes).
3. Update `docs/USER_GUIDE.md` and `docs/Frontend Architecture.md` to describe the new system, the standard, and the server-backed settings.

**Exit criteria:** no `useHotkeys` references; one source of truth for all hotkeys; docs updated.

---

## 7. Persistence: server-side

Hotkey preferences are stored **server-side against the user's account**, not in `localStorage`. This is a firm requirement, not a phase-1 simplification.

### 7.1 Storage (auth database)

Add a generic per-user settings table to `AuthStore` (`src/auth/storage.py`) so future per-user preferences can reuse it:

```sql
CREATE TABLE IF NOT EXISTS user_settings (
    user_id TEXT NOT NULL,
    setting_key TEXT NOT NULL,
    setting_json TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (user_id, setting_key)
);
```

Hotkey overrides are stored under `setting_key = 'hotkeys'` as a JSON object `{ hotkeyId: KeySpec }`. The backend stores the JSON **opaquely** — it does not need to know the frontend's hotkey ids. Validating "is this a real hotkey" is a frontend concern; the backend only enforces that the value is a well-formed JSON object of key specs.

`AuthStore` gains three methods:

```python
def get_user_setting(self, user_id: str, key: str) -> dict | None
def set_user_setting(self, user_id: str, key: str, value: dict) -> None
def delete_user_setting(self, user_id: str, key: str) -> None
```

### 7.2 API

A new router `src/web_app/api/settings.py`, registered in `src/web_app/api/__init__.py` `_ROUTERS`:

```python
router = APIRouter(prefix="/api/settings", tags=["settings"])

class HotkeyOverride(BaseModel):
    model_config = ConfigDict(extra="forbid")
    key: str = Field(min_length=1, max_length=30)
    shift: bool = False
    ctrl: bool = False
    alt: bool = False
    meta: bool = False

class HotkeysBody(BaseModel):
    model_config = ConfigDict(extra="forbid")
    overrides: dict[str, HotkeyOverride] = Field(default_factory=dict)

@router.get("/hotkeys", response_model=HotkeysBody)
def get_hotkeys(user: CurrentUser) -> HotkeysBody: ...

@router.put("/hotkeys", response_model=HotkeysBody)
def put_hotkeys(user: CurrentUser, body: HotkeysBody) -> HotkeysBody: ...

@router.delete("/hotkeys")
def reset_hotkeys(user: CurrentUser) -> dict: ...
```

- `GET` returns the current user's overrides (or `{}`).
- `PUT` replaces the whole overrides map (atomic and simple; the map is small, so sending it in full on each change is fine).
- `DELETE` clears all overrides (reset to defaults).

All three use the `CurrentUser` dependency, so they work in **both** signed-in and auth-disabled modes. In auth-disabled mode the principal is the shared `local` user (`user_id="local"`), so settings are stored under that single principal — correct for a single-user local workspace.

### 7.3 Frontend store

`hotkeys/store.ts` (§3.8) is API-backed via TanStack Query — no `localStorage`:

- `useHotkeyOverrides()` → `useQuery({ queryKey: ['settings','hotkeys'], queryFn: () => apiRequest('/api/settings/hotkeys') })`.
- Rebind → `useMutation` that `PUT`s the new full map, then invalidates the query.
- Reset → `useMutation` that `DELETE`s, then invalidates.

The in-memory query cache is the only client-side cache; it is refilled from the server on load.

### 7.4 Why server-side

- The user's bindings follow their **account** across devices and browsers — the natural scope for a per-user preference.
- Single source of truth; enables future features (admin-visible defaults, per-role defaults, export/import).
- Consistent with how the app already stores per-user state (auth, chat profiles) in SQLite behind the `CurrentUser` dependency.

---

## 8. Testing strategy

- **Unit (new module):** key normalization and matching (including modifiers and `Space`), conflict detection, store load/save/reset, registry active-scope stack ordering.
- **Per-screen:** for each migrated screen, a test that renders it, presses each key, and asserts the corresponding action fired (and that typing-target keys are ignored). Reuse the existing `fireEvent.keyDown` pattern from `GlobalHotkeys.test.tsx`.
- **Arbitration:** a test that mounts a page scope and a child scope binding the same key and asserts the child wins (reverse mount order).
- **Settings UI:** a test that rebinding a key updates the store, takes effect immediately, blocks on conflict, and resets correctly.
- **Regression:** the existing per-feature test suites (`ChatPage.test.tsx`, `PortfolioWorkspace.test.tsx`, `PipelineKeyboard.test.tsx`, etc.) must stay green throughout, proving behavior is preserved.

---

## 9. Risks and mitigations

| Risk | Mitigation |
|---|---|
| Subtle behavior change during migration | Behavior-preserving per screen; existing tests are the safety net; the §2.3 inventory is the reference for "identical keys." |
| Key conflicts after a user rebinds | Live conflict detection in the settings UI blocks the save; the registry validates defaults at load. |
| Modifier-key handling regressions (new feature) | `keys.ts` is unit-tested for every modifier combination; the dispatcher ignores undeclared modifier-held keys, as today. |
| Scope priority surprises (inner vs outer) | Reverse-mount-order rule is documented and tested; scopes are small and per-component, so precedence is local and predictable. |
| Large blast radius (30 files) | Phased, one screen at a time, each phase independently shippable; legacy `useHotkeys` stays until Phase 5. |
| Settings UI scope creep | Keep it to view/rebind/reset; no per-key enable/disable or per-key delay tuning in v1. |

---

## 10. Effort estimate (rough)

- Phase 0 (infrastructure + backend settings API + tests): the largest single chunk — the frontend module, the `user_settings` table, and the `/api/settings/hotkeys` router, plus their tests.
- Phases 1–2 (migrate 12 screens + unify help): mechanical, ~1–2 hours per screen including tests.
- Phase 3 (server-backed settings UI): one focused build.
- Phase 4 (align defaults to the standard): small — two key changes plus documentation labels and the registry validation rule.
- Phase 5 (cleanup + docs): small.

The design's payoff is that after Phase 0, every subsequent change (new screen, new key, changed key) is a one-file edit, and the settings/help surfaces maintain themselves.

---

## 11. Open questions / decisions needed

Decided (per review): persistence is **server-side** (§7); standard-alignment is **required** (§4.4, Phase 4).

Remaining:

1. **Settings location:** Account-page section (recommended) vs. a dedicated `/keyboard` route.
2. **Modifier chords:** is there a specific set of `Ctrl`/`Alt` chords (e.g. `Ctrl+Enter` run, `Ctrl+S` save) that should be standardized and moved into the system, or kept as form-level `onKeyDown` handlers?
3. **Per-key enable/disable:** should the settings UI allow disabling a hotkey entirely (in addition to rebinding)? Recommended: no, for v1 simplicity.
4. **Alignment confirmations:** the two proposed key changes in §4.4 (Pipeline `R`→`r`; Portfolio Activity `x` alias) — confirm before Phase 4.

---

## Appendix A — What gets deleted after the migration

- `frontend-v2/src/hooks/useHotkeys.ts` (hook + `isTypingTarget`, the latter moved to `hotkeys/`).
- 12 per-screen `SHORTCUTS` / `ANYWHERE` constants.
- `features/research/researchShortcuts.ts` and `features/chat/chatShortcuts.ts` (content folded into scopes).
- The `?`/dialog state and the `setTimeout` race in `GlobalHotkeys`.

## Appendix B — What stays

- `components/pageShortcuts.ts` (the `G` destinations) — folded into the global scope's metadata but the data is reused.
- `components/ShortcutsDialog.tsx` — kept, made registry-driven.
- `components/GlobalHotkeys.tsx` — kept for `G`-then-letter and `Shift+Tab`; the `?` race is removed.
- All existing per-feature test suites.
