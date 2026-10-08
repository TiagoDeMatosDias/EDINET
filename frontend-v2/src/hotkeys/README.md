# Hotkeys

Every keyboard shortcut in the workstation is declared here or in a screen's
`*Hotkeys.ts` file and dispatched through one hook. The help list (`?`) and the
Account page's **Keyboard shortcuts** section are generated from the same
declarations, and each user's rebindings are stored on their account
(`GET/PUT/DELETE /api/settings/hotkeys`, table `auth.db.user_settings`).

| File | Role |
|---|---|
| `types.ts` | `KeySpec`, `HotkeyDef`, `ScopeDef`, `Scope` |
| `keys.ts` | Parse (`'Shift+N'`), normalize, match, and format keys; keys that cannot be rebound |
| `registry.ts` | `defineScope`, the scope catalog, conflict detection, mounted scopes, `openHotkeyHelp` |
| `standard.ts` | The standard key roles and the check that defaults follow them |
| `catalog.ts` | Imports every screen's declarations so settings list all of them |
| `useHotkeyScope.ts` | The one hook screens call |
| `HotkeyProvider.tsx`, `settingsContext.ts` | The signed-in user's overrides, loaded from and saved to the server |
| `HotkeyKbd.tsx`, `useHotkeyText.ts` | Inline key hints that follow the user's binding |
| `HotkeyHelpDialog.tsx`, `HotkeyHelpButton.tsx` | The `?` list of the mounted screen plus the global keys |
| `HotkeySettingsPanel.tsx` | The Account-page editor |

## Add a hotkey to a screen

1. Open the screen's `features/<screen>/<screen>Hotkeys.ts`.
2. Add one entry: a stable `id`, its default `keys`, a `label`, and optionally a
   `group`, a standard `role`, or a `description` of a screen-specific use.
3. Pass a handler for that id in the screen's `useHotkeyScope(scope, handlers)`
   call. Where the screen shows the key, render `<HotkeyKbd hotkey={scope.byId.<id>} />`
   (or `useHotkeyText` in a `title`), never a literal letter.

The help list, the settings panel, and conflict checks pick it up. Ids are
stored with user overrides, so rename one only together with a migration.

## Add hotkeys to a new screen

1. Create `features/<screen>/<screen>Hotkeys.ts` with `defineScope({ id, label, screen, hotkeys })`.
   A panel or tab mounted inside another scope sets `parent` to that scope's id:
   keys may repeat across sibling tabs, but not along a parent chain.
2. Import the file in `catalog.ts`.
3. Call `useHotkeyScope(scope, handlers, { enabled })` in the component.
4. Use the standard key roles below.

## Rules

- A key is a string: `'n'`, `'Shift+N'` (also written `'N'`), `'Ctrl+Enter'`,
  `'ArrowDown'`, `'?'`. Shift distinguishes letters and named keys; for symbols
  the keyboard layout decides, so `'?'` matches with or without Shift. With
  another modifier, Shift must be named: `'Ctrl+S'` has no Shift.
- A binding never fires with a Ctrl, Alt, or Cmd it does not name, while the
  user is typing in a field, or after another listener claimed the event.
  `whileTyping: true` lets a chord (Ctrl/Alt/Cmd + key) fire in fields too, such
  as Screening's `Ctrl+Enter`; such a hotkey can only be rebound to another chord.
- `fixed: true` documents a key handled inside a control (a focused list, a
  text box): it is listed but not dispatched by the scope or rebindable.
- Inner scopes mount first, so a tab's keys win over its page's. Defaults never
  rely on that: `catalog.test.ts` fails if two scopes that can be mounted
  together share a default key, and the settings panel refuses such a rebinding.

## Standard key roles

Global, always available: `/` search companies, `?` shortcuts for this screen,
`G` then a letter go to a page, `Shift+Tab` leave the field, `Esc` close.
`/`, `?`, and `g` are reserved for the global scope.

| Key | Role |
|---|---|
| `1`–`9` | Tab or section N |
| `j` / `k`, `↓` / `↑` | Next / previous in a list |
| `[` / `]` | Previous / next page, tab, step, or cycle |
| `Enter` | Open or confirm the current item |
| `f` | Focus the filter or search field |
| `n` | New / create |
| `a` | Add |
| `o` | Open |
| `x` | Delete or remove (press twice where destructive) |
| `d` | Download or details |
| `r` | Run, refresh, or reset |
| `Esc` | Close or cancel |

Prefer a single unmodified letter for a primary action and Shift for its
variant (`n` add rule, `Shift+N` add group). A standard key used for another job
needs a `description` saying so (`d` is the as-of date on Screen, the maturity
on Bonds); `standard.ts` warns in development and `catalog.test.ts` enforces it.
