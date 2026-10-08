/** A key plus the modifiers that must be held for it. ``key`` is normalized (see ``keys.ts``). */
export interface KeySpec {
  key: string
  shift?: boolean
  ctrl?: boolean
  alt?: boolean
  meta?: boolean
}

/** A key written as text (``'n'``, ``'Shift+N'``, ``'ArrowDown'``, ``'Ctrl+Enter'``) or as a spec. */
export type KeyInput = string | KeySpec

/**
 * The shared jobs that standard keys are reserved for (see README.md). A hotkey
 * bound to a standard key declares the role it plays, or a ``description``
 * explaining the screen-specific use.
 */
export type StandardRole =
  | 'tab' | 'next' | 'previous' | 'open' | 'filter' | 'new' | 'add' | 'delete'
  | 'download' | 'details' | 'run' | 'refresh' | 'reset' | 'close' | 'help' | 'search'

export interface HotkeyDef<Id extends string = string> {
  /** Stable and unique within the scope; stored with user overrides, so never rename casually. */
  id: Id
  /** The default key, or several equivalent keys (``['j', 'ArrowDown']``). */
  keys: KeyInput | readonly KeyInput[]
  label: string
  group?: string
  description?: string
  role?: StandardRole
  /**
   * Handled inside a control (a focused list, a text field) rather than by the
   * scope: listed in help and settings, but not dispatched here or rebindable.
   */
  fixed?: boolean
  /** Also fires while typing in a field. Only for chords with Ctrl, Alt, or Cmd, which never type text. */
  whileTyping?: boolean
}

export interface ScopeDef<Id extends string = string> {
  /** ``'screening'``, ``'filing-viewer.report'`` … — prefixes every hotkey id in the scope. */
  id: string
  /** The panel this scope belongs to, shown above its keys. */
  label: string
  /** The screen it appears on; settings group scopes by screen. */
  screen: string
  /** The scope this one is mounted inside. Keys may repeat across siblings (tabs), never along a parent chain. */
  parent?: string
  description?: string
  hotkeys: readonly HotkeyDef<Id>[]
}

export interface Hotkey extends HotkeyDef {
  /** ``<scope id>.<hotkey id>``: the key user overrides are stored under. */
  fullId: string
  scopeId: string
  defaults: KeySpec[]
}

export interface Scope<Id extends string = string> {
  id: string
  label: string
  screen: string
  parent?: string
  description?: string
  hotkeys: Hotkey[]
  byId: Record<Id, Hotkey>
}

export type HotkeyHandler = (event: KeyboardEvent) => void

/** User overrides keyed by ``Hotkey.fullId``; absent ids use their defaults. */
export type HotkeyOverrides = Record<string, KeySpec[]>
