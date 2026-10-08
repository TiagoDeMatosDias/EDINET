import type { KeyInput, KeySpec } from './types'

const ALIASES: Record<string, string> = {
  ' ': 'Space', Spacebar: 'Space', Esc: 'Escape', Del: 'Delete',
  Up: 'ArrowUp', Down: 'ArrowDown', Left: 'ArrowLeft', Right: 'ArrowRight',
  '↑': 'ArrowUp', '↓': 'ArrowDown', '←': 'ArrowLeft', '→': 'ArrowRight',
}

const MODIFIER_KEYS = new Set(['Shift', 'Control', 'Alt', 'Meta', 'AltGraph', 'CapsLock', 'OS', 'Hyper', 'Super'])

const isLetter = (key: string) => /^[a-z]$/.test(key)

/** Lowercase letters (case comes from ``shift``), give named keys one spelling. */
export function normalizeKey(key: string): string {
  const aliased = ALIASES[key] ?? key
  return aliased.length === 1 ? aliased.toLowerCase() : aliased
}

/**
 * Whether Shift is part of the key's identity. For letters and named keys it is
 * (``n`` vs ``Shift+N``, ``Tab`` vs ``Shift+Tab``); for symbols the layout
 * decides (``?`` needs Shift on some keyboards and not on others), so it is not.
 */
export function shiftMatters(key: string): boolean {
  return key.length > 1 || isLetter(key)
}

/** Parse ``'Shift+N'``, ``'N'`` (same thing), ``'Ctrl+S'`` (no Shift), ``'?'``, or pass a spec through normalized. */
export function parseKey(input: KeyInput): KeySpec {
  if (typeof input !== 'string') return canonical({ ...input, key: normalizeKey(input.key) })
  const spec: KeySpec = { key: '' }
  // ``+`` alone (or at the end, ``Shift++``) is the key itself.
  const parts = input === '+' ? ['+'] : input.endsWith('++') ? [...input.slice(0, -2).split('+'), '+'] : input.split('+')
  for (const part of parts.slice(0, -1)) {
    const name = part.trim().toLowerCase()
    if (name === 'shift') spec.shift = true
    else if (name === 'ctrl' || name === 'control') spec.ctrl = true
    else if (name === 'alt' || name === 'option') spec.alt = true
    else if (name === 'meta' || name === 'cmd' || name === 'command') spec.meta = true
    else throw new Error(`Unknown modifier "${part}" in hotkey "${input}"`)
  }
  const raw = parts[parts.length - 1]
  if (!raw) throw new Error(`Hotkey "${input}" has no key`)
  // A lone upper-case letter is written for Shift+letter ('N'); with modifiers,
  // Shift must be named, as in the usual notation ('Ctrl+S' has no Shift).
  if (parts.length === 1 && raw.length === 1 && raw !== raw.toLowerCase()) spec.shift = true
  spec.key = normalizeKey(raw)
  return canonical(spec)
}

/** Drop false flags and Shift where it carries no meaning, so equal keys compare equal. */
export function canonical(spec: KeySpec): KeySpec {
  const out: KeySpec = { key: spec.key }
  if (spec.shift && shiftMatters(spec.key)) out.shift = true
  if (spec.ctrl) out.ctrl = true
  if (spec.alt) out.alt = true
  if (spec.meta) out.meta = true
  return out
}

export function keyFromEvent(event: Pick<KeyboardEvent, 'key' | 'shiftKey' | 'ctrlKey' | 'altKey' | 'metaKey'>): KeySpec {
  return { key: normalizeKey(event.key), shift: event.shiftKey, ctrl: event.ctrlKey, alt: event.altKey, meta: event.metaKey }
}

export function isModifierOnly(spec: KeySpec): boolean {
  return MODIFIER_KEYS.has(spec.key) || spec.key === 'Dead' || spec.key === 'Unidentified'
}

/** Does a pressed key (from ``keyFromEvent``) trigger *spec*? */
export function keysMatch(spec: KeySpec, pressed: KeySpec): boolean {
  if (spec.key !== pressed.key) return false
  if (Boolean(spec.ctrl) !== Boolean(pressed.ctrl) || Boolean(spec.alt) !== Boolean(pressed.alt) || Boolean(spec.meta) !== Boolean(pressed.meta)) return false
  return !shiftMatters(spec.key) || Boolean(spec.shift) === Boolean(pressed.shift)
}

export const sameKey = (a: KeySpec, b: KeySpec) => keysMatch(canonical(a), canonical(b))

const NAMES: Record<string, string> = {
  ArrowUp: '↑', ArrowDown: '↓', ArrowLeft: '←', ArrowRight: '→', Escape: 'Esc', Space: 'Space',
}

/** The chips a key is drawn with: ``['Shift', 'N']``, ``['Ctrl', 'Enter']``, ``['?']``. */
export function keyChips(spec: KeySpec): string[] {
  const chips: string[] = []
  if (spec.ctrl) chips.push('Ctrl')
  if (spec.alt) chips.push('Alt')
  if (spec.meta) chips.push('Cmd')
  if (spec.shift && shiftMatters(spec.key)) chips.push('Shift')
  chips.push(NAMES[spec.key] ?? (spec.key.length === 1 ? spec.key.toUpperCase() : spec.key))
  return chips
}

export const formatKey = (spec: KeySpec) => keyChips(spec).join('+')

/** Text for several equivalent keys: ``'J / ↓'``. */
export const formatKeys = (specs: KeySpec[]) => specs.map(formatKey).join(' / ')

const BROWSER_CHORDS = new Set(['a', 'c', 'f', 'l', 'n', 'p', 'q', 'r', 's', 't', 'v', 'w', 'x', 'y', 'z'])

/** Why *spec* cannot be bound at all, whatever else uses it. */
export function reservedReason(spec: KeySpec): string | null {
  if (spec.key === 'Escape') return 'Esc cancels; it cannot be bound.'
  if (spec.key === 'Tab') return 'Tab moves between fields; it cannot be bound.'
  if ((spec.ctrl || spec.meta) && BROWSER_CHORDS.has(spec.key)) return `${formatKey(spec)} belongs to the browser.`
  return null
}
