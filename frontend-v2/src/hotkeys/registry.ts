import { canonical, formatKey, parseKey, sameKey } from './keys'
import { standardRoleIssues } from './standard'
import type { Hotkey, HotkeyOverrides, KeySpec, Scope, ScopeDef } from './types'

const scopes = new Map<string, Scope>()

const toList = <T,>(value: T | readonly T[]): T[] => (Array.isArray(value) ? [...(value as readonly T[])] : [value as T])

/**
 * Declare a screen's hotkeys once, at module load. Throws on duplicate ids or
 * duplicate default keys within the scope, so mistakes fail in tests.
 */
export function defineScope<const Id extends string>(def: ScopeDef<Id>): Scope<Id> {
  const seen = new Map<string, string>()
  const hotkeys: Hotkey[] = def.hotkeys.map(hotkey => {
    if (seen.has(hotkey.id)) throw new Error(`Hotkey scope "${def.id}" declares "${hotkey.id}" twice`)
    seen.set(hotkey.id, hotkey.id)
    return { ...hotkey, fullId: `${def.id}.${hotkey.id}`, scopeId: def.id, defaults: toList(hotkey.keys).map(parseKey) }
  })
  // Built-in keys belong to a focused control (a list, a field), so they may
  // repeat each other; dispatched keys must be unique within the scope.
  const dispatched = hotkeys.filter(hotkey => !hotkey.fixed)
  for (const [index, hotkey] of dispatched.entries()) {
    for (const other of dispatched.slice(index + 1)) {
      const clash = hotkey.defaults.find(spec => other.defaults.some(otherSpec => sameKey(spec, otherSpec)))
      if (clash) throw new Error(`Hotkey scope "${def.id}": "${hotkey.id}" and "${other.id}" both default to ${formatKey(clash)}`)
    }
  }
  const scope: Scope<Id> = {
    id: def.id, label: def.label, screen: def.screen, parent: def.parent, description: def.description, hotkeys,
    byId: Object.fromEntries(hotkeys.map(hotkey => [hotkey.id, hotkey])) as Record<Id, Hotkey>,
  }
  scopes.set(def.id, scope as Scope)
  // Drift from the standard key roles is reported while developing; catalog.test.ts enforces it.
  if (import.meta.env.DEV) standardRoleIssues([scope as Scope]).forEach(issue => console.warn(`Hotkeys: ${issue}`))
  return scope
}

/** Every declared scope, in declaration order (the catalog imports them all). */
export const allScopes = (): Scope[] => [...scopes.values()]

export const getScope = (id: string) => scopes.get(id)

export function findHotkey(fullId: string): Hotkey | undefined {
  for (const scope of scopes.values()) {
    const hotkey = scope.hotkeys.find(item => item.fullId === fullId)
    if (hotkey) return hotkey
  }
  return undefined
}

export function effectiveKeys(hotkey: Hotkey, overrides: HotkeyOverrides): KeySpec[] {
  const override = hotkey.fixed ? undefined : overrides[hotkey.fullId]?.map(canonical)
  // A key that also fires in fields must keep a modifier, or it would swallow typing.
  if (override?.length && (!hotkey.whileTyping || override.every(isChord))) return override
  return hotkey.defaults
}

/** Ctrl, Alt, or Cmd held: a key press that never types text. */
export const isChord = (spec: KeySpec) => Boolean(spec.ctrl || spec.alt || spec.meta)

export const isCustomized = (hotkey: Hotkey, overrides: HotkeyOverrides) => !hotkey.fixed && Boolean(overrides[hotkey.fullId]?.length)

function ancestors(scopeId: string): string[] {
  const chain: string[] = []
  let current = scopes.get(scopeId)?.parent
  while (current && !chain.includes(current)) {
    chain.push(current)
    current = scopes.get(current)?.parent
  }
  return chain
}

/**
 * Scopes that can be active at the same time as *scopeId*: itself, the scopes
 * it sits inside, the scopes inside it, and the always-on global scope. Sibling
 * tabs are never mounted together, so they may share keys.
 */
export function relatedScopes(scopeId: string): Scope[] {
  if (scopeId === GOTO_SCOPE_ID) return [scopes.get(GOTO_SCOPE_ID)].filter(Boolean) as Scope[]
  const up = new Set(ancestors(scopeId))
  return allScopes().filter(scope => {
    if (scope.id === GOTO_SCOPE_ID) return false
    if (scopeId === GLOBAL_SCOPE_ID || scope.id === GLOBAL_SCOPE_ID) return true
    return scope.id === scopeId || up.has(scope.id) || ancestors(scope.id).includes(scopeId)
  })
}

/** Hotkeys that would also fire for *keys* if *hotkey* were bound to them. */
export function findConflicts(hotkey: Hotkey, keys: KeySpec[], overrides: HotkeyOverrides): Hotkey[] {
  return relatedScopes(hotkey.scopeId)
    .flatMap(scope => scope.hotkeys)
    .filter(other => other.fullId !== hotkey.fullId && !other.fixed)
    .filter(other => effectiveKeys(other, overrides).some(spec => keys.some(key => sameKey(spec, key))))
}

// -- the scopes mounted right now, for the help dialog -----------------------

export const GLOBAL_SCOPE_ID = 'global'
export const GOTO_SCOPE_ID = 'goto'

let active: Scope[] = []
const listeners = new Set<() => void>()

/** Mark a scope as mounted; returns the function that unmounts it. */
export function activateScope(scope: Scope): () => void {
  active = [...active, scope]
  listeners.forEach(listener => listener())
  return () => {
    const index = active.lastIndexOf(scope)
    if (index >= 0) active = [...active.slice(0, index), ...active.slice(index + 1)]
    listeners.forEach(listener => listener())
  }
}

export function subscribeActiveScopes(listener: () => void) {
  listeners.add(listener)
  return () => { listeners.delete(listener) }
}

export const activeScopes = () => active

// -- opening the "?" list from a button --------------------------------------

const helpListeners = new Set<() => void>()

/** Open the shortcuts list (GlobalHotkeys shows it), as pressing "?" does. */
export function openHotkeyHelp() {
  helpListeners.forEach(listener => listener())
}

export function onHotkeyHelpRequest(listener: () => void) {
  helpListeners.add(listener)
  return () => { helpListeners.delete(listener) }
}
