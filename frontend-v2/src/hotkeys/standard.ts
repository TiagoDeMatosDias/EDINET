import { canonical } from './keys'
import type { Hotkey, Scope, StandardRole } from './types'

/**
 * The jobs standard keys are reserved for on every screen (README.md, "Standard
 * key roles"). A Shift variant counts as the same key: ``Shift+N`` is still "new".
 */
export const STANDARD_KEY_ROLES: Record<string, readonly StandardRole[]> = {
  j: ['next'], k: ['previous'], ArrowDown: ['next'], ArrowUp: ['previous'],
  '[': ['previous'], ']': ['next'],
  f: ['filter'], n: ['new'], a: ['add'], o: ['open'], x: ['delete'],
  d: ['download', 'details'], r: ['run', 'refresh', 'reset'],
  Enter: ['open'], Escape: ['close'],
  ...Object.fromEntries('123456789'.split('').map(digit => [digit, ['tab'] as const])),
}

/** Next and previous are one job: a hotkey that moves both ways may use either key. */
const STEP = new Set<StandardRole>(['next', 'previous'])

/** Keys only the global scope may use. */
export const GLOBAL_ONLY_KEYS = new Set(['/', '?', 'g'])

function issuesFor(scope: Scope, hotkey: Hotkey): string[] {
  const issues: string[] = []
  for (const spec of hotkey.defaults.map(canonical)) {
    if (spec.ctrl || spec.alt || spec.meta) continue
    if (scope.id !== 'global' && scope.id !== 'goto' && GLOBAL_ONLY_KEYS.has(spec.key)) {
      issues.push(`${hotkey.fullId}: "${spec.key}" is reserved for the global scope`)
      continue
    }
    const roles = STANDARD_KEY_ROLES[spec.key]
    if (!roles || scope.id === 'goto') continue
    if (hotkey.role && (roles.includes(hotkey.role) || (STEP.has(hotkey.role) && roles.some(role => STEP.has(role))))) continue
    if (hotkey.description) continue
    issues.push(`${hotkey.fullId}: "${spec.key}" is the standard key for ${roles.join('/')}; give the hotkey that role or a description of its screen-specific use`)
  }
  return issues
}

/** Every default that drifts from the standard roles without saying why. */
export function standardRoleIssues(scopes: Scope[]): string[] {
  return scopes.flatMap(scope => scope.hotkeys.flatMap(hotkey => issuesFor(scope, hotkey)))
}
