import { describe, expect, it } from 'vitest'

import './catalog'
import { screeningCommandsScope } from '../features/screening/screeningHotkeys'
import { formatKey, keyFromEvent, keysMatch, sameKey } from './keys'
import { allScopes, getScope, relatedScopes } from './registry'
import { standardRoleIssues } from './standard'

describe('hotkey catalog', () => {
  it('declares every screen once, with unique full ids', () => {
    const ids = allScopes().flatMap(scope => scope.hotkeys.map(hotkey => hotkey.fullId))
    expect(new Set(ids).size).toBe(ids.length)
    const screens = new Set(allScopes().map(scope => scope.screen))
    for (const screen of ['Anywhere', 'Overview', 'Screen', 'Analysis', 'Backtest', 'Portfolio', 'Filings', 'Filing viewer', 'Compare', 'Research', 'Chat', 'Account', 'Admin', 'Data pipeline', 'Profile']) {
      expect(screens).toContain(screen)
    }
  })

  it('gives every child scope a declared parent', () => {
    for (const scope of allScopes()) {
      if (scope.parent) expect(getScope(scope.parent), scope.id).toBeDefined()
    }
  })

  it('has no default key that fires twice where scopes are mounted together', () => {
    const clashes: string[] = []
    for (const scope of allScopes()) {
      for (const hotkey of scope.hotkeys.filter(item => !item.fixed)) {
        for (const other of relatedScopes(scope.id).flatMap(related => related.hotkeys)) {
          if (other.fixed || other.fullId <= hotkey.fullId) continue
          const shared = hotkey.defaults.find(spec => other.defaults.some(otherSpec => sameKey(spec, otherSpec)))
          if (shared) clashes.push(`${hotkey.fullId} and ${other.fullId} both use ${formatKey(shared)}`)
        }
      }
    }
    expect(clashes).toEqual([])
  })

  it('keeps every default on its standard role or explains the screen-specific use', () => {
    expect(standardRoleIssues(allScopes())).toEqual([])
  })

  it('saves and runs screens with Ctrl or Cmd and no Shift, as before', () => {
    const chord = (key: string, modifiers: Partial<KeyboardEvent>) => keyFromEvent({ key, shiftKey: false, ctrlKey: false, altKey: false, metaKey: false, ...modifiers })
    const fires = (id: 'run' | 'save', pressed: ReturnType<typeof chord>) => screeningCommandsScope.byId[id].defaults.some(spec => keysMatch(spec, pressed))
    expect(fires('save', chord('s', { ctrlKey: true }))).toBe(true)
    expect(fires('save', chord('s', { metaKey: true }))).toBe(true)
    expect(fires('save', chord('S', { ctrlKey: true, shiftKey: true }))).toBe(false)
    expect(fires('run', chord('Enter', { ctrlKey: true }))).toBe(true)
    expect(fires('run', chord('Enter', {}))).toBe(false)
  })
})
