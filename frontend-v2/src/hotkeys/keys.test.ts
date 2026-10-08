import { describe, expect, it } from 'vitest'

import { formatKey, keyChips, keyFromEvent, keysMatch, parseKey, reservedReason, sameKey } from './keys'

const press = (key: string, modifiers: Partial<Record<'shiftKey' | 'ctrlKey' | 'altKey' | 'metaKey', boolean>> = {}) =>
  keyFromEvent({ key, shiftKey: false, ctrlKey: false, altKey: false, metaKey: false, ...modifiers })

describe('hotkey keys', () => {
  it('parses text into canonical specs', () => {
    expect(parseKey('n')).toEqual({ key: 'n' })
    expect(parseKey('N')).toEqual({ key: 'n', shift: true })
    expect(parseKey('Shift+N')).toEqual({ key: 'n', shift: true })
    expect(parseKey('Ctrl+Enter')).toEqual({ key: 'Enter', ctrl: true })
    expect(parseKey('Cmd+S')).toEqual({ key: 's', meta: true })
    expect(parseKey('Ctrl+Shift+S')).toEqual({ key: 's', ctrl: true, shift: true })
    expect(parseKey('Esc')).toEqual({ key: 'Escape' })
    expect(parseKey('↓')).toEqual({ key: 'ArrowDown' })
    expect(parseKey('+')).toEqual({ key: '+' })
    // Shift carries no meaning for symbols: the layout decides whether it is needed.
    expect(parseKey('Shift+?')).toEqual({ key: '?' })
    expect(() => parseKey('Hyper+x')).toThrow(/Unknown modifier/)
  })

  it('matches letters by case through Shift, and symbols whatever Shift says', () => {
    expect(keysMatch(parseKey('n'), press('n'))).toBe(true)
    expect(keysMatch(parseKey('n'), press('N', { shiftKey: true }))).toBe(false)
    expect(keysMatch(parseKey('Shift+N'), press('N', { shiftKey: true }))).toBe(true)
    // Caps Lock: an upper-case letter without Shift is the plain key.
    expect(keysMatch(parseKey('n'), press('N'))).toBe(true)
    expect(keysMatch(parseKey('?'), press('?', { shiftKey: true }))).toBe(true)
    expect(keysMatch(parseKey('?'), press('?'))).toBe(true)
    expect(keysMatch(parseKey('Tab'), press('Tab', { shiftKey: true }))).toBe(false)
  })

  it('never fires a plain binding while Ctrl, Alt, or Cmd is held', () => {
    expect(keysMatch(parseKey('s'), press('s', { ctrlKey: true }))).toBe(false)
    expect(keysMatch(parseKey('Ctrl+S'), press('s', { ctrlKey: true }))).toBe(true)
    expect(keysMatch(parseKey('Ctrl+S'), press('S', { ctrlKey: true, shiftKey: true }))).toBe(false)
    expect(keysMatch(parseKey('Ctrl+Enter'), press('Enter', { ctrlKey: true, metaKey: true }))).toBe(false)
  })

  it('formats keys as chips', () => {
    expect(keyChips(parseKey('Shift+N'))).toEqual(['Shift', 'N'])
    expect(formatKey(parseKey('Ctrl+Enter'))).toBe('Ctrl+Enter')
    expect(formatKey(parseKey('ArrowDown'))).toBe('↓')
    expect(formatKey(parseKey('Escape'))).toBe('Esc')
    expect(sameKey(parseKey('N'), parseKey('Shift+n'))).toBe(true)
  })

  it('refuses keys that cannot be rebound', () => {
    expect(reservedReason(parseKey('Escape'))).toMatch(/Esc/)
    expect(reservedReason(parseKey('Tab'))).toMatch(/Tab/)
    expect(reservedReason(parseKey('Ctrl+w'))).toMatch(/browser/)
    expect(reservedReason(parseKey('Ctrl+k'))).toBeNull()
    expect(reservedReason(parseKey('h'))).toBeNull()
  })
})
