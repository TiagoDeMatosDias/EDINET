import { cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { useMemo } from 'react'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { AuthContext, type AuthContextValue } from '../features/auth/authContext'
import { HotkeyProvider } from './HotkeyProvider'
import { HotkeySettingsPanel } from './HotkeySettingsPanel'
import { parseKey } from './keys'
import { defineScope, effectiveKeys, findConflicts } from './registry'
import { HotkeySettingsContext, type HotkeySettingsValue } from './settingsContext'
import type { HotkeyOverrides } from './types'
import { useHotkeyScope } from './useHotkeyScope'

afterEach(() => { cleanup(); vi.unstubAllGlobals() })

const outer = defineScope({ id: 'test-outer', label: 'Outer', screen: 'Test screen', hotkeys: [
  { id: 'jump', keys: 'j', label: 'Jump', role: 'next' },
  { id: 'save', keys: 'Ctrl+s', label: 'Save', whileTyping: true },
  { id: 'list-only', keys: 'u', label: 'Built-in list key', fixed: true },
] })
const inner = defineScope({ id: 'test-outer.inner', label: 'Inner', screen: 'Test screen', parent: 'test-outer', hotkeys: [
  { id: 'jump', keys: 'j', label: 'Inner jump', role: 'next' },
  { id: 'other', keys: 'q', label: 'Other' },
] })
const sibling = defineScope({ id: 'test-outer.sibling', label: 'Sibling', screen: 'Test screen', parent: 'test-outer', hotkeys: [
  { id: 'q-too', keys: 'q', label: 'Sibling q' },
] })

function settings(overrides: HotkeyOverrides, save = vi.fn(async () => undefined)): HotkeySettingsValue {
  return { overrides, persistent: true, loading: false, saving: false, error: null, save, reset: vi.fn(async () => undefined) }
}

function Mounted({ onOuter, onInner, onSave }: { onOuter: () => void; onInner?: () => void; onSave?: () => void }) {
  useHotkeyScope(outer, { jump: onOuter, save: onSave, 'list-only': () => { throw new Error('fixed keys never dispatch') } })
  return onInner ? <Inner onInner={onInner} /> : <input aria-label="Field" />
}
function Inner({ onInner }: { onInner: () => void }) {
  useHotkeyScope(inner, useMemo(() => ({ jump: onInner }), [onInner]))
  return <input aria-label="Field" />
}

describe('useHotkeyScope', () => {
  it('lets the inner scope win a shared key and leaves fixed keys to their controls', () => {
    const onOuter = vi.fn()
    const onInner = vi.fn()
    render(<Mounted onOuter={onOuter} onInner={onInner} />)
    fireEvent.keyDown(document.body, { key: 'j' })
    expect(onInner).toHaveBeenCalledTimes(1)
    expect(onOuter).not.toHaveBeenCalled()
    expect(() => fireEvent.keyDown(document.body, { key: 'u' })).not.toThrow()
  })

  it('ignores typing, except for keys marked whileTyping', () => {
    const onOuter = vi.fn()
    const onSave = vi.fn()
    render(<Mounted onOuter={onOuter} onSave={onSave} />)
    const field = screen.getByLabelText('Field')
    fireEvent.keyDown(field, { key: 'j' })
    expect(onOuter).not.toHaveBeenCalled()
    fireEvent.keyDown(field, { key: 's', ctrlKey: true })
    expect(onSave).toHaveBeenCalledTimes(1)
  })

  it('follows user overrides at once', () => {
    const onOuter = vi.fn()
    const { rerender } = render(<HotkeySettingsContext.Provider value={settings({ 'test-outer.jump': [{ key: 'h' }] })}><Mounted onOuter={onOuter} /></HotkeySettingsContext.Provider>)
    fireEvent.keyDown(document.body, { key: 'j' })
    expect(onOuter).not.toHaveBeenCalled()
    fireEvent.keyDown(document.body, { key: 'h' })
    expect(onOuter).toHaveBeenCalledTimes(1)
    rerender(<HotkeySettingsContext.Provider value={settings({})}><Mounted onOuter={onOuter} /></HotkeySettingsContext.Provider>)
    fireEvent.keyDown(document.body, { key: 'j' })
    expect(onOuter).toHaveBeenCalledTimes(2)
  })
})

describe('registry', () => {
  it('finds conflicts along the parent chain and the global scope, not between sibling tabs', () => {
    expect(findConflicts(inner.byId.jump, [parseKey('q')], {}).map(hotkey => hotkey.fullId)).toEqual(['test-outer.inner.other'])
    expect(findConflicts(sibling.byId['q-too'], [parseKey('q')], {})).toEqual([])
    expect(findConflicts(outer.byId.jump, [parseKey('/')], {}).map(hotkey => hotkey.fullId)).toEqual(['global.search'])
  })

  it('rejects duplicate defaults within a scope', () => {
    expect(() => defineScope({ id: 'test-bad', label: 'Bad', screen: 'Test', hotkeys: [
      { id: 'a', keys: 'z', label: 'A' }, { id: 'b', keys: 'Z', label: 'B' }, { id: 'c', keys: 'z', label: 'C' },
    ] })).toThrow(/both default to Z/)
  })

  it('never lets a whileTyping key lose its modifier', () => {
    expect(effectiveKeys(outer.byId.save, { 'test-outer.save': [{ key: 'x' }] })).toEqual(outer.byId.save.defaults)
    expect(effectiveKeys(outer.byId.save, { 'test-outer.save': [{ key: 'k', alt: true }] })).toEqual([{ key: 'k', alt: true }])
  })
})

describe('HotkeySettingsPanel', () => {
  it('rebinds by key press, refuses conflicts and reserved keys, and resets', async () => {
    const save = vi.fn(async () => undefined)
    render(<HotkeySettingsContext.Provider value={settings({ 'test-outer.inner.other': [{ key: 'w' }] }, save)}><HotkeySettingsPanel /></HotkeySettingsContext.Provider>)
    fireEvent.change(screen.getByRole('combobox', { name: 'Screen' }), { target: { value: 'Test screen' } })
    const block = screen.getByRole('region', { name: 'Test screen shortcuts' })
    expect(within(block).getByRole('row', { name: 'Other (Inner)' })).toHaveTextContent('Custom')
    expect(within(block).getByRole('row', { name: 'Built-in list key (Outer)' })).toHaveTextContent('Built in')

    fireEvent.click(within(block).getByRole('button', { name: 'Change the key for Jump' }))
    fireEvent.keyDown(document.body, { key: 'w' })
    expect(await screen.findByRole('alert')).toHaveTextContent('W already does “Other” (Inner)')
    fireEvent.keyDown(document.body, { key: 'Tab' })
    expect(screen.getByRole('alert')).toHaveTextContent('Tab moves between fields')
    expect(save).not.toHaveBeenCalled()
    fireEvent.keyDown(document.body, { key: 'e' })
    expect(save).toHaveBeenCalledWith({ 'test-outer.inner.other': [{ key: 'w' }], 'test-outer.jump': [{ key: 'e' }] })

    fireEvent.click(within(block).getByRole('button', { name: 'Reset Other to its default' }))
    expect(save).toHaveBeenLastCalledWith({})
  })

  it('cancels a rebind with Esc', () => {
    const save = vi.fn(async () => undefined)
    render(<HotkeySettingsContext.Provider value={settings({}, save)}><HotkeySettingsPanel /></HotkeySettingsContext.Provider>)
    fireEvent.change(screen.getByRole('searchbox'), { target: { value: 'sibling q' } })
    fireEvent.click(screen.getByRole('button', { name: 'Change the key for Sibling q' }))
    expect(screen.getByText('Press the new key…')).toBeInTheDocument()
    fireEvent.keyDown(document.body, { key: 'Escape' })
    expect(screen.queryByText('Press the new key…')).not.toBeInTheDocument()
    expect(save).not.toHaveBeenCalled()
  })
})

describe('HotkeyProvider', () => {
  const auth = { user: { user_id: 'u1', username: 'alice', role: 'member', status: 'active' }, status: { mode: 'accounts' }, loading: false } as unknown as AuthContextValue

  function Probe() {
    useHotkeyScope(outer, { jump: () => document.body.setAttribute('data-jumped', 'yes') })
    return null
  }

  it('loads the account overrides from the server and saves changes there', async () => {
    const requests: Array<{ method: string; body?: string }> = []
    vi.stubGlobal('fetch', vi.fn(async (_path: string, init?: RequestInit) => {
      requests.push({ method: init?.method ?? 'GET', body: init?.body as string | undefined })
      const body = init?.method === 'PUT' ? JSON.parse(String(init.body)) : { overrides: { 'test-outer.jump': [{ key: 'h', shift: false, ctrl: false, alt: false, meta: false }] } }
      return new Response(JSON.stringify(body), { status: 200, headers: { 'Content-Type': 'application/json' } })
    }))
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    render(<QueryClientProvider client={client}><AuthContext.Provider value={auth}><HotkeyProvider><Probe /><HotkeySettingsPanel /></HotkeyProvider></AuthContext.Provider></QueryClientProvider>)
    await waitFor(() => expect(screen.getByRole('status')).toHaveTextContent('1 custom key'))
    fireEvent.keyDown(document.body, { key: 'h' })
    expect(document.body.getAttribute('data-jumped')).toBe('yes')

    fireEvent.click(screen.getByRole('button', { name: 'Reset Jump to its default' }))
    await waitFor(() => expect(requests.some(request => request.method === 'DELETE')).toBe(true))
  })
})
