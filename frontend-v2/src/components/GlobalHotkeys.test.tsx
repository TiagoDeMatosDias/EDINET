import { cleanup, fireEvent, render, screen } from '@testing-library/react'
import { MemoryRouter, useLocation } from 'react-router-dom'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { defineScope } from '../hotkeys/registry'
import { useHotkeyScope } from '../hotkeys/useHotkeyScope'
import { GlobalHotkeys } from './GlobalHotkeys'

function Where() {
  return <div aria-label="Path">{useLocation().pathname}</div>
}

const pageScope = defineScope({ id: 'test-page', label: 'Test page', screen: 'Test', hotkeys: [
  { id: 'save', keys: 's', label: 'Save the thing' },
  { id: 'jump', keys: 'p', label: 'Jump somewhere' },
] })

function Page({ onSave = () => undefined, onJump = () => undefined }: { onSave?: () => void; onJump?: () => void }) {
  useHotkeyScope(pageScope, { save: onSave, jump: onJump })
  return null
}

afterEach(cleanup)

const press = (key: string, target: Element = document.body) => fireEvent.keyDown(target, { key })

describe('global hotkeys', () => {
  it('goes to a page with G then a letter, showing the choices in between', () => {
    render(<MemoryRouter initialEntries={['/overview']}><GlobalHotkeys isAdmin={false} /><Where /></MemoryRouter>)

    press('g')
    expect(screen.getByRole('status')).toHaveTextContent('Go to')
    expect(screen.getByRole('status')).not.toHaveTextContent('Data pipeline')
    press('s')

    expect(screen.getByLabelText('Path')).toHaveTextContent('/screen')
    expect(screen.queryByRole('status')).not.toBeInTheDocument()
  })

  it('beats page shortcuts to the second key, and other keys or Esc cancel', () => {
    const pageKey = vi.fn()
    render(<MemoryRouter initialEntries={['/screen']}><GlobalHotkeys isAdmin /><Page onJump={pageKey} /><Where /></MemoryRouter>)

    press('g'); press('p')
    expect(screen.getByLabelText('Path')).toHaveTextContent('/portfolio')
    expect(pageKey).not.toHaveBeenCalled()

    press('g'); press('Escape')
    expect(screen.queryByRole('status')).not.toBeInTheDocument()
    press('g'); press('z')
    expect(screen.getByLabelText('Path')).toHaveTextContent('/portfolio')

    press('g'); press('d')
    expect(screen.getByLabelText('Path')).toHaveTextContent('/pipeline')
  })

  it('reaches Chat, Account, and (for administrators) Admin', () => {
    const { unmount } = render(<MemoryRouter initialEntries={['/overview']}><GlobalHotkeys isAdmin /><Where /></MemoryRouter>)
    press('g'); press('m')
    expect(screen.getByLabelText('Path')).toHaveTextContent('/chat')
    press('g'); press('u')
    expect(screen.getByLabelText('Path')).toHaveTextContent('/account')
    press('g'); press('n')
    expect(screen.getByLabelText('Path')).toHaveTextContent('/admin')
    unmount()

    render(<MemoryRouter initialEntries={['/overview']}><GlobalHotkeys isAdmin={false} signedIn={false} /><Where /></MemoryRouter>)
    press('g')
    expect(screen.getByRole('status')).not.toHaveTextContent('Admin')
    expect(screen.getByRole('status')).not.toHaveTextContent('Account')
    press('n')
    expect(screen.getByLabelText('Path')).toHaveTextContent('/overview')
  })

  it('leaves G alone while typing', () => {
    render(<MemoryRouter><GlobalHotkeys isAdmin={false} /><input aria-label="Field" /></MemoryRouter>)
    press('g', screen.getByLabelText('Field'))
    expect(screen.queryByRole('status')).not.toBeInTheDocument()
  })

  it('releases the focused field on Shift+Tab so shortcuts work again', () => {
    render(<MemoryRouter initialEntries={['/overview']}><GlobalHotkeys isAdmin={false} /><Where /><input aria-label="Field" /></MemoryRouter>)
    const field = screen.getByLabelText('Field')
    field.focus()
    expect(field).toHaveFocus()

    expect(fireEvent.keyDown(field, { key: 'Tab', shiftKey: true })).toBe(false)
    expect(field).not.toHaveFocus()

    press('g')
    expect(screen.getByRole('status')).toHaveTextContent('Go to')
  })

  it('leaves Shift+Tab alone when no field is focused', () => {
    render(<MemoryRouter><GlobalHotkeys isAdmin={false} /></MemoryRouter>)
    expect(fireEvent.keyDown(document.body, { key: 'Tab', shiftKey: true })).toBe(true)
  })

  it('lists the mounted screen\'s shortcuts and the global ones with ?', async () => {
    render(<MemoryRouter><GlobalHotkeys isAdmin={false} /><Page /></MemoryRouter>)
    press('?')
    const dialog = await screen.findByRole('dialog')
    expect(dialog).toHaveTextContent('Test page')
    expect(dialog).toHaveTextContent('Save the thing')
    expect(dialog).toHaveTextContent('Search companies')
    expect(dialog).toHaveTextContent('Go to a page')
    expect(dialog).toHaveTextContent('Research')
  })

  it('omits screens that are not mounted', async () => {
    render(<MemoryRouter><GlobalHotkeys isAdmin={false} /></MemoryRouter>)
    press('?')
    const dialog = await screen.findByRole('dialog')
    expect(dialog).not.toHaveTextContent('Save the thing')
    expect(dialog).toHaveTextContent('Anywhere')
  })
})
