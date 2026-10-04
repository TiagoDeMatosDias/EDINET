import { cleanup, fireEvent, render, screen } from '@testing-library/react'
import { MemoryRouter, useLocation } from 'react-router-dom'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { useHotkeys } from '../hooks/useHotkeys'
import { GlobalHotkeys } from './GlobalHotkeys'

function Where() {
  return <div aria-label="Path">{useLocation().pathname}</div>
}

function PageWithOwnHelp({ onHelp }: { onHelp: () => void }) {
  useHotkeys({ '?': onHelp, s: () => undefined })
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
    function Page() { useHotkeys({ p: pageKey }); return null }
    render(<MemoryRouter initialEntries={['/screen']}><GlobalHotkeys isAdmin /><Page /><Where /></MemoryRouter>)

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

  it('lists shortcuts with ? only where the page has no list of its own', async () => {
    const pageHelp = vi.fn()
    const { unmount } = render(<MemoryRouter><GlobalHotkeys isAdmin={false} /><PageWithOwnHelp onHelp={pageHelp} /></MemoryRouter>)
    press('?')
    await new Promise(resolve => setTimeout(resolve, 10))
    expect(pageHelp).toHaveBeenCalled()
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument()
    unmount()

    render(<MemoryRouter><GlobalHotkeys isAdmin={false} /></MemoryRouter>)
    press('?')
    const dialog = await screen.findByRole('dialog')
    expect(dialog).toHaveTextContent('Go to a page')
    expect(dialog).toHaveTextContent('Research')
  })
})
