import { cleanup, fireEvent, render, screen } from '@testing-library/react'
import { MemoryRouter, useLocation } from 'react-router-dom'
import { afterEach, describe, expect, it } from 'vitest'

import { readCursor, writeCursor, writeTrail } from './screenTrail'
import { ScreenTrailNav } from './ScreenTrailNav'

function Where() {
  const location = useLocation()
  return <div aria-label="Path">{location.pathname + location.search}</div>
}

afterEach(() => {
  cleanup()
  localStorage.clear()
})

describe('stepping through screen results from Analysis', () => {
  it('shows the position and steps with Shift+J and Shift+K, keeping the results place in step', () => {
    writeTrail({ codes: ['E1', 'E2', 'E3'], names: ['Alpha', 'Beta', 'Gamma'] })
    writeCursor({ signature: 'run', code: 'E2', sort: null })
    render(<MemoryRouter initialEntries={['/analyze/E2?from=screen']}><ScreenTrailNav current="E2" /><Where /></MemoryRouter>)

    expect(screen.getByText('2 of 3')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Next screened company' })).toHaveAttribute('title', 'Next: Gamma (Shift+J)')
    fireEvent.keyDown(document.body, { key: 'J' })

    expect(screen.getByLabelText('Path')).toHaveTextContent('/analyze/E3?from=screen')
    expect(readCursor()).toMatchObject({ signature: 'run', code: 'E3', refocus: true })
  })

  it('only links back when the company is not in the stored results', () => {
    writeTrail({ codes: ['E1'], names: ['Alpha'] })
    render(<MemoryRouter><ScreenTrailNav current="E9" /></MemoryRouter>)

    expect(screen.getByRole('link', { name: 'Screen' })).toHaveAttribute('href', '/screen')
    expect(screen.queryByRole('button', { name: 'Next screened company' })).not.toBeInTheDocument()
  })
})
