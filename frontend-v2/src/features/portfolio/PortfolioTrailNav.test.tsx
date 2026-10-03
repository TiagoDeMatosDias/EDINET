import { cleanup, fireEvent, render, screen } from '@testing-library/react'
import { MemoryRouter, useLocation } from 'react-router-dom'
import { afterEach, describe, expect, it } from 'vitest'

import { readPortfolioTrail, trailEntry, writePortfolioTrail } from './portfolioTrail'
import { PortfolioTrailNav } from './PortfolioTrailNav'

function Where() {
  const location = useLocation()
  return <div aria-label="Path">{location.pathname + location.search}</div>
}

afterEach(() => {
  cleanup()
  localStorage.clear()
})

const HOLDINGS = [
  { symbol: 'VWCE', performance: { name: 'Vanguard FTSE All-World' } },
  { symbol: '5984.T', performance: { name: 'Kanefusa', edinet_code: 'E01437' } },
  { symbol: 'BTI', performance: { name: 'British American Tobacco' } },
]

describe('stepping through portfolio holdings from Analysis', () => {
  it('opens Tokyo holdings by EDINET code and others by ticker, with Shift+J and Shift+K', () => {
    writePortfolioTrail({ entries: HOLDINGS.map(trailEntry), current: 'E01437' })
    render(<MemoryRouter initialEntries={['/analyze/E01437?from=portfolio']}><PortfolioTrailNav current="E01437" /><Where /></MemoryRouter>)

    expect(screen.getByText('2 of 3')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Next holding' })).toHaveAttribute('title', 'Next: British American Tobacco (Shift+J)')
    fireEvent.keyDown(document.body, { key: 'J' })
    expect(screen.getByLabelText('Path')).toHaveTextContent('/analyze?ticker=BTI&from=portfolio')
    expect(readPortfolioTrail()).toMatchObject({ current: 'BTI', refocus: true })
  })

  it('recognises a ticker page by its symbol and links back to the portfolio', () => {
    writePortfolioTrail({ entries: HOLDINGS.map(trailEntry) })
    render(<MemoryRouter><PortfolioTrailNav current="VWCE" /><Where /></MemoryRouter>)

    expect(screen.getByText('1 of 3')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Previous holding' })).toBeDisabled()
    expect(screen.getByRole('link', { name: 'Portfolio' })).toHaveAttribute('href', '/portfolio')
    fireEvent.keyDown(document.body, { key: 'J' })
    expect(screen.getByLabelText('Path')).toHaveTextContent('/analyze/E01437?from=portfolio')
  })
})
