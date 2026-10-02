import { cleanup, fireEvent, render, screen, within } from '@testing-library/react'
import { MemoryRouter, Route, Routes, useLocation } from 'react-router-dom'
import { afterEach, describe, expect, it } from 'vitest'

import type { ScreeningResult } from '../../api/types'
import { cellText } from './resultFormat'
import { ResultsTable } from './ResultsTable'
import { readCursor, readTrail, resultSignature, writeCursor } from './screenTrail'

afterEach(() => {
  cleanup()
  localStorage.clear()
})

const codes = Array.from({ length: 150 }, (_, index) => `E${String(index).padStart(5, '0')}`)
const MANY: ScreeningResult = {
  columns: ['Company_Code', 'Company_Name', 'Score'],
  rows: codes.map((code, index) => [code, `Company ${index}`, 150 - index]),
  row_count: 150,
  column_formats: {},
}

function Where() {
  const location = useLocation()
  return <div aria-label="Path">{location.pathname + location.search}</div>
}

function renderResults() {
  return render(<MemoryRouter initialEntries={['/screen']}><Routes>
    <Route path="/screen" element={<ResultsTable result={MANY} columnInfo={column => ({ label: column })} />} />
    <Route path="/analyze/:code" element={<Where />} />
  </Routes></MemoryRouter>)
}

const focusedName = () => {
  const focused = document.activeElement
  return focused?.tagName === 'TR' ? focused.querySelector('th a')?.textContent : undefined
}

describe('screening results', () => {
  it('formats cells by their declared format and abbreviates large amounts', () => {
    expect(cellText(0.0329, 'percent')).toBe('3.3%')
    expect(cellText(45_513_256_365_990)).toBe('45.51T')
    expect(cellText(11.0708)).toBe('11.07')
    expect(cellText(null)).toBe('—')
  })

  it('leads with the linked company, folds code and ticker into it, and sorts numbers', () => {
    render(<MemoryRouter><ResultsTable result={{
      columns: ['Company_Code', 'Company_Ticker', 'Company_Name', 'P/E ratio', 'LatestPrice', 'PriceDate'],
      rows: [['E1', '10010', 'Alpha', 12, 100, '2026-09-29'], ['E2', '20020', 'Beta', 8, 50, '2026-09-29'], ['E3', '30030', 'Gamma', null, 70, '2026-09-29']],
      row_count: 3,
      column_formats: {},
    }} columnInfo={column => ({ label: column === 'LatestPrice' ? 'Price' : column })} /></MemoryRouter>)

    const headers = screen.getAllByRole('columnheader').map(header => header.textContent)
    expect(headers).toEqual(['Company_Name', 'Price', 'P/E ratio'])
    expect(screen.getByRole('link', { name: 'Alpha' })).toHaveAttribute('href', '/analyze/E1?from=screen')
    fireEvent.click(within(screen.getAllByRole('columnheader')[2]).getByRole('button'))
    const names = () => screen.getAllByRole('rowheader').map(cell => cell.querySelector('a')?.textContent)
    expect(names()).toEqual(['Alpha', 'Beta', 'Gamma'])
    fireEvent.click(within(screen.getAllByRole('columnheader')[2]).getByRole('button'))
    expect(names()).toEqual(['Beta', 'Alpha', 'Gamma'])
  })

  it('enters the list from anywhere, moves across pages, and opens the company with Enter', () => {
    renderResults()

    fireEvent.keyDown(document.body, { key: 'ArrowDown' })
    expect(focusedName()).toBe('Company 0')
    fireEvent.keyDown(document.activeElement!, { key: 'End' })
    expect(focusedName()).toBe('Company 149')
    expect(screen.getByText('101–150 of 150')).toBeInTheDocument()
    fireEvent.keyDown(document.activeElement!, { key: 'k' })
    fireEvent.keyDown(document.activeElement!, { key: 'Enter' })

    expect(screen.getByLabelText('Path')).toHaveTextContent('/analyze/E00148?from=screen')
    expect(readTrail()?.codes).toHaveLength(150)
    expect(readCursor()).toMatchObject({ code: 'E00148', refocus: true })
  })

  it('pages with [ and ] and keeps paging inside the list when a row has focus', () => {
    renderResults()

    fireEvent.keyDown(document.body, { key: ']' })
    expect(screen.getByText('101–150 of 150')).toBeInTheDocument()
    fireEvent.keyDown(document.body, { key: 'j' })
    expect(focusedName()).toBe('Company 100')
    fireEvent.keyDown(document.activeElement!, { key: 'k' })
    expect(focusedName()).toBe('Company 99')
    expect(screen.getByText('1–100 of 150')).toBeInTheDocument()
  })

  it('comes back to the company last opened', () => {
    writeCursor({ signature: resultSignature(MANY.columns, MANY.row_count, codes), code: 'E00120', sort: null, refocus: true })
    renderResults()

    expect(focusedName()).toBe('Company 120')
    expect(screen.getByText('101–150 of 150')).toBeInTheDocument()
  })

  it('starts at the top for different results', () => {
    writeCursor({ signature: 'another run', code: 'E00120', sort: null, refocus: true })
    renderResults()

    expect(focusedName()).toBeUndefined()
    expect(screen.getByText('1–100 of 150')).toBeInTheDocument()
  })
})
