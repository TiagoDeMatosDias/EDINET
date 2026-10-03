import { cleanup, fireEvent, render, screen, within } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { PortfolioTable, type TableColumn } from './PortfolioTable'

afterEach(cleanup)

type Row = { symbol: string; value: number }

const ROWS: Row[] = [
  { symbol: 'AAA', value: 10 },
  { symbol: 'BBB', value: 30 },
  { symbol: 'CCC', value: 20 },
]

const COLUMNS: TableColumn<Row>[] = [
  { id: 'symbol', header: 'Holding', rowHeader: true, sortValue: row => row.symbol, cell: row => row.symbol },
  { id: 'value', header: 'Value', numeric: true, sortValue: row => row.value, cell: row => String(row.value) },
]

function focusedSymbol() {
  const row = document.activeElement
  return row?.tagName === 'TR' ? row.querySelector('th')?.textContent : null
}

describe('PortfolioTable', () => {
  it('enters with J, moves with J/K and arrows, and opens with Enter and A', () => {
    const onOpen = vi.fn()
    const onSecondary = vi.fn()
    render(<PortfolioTable label="Holdings" rows={ROWS} columns={COLUMNS} rowKey={row => row.symbol} onOpen={onOpen} onSecondary={onSecondary} secondaryLabel="analysis" initialSort={{ column: 'value', direction: 'desc' }} />)

    fireEvent.keyDown(document.body, { key: 'j' })
    expect(focusedSymbol()).toBe('BBB')
    fireEvent.keyDown(document.activeElement!, { key: 'j' })
    fireEvent.keyDown(document.activeElement!, { key: 'ArrowDown' })
    expect(focusedSymbol()).toBe('AAA')
    fireEvent.keyDown(document.activeElement!, { key: 'k' })
    expect(focusedSymbol()).toBe('CCC')
    fireEvent.keyDown(document.activeElement!, { key: 'Enter' })
    expect(onOpen).toHaveBeenCalledWith(ROWS[2])
    fireEvent.keyDown(document.activeElement!, { key: 'a' })
    expect(onSecondary).toHaveBeenCalledWith(ROWS[2])
    fireEvent.keyDown(document.activeElement!, { key: 'Home' })
    expect(focusedSymbol()).toBe('BBB')
  })

  it('sorts from the headers and reports the displayed order once per change', () => {
    const onOrderChange = vi.fn()
    render(<PortfolioTable label="Holdings" rows={ROWS} columns={COLUMNS} rowKey={row => row.symbol} onOrderChange={onOrderChange} />)
    const table = screen.getByRole('table', { name: 'Holdings' })
    expect(within(table).getAllByRole('rowheader').map(cell => cell.textContent)).toEqual(['AAA', 'BBB', 'CCC'])

    fireEvent.click(screen.getByRole('button', { name: 'Value' }))
    expect(within(table).getAllByRole('rowheader').map(cell => cell.textContent)).toEqual(['BBB', 'CCC', 'AAA'])
    expect(screen.getByRole('columnheader', { name: 'Value' })).toHaveAttribute('aria-sort', 'descending')
    expect(onOrderChange).toHaveBeenCalledTimes(2)
    expect(onOrderChange).toHaveBeenLastCalledWith([ROWS[1], ROWS[2], ROWS[0]])
  })

  it('sorts dates newest first and only flips a table that starts sorted by that column', () => {
    const DAYS = [{ symbol: 'B', day: '2024-02-01' }, { symbol: 'A', day: '2024-01-01' }, { symbol: 'C', day: '2024-03-01' }]
    const columns: TableColumn<{ symbol: string; day: string }>[] = [
      { id: 'symbol', header: 'Holding', rowHeader: true, sortValue: row => row.symbol, cell: row => row.symbol },
      { id: 'day', header: 'Date', sortFirst: 'desc', sortValue: row => row.day, cell: row => row.day },
    ]
    render(<PortfolioTable label="Days" rows={DAYS} columns={columns} rowKey={row => row.symbol} initialSort={{ column: 'day', direction: 'desc' }} />)
    const order = () => within(screen.getByRole('table', { name: 'Days' })).getAllByRole('rowheader').map(cell => cell.textContent)
    const header = screen.getByRole('columnheader', { name: 'Date' })
    expect(order()).toEqual(['C', 'B', 'A'])

    fireEvent.click(screen.getByRole('button', { name: 'Date' }))
    expect(order()).toEqual(['A', 'B', 'C'])
    expect(header).toHaveAttribute('aria-sort', 'ascending')
    fireEvent.click(screen.getByRole('button', { name: 'Date' }))
    expect(order()).toEqual(['C', 'B', 'A'])
    expect(header).toHaveAttribute('aria-sort', 'descending')

    // Another column cycles through both directions and back to the table's own order.
    fireEvent.click(screen.getByRole('button', { name: 'Holding' }))
    fireEvent.click(screen.getByRole('button', { name: 'Holding' }))
    expect(order()).toEqual(['C', 'B', 'A'])
    fireEvent.click(screen.getByRole('button', { name: 'Holding' }))
    expect(header).toHaveAttribute('aria-sort', 'descending')
  })

  it('starts on a remembered row with focus, for coming back from Analysis', () => {
    render(<PortfolioTable label="Holdings" rows={ROWS} columns={COLUMNS} rowKey={row => row.symbol} initialCursor="CCC" />)
    expect(focusedSymbol()).toBe('CCC')
  })

  it('pages long lists with [ and ]', () => {
    const many = Array.from({ length: 5 }, (_, index) => ({ symbol: `S${index}`, value: index }))
    render(<PortfolioTable label="Activity" rows={many} columns={COLUMNS} rowKey={row => row.symbol} pageSize={2} />)
    expect(screen.getByText('1–2 of 5')).toBeInTheDocument()
    fireEvent.keyDown(document.body, { key: ']' })
    expect(screen.getByText('3–4 of 5')).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'Next page' }))
    expect(screen.getByText('5–5 of 5')).toBeInTheDocument()
  })
})
