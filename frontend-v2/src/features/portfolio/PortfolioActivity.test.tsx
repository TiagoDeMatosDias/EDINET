import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react'
import type { ReactElement } from 'react'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { PortfolioActivity } from './PortfolioActivity'
import type { Transaction } from './portfolioTypes'
import { recordsCsv, selectedRecords, selectionPayload } from './recordsDelete'

afterEach(() => {
  cleanup()
  vi.unstubAllGlobals()
})

function withQueries(element: ReactElement) {
  return <QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}>{element}</QueryClientProvider>
}

/** The delete endpoint: a preview without ``confirm``, a result with it. */
function stubDeletes() {
  const bodies: Array<Record<string, unknown>> = []
  vi.stubGlobal('fetch', vi.fn((_input: RequestInfo | URL, init?: RequestInit) => {
    const body = JSON.parse(String(init?.body ?? '{}')) as Record<string, unknown>
    bodies.push(body)
    const count = Array.isArray(body.ids) ? body.ids.length : 55
    const payload = body.confirm
      ? { deleted: count, remaining: 55 - count, daily_rows: 10, holdings_count: 2 }
      : { preview: { records: count, by_type: { DIVIDEND: count }, first_date: '2026-07-01', last_date: '2026-07-31', symbols: count, source_files: ['shade.xml'], remaining: 55 - count } }
    return Promise.resolve(new Response(JSON.stringify(payload), { status: 200, headers: { 'Content-Type': 'application/json' } }))
  }))
  return bodies
}

function transactions(count: number): Transaction[] {
  return Array.from({ length: count }, (_, index) => ({
    id: index,
    trade_date: `2026-07-${String(31 - (index % 28)).padStart(2, '0')}`,
    activity_type: index % 2 ? 'TRADE' : 'DIVIDEND',
    symbol: `SYM${index}`,
    description: `Record ${index}`,
    amount: index + 1,
    currency: 'EUR',
    source_file: 'shade.xml',
  }))
}

describe('PortfolioActivity', () => {
  it('marks the broker’s corrections and can hide the records that cancel out', () => {
    const tax = 'O CASH DIVIDEND USD 0.2345 PER SHARE - US TAX'
    const records: Transaction[] = [
      { id: 1, trade_date: '2021-03-15', activity_type: 'DIVIDEND', symbol: 'O', description: 'O CASH DIVIDEND', amount: 23.45, currency: 'USD' },
      { id: 2, trade_date: '2021-03-15', activity_type: 'WITHHOLDING_TAX', symbol: 'O', description: tax, amount: -3.52, currency: 'USD' },
      { id: 3, trade_date: '2021-03-15', report_date: '2022-02-04', activity_type: 'WITHHOLDING_TAX', symbol: 'O', description: tax, amount: 3.52, currency: 'USD' },
      { id: 4, trade_date: '2021-03-15', report_date: '2022-02-04', activity_type: 'WITHHOLDING_TAX', symbol: 'O', description: tax, amount: -1.09, currency: 'USD' },
      { id: 5, trade_date: '2021-03-16', activity_type: 'TRADE', asset_category: 'CASH', symbol: 'EUR.USD', description: 'EUR.USD', quantity: -97, trade_money: -117.92, net_cash: 0, currency: 'USD' },
    ]
    render(<PortfolioActivity data={records} activity={{ DIVIDEND: 1, WITHHOLDING_TAX: 3, TRADE: 1 }} isLoading={false} onOpenDetail={vi.fn()} />)

    expect(screen.getByText('Reversal · booked 4 Feb 2022')).toBeInTheDocument()
    expect(screen.getByText('Corrected · booked 4 Feb 2022')).toBeInTheDocument()
    // A currency conversion shows both balances it moves.
    expect(screen.getByText('-€97.00')).toBeInTheDocument()
    expect(screen.getByText('+$117.92')).toBeInTheDocument()

    fireEvent.click(screen.getByRole('checkbox', { name: 'Hide 2 reversed records' }))
    expect(screen.getByText('3 of 5 records')).toBeInTheDocument()
    expect(screen.queryByText('Reversal · booked 4 Feb 2022')).not.toBeInTheDocument()
  })

  it('paginates a large activity ledger and keeps every record reachable', () => {
    render(<PortfolioActivity
      data={transactions(55)}
      activity={{ TRADE: 27, DIVIDEND: 28 }}
      dateRange={{ min_date: '2020-12-14', max_date: '2026-07-31' }}
      isLoading={false}
      onOpenDetail={vi.fn()}
    />)

    expect(screen.getByText('1–50 of 55')).toBeInTheDocument()
    expect(screen.getByText('Record 49')).toBeInTheDocument()
    expect(screen.queryByText('Record 54')).not.toBeInTheDocument()

    fireEvent.click(screen.getByRole('button', { name: 'Next page' }))

    expect(screen.getByText('51–55 of 55')).toBeInTheDocument()
    expect(screen.getByText('Record 54')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Next page' })).toBeDisabled()
  })

  it('opens an individual transaction from the filtered ledger', () => {
    const onOpenDetail = vi.fn()
    render(<PortfolioActivity data={transactions(3)} activity={{ TRADE: 1, DIVIDEND: 2 }} isLoading={false} onOpenDetail={onOpenDetail} />)

    fireEvent.change(screen.getByRole('textbox', { name: 'Search activity' }), { target: { value: 'SYM2' } })
    fireEvent.click(screen.getByRole('button', { name: 'View transaction from 2026-07-29' }))

    expect(onOpenDetail).toHaveBeenCalledWith(expect.objectContaining({ kind: 'transaction', transaction: expect.objectContaining({ symbol: 'SYM2' }) }))
  })

  it('selects records with Space and their boxes, then deletes them after a preview', async () => {
    const bodies = stubDeletes()
    const onDeleted = vi.fn()
    const rows = transactions(4).map((row, index) => ({ ...row, id: index + 1 }))
    render(withQueries(<PortfolioActivity data={rows} activity={{}} isLoading={false} hotkeys onOpenDetail={vi.fn()} onDeleted={onDeleted} />))

    fireEvent.keyDown(document.body, { key: 'j' })
    fireEvent.keyDown(document.activeElement!, { key: ' ' })
    fireEvent.click(screen.getAllByRole('checkbox')[2])
    expect(screen.getByText('2 selected')).toBeInTheDocument()

    fireEvent.keyDown(document.body, { key: 'Delete' })
    const dialog = await screen.findByRole('alertdialog', { name: 'Delete 2 selected records' })
    expect(dialog).toBeInTheDocument()
    expect(await screen.findByText(/permanently deletes/)).toHaveTextContent('2 records')
    expect(screen.getByRole('button', { name: 'Cancel' })).toHaveFocus()
    fireEvent.click(screen.getByRole('button', { name: 'Delete 2 records' }))

    await waitFor(() => expect(onDeleted).toHaveBeenCalledWith(expect.objectContaining({ deleted: 2 }), { kind: 'records', ids: [1, 3] }))
    expect(bodies.at(-1)).toEqual({ ids: [1, 3], confirm: true })
    expect(screen.queryByRole('alertdialog')).not.toBeInTheDocument()
    expect(screen.queryByText('2 selected')).not.toBeInTheDocument()
  })

  it('selects every record the filters show and cancels with Escape', async () => {
    stubDeletes()
    const rows = transactions(6).map((row, index) => ({ ...row, id: index + 1 }))
    render(withQueries(<PortfolioActivity data={rows} activity={{}} isLoading={false} hotkeys onOpenDetail={vi.fn()} />))
    fireEvent.change(screen.getByRole('combobox', { name: 'Activity type' }), { target: { value: 'TRADE' } })
    fireEvent.click(screen.getByRole('button', { name: 'Select all 3 shown' }))
    expect(screen.getByText('3 selected')).toBeInTheDocument()

    fireEvent.click(screen.getByRole('button', { name: /Delete 3 selected/ }))
    await screen.findByRole('alertdialog')
    fireEvent.keyDown(screen.getByRole('button', { name: 'Cancel' }), { key: 'Escape' })
    expect(screen.queryByRole('alertdialog')).not.toBeInTheDocument()
    expect(screen.getByText('3 selected')).toBeInTheDocument()
  })

  it('clears everything only after the word delete is typed', async () => {
    const bodies = stubDeletes()
    const rows = transactions(3).map((row, index) => ({ ...row, id: index + 1 }))
    render(withQueries(<PortfolioActivity data={rows} activity={{}} isLoading={false} imports={[{ source_file: 'shade.xml', records: 3, first_date: '2026-07-29', last_date: '2026-07-31', imported_at: '2026-08-01 10:00:00', symbols: 3 }]} onOpenDetail={vi.fn()} />))

    fireEvent.click(screen.getByRole('button', { name: /Clear all portfolio data/ }))
    await screen.findByText(/permanently deletes/)
    const confirm = screen.getByRole('button', { name: 'Delete 55 records' })
    expect(confirm).toBeDisabled()
    fireEvent.change(screen.getByRole('textbox', { name: 'Type delete to confirm' }), { target: { value: 'Delete' } })
    expect(confirm).toBeEnabled()
    expect(bodies[0]).toEqual({ everything: true })
  })
})

describe('deleting records', () => {
  it('builds payloads, finds the records a selection covers, and writes them as CSV', () => {
    const rows: Transaction[] = [
      { id: 1, trade_date: '2026-01-02', activity_type: 'DIVIDEND', symbol: 'MO', description: 'MO, "cash" dividend', amount: 10, currency: 'USD', source_file: 'a.xml' },
      { id: 2, trade_date: '2026-01-03', activity_type: 'TRADE', symbol: 'AFL', amount: 0, currency: 'USD', source_file: 'b.xml' },
    ]
    expect(selectionPayload({ kind: 'files', files: ['a.xml'] })).toEqual({ source_files: ['a.xml'] })
    expect(selectedRecords(rows, { kind: 'files', files: ['b.xml'] }).map(row => row.id)).toEqual([2])
    expect(selectedRecords(rows, { kind: 'records', ids: [1] }).map(row => row.id)).toEqual([1])
    const csv = recordsCsv(rows)
    expect(csv.split('\r\n')[0]).toBe('id,trade_date,activity_type,symbol,description,quantity,trade_price,trade_money,proceeds,amount,net_cash,commission,currency,buy_sell,source_file')
    expect(csv).toContain('"MO, ""cash"" dividend"')
  })
})
