import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { cleanup, fireEvent, render, screen, within } from '@testing-library/react'
import type { ReactNode } from 'react'
import { MemoryRouter } from 'react-router-dom'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import type { CompanyOwnership, OfficerRow, ShareholderRow } from './ownershipTypes'
import { PeoplePanel } from './PeoplePanel'
import { ShareholdersPanel } from './ShareholdersPanel'

const holder = (rank: number, name: string, shares: number, ratio: number, other_holdings: number): ShareholderRow => ({
  entity_id: `H${rank}`, kind: 'organization', name, rank, filed_name: name, account: null, account_ja: null, address: null,
  shares, ratio, own_shares: false, other_holdings, positions: 0, change: null, since: '2024-03-31',
})
const officer = (name: string, title: string, age: number, shares: number | null): OfficerRow => ({
  entity_id: name, name, name_ja: null, title, title_ja: null, representative: false, outside: false, auditor: false,
  birth_date: null, age, shares, other_shares: null, ratio: shares == null ? null : shares / 1e6, term_start: null, term_end: null, term: null,
  other_positions: 0, holdings: 0, since: null,
})

// As the report ranks them; the third holder's other holdings put it first by "Also holds".
const HOLDERS = [holder(1, 'Master Trust Bank', 300_000, 0.3, 40), holder(2, 'Alpha Insurance', 200_000, 0.2, 5), holder(3, 'Custody Bank', 100_000, 0.1, 90)]
const OFFICERS = [officer('Taro Yamada', 'President', 61, 5_000), officer('Hanako Sato', 'Director', 55, null), officer('Ken Abe', 'Outside director', 70, 20_000)]
const OWNERSHIP: CompanyOwnership = {
  edinet_code: 'E1', company_name: 'Alpha Motor', company_name_ja: null, ticker: '10000', today: '2026-10-16', latest_period: '2026-03-31',
  stale: false, reports: 2, holder: null,
  shareholders: {
    doc_id: 'S100ANN1', period_end: '2026-03-31', as_of: '2026-03-31', edinet_url: 'https://disclosure2.edinet-fsa.go.jp/', top_ratio: 0.6, top_shares: 600_000,
    issued_shares: 1e6, treasury_shares: null, treasury_ratio: null, shareholders: 1200, distribution: [], rows: HOLDERS, voting: [],
    history: { periods: ['2025-03-31', '2026-03-31'], holders: HOLDERS.map(row => ({ ...row, ratios: [(row.ratio ?? 0) / 2, row.ratio] })) },
  },
  people: {
    doc_id: 'S100ANN1', period_end: '2026-03-31', as_of: '2026-06-25', edinet_url: 'https://disclosure2.edinet-fsa.go.jp/', proposed: false, source: 'tagged',
    men: 2, women: 1, outside: 1, average_age: 62, total_shares: 25_000, total_ratio: 0.025, rows: OFFICERS, departed: [], previous_period: null,
  },
}

function json(value: unknown) {
  return Promise.resolve(new Response(JSON.stringify(value), { status: 200, headers: { 'Content-Type': 'application/json' } }))
}

function renderPanel(element: ReactNode) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return render(<QueryClientProvider client={client}><MemoryRouter>{element}</MemoryRouter></QueryClientProvider>)
}

/** A cell of each body row, read without the screen-reader label of the holder's kind: the rank or name the order is read by. */
const order = (table: HTMLElement, cell = 0) => within(table).getAllByRole('row').slice(1).map(row => {
  const element = row.querySelectorAll('th, td')[cell]
  return (element.querySelector('a') ?? element).textContent
})

beforeEach(() => { vi.stubGlobal('fetch', vi.fn(() => json(OWNERSHIP))) })
afterEach(() => { cleanup(); vi.unstubAllGlobals() })

describe('ownership tables', () => {
  it('sorts the shareholders by a clicked column, then back to the report’s ranking', async () => {
    renderPanel(<ShareholdersPanel companyCode="E1" marketCap={null} />)
    const table = await screen.findByRole('table', { name: 'Major shareholders' })
    const header = (name: string) => within(table).getByRole('button', { name })
    expect(order(table)).toEqual(['1', '2', '3'])
    fireEvent.click(header('Shares'))
    expect(order(table)).toEqual(['1', '2', '3'])
    expect(within(table).getByRole('columnheader', { name: /Shares/ })).toHaveAttribute('aria-sort', 'descending')
    fireEvent.click(header('Shares'))
    expect(order(table)).toEqual(['3', '2', '1'])
    expect(within(table).getByRole('columnheader', { name: /Shares/ })).toHaveAttribute('aria-sort', 'ascending')
    fireEvent.click(header('Shares'))
    expect(order(table)).toEqual(['1', '2', '3'])
    fireEvent.click(header('Also holds'))
    expect(order(table)).toEqual(['3', '1', '2'])
    fireEvent.click(header('Shareholder'))
    expect(order(table, 1)).toEqual(['Alpha Insurance', 'Custody Bank', 'Master Trust Bank'])
  })

  it('sorts the shareholders by year in the history table', async () => {
    renderPanel(<ShareholdersPanel companyCode="E1" marketCap={null} />)
    fireEvent.click(await screen.findByRole('button', { name: /History/ }))
    const table = screen.getByRole('table', { name: 'Shareholders by year' })
    fireEvent.click(within(table).getByRole('button', { name: 'FY2025/3' }))
    expect(order(table)).toEqual(['Master Trust Bank', 'Alpha Insurance', 'Custody Bank'])
    fireEvent.click(within(table).getByRole('button', { name: 'FY2025/3' }))
    expect(order(table)).toEqual(['Custody Bank', 'Alpha Insurance', 'Master Trust Bank'])
  })

  it('sorts the officers by shares held, with those who hold none last', async () => {
    renderPanel(<PeoplePanel companyCode="E1" marketCap={null} />)
    const table = await screen.findByRole('table', { name: 'Related people' })
    const header = (name: string) => within(table).getByRole('button', { name })
    expect(order(table)).toEqual(['Taro Yamada', 'Hanako Sato', 'Ken Abe'])
    fireEvent.click(header('Shares held'))
    expect(order(table)).toEqual(['Ken Abe', 'Taro Yamada', 'Hanako Sato'])
    fireEvent.click(header('Shares held'))
    expect(order(table)).toEqual(['Taro Yamada', 'Ken Abe', 'Hanako Sato'])
    fireEvent.click(header('Shares held'))
    expect(order(table)).toEqual(['Taro Yamada', 'Hanako Sato', 'Ken Abe'])
    fireEvent.click(header('Age'))
    expect(order(table)).toEqual(['Hanako Sato', 'Taro Yamada', 'Ken Abe'])
  })
})
