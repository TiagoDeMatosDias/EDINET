import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { MemoryRouter, useLocation } from 'react-router-dom'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import ComparisonPage from './ComparisonPage'
import { GlobalHotkeys } from '../../components/GlobalHotkeys'

// jsdom has no canvas; the charts are covered by the screenshots, not here.
vi.mock('react-chartjs-2', () => ({ Line: () => null, Bar: () => null, Scatter: () => null }))

function json(value: unknown) {
  return Promise.resolve(new Response(JSON.stringify(value), { status: 200, headers: { 'Content-Type': 'application/json' } }))
}

const DEFINITIONS = {
  PERatio: { label: 'P/E', group: 'Valuation', direction: 'lower' },
  ReturnOnEquity: { label: 'ROE', group: 'Quality', direction: 'higher', format: 'percent' },
  Revenue: { label: 'Revenue', group: 'Income', format: 'money', currency: 'reporting' },
  EnterpriseValueToSales: { label: 'EV/Sales', group: 'Valuation', direction: 'lower' },
}
const STANDARD = ['PERatio', 'EnterpriseValueToSales', 'ReturnOnEquity', 'Revenue']

const COMPANIES: Record<string, { name: string; metrics: Record<string, number | null> }> = {
  E1: { name: 'Alpha Motor Corporation', metrics: { PERatio: 11, EnterpriseValueToSales: null, ReturnOnEquity: 0.18, Revenue: 18e12 } },
  E2: { name: 'Beta Motor Co., Ltd.', metrics: { PERatio: 7, EnterpriseValueToSales: null, ReturnOnEquity: -0.05, Revenue: 12e12 } },
  E3: { name: 'Gamma Industries', metrics: { PERatio: 15, EnterpriseValueToSales: null, ReturnOnEquity: 0.1, Revenue: 4e12 } },
  E4: { name: 'Delta Parts', metrics: { PERatio: 9, EnterpriseValueToSales: null, ReturnOnEquity: 0.08, Revenue: 2e12 } },
}

const snapshots: Array<{ company_codes: string[]; metrics: string[] }> = []

function stub() {
  snapshots.length = 0
  vi.stubGlobal('fetch', vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
    const path = String(input)
    if (path.startsWith('/api/comparison/metrics')) return json({ tables: { IncomeStatement: ['Gross profit', 'Net sales'] }, definitions: DEFINITIONS, default_metrics: STANDARD })
    if (path.startsWith('/api/comparison/snapshot')) {
      const body = JSON.parse(String(init?.body)) as { company_codes: string[]; metrics: string[] }
      snapshots.push(body)
      return json({
        companies: body.company_codes.map(code => ({
          company_code: code,
          company: { company_name: COMPANIES[code].name, ticker: `${code.slice(1)}0`, industry: 'Transportation Equipments' },
          metrics: { ...COMPANIES[code].metrics, ...Object.fromEntries(body.metrics.filter(metric => metric.includes('.')).map(metric => [metric, 1e12])) },
          percentiles: {},
          market: { price_currency: 'JPY' },
          reporting_currency: 'JPY',
          period_end: '2026-03-31',
        })),
        requested: body.company_codes,
        missing: [],
        metrics: body.metrics,
        metric_definitions: {},
      })
    }
    if (path.startsWith('/api/comparison/peers')) {
      const selected = new URL(path, 'http://x').searchParams.get('codes')?.split(',') ?? []
      return json({
        company_codes: selected,
        industries: [{ industry: 'Transportation Equipments', candidates: 3 }],
        total: 3,
        peers: ['E3', 'E4', 'E2'].filter(code => !selected.includes(code)).map(code => ({ company_code: code, ticker: `${code.slice(1)}0`, company_name: COMPANIES[code].name, industry: 'Transportation Equipments', MarketCap: 1e12, PERatio: 10, PriceToBook: 1, ReturnOnEquity: 0.1, DividendsYield: 0.02, nearest_code: selected[0], size_ratio: 0.5, price_currency: 'JPY' })),
      })
    }
    if (path.startsWith('/api/research/comparison-templates')) return json({ templates: [{ template_id: 't-1', name: 'Autos', companies_json: '["E1","E2"]', metrics_json: '["PERatio","ReturnOnEquity"]' }] })
    if (path.startsWith('/api/research/recent-work')) return json({ items: [
      { work_id: 'w-1', kind: 'comparison', title: 'Comparison · Alpha vs Gamma', subtitle: '2 companies · 21 metrics', href: '/compare?companies=E1,E3', occurred_at: '2026-10-02T09:00:00+00:00' },
      { work_id: 'w-2', kind: 'company', title: 'Alpha', href: '/analyze/E1', occurred_at: '2026-10-02T09:00:00+00:00' },
    ] })
    if (path.startsWith('/api/comparison/trends')) return json({ companies: [], metrics: ['Revenue'], metric_definitions: { Revenue: DEFINITIONS.Revenue } })
    if (path.startsWith('/api/security/search')) {
      const query = new URL(path, 'http://x').searchParams.get('q') ?? ''
      return json({ results: COMPANIES[query] ? [{ company_code: query, ticker: `${query.slice(1)}0`, company_name: COMPANIES[query].name, industry: 'Transportation Equipments' }] : [] })
    }
    return json({})
  }))
}

function Location() {
  const location = useLocation()
  return <output data-testid="location">{location.search}</output>
}

function renderPage(search: string) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return render(<QueryClientProvider client={client}><MemoryRouter initialEntries={[`/compare${search}`]}><ComparisonPage /><GlobalHotkeys isAdmin={false} /><Location /></MemoryRouter></QueryClientProvider>)
}

const table = () => screen.getByRole('table', { name: 'Comparison by metric' })
const columnNames = () => within(table().querySelector('thead')!).getAllByRole('columnheader').slice(1).map(cell => cell.textContent)
const cursorRow = () => within(table()).getAllByRole('row').find(row => row.getAttribute('aria-selected') === 'true')

beforeEach(() => { stub(); window.localStorage.clear() })
afterEach(() => { cleanup(); vi.unstubAllGlobals() })

describe('ComparisonPage', () => {
  it('compares as soon as two companies are chosen and ranks each row', async () => {
    renderPage('?companies=E1,E2')
    await waitFor(() => expect(table()).toBeInTheDocument())
    expect(snapshots[0]).toEqual({ company_codes: ['E1', 'E2'], metrics: STANDARD })
    expect(columnNames()).toEqual([expect.stringContaining('Alpha Motor'), expect.stringContaining('Beta Motor')])
    // Lower P/E is better, higher ROE is better.
    expect(within(table()).getByText('7')).toHaveClass('is-best')
    expect(within(table()).getByText('18.0%')).toHaveClass('is-best')
    // EV/Sales has no values, so it is hidden until asked for.
    expect(within(table()).queryByText('EV/Sales')).not.toBeInTheDocument()
    fireEvent.click(screen.getByRole('checkbox', { name: /Hide empty/ }))
    expect(within(table()).getByText('EV/Sales')).toBeInTheDocument()
  })

  it('moves through the table, sorts, and hides metrics from the keyboard', async () => {
    renderPage('?companies=E1,E2,E3')
    await waitFor(() => expect(table()).toBeInTheDocument())
    expect(cursorRow()).toHaveTextContent('P/E')
    fireEvent.keyDown(window, { key: 'j' })
    expect(cursorRow()).toHaveTextContent('ROE')
    expect(cursorRow()).toHaveFocus()
    fireEvent.keyDown(document.activeElement!, { key: 'ArrowUp' })
    expect(cursorRow()).toHaveTextContent('P/E')

    fireEvent.keyDown(document.activeElement!, { key: 's' })
    expect(columnNames()).toEqual([expect.stringContaining('Beta'), expect.stringContaining('Alpha'), expect.stringContaining('Gamma'), 'Median'])
    expect(screen.getByRole('button', { name: 'Original order' })).toBeInTheDocument()

    fireEvent.keyDown(document.activeElement!, { key: 'x' })
    expect(within(table()).queryByText('P/E')).not.toBeInTheDocument()
    expect(cursorRow()).toHaveTextContent('ROE')
    expect(screen.getByTestId('location').textContent).toBe('?companies=E1%2CE2%2CE3&hide=PERatio')
    // Showing or hiding a standard metric needs no new request.
    expect(snapshots).toHaveLength(1)
  })

  it('reorders and removes companies from the keyboard', async () => {
    renderPage('?companies=E1,E2,E3')
    await waitFor(() => expect(table()).toBeInTheDocument())
    fireEvent.keyDown(window, { key: 'l' })
    fireEvent.keyDown(window, { key: '[' })
    expect(screen.getByTestId('location').textContent).toBe('?companies=E2%2CE1%2CE3')
    fireEvent.keyDown(window, { key: 'X', shiftKey: true })
    expect(screen.getByTestId('location').textContent).toBe('?companies=E1%2CE3')
  })

  it('suggests peers and adds them', async () => {
    renderPage('?companies=E1,E2')
    const add = await screen.findByRole('button', { name: 'Add Gamma Industries' })
    fireEvent.click(add)
    await waitFor(() => expect(snapshots.at(-1)?.company_codes).toEqual(['E1', 'E2', 'E3']))
    // P goes to the list, Enter adds the focused peer.
    await waitFor(() => expect(screen.queryByRole('button', { name: 'Add Gamma Industries' })).not.toBeInTheDocument())
    fireEvent.keyDown(window, { key: 'p' })
    expect(document.activeElement).toHaveTextContent('Delta Parts')
    fireEvent.keyDown(document.activeElement!, { key: 'Enter' })
    await waitFor(() => expect(snapshots.at(-1)?.company_codes).toEqual(['E1', 'E2', 'E3', 'E4']))
  })

  it('keeps the focus in the peer list so Enter adds one peer after another', async () => {
    renderPage('?companies=E1')
    await screen.findByRole('button', { name: 'Add Gamma Industries' })
    fireEvent.keyDown(window, { key: 'p' })
    expect(document.activeElement).toHaveTextContent('Gamma Industries')
    fireEvent.keyDown(document.activeElement!, { key: 'Enter' })
    await waitFor(() => expect(document.activeElement).toHaveTextContent('Delta Parts'))
    fireEvent.keyDown(document.activeElement!, { key: 'Enter' })
    await waitFor(() => expect(screen.getByTestId('location').textContent).toBe('?companies=E1%2CE3%2CE4'))
  })

  it('names a single company sent from Analysis and offers its closest peers', async () => {
    renderPage('?companies=E1')
    expect(await screen.findByRole('link', { name: 'Alpha Motor Corporation' })).toBeInTheDocument()
    expect(screen.getByText(/Add one more company/)).toBeInTheDocument()
    fireEvent.click(await screen.findByRole('button', { name: /Add 3 closest/ }))
    await waitFor(() => expect(snapshots.at(-1)?.company_codes).toEqual(['E1', 'E3', 'E4', 'E2']))
  })

  it('toggles standard metrics and searches statement columns', async () => {
    renderPage('?companies=E1,E2')
    await waitFor(() => expect(table()).toBeInTheDocument())
    const roe = screen.getByRole('button', { name: 'ROE', pressed: true })
    fireEvent.click(roe)
    expect(screen.getByRole('button', { name: 'ROE', pressed: false })).toBeInTheDocument()
    expect(within(table()).queryByText('ROE')).not.toBeInTheDocument()

    fireEvent.keyDown(window, { key: 'm' })
    const search = screen.getByRole('combobox', { name: 'Add a metric' })
    expect(search).toHaveFocus()
    fireEvent.change(search, { target: { value: 'gross' } })
    fireEvent.keyDown(search, { key: 'Enter' })
    await waitFor(() => expect(snapshots.at(-1)?.metrics).toEqual([...STANDARD, 'IncomeStatement.Gross profit']))
    expect(await within(table()).findByText('Gross profit')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Remove metric Gross profit' })).toBeInTheDocument()
    expect(screen.getByTestId('location').textContent).toBe('?companies=E1%2CE2&hide=ReturnOnEquity&add=IncomeStatement.Gross+profit')
  })

  it('opens a link with hidden and added metrics', async () => {
    renderPage('?companies=E1,E2&hide=PERatio,Revenue&add=IncomeStatement.Net+sales')
    await waitFor(() => expect(table()).toBeInTheDocument())
    expect(snapshots[0].metrics).toEqual([...STANDARD, 'IncomeStatement.Net sales'])
    expect(within(table()).queryByText('P/E')).not.toBeInTheDocument()
    expect(within(table()).getByText('ROE')).toBeInTheDocument()
    expect(within(table()).getByText('Net sales')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'P/E', pressed: false })).toBeInTheDocument()
  })

  it('starts from a saved or recent comparison', async () => {
    renderPage('')
    expect(await screen.findByRole('link', { name: /Alpha vs Gamma/ })).toHaveAttribute('href', '/compare?companies=E1,E3')
    expect(screen.queryByRole('link', { name: /^Alpha$/ })).not.toBeInTheDocument()
    await waitFor(() => expect(screen.getByRole('button', { name: 'P/E', pressed: true })).toBeInTheDocument())
    fireEvent.click(screen.getByRole('button', { name: /Autos/ }))
    expect(screen.getByTestId('location').textContent).toBe('?companies=E1%2CE2&hide=EnterpriseValueToSales%2CRevenue')
    await waitFor(() => expect(table()).toBeInTheDocument())
    expect(within(table()).queryByText('Revenue')).not.toBeInTheDocument()
  })

  it('lists the page shortcuts on ?', async () => {
    renderPage('?companies=E1,E2')
    await waitFor(() => expect(table()).toBeInTheDocument())
    fireEvent.keyDown(window, { key: '?' })
    const dialog = screen.getByRole('dialog', { name: 'Keyboard shortcuts' })
    expect(within(dialog).getByText('Sort companies by the metric, best first')).toBeInTheDocument()
    expect(within(dialog).getByText('Add the closest peer')).toBeInTheDocument()
  })
})
