import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { AuthContext, type AuthContextValue } from '../auth/authContext'
import PortfolioWorkspace from './PortfolioWorkspace'

// jsdom has no canvas; the charts are covered by the screenshots, not here.
vi.mock('react-chartjs-2', () => ({ Line: () => null, Bar: () => null }))

function json(value: unknown) {
  return Promise.resolve(new Response(JSON.stringify(value), { status: 200, headers: { 'Content-Type': 'application/json' } }))
}

const SERIES = Array.from({ length: 30 }, (_, index) => ({
  date: `2026-0${index < 20 ? 8 : 9}-${String((index % 20) + 1).padStart(2, '0')}`,
  cumulative_return: index * 0.002,
  drawdown: 0,
  value: 1000 + index * 2,
  invested: 1000,
  benchmark: index * 0.001,
}))

const requests: string[] = []

function stub() {
  requests.length = 0
  vi.stubGlobal('fetch', vi.fn((input: RequestInfo | URL) => {
    const path = String(input)
    requests.push(path)
    if (path.startsWith('/api/portfolio/display-currencies')) return json(['EUR', 'USD'])
    if (path.startsWith('/api/portfolio/benchmarks')) return json([{ ticker: 'VWCE', label: 'FTSE All-World', detail: 'accumulating', available: true }])
    if (path.startsWith('/api/portfolio/data-quality')) return json({ today: '2026-10-03', display_currency: 'EUR', valuation_date: '2026-10-02', first_date: '2020-12-14', days_behind: 1, last_transaction: '2026-07-10', holdings: [], large_moves: [], fx: { source: 'ECB euro reference rates', last_dates: {} }, risk_free: { kind: 'series', supported: true }, inflation: {}, issues: [{ level: 'warning', code: 'stale_prices', message: '1 holding(s) use old quotes.' }] })
    if (path.startsWith('/api/portfolio/date-range')) return json({ min_date: '2020-12-14', max_date: '2026-07-10' })
    if (path.startsWith('/api/portfolio/activity-summary')) return json({ by_activity: { TRADE: 2 } })
    if (path.startsWith('/api/portfolio/transactions')) return json([])
    if (path.startsWith('/api/portfolio/holdings/performance')) return json([{ symbol: 'AAA', asset_category: 'STK', currency: 'USD', market_value: 900, market_value_display: 900, is_open: true, performance: { name: 'Alpha', pnl_display: 100, cost_basis_display: 800 } }])
    if (path.startsWith('/api/portfolio/performance')) return json({ start_date: '2026-08-01', end_date: '2026-09-10', base_currency: 'EUR', total_return: 0.058, series: SERIES, period: { start: '2026-08-01', end: '2026-09-10', days: 40, years: 0.11, observations: 30, annualized: false }, monthly_returns: [], annual_returns: [], benchmark: { ticker: 'VWCE', available: true, total_return: 0.029 }, risk_free: { kind: 'series', currency: 'EUR' }, warnings: [] })
    if (path.startsWith('/api/portfolio/income')) return json({ currency: 'EUR', valuation_date: '2026-10-02', total_net: 85, payments: [{ date: '2026-07-10', symbol: 'AAA', type: 'Dividend', currency: 'USD', per_share: 1, shares: 100, gross_native: 100, tax_native: -15, net_native: 85, gross: 100, tax: -15, net: 85, withholding_rate: 0.15 }], companies: [{ symbol: 'AAA', name: 'Alpha', currency: 'USD', is_held: true, shares_held: 100, gross: 100, tax: -15, net: 85, withholding_rate: 0.15, share_of_income: 1, payments: 1, first_date: '2026-07-10', last_date: '2026-07-10', frequency: 1, ttm_net: 85, ttm_per_share: 1, latest_per_share: 1, latest_date: '2026-07-10', per_share_growth_1y: null, current_yield: 0.02, yield_on_cost: 0.025, annual: [] }] })
    if (path.startsWith('/api/portfolio/charts/')) return json({ labels: [], values: [], total: 0, currency: 'EUR' })
    return json({})
  }))
}

const AUTH: AuthContextValue = {
  user: { user_id: 'u-1', username: 'alice', role: 'member', status: 'active' },
  status: { mode: 'accounts', registration_open: false, bootstrap_required: false, password_min_length: 15 },
  loading: false,
  login: vi.fn(),
  register: vi.fn(),
  logout: vi.fn(),
}

function renderWorkspace() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return render(<QueryClientProvider client={client}><AuthContext.Provider value={AUTH}><MemoryRouter initialEntries={['/portfolio']}><PortfolioWorkspace /></MemoryRouter></AuthContext.Provider></QueryClientProvider>)
}

beforeEach(stub)
afterEach(() => {
  cleanup()
  vi.unstubAllGlobals()
  localStorage.clear()
})

const tab = (name: string) => [...screen.getByRole('navigation', { name: 'Portfolio sections' }).querySelectorAll('button')].find(button => button.title.startsWith(name))

describe('PortfolioWorkspace keyboard', () => {
  it('switches sections with 1 to 6 and remembers the choice', async () => {
    renderWorkspace()
    expect(await screen.findByText('Valued as of 2 Oct 2026')).toBeInTheDocument()

    fireEvent.keyDown(document.body, { key: '3' })
    expect(tab('Performance')).toHaveAttribute('aria-pressed', 'true')
    fireEvent.keyDown(document.body, { key: '6' })
    expect(tab('Data & method')).toHaveAttribute('aria-pressed', 'true')
    expect(localStorage.getItem('portfolio.tab')).toBe('"data"')
  })

  it('steps the period with - and + and asks for that window', async () => {
    renderWorkspace()
    await screen.findByText('Valued as of 2 Oct 2026')
    expect(screen.getByRole('button', { name: 'All' })).toHaveAttribute('aria-pressed', 'true')

    fireEvent.keyDown(document.body, { key: '+' })
    expect(screen.getByRole('button', { name: '5Y' })).toHaveAttribute('aria-pressed', 'true')
    await waitFor(() => expect(requests.some(path => path.startsWith('/api/portfolio/performance') && path.includes('start_date=2021-10-02'))).toBe(true))
    fireEvent.keyDown(document.body, { key: '-' })
    expect(screen.getByRole('button', { name: 'All' })).toHaveAttribute('aria-pressed', 'true')
  })

  it('focuses the currency and benchmark choices, and lists every shortcut with ?', async () => {
    renderWorkspace()
    await screen.findByText('Valued as of 2 Oct 2026')

    fireEvent.keyDown(document.body, { key: 'c' })
    expect(screen.getByRole('combobox', { name: 'Display currency' })).toHaveFocus()
    fireEvent.keyDown(document.activeElement!, { key: 'b' })
    // A focused select is a typing target, so page keys pause until it is left.
    expect(screen.getByRole('combobox', { name: 'Display currency' })).toHaveFocus()
    screen.getByRole('combobox', { name: 'Display currency' }).blur()
    fireEvent.keyDown(document.body, { key: 'b' })
    expect(screen.getByRole('combobox', { name: 'Benchmark' })).toHaveFocus()
    screen.getByRole('combobox', { name: 'Benchmark' }).blur()

    fireEvent.keyDown(document.body, { key: '?' })
    expect(screen.getByRole('dialog', { name: 'Keyboard shortcuts' })).toBeInTheDocument()
    expect(screen.getByText('Refresh market prices, then rebuild (operators)')).toBeInTheDocument()
  })

  it('shows the data check and hides price refreshing from members', async () => {
    renderWorkspace()
    expect(await screen.findByRole('button', { name: /1 data warning/ })).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /Refresh prices/ })).not.toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: /1 data warning/ }))
    expect(tab('Data & method')).toHaveAttribute('aria-pressed', 'true')
  })

  it('finds paying companies with F and clears the choice with X on the Income tab', async () => {
    localStorage.setItem('portfolio.incomeCompanies', JSON.stringify(['AAA']))
    renderWorkspace()
    await screen.findByText('Valued as of 2 Oct 2026')
    fireEvent.keyDown(document.body, { key: '4' })
    expect(await screen.findByRole('button', { name: 'Remove AAA' })).toBeInTheDocument()
    expect(screen.getByText('AAA · Alpha')).toBeInTheDocument()

    fireEvent.keyDown(document.body, { key: 'f' })
    expect(screen.getByRole('combobox', { name: 'Filter companies' })).toHaveFocus()
    screen.getByRole('combobox', { name: 'Filter companies' }).blur()
    fireEvent.keyDown(document.body, { key: 'x' })
    expect(screen.queryByRole('button', { name: 'Remove AAA' })).not.toBeInTheDocument()
    expect(localStorage.getItem('portfolio.incomeCompanies')).toBe('[]')
  })

  it('asks for an import when there are no records, as after clearing everything', async () => {
    const backend = globalThis.fetch
    vi.stubGlobal('fetch', vi.fn((input: RequestInfo | URL, init?: RequestInit) => String(input).startsWith('/api/portfolio/activity-summary') ? json({ by_activity: {} }) : backend(input, init)))
    renderWorkspace()
    expect(await screen.findByText('Import IBKR Flex Query XML')).toBeInTheDocument()
    expect(screen.queryByRole('navigation', { name: 'Portfolio sections' })).not.toBeInTheDocument()
  })
})
