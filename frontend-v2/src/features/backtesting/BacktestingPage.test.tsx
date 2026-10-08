import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { MemoryRouter, Route, Routes, useLocation } from 'react-router-dom'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { calendarYears, chainRuns, fanBands, portfolioPayload, summarize, type RunRow } from './backtestModel'
import BacktestingPage from './BacktestingPage'

vi.mock('react-chartjs-2', () => ({ Line: () => null, Bar: () => null }))

const calls: Array<{ method: string; path: string; body?: Record<string, unknown> }> = []
let jobPolls = 0

const SINGLE = {
  id: '20261004_100000_aaaaaaaa',
  status: 'complete',
  summary: { total_return: 0.25, annualized_return: 0.118, benchmark_total_return: 0.1, benchmark_annualized_return: 0.049, excess_annualized_return: 0.069, information_ratio: 0.8, tracking_error: 0.09, max_drawdown: -0.2, start_date: '2024-01-04', end_date: '2025-12-30', tickers: ['7203'], warnings: [] },
  chart_data: { cumulative: [{ date: '2024-01-04', portfolio: 0, benchmark: 0 }, { date: '2024-12-30', portfolio: 0.1, benchmark: 0.05 }, { date: '2025-12-30', portfolio: 0.25, benchmark: 0.1 }], drawdown: [], decomposition: [] },
  per_company: [{ Ticker: '7203', weight: 1, price_return: 0.2, dividend_return: 0.05, total_return: 0.25, weighted_total: 0.25 }],
  warnings: ['Benchmark covers only part of the period'],
}

function run(period: string, duration: string, annual: number, bench: number, status: RunRow['status'] = 'ok'): RunRow {
  return { period, weighting: 'equal', duration, status, companies: 3, annualized_return: annual, total_return: annual, benchmark_annualized_return: bench, benchmark_total_return: bench, excess_annualized_return: annual - bench, beat_benchmark: annual > bench, sharpe_ratio: 1, max_drawdown: -0.1, start: period, end: `${Number(period.slice(0, 4)) + 1}${period.slice(4)}`, requested_end: `${Number(period.slice(0, 4)) + 1}${period.slice(4)}` }
}

const ROLLING = {
  id: '20261004_110000_bbbbbbbb',
  status: 'complete',
  kind: 'rolling',
  aggregate: { complete_backtests: 3 },
  config: { cadence: 'yearly', max_companies: 10, ranking_algorithm: 'weighted_percentile', benchmark_ticker: 'TPX' },
  runs: [run('2019-01-01', '1yr', 0.12, 0.05), run('2020-01-01', '1yr', -0.04, 0.02), run('2021-01-01', '1yr', 0.2, 0.1), run('2025-06-01', '1yr', 0.01, 0.0, 'truncated')],
  paths: { '2019-01-01|equal|1yr': [['2019-01-31', 0.01, 0.0], ['2019-12-31', 0.12, 0.05]] },
  period_holdings: { '2019-01-01': ['7203', '6758'] },
  run_holdings: {
    '2019-01-01|equal|1yr': [
      { ticker: '7203', weight: 0.5, price_return: 0.18, dividend_return: 0.03, total_return: 0.21, weighted_total: 0.105 },
      { ticker: '6758', weight: 0.5, price_return: 0.0, dividend_return: 0.01, total_return: 0.01, weighted_total: 0.005 },
    ],
    '2021-01-01|equal|1yr': [{ ticker: '7203', weight: 1, price_return: 0.18, dividend_return: 0.02, total_return: 0.2, weighted_total: 0.2 }],
  },
  names: { 7203: 'TOYOTA MOTOR CORPORATION', 6758: 'SONY GROUP CORPORATION' },
}

const DETAIL = {
  id: '20261004_100000_aaaaaaaa', base_currency: 'JPY', initial_capital: 1_000_000,
  dates: ['2024-01-04', '2025-12-30'], portfolio: [0, 0.25],
  holdings: [{ ticker: '7203', name: 'TOYOTA MOTOR CORPORATION', currency: 'JPY', weight: 1, start_price: 2500, end_price: 3000, price_return: 0.2, dividend_return: 0.05, total_return: 0.25, contribution: 0.25, capital_invested: 1_000_000, shares: 400, market_value: 1_200_000, dividends_received: 50_000, growth: [1, 1.25],
    years: [{ year: 2024, start_date: '2024-01-04', end_date: '2024-12-30', start_price: 2500, end_price: 2750, price_return: 0.1, dividend_return: 0.02, total_return: 0.12, contribution: 0.12, dividend_per_share: 50, dividends_received: 20000, start_weight: 1 }],
    dividends: [{ ticker: '7203', record_date: '2024-03-31', credited_on: '2024-04-01', payment: 'final', per_share: 50, reported_per_share: 250, split_factor: 0.2, shares: 400, cash: 20000, currency: 'JPY' }] }],
  allocation: { holdings: { 7203: [1, 0.96] }, cash: [0, 0.04] },
  contribution: { 7203: [0, 0.25] },
  contribution_by_year: [{ year: 2024, 7203: 0.12 }],
  dividend_payments: [{ ticker: '7203', record_date: '2024-03-31', credited_on: '2024-04-01', payment: 'final', per_share: 50, reported_per_share: 250, split_factor: 0.2, shares: 400, cash: 20000, currency: 'JPY' }],
}

const SAVED = [
  { id: ROLLING.id, created: ROLLING.id, has_zip: true, kind: 'rolling', title: 'Rolling screen · yearly', subtitle: '1yr · top 10', headline: { mean_annualized_return: 0.093, win_rate: 0.67 } },
  { id: SINGLE.id, created: SINGLE.id, has_zip: true, kind: 'single', title: 'Backtest · 7203', subtitle: '2024 to 2025', headline: { annualized_return: 0.118, excess_return: 0.15 } },
]

function respond(value: unknown, status = 200) {
  return Promise.resolve(new Response(JSON.stringify(value), { status, headers: { 'Content-Type': 'application/json' } }))
}

beforeEach(() => {
  calls.length = 0
  jobPolls = 0
  localStorage.clear()
  vi.stubGlobal('fetch', vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
    const path = String(input)
    const method = init?.method ?? 'GET'
    calls.push({ method, path, body: init?.body ? JSON.parse(String(init.body)) as Record<string, unknown> : undefined })
    if (path === '/api/backtesting/benchmarks') return respond({ benchmarks: [{ ticker: 'TPX', label: 'TOPIX', detail: 'TOPIX index', available: true, first_date: '2015-01-05', last_date: '2026-05-15', currency: 'JPY' }, { ticker: 'IWDA', label: 'MSCI World', detail: 'iShares', available: false, first_date: null, last_date: null, currency: null }] })
    if (path === '/api/backtesting/base-currencies') return respond({ currencies: ['JPY', 'EUR'] })
    if (path === '/api/backtesting/available-tickers') return respond({ tickers: ['72030'] })
    if (path === '/api/backtesting/list') return respond({ backtests: SAVED })
    if (path === '/api/backtesting/rolling-jobs' && method === 'GET') return respond({ jobs: [] })
    if (path.startsWith('/api/backtesting/rolling-periods')) return respond({ periods: [], count: 7, estimated_backtests: 7 })
    if (path === '/api/backtesting/run') return respond(SINGLE)
    if (path === '/api/backtesting/rolling-jobs' && method === 'POST') return respond({ job_id: 'job1', status: 'queued', progress: {}, result_id: null, error: null, created_at: 1 }, 202)
    if (path === '/api/backtesting/rolling-jobs/job1') {
      jobPolls += 1
      return respond(jobPolls < 2
        ? { job_id: 'job1', status: 'running', progress: { completed_backtests: 2, total_backtests: 7, period_index: 1, total_periods: 7, phase: 'Screening at 2020-01-01…' }, result_id: null, error: null, created_at: 1 }
        : { job_id: 'job1', status: 'complete', progress: {}, result_id: ROLLING.id, error: null, created_at: 1 })
    }
    if (path === `/api/backtesting/result/${ROLLING.id}`) return respond(ROLLING)
    if (path === `/api/backtesting/result/${SINGLE.id}`) return respond(SINGLE)
    if (path === `/api/backtesting/result/${SINGLE.id}/detail`) return respond(DETAIL)
    return respond({ detail: 'not found' }, 404)
  }))
})
afterEach(() => { cleanup(); vi.unstubAllGlobals(); vi.useRealTimers() })

function Location() {
  return <output data-testid="location">{useLocation().search}</output>
}

function renderPage(path = '/backtest') {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return render(<QueryClientProvider client={client}><MemoryRouter initialEntries={[path]}><Routes><Route path="/backtest" element={<><BacktestingPage /><Location /></>} /></Routes></MemoryRouter></QueryClientProvider>)
}

describe('BacktestingPage', () => {
  it('runs a portfolio from the keyboard with weights as fractions and shows the comparison', async () => {
    renderPage('/backtest?symbol=7203')
    await screen.findByRole('option', { name: /TOPIX · TPX/ })
    expect(screen.getByRole('option', { name: /MSCI World · IWDA \(no prices\)/ })).toBeDisabled()
    fireEvent.keyDown(window, { key: 'r' })
    await screen.findByRole('region', { name: 'Backtest result' })
    const body = calls.find(call => call.path === '/api/backtesting/run')?.body
    expect(body).toMatchObject({ portfolio: { 7203: { mode: 'weight', value: 1 } }, benchmark_ticker: 'TPX', benchmark_mode: 'ticker' })
    expect(screen.getByText('Excess ann.').nextSibling).toHaveTextContent('+6.90%')
    expect(screen.getByText('Benchmark covers only part of the period')).toBeInTheDocument()
    expect(screen.getByTestId('location')).toHaveTextContent(`result=${SINGLE.id}`)

    // Drill down: the holding, then its dividends with the split factor.
    const holding = await screen.findByRole('row', { name: /7203 Toyota Motor Corporation/ })
    fireEvent.click(holding)
    const panel = screen.getByRole('region', { name: '7203 Toyota Motor Corporation detail' })
    expect(within(panel).getByText('Contribution', { selector: 'dt' }).nextSibling).toHaveTextContent('+25.00%')
    fireEvent.click(screen.getByRole('tab', { name: 'Dividends' }))
    expect(screen.getAllByText('0.200').length).toBeGreaterThan(0)
    expect(screen.getByRole('button', { name: /Share HTML/ })).toBeInTheDocument()
  })

  it('runs the screening draft as a background job with its ranking, then shows every run', async () => {
    localStorage.setItem('shade.screening.draft', JSON.stringify({
      name: 'Cheap quality', criteria: [{ id: 'x', table: 'Valuation', column: 'PER', operator: '<', value: 12 }], criteria_match: 'all', columns: ['CompanyInfo.Company_Ticker'],
      computed_columns: [{ name: 'Yield', formula_type: 'ratio' }], ranking_algorithm: 'weighted_percentile', ranking_rules: [{ column: 'Yield', direction: 'desc', weight: 1 }],
    }))
    renderPage()
    await screen.findByRole('option', { name: /TOPIX · TPX/ })
    fireEvent.keyDown(window, { key: '2' })
    expect(await screen.findByText('Cheap quality')).toBeInTheDocument()
    fireEvent.keyDown(window, { key: 'r' })
    await waitFor(() => expect(calls.some(call => call.method === 'POST' && call.path === '/api/backtesting/rolling-jobs')).toBe(true))
    const body = calls.find(call => call.method === 'POST' && call.path === '/api/backtesting/rolling-jobs')?.body
    expect(body).toMatchObject({ ranking_algorithm: 'weighted_percentile', ranking_rules: [{ column: 'Yield', direction: 'desc', weight: 1 }], computed_columns: [{ name: 'Yield', formula_type: 'ratio' }], benchmark_ticker: 'TPX', cadence: 'quarterly' })
    expect((body?.criteria as Array<Record<string, unknown>>)[0]).not.toHaveProperty('id')

    const result = await screen.findByRole('region', { name: 'Backtest set result' }, { timeout: 5000 })
    expect(within(result).getByText('Complete runs').nextSibling).toHaveTextContent('3')
    expect(within(result).getByText(/1 runs end before their holding period/)).toBeInTheDocument()
    expect(within(result).getByText('Beat benchmark').nextSibling).toHaveTextContent('67%')

    for (let step = 0; step < 4; step += 1) fireEvent.keyDown(window, { key: ']' })
    // Companies: how each company did across the runs it was held in, then its runs.
    expect(within(result).getByRole('tab', { name: 'Companies' })).toHaveAttribute('aria-selected', 'true')
    const company = within(within(result).getByRole('tabpanel')).getByRole('row', { name: /7203 Toyota Motor Corporation/ })
    expect(company).toHaveTextContent('2 of 3')
    fireEvent.click(company)
    fireEvent.click(within(within(result).getByRole('tabpanel')).getByRole('row', { name: /Run from 2021-01/ }))
    expect(await within(result).findByRole('complementary', { name: 'Selected run' })).toHaveTextContent('2021-01')
    fireEvent.keyDown(window, { key: 'Escape' })

    fireEvent.keyDown(window, { key: ']' })
    expect(within(result).getByRole('tab', { name: 'Runs' })).toHaveAttribute('aria-selected', 'true')
    const rows = within(within(result).getByRole('tabpanel')).getAllByRole('row').slice(1)
    rows[0].focus()
    fireEvent.keyDown(rows[0], { key: 'Enter' })
    const detail = await within(result).findByRole('complementary', { name: 'Selected run' })
    // The run's holdings, by what each added to its return.
    expect(within(detail).getByRole('link', { name: '6758 Sony Group Corporation' })).toHaveAttribute('href', '/analyze?ticker=6758&from=backtest')
    expect(within(detail).getByText('+10.50%')).toBeInTheDocument()
    fireEvent.keyDown(window, { key: 'Escape' })
    expect(within(result).queryByRole('complementary', { name: 'Selected run' })).not.toBeInTheDocument()
  })

  it('walks the saved list with J/K and opens a result with Enter', async () => {
    renderPage()
    await screen.findByText('Rolling screen · yearly')
    fireEvent.keyDown(window, { key: 'l' })
    const rows = within(screen.getByRole('region', { name: /Saved results/ })).getAllByRole('row').slice(1)
    expect(rows[0]).toHaveFocus()
    fireEvent.keyDown(rows[0], { key: 'j' })
    expect(rows[1]).toHaveFocus()
    fireEvent.keyDown(rows[1], { key: 'Enter' })
    await screen.findByRole('region', { name: 'Backtest result' })
    expect(screen.getByTestId('location')).toHaveTextContent(`result=${SINGLE.id}`)
  })
})

describe('backtest model', () => {
  it('turns percent weights into fractions and merges repeated tickers', () => {
    expect(portfolioPayload([
      { id: '1', ticker: '7203', mode: 'weight', value: 30 },
      { id: '2', ticker: ' 7203 ', mode: 'weight', value: 20 },
      { id: '3', ticker: '6758', mode: 'shares', value: 100 },
      { id: '4', ticker: '', mode: 'weight', value: 50 },
    ])).toEqual({ 7203: { mode: 'weight', value: 0.5 }, 6758: { mode: 'shares', value: 100 } })
  })

  it('summarizes complete runs only and chains back-to-back holds', () => {
    const rows = ROLLING.runs
    expect(summarize(rows)).toMatchObject({ count: 3, winRate: 2 / 3 })
    expect(summarize(rows).mean).toBeCloseTo(0.0933, 3)
    const chain = chainRuns(rows)
    expect(chain.map(point => point.period)).toEqual(['2019-01-01', '2019-01-01', '2020-01-01', '2021-01-01', '2025-06-01'])
    expect(chain[3].portfolio).toBeCloseTo(1.12 * 0.96 * 1.2, 6)
  })

  it('computes calendar-year returns and percentile bands', () => {
    expect(calendarYears(SINGLE.chart_data.cumulative).map(row => [row.year, Number(row.portfolio?.toFixed(4)), Number(row.benchmark?.toFixed(4))])).toEqual([['2024', 0.1, 0.05], ['2025', 0.1364, 0.0476]])
    const paths = [0.1, 0.2, 0.3, 0.4].map(end => [['2020-01-31', 0, 0], ['2020-02-29', end, 0.05]] as Array<[string, number, number]>)
    const bands = fanBands(paths)
    expect(bands[1]).toMatchObject({ month: 1, n: 4 })
    expect(bands[1].p50).toBeCloseTo(1.25, 6)
  })
})
