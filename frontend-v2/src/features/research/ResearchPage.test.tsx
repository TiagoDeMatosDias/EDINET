import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { MemoryRouter, useLocation } from 'react-router-dom'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { addDays, blackScholes } from './pricing/optionsModel'
import { localToday } from './researchModel'
import ResearchPage from './ResearchPage'
import { GlobalHotkeys } from '../../components/GlobalHotkeys'

// jsdom has no canvas; the charts are covered by the screenshots, not here.
vi.mock('react-chartjs-2', () => ({ Line: () => null, Bar: () => null }))

function json(value: unknown, status = 200) {
  return Promise.resolve(new Response(JSON.stringify(value), { status, headers: { 'Content-Type': 'application/json' } }))
}

const DEFINITIONS = {
  LatestPrice: { label: 'Price', group: 'Market', format: 'money', currency: 'price' },
  PERatio: { label: 'P/E', group: 'Valuation', direction: 'lower' },
}

const BOOK = {
  companies: [
    { company_code: 'E1', company_name: 'Alpha Motor', ticker: '10000', industry: 'Autos', tags: ['Autos', 'Favorite'], thesis_status: 'watch', thesis: 'Hybrid lead', target_value: 3000, target_currency: 'JPY', review_on: '2020-01-01', note_count: 1, alert_count: 1, alerts_triggered: 1, updated_at: '2026-10-02T10:00:00+00:00', price_currency: 'JPY', LatestPrice: 2500, PERatio: 10, DividendsYield: 0.03 },
    { company_code: 'MO', company_name: 'ALTRIA GROUP INC', ticker: 'MO', kind: 'security', position: 'open', tags: ['Open position'], thesis_status: null, note_count: 0, alert_count: 0, alerts_triggered: 0, updated_at: '2026-09-30T10:00:00+00:00', price_currency: 'USD', LatestPrice: 67.35 },
    { company_code: 'E2', company_name: 'Beta Bank', ticker: '20000', industry: 'Banks', tags: ['Yield'], thesis_status: null, note_count: 0, alert_count: 0, alerts_triggered: 0, updated_at: '2026-10-01T10:00:00+00:00', price_currency: 'JPY', LatestPrice: 800, PERatio: 8, DividendsYield: 0.045 },
  ],
  tags: [{ name: 'Autos', member_count: 1 }, { name: 'Favorite', member_count: 1 }, { name: 'Open position', member_count: 1 }, { name: 'Yield', member_count: 1 }],
  alerts: [{ alert_id: 'a1', name: 'P/E < 12', edinet_code: 'E1', company_name: 'Alpha Motor', metric: 'PERatio', operator: '<', value: 12, enabled: true, current_value: 10, triggered: true, price_currency: 'JPY', created_at: '2026-10-01T00:00:00+00:00' }],
  metric_definitions: DEFINITIONS,
}

const NOTES = [{ note_id: 'n1', title: 'Margins', body: 'Margins\nHeld up despite the yen.', edinet_code: 'E1', version: 1, updated_at: '2026-10-02T10:00:00+00:00' }]

const PRICING = {
  company: { company_code: 'E1', company_name: 'Alpha Motor', ticker: '10000', industry: 'Autos' },
  currency: { price: 'JPY', reporting: 'JPY' },
  spot: 100,
  price_date: '2026-10-02',
  dividend_yield: 0,
  market_cap: 1500,
  volatility: { estimates: [{ window: '1M', days: 21, value: 0.3 }, { window: '1Y', days: 252, value: 0.2 }], history: [], observations: 800 },
  credit: {
    period: '2026-03-31',
    lines: { Revenue: 1200, OperatingIncome: 100, NetIncome: 60, TotalAssets: 1000, TotalEquity: 600, TotalLiabilities: 400, CurrentAssets: 500, CurrentLiabilities: 300, RetainedEarnings: 300, InterestExpense: 10, Cash: 50 },
    debt: { Bonds: 200 },
    debt_total: 200,
    previous_debt_total: 180,
    interest_coverage: 10,
    cost_of_debt: 0.0526,
  },
  financial: false,
}

const calls: Array<{ method: string; path: string; body?: unknown }> = []

function stub() {
  calls.length = 0
  vi.stubGlobal('fetch', vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
    const path = String(input)
    const method = init?.method ?? 'GET'
    const body = init?.body ? JSON.parse(String(init.body)) as unknown : undefined
    calls.push({ method, path, body })
    if (path === '/api/research/book') return json(BOOK)
    if (path.startsWith('/api/research/notes')) {
      if (method !== 'GET') return json({ note_id: 'n2' }, method === 'POST' ? 201 : 200)
      const code = new URL(path, 'http://x').searchParams.get('edinet_code')
      return json({ notes: NOTES.filter(note => !code || note.edinet_code === code) })
    }
    if (path.startsWith('/api/research/companies/')) {
      const code = path.split('/').pop()
      const record = code === 'E1' ? { edinet_code: 'E1', thesis_status: 'watch', thesis: 'Hybrid lead', target_value: 3000, target_currency: 'JPY', review_on: '2020-01-01', version: 2 } : { edinet_code: code, version: 0 }
      return json(method === 'PATCH' ? { ...record, ...(body as object) } : record)
    }
    if (path.startsWith('/api/research/tags/')) return json({ tags: (body as { tags: string[] }).tags.map(tag => ({ tag })) })
    if (path === '/api/tags') return json({ tags: BOOK.tags })
    if (path.startsWith('/api/tags/')) return json({ tags: path.endsWith('E1') ? ['Autos', 'Favorite'] : path.endsWith('MO') ? ['Open position'] : ['Yield'] })
    if (path.startsWith('/api/research/alerts')) return json({}, method === 'POST' ? 201 : 200)
    if (path.startsWith('/api/research/pricing/')) return json(PRICING)
    if (path.startsWith('/api/security/search')) return json({ results: path.includes('q=alpha') ? [{ company_code: 'E1', company_name: 'Alpha Motor', ticker: '10000' }] : [] })
    return json({})
  }))
}

function Location() {
  return <output data-testid="location">{useLocation().search}</output>
}

function renderPage(search = '') {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return render(<QueryClientProvider client={client}><MemoryRouter initialEntries={[`/research${search}`]}><ResearchPage /><GlobalHotkeys isAdmin={false} /><Location /></MemoryRouter></QueryClientProvider>)
}

const location = () => screen.getByTestId('location').textContent
const table = () => screen.getByRole('table', { name: 'Companies in your research' })
const press = (key: string, options: Partial<KeyboardEventInit> = {}) => fireEvent.keyDown(window, { key, ...options })
const lastCall = (method: string, prefix: string) => [...calls].reverse().find(call => call.method === method && call.path.startsWith(prefix))

beforeEach(() => { stub(); window.localStorage.clear() })
afterEach(() => { cleanup(); vi.unstubAllGlobals() })

describe('ResearchPage', () => {
  it('lists followed companies with their research and moves through them from the keyboard', async () => {
    renderPage()
    await waitFor(() => expect(table()).toBeInTheDocument())
    const rows = within(table()).getAllByRole('row').slice(1)
    expect(rows[0]).toHaveTextContent('Alpha Motor')
    expect(rows[0]).toHaveTextContent('Watch')
    expect(rows[0]).toHaveTextContent('+20.0%')
    expect(rows[0]).toHaveAttribute('aria-selected', 'true')
    expect(screen.getByRole('heading', { name: 'Alpha Motor' })).toBeInTheDocument()
    expect(screen.getByText(/1 alert triggered · 1 review due/)).toBeInTheDocument()

    press('j')
    expect(location()).toBe('?company=E2')
    await waitFor(() => expect(screen.getByRole('heading', { name: 'Beta Bank' })).toBeInTheDocument())
    expect(within(table()).getAllByRole('row')[2]).toHaveFocus()
  })

  it('filters by tag with [ and ] and by status', async () => {
    renderPage()
    await waitFor(() => expect(table()).toBeInTheDocument())
    press(']')
    expect(location()).toBe('?tag=Open+position')
    expect(within(table()).getAllByRole('row')).toHaveLength(2)
    press(']')
    expect(location()).toBe('?tag=Autos')
    press('[')
    press('[')
    expect(location()).toBe('')
    fireEvent.click(within(screen.getByRole('group', { name: 'Filter by status' })).getByRole('button', { name: 'Alert triggered' }))
    expect(location()).toBe('?status=alerts')
    expect(within(table()).getAllByRole('row')).toHaveLength(2)
  })

  it('saves the status, thesis, tags, and notes from the shared research panel', async () => {
    renderPage('?company=E1')
    const status = await screen.findByRole('group', { name: 'Thesis status' })
    await waitFor(() => expect(within(status).getByRole('button', { name: 'Watch' })).toHaveAttribute('aria-pressed', 'true'))

    fireEvent.click(within(status).getByRole('button', { name: 'Buy' }))
    await waitFor(() => expect(lastCall('PATCH', '/api/research/companies/E1')?.body).toEqual({ thesis_status: 'buy', target_value: 3000, target_currency: 'JPY', review_on: '2020-01-01', thesis: 'Hybrid lead' }))

    press('e')
    const thesis = screen.getByRole('textbox', { name: /Thesis/ })
    expect(thesis).toHaveFocus()
    fireEvent.change(thesis, { target: { value: 'Hybrid lead and pricing power' } })
    fireEvent.keyDown(thesis, { key: 'Enter', ctrlKey: true })
    await waitFor(() => expect(lastCall('PATCH', '/api/research/companies/E1')?.body).toMatchObject({ thesis: 'Hybrid lead and pricing power' }))

    fireEvent.click(document.body)
    press('t')
    const tag = screen.getByRole('combobox', { name: 'Add a tag' })
    expect(tag).toHaveFocus()
    fireEvent.change(tag, { target: { value: 'Quality' } })
    fireEvent.submit(tag.closest('form')!)
    await waitFor(() => expect(lastCall('PUT', '/api/research/tags/E1')?.body).toEqual({ tags: ['Autos', 'Favorite', 'Quality'] }))

    fireEvent.click(document.body)
    press('n')
    const note = screen.getByRole('textbox', { name: 'New note' })
    expect(note).toHaveFocus()
    fireEvent.change(note, { target: { value: 'Q1 margins\nHeld up.' } })
    fireEvent.keyDown(note, { key: 'Enter', ctrlKey: true })
    await waitFor(() => expect(lastCall('POST', '/api/research/notes')?.body).toEqual({ title: 'Q1 margins', body: 'Q1 margins\nHeld up.', edinet_code: 'E1' }))
    expect(within(screen.getByRole('region', { name: 'Alerts' })).getByText('Triggered ·').parentElement).toHaveTextContent('P/E < 12')
  })

  it('deletes a note with X pressed twice on the Notes tab', async () => {
    renderPage()
    await waitFor(() => expect(table()).toBeInTheDocument())
    press('2')
    expect(location()).toBe('?tab=notes')
    await screen.findByText('Held up despite the yen.')
    press('x')
    expect(screen.getByRole('button', { name: 'Delete?: Delete note' })).toBeInTheDocument()
    press('x')
    await waitFor(() => expect(lastCall('DELETE', '/api/research/notes/n1')).toBeDefined())
  })

  it('prices options from the company’s price and volatility, and steps the strike', async () => {
    renderPage('?tab=options&company=E1')
    const values = await screen.findByRole('region', { name: 'Option values' })
    const expected = blackScholes('call', { spot: 100, strike: 100, years: 90 / 365, rate: 0.01, dividendYield: 0, volatility: 0.2 }).price
    await waitFor(() => expect(within(values).getAllByRole('row')[1]).toHaveTextContent(`¥${expected.toLocaleString(undefined, { maximumFractionDigits: 2 })}`))
    expect(screen.getByRole('button', { name: /^1Y/ })).toHaveAttribute('aria-pressed', 'true')

    press(']')
    expect(screen.getByRole('textbox', { name: 'Strike' })).toHaveValue('102')
    press('v')
    expect(screen.getByRole('textbox', { name: 'Volatility' })).toHaveValue('30')
    fireEvent.change(screen.getByRole('combobox', { name: 'Strategy preset' }), { target: { value: 'straddle' } })
    expect(screen.getAllByRole('combobox', { name: /Leg \d instrument/ }).map(select => (select as HTMLSelectElement).value)).toEqual(['call', 'put'])
  })

  it('shows the company’s default risk and prices its bond with it', async () => {
    renderPage('?tab=bonds&company=E1')
    const profile = await screen.findByRole('region', { name: 'Credit profile' })
    expect(within(profile).getAllByText(/· safe$/, { selector: 'dd' })).toHaveLength(2)
    expect(within(profile).getByText('10.0×')).toBeInTheDocument()
    expect(screen.getByRole('radio', { name: /Merton model/ })).toBeChecked()
    // The coupon starts at the company's own cost of debt, rounded.
    expect(screen.getByRole('textbox', { name: 'Coupon' })).toHaveValue('5.25')
    fireEvent.click(screen.getByRole('radio', { name: 'None' }))
    const price = screen.getByRole('region', { name: 'Bond price' })
    expect(within(price).getByText('Credit spread').nextSibling).toHaveTextContent('0 bp')
  })

  it('marks holdings with their position and keeps those tags locked', async () => {
    renderPage('?company=MO')
    const heading = await screen.findByRole('heading', { name: /ALTRIA GROUP INC/ })
    expect(heading).toHaveTextContent('Open')
    // A holding without EDINET filings opens in Analysis by its ticker, and has no peers or credit view.
    const links = screen.getByRole('navigation', { name: 'Open this company' })
    expect(within(links).getByRole('link', { name: /Analysis/ })).toHaveAttribute('href', '/analyze?ticker=MO')
    expect(within(links).queryByRole('link', { name: 'Peers' })).not.toBeInTheDocument()
    expect(within(links).queryByRole('link', { name: 'Credit' })).not.toBeInTheDocument()
    await screen.findByTitle(/Follows your portfolio: it changes/)
    expect(screen.queryByRole('button', { name: 'Remove tag Open position' })).not.toBeInTheDocument()

    const chips = screen.getByRole('group', { name: 'Filter by tag' })
    expect(within(chips).getAllByRole('button')[1]).toHaveTextContent('Open position')
    fireEvent.click(within(chips).getByRole('button', { name: /Open position/ }))
    expect(location()).toBe('?company=MO&tag=Open+position')
    expect(screen.getByText(/follows your portfolio: a company moves between/)).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /Delete tag/ })).not.toBeInTheDocument()
  })

  it('sets the expiry by date and takes trading fees out of the breakevens', async () => {
    renderPage('?tab=options&company=E1')
    const values = await screen.findByRole('region', { name: 'Option values' })
    await waitFor(() => expect(screen.getByRole('textbox', { name: 'Spot' })).toHaveValue('100'))
    fireEvent.change(screen.getByLabelText('Expiry date'), { target: { value: addDays(localToday(), 30) } })
    expect(screen.getByRole('textbox', { name: 'Days to expiry' })).toHaveValue('30')

    const call = blackScholes('call', { spot: 100, strike: 100, years: 30 / 365, rate: 0.01, dividendYield: 0, volatility: 0.2 }).price
    const money = (value: number) => `¥${value.toLocaleString(undefined, { maximumFractionDigits: 2 })}`
    const breakeven = () => within(values).getAllByRole('row').find(row => row.textContent?.startsWith('Breakeven'))!
    expect(breakeven()).toHaveTextContent(money(100 + call))
    // ¥500 a contract of 100 shares, in and out: ¥10 a share.
    fireEvent.change(screen.getByRole('textbox', { name: 'Fee per contract' }), { target: { value: '500' } })
    fireEvent.click(screen.getByRole('checkbox', { name: /Also when closing/ }))
    expect(breakeven()).toHaveTextContent(money(100 + call + 10))
    expect(breakeven()).toHaveTextContent('with fees')
    expect(within(screen.getByRole('region', { name: 'Strategy' })).getByText('Fees').nextSibling).toHaveTextContent('¥10')
    expect(JSON.parse(window.localStorage.getItem('research.options.fees')!)).toEqual({ mode: 'contract', perContract: 500, percent: 0, contractSize: 100, roundTrip: true })

    // As a share of the premium instead: 1% of the call, charged once.
    fireEvent.click(screen.getByRole('checkbox', { name: /Also when closing/ }))
    fireEvent.click(screen.getByRole('button', { name: '% of value' }))
    fireEvent.change(screen.getByRole('textbox', { name: 'Fee rate' }), { target: { value: '1' } })
    expect(breakeven()).toHaveTextContent(money(100 + call * 1.01))
  })

  it('charges a bond trading fee against the yield', async () => {
    renderPage('?tab=bonds')
    const price = await screen.findByRole('region', { name: 'Bond price' })
    expect(within(price).getByText('Yield after fee').nextSibling).toHaveTextContent('—')
    fireEvent.change(screen.getByRole('textbox', { name: 'Trading fee' }), { target: { value: '0.5' } })
    const before = Number(within(price).getByText('Yield to maturity').nextSibling!.textContent!.replace('%', ''))
    const after = Number(within(price).getByText('Yield after fee').nextSibling!.textContent!.replace('%', ''))
    expect(after).toBeLessThan(before)
    expect(Number(within(price).getByText('Price with fee').nextSibling!.textContent)).toBeCloseTo(Number(within(price).getByText('Price with default risk').nextSibling!.textContent) + 0.5, 3)

    // A set amount per bond: 0.25 on 100 face.
    fireEvent.click(screen.getByRole('button', { name: 'Set' }))
    fireEvent.change(screen.getByRole('textbox', { name: 'Trading fee' }), { target: { value: '0.25' } })
    expect(Number(within(price).getByText('Price with fee').nextSibling!.textContent)).toBeCloseTo(Number(within(price).getByText('Price with default risk').nextSibling!.textContent) + 0.25, 3)
    expect(window.localStorage.getItem('research.bonds.feeMode')).toBe('"amount"')
  })

  it('sets a bond maturity by date', async () => {
    renderPage('?tab=bonds')
    await screen.findByRole('region', { name: 'Bond price' })
    fireEvent.change(screen.getByLabelText('Maturity date'), { target: { value: addDays(localToday(), 730) } })
    expect(Number((screen.getByRole('textbox', { name: 'Years to maturity' }) as HTMLInputElement).value)).toBeCloseTo(730 / 365.25, 2)
    press('d')
    expect(screen.getByLabelText('Maturity date')).toHaveFocus()
  })

  it('adds an alert from the keyboard: N, pick with Enter, type the condition and value, Enter', async () => {
    renderPage('?tab=alerts')
    await screen.findByRole('table', { name: 'Alerts' })
    press('n')
    const picker = screen.getByRole('combobox', { name: 'Company' })
    expect(picker).toHaveFocus()
    fireEvent.change(picker, { target: { value: 'alpha' } })
    await screen.findByRole('option', { name: /Alpha Motor/ })
    fireEvent.keyDown(picker, { key: 'Enter' })
    const value = screen.getByRole('textbox', { name: 'Value' })
    expect(value).toHaveFocus()
    fireEvent.change(value, { target: { value: '<= 1,500' } })
    expect(screen.getByRole('combobox', { name: 'Condition' })).toHaveValue('<=')
    fireEvent.submit(value.closest('form')!)
    await waitFor(() => expect(lastCall('POST', '/api/research/alerts')?.body).toEqual({ name: 'Price ≤ 1,500', edinet_code: 'E1', metric: 'LatestPrice', operator: '<=', value: 1500 }))
    await waitFor(() => expect(picker).toHaveFocus())
  })

  it('deletes an alert with X pressed twice', async () => {
    renderPage('?tab=alerts')
    await screen.findByRole('table', { name: 'Alerts' })
    press('x')
    expect(lastCall('DELETE', '/api/research/alerts')).toBeUndefined()
    press('x')
    await waitFor(() => expect(lastCall('DELETE', '/api/research/alerts')?.path).toBe('/api/research/alerts/a1'))
  })

  it('lists the shortcuts for the tab in use', async () => {
    renderPage('?tab=bonds')
    press('?')
    const dialog = await screen.findByRole('dialog')
    expect(within(dialog).getByText('Maturity one year shorter')).toBeInTheDocument()
    expect(within(dialog).getByText('Bonds & credit tab')).toBeInTheDocument()
    // Only the tab in use is listed, not its siblings.
    expect(within(dialog).queryByText('Download the list as CSV')).not.toBeInTheDocument()
  })
})
