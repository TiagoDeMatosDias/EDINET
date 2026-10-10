import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { MemoryRouter, useLocation } from 'react-router-dom'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import ResearchPage from '../research/ResearchPage'
import { DEFAULT_FILTERS, displayTicker, filterBonds, formatBp, formatYen, marketCsv, marketTags, matchTag, offeringText, ratingGroup, securityText, sortBonds } from './bondFormat'
import type { MarketBond } from './bondTypes'
import { CompanyBondsPanel } from './CompanyBondsPanel'

// jsdom has no canvas; the charts are covered by the screenshots, not here.
vi.mock('react-chartjs-2', () => ({ Line: () => null, Bar: () => null, Scatter: () => null }))

function json(value: unknown, status = 200) {
  return Promise.resolve(new Response(JSON.stringify(value), { status, headers: { 'Content-Type': 'application/json' } }))
}

const CURVE = { date: '2026-10-07', points: [{ tenor: 1, yield: 0.016 }, { tenor: 10, yield: 0.03 }], year_ago: null }
const COMPANIES = {
  E1: { company_name: 'Alpha Motor', ticker: '10000', industry: 'Autos', listed: 1 },
  E2: { company_name: 'Beta Bank', ticker: '20000', industry: 'Banks', listed: 1 },
}
const MARKET_BONDS: MarketBond[] = [
  { bond_id: 'B1', edinet_code: 'E1', label: '第5回無担保社債', series: 5, currency: 'JPY', seniority: 'senior', coupon: 0.0125, coupon_kind: 'fixed', frequency: 2, issue_date: '2025-09-20', maturity: '2030-09-19', outstanding: 10e9, rating: 'A+', rating_agency: 'R&I', rating_notch: 5, issue_spread: 0.0042, years_to_maturity: 3.95, horizon: 3.95, horizon_to: 'maturity', jgb_now: 0.02, model_yield: 0.0242, model_price: 95.5, market_date: '2026-10-07', market_price: 95.4, market_yield: 0.0244, market_spread: 0.0044, market_reporters: 5, spread: 0.0044, spread_basis: 'market' },
  { bond_id: 'B2', edinet_code: 'E2', label: '第1回期限前償還条項付無担保社債(劣後特約付)', series: 1, currency: 'JPY', seniority: 'subordinated', features: ['callable', 'subordinated'], coupon: 0.021, coupon_kind: 'fixed-to-floating', frequency: 2, issue_date: '2026-01-20', maturity: '2036-01-20', call_date: '2031-01-20', outstanding: 50e9, rating: 'A', rating_agency: 'JCR', rating_notch: 6, issue_spread: 0.0098, years_to_maturity: 9.3, horizon: 4.3, horizon_to: 'call', jgb_now: 0.021, model_yield: 0.031, model_price: 95.9, spread: 0.0098, spread_basis: 'issue' },
  { bond_id: 'B3', edinet_code: 'E2', label: '第2回無担保社債(銀行保証付)', currency: 'JPY', seniority: 'senior', features: ['guaranteed'], coupon: 0.004, coupon_kind: 'fixed', maturity: '2028-03-31', outstanding: 300e6, private: 1, years_to_maturity: 1.5, horizon: 1.5, horizon_to: 'maturity' },
]
const BOND = {
  ...MARKET_BONDS[0],
  ...COMPANIES.E1,
  issuer: 'Alpha Motor', is_parent: 1, name: 'Alpha Motor第5回無担保社債', features: [], perpetual: 0, call_date: null, maturity_text: '2030年9月19日',
  amount_issued: 10e9, issue_price: 100, outstanding_as_of: '2025-09-20', current_portion: null, status: 'outstanding', collateral: '本社債には担保及び保証は付されておらず、また本社債のために特に留保されている資産はない。',
  offering: '一般募集', private: 0, ratings: [{ agency: 'R&I', rating: 'A+' }], rating_inferred: 0, issue_yield: 0.0125, issue_tenor: 5, jgb_at_issue: 0.0083,
  issuance_doc_id: 'S100ISS1', issuance_submitted_at: '2025-09-14', schedule_doc_id: 'S100ANN1', schedule_period_end: '2026-03-31',
  issuance_url: 'https://disclosure2.edinet-fsa.go.jp/WZEK0040.aspx?S100ISS1,,,', schedule_url: 'https://disclosure2.edinet-fsa.go.jp/WZEK0040.aspx?S100ANN1,,,',
  jsda_code: '041056367', jsda_name: 'アルファ自動車5', market_change: -0.05,
}
const DETAIL = {
  today: '2026-10-09',
  bond: BOND,
  issuance: { doc_id: 'S100ISS1', name: BOND.name, amount: 10e9, denomination: 1e8, issue_price: 100, coupon_text: '年1.250%', interest_dates: '毎年3月20日及び9月20日', maturity_text: '2030年9月19日', offering: '一般募集', collateral: BOND.collateral, negative_pledge: 1, covenants: '', ratings: BOND.ratings, stored: 1 },
  valuation: { peer_count: 12, tenor_window: 2, peer_spread: 0.0035, fair_yield: 0.0235, fair_price: 95.8, spread_quartiles: [0.003, 0.0035, 0.004], quoted_peers: 10, relative_spread: 0.0009 },
  market_history: [{ date: '2026-10-06', price: 95.45, yield: 0.0243 }, { date: '2026-10-07', price: 95.4, yield: 0.0244 }],
  issuer_bonds: [],
  similar: [{ ...MARKET_BONDS[1], ...COMPANIES.E2 }],
  spread_curve: { peers: [{ bond_id: 'B2', company_name: 'Beta Bank', horizon: 4.3, spread: 0.0098, rating: 'A', quoted: false }], issuer: [] },
  curve: CURVE,
}
const COMPANY = {
  edinet_code: 'E1',
  company_name: 'Alpha Motor',
  today: '2026-10-09',
  curve: CURVE,
  summary: { outstanding_count: 1, total_outstanding: 10e9, foreign_currency_count: 0, not_separately_reported: 0, average_coupon: 0.0125, average_years: 3.95, next_maturity: '2030-09-19', due_within_year: null, as_of: '2026-03-31', ratings: BOND.ratings, rating: 'A+', rated_on: '2025-09-20', median_spread: 0.0042, rating_peer_spread: 0.0035 },
  ladder: [{ year: 2030, parent: 10e9, group: 0 }],
  bonds: [BOND, { ...BOND, bond_id: 'B0', label: '第4回無担保社債', series: 4, maturity: '2025-09-19', status: 'matured', issuance_doc_id: null, issuance_url: null }],
  documents: [
    { doc_id: 'S100ANN1', kind: 'annual', submitted_at: '2026-06-25', period_end: '2026-03-31', bond_count: 2, description: '有価証券報告書', edinet_url: 'https://disclosure2.edinet-fsa.go.jp/WZEK0040.aspx?S100ANN1,,,', stored: false, in_catalog: true },
    { doc_id: 'S100ISS1', kind: 'issuance', submitted_at: '2025-09-14', period_end: null, bond_count: 1, description: '発行登録追補書類', edinet_url: 'https://disclosure2.edinet-fsa.go.jp/WZEK0040.aspx?S100ISS1,,,', stored: true, in_catalog: false },
  ],
}
const PRICING = {
  company: { company_code: 'E1', company_name: 'Alpha Motor', ticker: '10000', industry: 'Autos' },
  currency: { price: 'JPY', reporting: 'JPY' },
  spot: 100, price_date: '2026-10-02', dividend_yield: 0, market_cap: 1500,
  volatility: { estimates: [{ window: '1Y', days: 252, value: 0.2 }], history: [], observations: 800 },
  credit: { period: '2026-03-31', lines: { TotalAssets: 1000, TotalLiabilities: 400 }, debt: {}, debt_total: 200, previous_debt_total: 180, interest_coverage: 10, cost_of_debt: 0.0526 },
  financial: false,
}

// Research state as the book sends it: Gamma is tagged but has no bonds.
const TAGGED = [
  { company_code: 'E1', company_name: 'Alpha Motor', ticker: '10000', tags: ['Carmakers', 'Open position'], note_count: 0, alert_count: 0, alerts_triggered: 0 },
  { company_code: 'E2', company_name: 'Beta Bank', ticker: '20000', tags: ['Lenders'], note_count: 0, alert_count: 0, alerts_triggered: 0 },
  { company_code: 'E3', company_name: 'Gamma Foods', ticker: '30000', tags: ['Lenders', 'Staples'], note_count: 0, alert_count: 0, alerts_triggered: 0 },
]

let noBondData = false
let bookCompanies: typeof TAGGED = []
function stub() {
  noBondData = false
  bookCompanies = []
  vi.stubGlobal('fetch', vi.fn((input: RequestInfo | URL) => {
    const path = String(input)
    if (path.startsWith('/api/bonds/') && noBondData) return json({ detail: 'No bond data yet: run the Update bonds pipeline step.' }, 503)
    if (path.startsWith('/api/bonds/market')) return json({ today: '2026-10-09', curve: CURVE, companies: COMPANIES, bonds: MARKET_BONDS, industries: ['Autos', 'Banks'], updates: {} })
    if (path.startsWith('/api/bonds/bond/')) return json(path.endsWith('B1') ? DETAIL : { ...DETAIL, bond: { ...BOND, bond_id: path.split('/').pop() } })
    if (path.startsWith('/api/bonds/company/')) return json(COMPANY)
    if (path.startsWith('/api/research/pricing/')) return json(PRICING)
    if (path === '/api/research/book') return json({ companies: bookCompanies, tags: [], alerts: [], metric_definitions: {} })
    return json({ notes: [], tags: [] })
  }))
}

function Location() {
  return <output data-testid="location">{useLocation().pathname}{useLocation().search}</output>
}

function renderAt(path: string, element = <ResearchPage />) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return render(<QueryClientProvider client={client}><MemoryRouter initialEntries={[path]}>{element}<Location /></MemoryRouter></QueryClientProvider>)
}

const location = () => screen.getByTestId('location').textContent
const press = (key: string) => fireEvent.keyDown(window, { key })

beforeEach(() => { stub(); window.localStorage.clear() })
afterEach(() => { cleanup(); vi.unstubAllGlobals() })

describe('bond formatting', () => {
  it('formats amounts, spreads, tickers, and Japanese terms in plain English', () => {
    expect(formatYen(30e9)).toBe('¥30bn')
    expect(formatYen(1.25e12)).toBe('¥1.25tn')
    expect(formatYen(500e6)).toBe('¥500m')
    expect(formatBp(0.00345)).toBe('35 bp')
    expect(formatBp(0.0005, true)).toBe('+5 bp')
    expect(displayTicker('72720')).toBe('7272')
    expect(displayTicker('130A0')).toBe('130A')
    expect(ratingGroup(5)).toBe('A')
    expect(ratingGroup(null)).toBe('Not rated')
    expect(securityText({ collateral: 'なし' })).toBe('Unsecured')
    expect(securityText({ features: ['general-mortgage'], collateral: '一般担保' })).toBe('General mortgage')
    expect(offeringText('一般募集')).toBe('Public offering')
  })

  it('filters and sorts bonds, keeping missing values last', () => {
    const shown = filterBonds(MARKET_BONDS, COMPANIES, DEFAULT_FILTERS)
    expect(shown.map(bond => bond.bond_id)).toEqual(['B1', 'B2'])
    expect(filterBonds(MARKET_BONDS, COMPANIES, { ...DEFAULT_FILTERS, includePrivate: true, query: 'beta' }).map(bond => bond.bond_id)).toEqual(['B2', 'B3'])
    expect(filterBonds(MARKET_BONDS, COMPANIES, { ...DEFAULT_FILTERS, minYears: 5 }).map(bond => bond.bond_id)).toEqual(['B2'])
    expect(sortBonds(MARKET_BONDS, COMPANIES, 'spread', true).map(bond => bond.bond_id)).toEqual(['B2', 'B1', 'B3'])
    expect(sortBonds(MARKET_BONDS, COMPANIES, 'spread', false).map(bond => bond.bond_id)).toEqual(['B1', 'B2', 'B3'])
    const csv = marketCsv(shown, COMPANIES).split('\n')
    expect(csv[0]).toMatch(/^Company,Ticker,EDINET code/)
    expect(csv[1]).toContain('Alpha Motor,10000,E1,Autos,第5回無担保社債,Senior unsecured,JPY,1.250')
  })

  it('reads the user’s tags against the bonds listed', () => {
    const { tags, byIssuer } = marketTags(TAGGED, MARKET_BONDS)
    // Position tags lead; a tag whose companies have no bonds is left out.
    expect(tags).toEqual([
      { name: 'Open position', members: 1, issuers: ['E1'] },
      { name: 'Carmakers', members: 1, issuers: ['E1'] },
      { name: 'Lenders', members: 2, issuers: ['E2'] },
    ])
    expect(byIssuer).toEqual({ E1: ['Carmakers', 'Open position'], E2: ['Lenders'] })
    expect(matchTag(tags, 'len')?.name).toBe('Lenders')
    expect(matchTag(tags, 'position')?.name).toBe('Open position')
    expect(matchTag(tags, ' ')).toBeUndefined()
    expect(matchTag(tags, 'staples')).toBeUndefined()
  })

  it('filters bonds to the issuers under any chosen tag, and finds tags by text', () => {
    const { byIssuer } = marketTags(TAGGED, MARKET_BONDS)
    const shown = (filters: Partial<typeof DEFAULT_FILTERS>) => filterBonds(MARKET_BONDS, COMPANIES, { ...DEFAULT_FILTERS, includePrivate: true, ...filters }, byIssuer).map(bond => bond.bond_id)
    expect(shown({ tags: ['Carmakers'] })).toEqual(['B1'])
    expect(shown({ tags: ['Lenders'] })).toEqual(['B2', 'B3'])
    expect(shown({ tags: ['Carmakers', 'Lenders'] })).toEqual(['B1', 'B2', 'B3'])
    expect(shown({ tags: ['Staples'] })).toEqual([])
    // Other filters still apply within the tag.
    expect(shown({ tags: ['Lenders'], includePrivate: false })).toEqual(['B2'])
    expect(shown({ query: 'lend' })).toEqual(['B2', 'B3'])
  })
})

describe('Bond market', () => {
  it('lists public yen bonds largest first and shows the selected bond with its peers', async () => {
    renderAt('/research?tab=bond-market')
    const table = await screen.findByRole('table', { name: 'Bonds' })
    const rows = within(table).getAllByRole('row').slice(1)
    expect(rows).toHaveLength(2)
    expect(rows[0]).toHaveTextContent('Beta Bank')
    expect(rows[0]).toHaveTextContent('4.3y to call')
    // Without a quote the spread is the one at issue, marked i, and the yield an estimate, marked *.
    expect(rows[0]).toHaveTextContent('98 bpi')
    expect(rows[1]).toHaveTextContent('44 bp')
    expect(rows[1]).toHaveTextContent('2.44%')
    expect(screen.getByText(/2 of 3 bonds/)).toBeInTheDocument()

    fireEvent.click(rows[1])
    expect(location()).toBe('/research?tab=bond-market&bond=B1')
    const detail = await screen.findByRole('region', { name: 'Valuation' })
    expect(within(detail).getByText(/cheaper/)).toBeInTheDocument()
    expect(within(detail).getByText('95.800')).toBeInTheDocument()
    // A quoted bond shows its reference price and the spread it implies, against the one at issue.
    expect(within(detail).getByText('95.40')).toBeInTheDocument()
    expect(within(detail).getByText('+2 bp since issue', { exact: false })).toBeInTheDocument()
    const terms = screen.getByRole('region', { name: 'Terms' })
    expect(within(terms).getByText('Unsecured')).toBeInTheDocument()
    expect(within(terms).getByText('Public offering')).toBeInTheDocument()
    expect(within(screen.getByRole('region', { name: 'Similar bonds' })).getByText('Beta Bank')).toBeInTheDocument()
  })

  it('narrows by rating and issuer from the keyboard and sends the bond to the calculator', async () => {
    renderAt('/research?tab=bond-market&bond=B1')
    const table = await screen.findByRole('table', { name: 'Bonds' })
    press(']')
    expect(screen.getByRole('button', { name: 'AAA' })).toHaveAttribute('aria-pressed', 'true')
    expect(screen.getByText('No bonds match.', { exact: false })).toBeInTheDocument()
    press('[')
    await waitFor(() => expect(within(screen.getByRole('table', { name: 'Bonds' })).getAllByRole('row')).toHaveLength(3))
    expect(table).toBeTruthy()
    press('i')
    expect(location()).toBe('/research?tab=bond-market&bond=B1&issuer=E1')
    await waitFor(() => expect(within(screen.getByRole('table', { name: 'Bonds' })).getAllByRole('row')).toHaveLength(2))
    press('c')
    expect(location()).toBe('/research?tab=bonds&company=E1&bond=B1')
  })

  it('narrows to the bonds of companies under one or more tags', async () => {
    bookCompanies = TAGGED
    renderAt('/research?tab=bond-market')
    await screen.findByRole('table', { name: 'Bonds' })
    const bondRows = () => within(screen.getByRole('table', { name: 'Bonds' })).getAllByRole('row').slice(1)
    const tags = await screen.findByRole('group', { name: 'Filter by tag' })
    // Each chip counts the tag's companies with bonds; Staples has none and is not offered.
    expect(within(tags).getAllByRole('button').map(chip => chip.textContent)).toEqual(['Open position 1', 'Carmakers 1', 'Lenders 1'])
    expect(within(tags).getByRole('button', { name: /Lenders/ })).toHaveAttribute('title', '1 of the 2 companies tagged “Lenders” has bonds listed: Beta Bank')
    expect(bondRows()).toHaveLength(2)

    fireEvent.click(within(tags).getByRole('button', { name: /Carmakers/ }))
    expect(within(tags).getByRole('button', { name: /Carmakers/ })).toHaveAttribute('aria-pressed', 'true')
    expect(bondRows()).toHaveLength(1)
    expect(bondRows()[0]).toHaveTextContent('Alpha Motor')
    expect(screen.getByText(/1 of 3 bonds/)).toBeInTheDocument()

    fireEvent.click(within(tags).getByRole('button', { name: /Lenders/ }))
    expect(bondRows().map(row => row.textContent)).toEqual([expect.stringContaining('Beta Bank'), expect.stringContaining('Alpha Motor')])

    fireEvent.click(within(tags).getByRole('button', { name: /Carmakers/ }))
    expect(bondRows()).toHaveLength(1)
    expect(bondRows()[0]).toHaveTextContent('Beta Bank')
  })

  it('finds a tag from the filter box and applies it with Enter', async () => {
    bookCompanies = TAGGED
    renderAt('/research?tab=bond-market')
    await screen.findByRole('table', { name: 'Bonds' })
    const bondRows = () => within(screen.getByRole('table', { name: 'Bonds' })).getAllByRole('row').slice(1)
    const tags = await screen.findByRole('group', { name: 'Filter by tag' })
    const filter = screen.getByRole('textbox', { name: 'Filter bonds' })
    expect(filter).toHaveAttribute('placeholder', 'Filter by issuer, ticker, bond, or tag')

    // Typing a tag's name already lists its companies' bonds, and marks the chip Enter applies.
    fireEvent.change(filter, { target: { value: 'car' } })
    expect(bondRows()).toHaveLength(1)
    expect(bondRows()[0]).toHaveTextContent('Alpha Motor')
    expect(within(tags).getByRole('button', { name: /Carmakers/ })).toHaveTextContent('Enter')
    fireEvent.keyDown(filter, { key: 'Enter' })
    expect(filter).toHaveValue('')
    expect(within(tags).getByRole('button', { name: /Carmakers/ })).toHaveAttribute('aria-pressed', 'true')
    expect(bondRows()).toHaveLength(1)

    // A second tag adds its issuers; naming a chosen tag again turns it off.
    fireEvent.change(filter, { target: { value: 'lend' } })
    fireEvent.keyDown(filter, { key: 'Enter' })
    expect(bondRows()).toHaveLength(2)
    fireEvent.change(filter, { target: { value: 'carmakers' } })
    fireEvent.keyDown(filter, { key: 'Enter' })
    expect(within(tags).getByRole('button', { name: /Carmakers/ })).toHaveAttribute('aria-pressed', 'false')
    expect(bondRows()).toHaveLength(1)
    expect(bondRows()[0]).toHaveTextContent('Beta Bank')

    // Text that names no tag is an ordinary filter, and Clear filters drops the tags too.
    fireEvent.change(filter, { target: { value: 'alpha' } })
    fireEvent.keyDown(filter, { key: 'Enter' })
    expect(screen.getByText('No bonds match.', { exact: false })).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'Clear filters' }))
    expect(bondRows()).toHaveLength(2)
    expect(within(tags).getByRole('button', { name: /Lenders/ })).toHaveAttribute('aria-pressed', 'false')
  })

  it('offers no tag filter when no tagged company has bonds', async () => {
    renderAt('/research?tab=bond-market')
    await screen.findByRole('table', { name: 'Bonds' })
    expect(screen.queryByRole('group', { name: 'Filter by tag' })).not.toBeInTheDocument()
    expect(screen.getByRole('textbox', { name: 'Filter bonds' })).toHaveAttribute('placeholder', 'Filter by issuer, ticker, or bond')
  })

  it('explains how to load bond data when there is none', async () => {
    noBondData = true
    renderAt('/research?tab=bond-market')
    expect(await screen.findByText('No bond data yet')).toBeInTheDocument()
  })
})

describe('Bond calculator', () => {
  it('prices a stored bond on its own terms and today’s JGB yield', async () => {
    renderAt('/research?tab=bonds&company=E1&bond=B1')
    await screen.findByText(/Pricing/)
    await waitFor(() => expect(screen.getByRole('textbox', { name: 'Coupon' })).toHaveValue('1.25'))
    expect(screen.getByRole('textbox', { name: 'Risk-free yield' })).toHaveValue('2')
    expect(screen.getByRole('radio', { name: 'A credit spread' })).toBeChecked()
    expect(screen.getByRole('link', { name: 'Compare it with similar bonds' })).toHaveAttribute('href', '/research?tab=bond-market&bond=B1&issuer=E1')
  })
})

describe('Company bonds', () => {
  it('summarises the company’s bonds and hides matured ones until asked', async () => {
    renderAt('/analyze/E1', <CompanyBondsPanel companyCode="E1" />)
    const table = await screen.findByRole('table', { name: 'Bonds' })
    expect(screen.getByText('¥10bn', { selector: 'strong' })).toBeInTheDocument()
    expect(screen.getByText('+7 bp')).toBeInTheDocument()
    expect(within(table).getAllByRole('row')).toHaveLength(2)
    press('h')
    await waitFor(() => expect(within(screen.getByRole('table', { name: 'Bonds' })).getAllByRole('row')).toHaveLength(3))
    expect(screen.getByRole('link', { name: /Terms/ })).toHaveAttribute('href', BOND.issuance_url)
    expect(screen.getByRole('link', { name: /Price/ })).toHaveAttribute('href', '/research?tab=bonds&company=E1&bond=B1')
    fireEvent.click(within(table).getAllByRole('row')[1])
    expect(location()).toBe('/research?tab=bond-market&bond=B1&issuer=E1')
  })
})
