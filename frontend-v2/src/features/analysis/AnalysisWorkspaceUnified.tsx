import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { BarChart3, Download, ExternalLink, GitCompare, Keyboard, RefreshCw } from 'lucide-react'
import { useCallback, useEffect, useRef, useState, type ReactNode } from 'react'
import { Link, useNavigate, useParams, useSearchParams } from 'react-router-dom'

import { ApiError, apiPost, apiRequest, queryString } from '../../api/client'
import type { SecurityHistory, SecurityOverview } from '../../api/types'
import { EmptyState, ErrorState, LoadingState } from '../../components/Feedback'
import { PageHeader } from '../../components/Page'
import { ShortcutsDialog, type ShortcutGroup } from '../../components/ShortcutsDialog'
import { Tip } from '../../components/Tooltip'
import { useHotkeys } from '../../hooks/useHotkeys'
import { formatMetricValue, groupMetrics, type MetricDefinition } from '../../metrics'
import { useAuth } from '../auth/authContext'
import { PortfolioTrailNav } from '../portfolio/PortfolioTrailNav'
import { CompanyResearchPanel } from '../research/CompanyResearchPanel'
import { ResearchBadge } from '../research/ResearchBadge'
import { ScreenTrailNav } from '../screening/ScreenTrailNav'
import { downloadTextFile, safeFileName } from './downloads'
import { FilingsPanel } from './FilingsPanel'
import { FinancialHistoryWorkspace } from './FinancialHistoryWorkspace'
import { buildCompanyReport, type SnapshotGroup } from './markdownReport'
import { PricePanel } from './PricePanel'
import type { PriceHistoryRow } from './priceHistoryRanges'
import './analysis.css'

const SECTIONS = [
  { id: 'overview', label: 'Overview' },
  { id: 'financials', label: 'Financials' },
  { id: 'filings', label: 'Filings' },
] as const
type SectionId = typeof SECTIONS[number]['id']

const SHORTCUTS: ShortcutGroup[] = [
  { title: 'Anywhere', shortcuts: [
    { keys: ['/'], label: 'Search companies' },
    { keys: ['?'], label: 'Show or hide this list' },
    { keys: ['Esc'], label: 'Close a menu or leave a field' },
  ] },
  { title: 'Opened from a screen or the portfolio', shortcuts: [
    { keys: ['Shift+J'], label: 'Next company in the list' },
    { keys: ['Shift+K'], label: 'Previous company in the list' },
    { keys: ['G S', 'G P'], label: 'Back to the screen results or the portfolio' },
  ] },
  { title: 'This company', shortcuts: [
    { keys: ['1', '2', '3'], label: 'Jump to Overview, Financials, Filings' },
    { keys: ['-', '='], label: 'Widen or narrow the price range' },
    { keys: ['T'], label: 'Add a tag' },
    { keys: ['N'], label: 'Write a research note' },
    { keys: ['P'], label: 'Compare with peers' },
    { keys: ['B'], label: 'Backtest this ticker' },
    { keys: ['O'], label: 'Open the latest filing' },
  ] },
  { title: 'Financial statements', shortcuts: [
    { keys: ['[', ']'], label: 'Previous or next statement' },
    { keys: ['V'], label: 'Cycle Values, YoY, Common size' },
    { keys: ['F'], label: 'Filter lines' },
    { keys: ['E'], label: 'Show or hide empty lines' },
    { keys: ['C'], label: 'Switch bars and lines' },
    { keys: ['X'], label: 'Clear the chart' },
    { keys: ['↑', '↓'], label: 'Move between lines (also J, K)' },
    { keys: ['Space'], label: 'Chart or un-chart the focused line' },
  ] },
]

/** Dense panels abbreviate magnitudes: "¥23.66 Trillion" → "¥23.66T". */
function abbreviate(text: string) {
  return text.replace(/ (Trillion|Billion|Million|Thousand)$/, (_, unit: string) => unit[0])
}

function stringOrNull(value: unknown) {
  return typeof value === 'string' && value ? value : null
}

function numberOrNull(value: unknown) {
  return typeof value === 'number' && Number.isFinite(value) ? value : null
}

const longDate = new Intl.DateTimeFormat('en-GB', { day: 'numeric', month: 'short', year: 'numeric', timeZone: 'UTC' })

function formatDay(value: unknown) {
  const text = stringOrNull(value)
  if (!text) return ''
  const time = Date.parse(`${text.slice(0, 10)}T00:00:00Z`)
  return Number.isFinite(time) ? longDate.format(time) : text
}

/** Exchange code for Japanese quote sites: "7203.T" → "7203". */
function localCode(yahooSymbol: string) {
  return /^([0-9A-Z]{4})\.T$/.exec(yahooSymbol)?.[1] ?? ''
}

function Quote({ market, metrics, formatMetric, refresh }: { market: Record<string, unknown>; metrics: Record<string, number | null>; formatMetric: (key: string, value: number | null | undefined) => string; refresh?: ReactNode }) {
  const price = numberOrNull(market.latest_price) ?? metrics.LatestPrice ?? null
  if (price === null) return null
  const previous = numberOrNull(market.previous_price)
  const change = numberOrNull(market.change_pct_1d)
  const low = numberOrNull(market.range_52w_low)
  const high = numberOrNull(market.range_52w_high)
  const position = low !== null && high !== null && high > low ? Math.min(100, Math.max(0, ((price - low) / (high - low)) * 100)) : null
  const sign = (value: number) => value > 0 ? '+' : value < 0 ? '−' : ''
  return <div className="quote">
    <div className="quote__main">
      <strong className="quote__price">{formatMetric('LatestPrice', price)}</strong>
      {change !== null && <span className={`quote__change ${change < 0 ? 'neg' : 'pos'}`}>
        {previous !== null && <>{sign(price - previous)}{formatMetric('LatestPrice', Math.abs(price - previous))} </>}
        ({sign(change)}{Math.abs(change * 100).toFixed(2)}%)
      </span>}
      <span className="quote__date">{market.latest_price_date ? `Close ${formatDay(market.latest_price_date)}` : ''}{refresh}</span>
    </div>
    <dl className="quote__facts">
      {metrics.MarketCap != null && <div><dt><Tip content="Latest price × shares issued as of the latest annual filing date.">Market cap</Tip></dt><dd title={formatMetric('MarketCap', metrics.MarketCap)}>{abbreviate(formatMetric('MarketCap', metrics.MarketCap))}</dd></div>}
      {position !== null && <div className="quote__range">
        <dt><Tip content="Lowest and highest close over the past 52 weeks; the marker shows where the latest close sits.">52-week range</Tip></dt>
        <dd>
          <span>{formatMetric('LatestPrice', low)}</span>
          <span className="range-track" role="img" aria-label={`Latest close at ${Math.round(position)}% of the 52-week range`}><span className="range-track__marker" style={{ left: `${position}%` }} /></span>
          <span>{formatMetric('LatestPrice', high)}</span>
        </dd>
      </div>}
    </dl>
  </div>
}

function KeyStats({ metrics, definitions, format, period, hasPrice }: { metrics: Record<string, number | null>; definitions: Record<string, MetricDefinition>; format: (key: string, value: number | null | undefined) => string; period: string; hasPrice: boolean }) {
  const groups = groupMetrics(Object.keys(definitions), definitions).filter(group => group.group !== 'Market')
  return <div className="key-stats">
    {groups.map(({ group, metrics: keys }) => {
      const empty = keys.every(key => metrics[key] == null)
      return <section className="key-stats__group" key={group}>
        <h3>{group}</h3>
        {empty
          ? <p className="key-stats__none">{group === 'Valuation' && !hasPrice ? 'Needs a market price' : 'Not reported in the stored filings'}</p>
          : <dl>{keys.map(key => {
            const definition = definitions[key]
            return <div key={key} className={metrics[key] == null ? 'is-empty' : undefined}>
              <dt><Tip content={<span className="tip-lines"><strong>{definition.label}</strong>{definition.description && <span>{definition.description}</span>}{definition.format !== 'money' && period && key !== 'LatestPrice' && <span>Fiscal year ending {period}.</span>}</span>}>{definition.label}</Tip></dt>
              <dd title={format(key, metrics[key])}>{abbreviate(format(key, metrics[key]))}</dd>
            </div>
          })}</dl>}
      </section>
    })}
  </div>
}

interface RecentCompany { work_id: string; kind: string; title: string; subtitle?: string | null; href: string; occurred_at: string }

/** The starting point when no company is open: pick up where you left off, or search. */
function StartAnalysis() {
  const recent = useQuery({
    queryKey: ['recent-work'],
    queryFn: () => apiRequest<{ items: RecentCompany[] }>('/api/research/recent-work?limit=50'),
    retry: false,
  })
  const companies = (recent.data?.items ?? []).filter(item => item.kind === 'company').slice(0, 12)
  return <div className="stack dense-page analysis-empty-page">
    <PageHeader eyebrow="Company research" title="Analyze a company" description="Search by name, ticker, EDINET code, or industry to open prices, statements, ratios, and filings." />
    {companies.length > 0 && <section className="panel recent-companies" aria-labelledby="recent-companies-title">
      <header className="panel__header"><h3 id="recent-companies-title">Recently viewed</h3><span className="panel__meta">Press <kbd>/</kbd> to search for another</span></header>
      <ul>{companies.map(item => <li key={item.work_id}><Link to={item.href}><strong>{item.title}</strong><small>{item.subtitle}</small><span className="muted">{formatDay(item.occurred_at)}</span></Link></li>)}</ul>
    </section>}
    {!companies.length && !recent.isLoading && <EmptyState title="Press / to search" description="Enter a name, ticker, EDINET code, or industry in the search bar above and choose a result." />}
  </div>
}

function Section({ id, title, aside, children, hideTitle = false }: { id: SectionId; title: string; aside?: ReactNode; children: ReactNode; hideTitle?: boolean }) {
  return <section id={id} className="analysis-section" aria-labelledby={`${id}-title`}>
    <header className={hideTitle ? 'analysis-section__header sr-only' : 'analysis-section__header'}>
      <h2 id={`${id}-title`} tabIndex={-1}>{title}</h2>
      {aside}
    </header>
    {children}
  </section>
}

function useActiveSection(ready: boolean) {
  const [active, setActive] = useState<SectionId>('overview')
  useEffect(() => {
    if (!ready || typeof IntersectionObserver === 'undefined') return
    const observer = new IntersectionObserver(entries => {
      const visible = entries.filter(entry => entry.isIntersecting).sort((a, b) => a.boundingClientRect.top - b.boundingClientRect.top)
      if (visible[0]) setActive(visible[0].target.id as SectionId)
    }, { rootMargin: '-120px 0px -55% 0px' })
    SECTIONS.forEach(section => { const element = document.getElementById(section.id); if (element) observer.observe(element) })
    return () => observer.disconnect()
  }, [ready])
  return active
}

function jumpTo(id: SectionId) {
  const section = document.getElementById(id)
  if (!section) return
  section.scrollIntoView({ behavior: 'smooth', block: 'start' })
  section.querySelector<HTMLElement>('h2')?.focus({ preventScroll: true })
}

function Profile({ description, provenance, facts, links }: { description: string; provenance: string; facts: Array<[string, ReactNode, string?]>; links: Array<{ label: string; href: string; external?: boolean; hint: string }> }) {
  const [expanded, setExpanded] = useState(false)
  const long = description.length > 420
  return <div className="profile">
    <div className="profile__about">
      <h3>Business</h3>
      {description
        ? <p className={long && !expanded ? 'profile__description is-clamped' : 'profile__description'}>{description}</p>
        : <p className="profile__description muted">No business description in the stored filings or external profile.</p>}
      <div className="profile__about-foot">
        {long && <button type="button" className="text-button" aria-expanded={expanded} onClick={() => setExpanded(!expanded)}>{expanded ? 'Show less' : 'Show more'}</button>}
        {provenance && <small className="muted">Source: {provenance}</small>}
      </div>
    </div>
    <dl className="profile__facts">
      {facts.map(([label, value, hint]) => <div key={label}><dt>{hint ? <Tip content={hint}>{label}</Tip> : label}</dt><dd>{value || <span className="muted">—</span>}</dd></div>)}
    </dl>
    <div className="profile__side">
      <h3>Research</h3>
      <ul className="profile__links">
        {links.map(link => <li key={link.href}>{link.external
          ? <a href={link.href} target="_blank" rel="noreferrer" title={link.hint}>{link.label}<ExternalLink aria-hidden="true" /></a>
          : <Link to={link.href} title={link.hint}>{link.label}</Link>}</li>)}
      </ul>
    </div>
  </div>
}

export default function AnalysisWorkspaceUnified() {
  const { companyCode: routeCompanyCode } = useParams()
  const [params] = useSearchParams()
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const companyCode = routeCompanyCode ?? ''
  const tickerParam = params.get('ticker') ?? ''
  const lookup = companyCode || tickerParam
  const lookupKey = companyCode || `ticker:${tickerParam}`
  const overview = useQuery({
    queryKey: ['security-overview', lookupKey],
    enabled: Boolean(lookup),
    queryFn: () => apiRequest<SecurityOverview>(`/api/security/overview${queryString({ company_code: companyCode || undefined, ticker: companyCode ? undefined : tickerParam })}`),
  })
  const company = overview.data?.company ?? {}
  const metrics = overview.data?.metrics ?? {}
  const market = overview.data?.market ?? {}
  const canonicalCode = String(company.edinet_code ?? company.company_code ?? companyCode ?? '')
  const name = String(company.company_name ?? lookup ?? 'Company')
  const ticker = String(company.ticker ?? '')
  const history = useQuery({
    queryKey: ['security-history', canonicalCode],
    enabled: Boolean(canonicalCode),
    queryFn: () => apiRequest<SecurityHistory>(`/api/security/history${queryString({ company_code: canonicalCode, periods: 16 })}`),
    retry: false,
  })
  const prices = useQuery({
    queryKey: ['security-prices', ticker],
    enabled: Boolean(ticker),
    queryFn: () => apiRequest<{ prices: PriceHistoryRow[] }>(`/api/security/price-history${queryString({ ticker, adjusted: true })}`),
  })
  const metricPeriod = String(overview.data?.metadata?.comparison_metric_period_end ?? overview.data?.metadata?.last_financial_period_end ?? '')
  const auth = useAuth()
  // Provider refreshes write shared market data; the API allows operators and admins only.
  // With authentication disabled the server treats every request as the local admin.
  const canRefreshPrice = auth.status?.mode === 'disabled' || auth.user?.role === 'admin' || auth.user?.role === 'operator'
  const updatePrice = useMutation({
    mutationFn: () => apiPost('/api/security/update-price', { ticker }),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['security-overview', lookupKey] })
      void queryClient.invalidateQueries({ queryKey: ['security-prices', ticker] })
    },
  })
  const tagInput = useRef<HTMLInputElement>(null)
  const noteInput = useRef<HTMLTextAreaElement>(null)
  const [showShortcuts, setShowShortcuts] = useState(false)
  const closeShortcuts = useCallback(() => setShowShortcuts(false), [])
  const ready = Boolean(overview.data)
  const activeSection = useActiveSection(ready)
  const header = useRef<HTMLDivElement>(null)
  const [headerHidden, setHeaderHidden] = useState(false)
  useEffect(() => {
    const element = header.current
    if (!ready || !element || typeof IntersectionObserver === 'undefined') return
    // Once the company header scrolls away, the sticky section bar carries the name and price.
    const observer = new IntersectionObserver(([entry]) => setHeaderHidden(!entry.isIntersecting), { rootMargin: '-110px 0px 0px 0px' })
    observer.observe(element)
    return () => observer.disconnect()
  }, [ready])
  useHotkeys({
    '?': () => setShowShortcuts(true),
    1: () => jumpTo('overview'),
    2: () => jumpTo('financials'),
    3: () => jumpTo('filings'),
    t: () => { tagInput.current?.focus(); tagInput.current?.scrollIntoView({ block: 'center', behavior: 'smooth' }) },
    n: () => { noteInput.current?.focus(); noteInput.current?.scrollIntoView({ block: 'center', behavior: 'smooth' }) },
    p: () => { if (canonicalCode) navigate(`/compare?companies=${encodeURIComponent(canonicalCode)}`) },
    b: () => { if (ticker) navigate(`/backtest?symbol=${encodeURIComponent(ticker)}`) },
  }, ready && !showShortcuts)

  if (!lookup) return <StartAnalysis />
  if (overview.isLoading) return <LoadingState label="Loading company analysis" />
  if (overview.isError && overview.error instanceof ApiError && overview.error.status === 404) {
    return <div className="stack dense-page analysis-empty-page"><PageHeader eyebrow="Company research" title="Company not found" description={`No company in the research database matches “${lookup}”.`} actions={params.get('from') === 'portfolio' ? <PortfolioTrailNav current={tickerParam} /> : undefined} /><EmptyState title="Press / to search" description="Enter a name, ticker, EDINET code, or industry and choose a result." /></div>
  }
  if (overview.isError) return <ErrorState error={overview.error} retry={() => overview.refetch()} />

  const metricDefinitions = overview.data?.metric_definitions ?? {}
  const currencies = { price: stringOrNull(market.price_currency), reporting: stringOrNull(overview.data?.metadata?.reporting_currency) }
  const formatMetric = (key: string, value: number | null | undefined) => formatMetricValue(metricDefinitions[key], value, currencies)
  const formatPrice = (value: number) => formatMetricValue(metricDefinitions.LatestPrice, value, currencies)
  const snapshotGroups: SnapshotGroup[] = groupMetrics(Object.keys(metricDefinitions), metricDefinitions)
    .map(({ group, metrics: keys }) => ({ title: group, metrics: keys.map(key => [key, metricDefinitions[key].label] as [string, string]) }))
  const qualityFlags = overview.data?.metadata?.data_quality_flags
  const tickerOnly = Array.isArray(qualityFlags) && qualityFlags.includes('ticker_only_no_company_record')
  // Research keys a holding without EDINET filings (a US share, an ETF) by its symbol, as the portfolio does.
  const researchCode = canonicalCode || (tickerOnly ? tickerParam : '')
  const yahooSymbol = String(company.yahoo_symbol ?? '')
  const exchangeCode = localCode(yahooSymbol)
  // The filing's own business description comes first; the external profile is a labelled fallback.
  const descriptionSource = company.description_source as { kind?: string; label?: string; symbol?: string } | null | undefined
  const businessDescription = String((descriptionSource?.kind === 'external' ? company.yahoo_description : company.description_summary || company.description) ?? '').trim()
  const descriptionProvenance = businessDescription && descriptionSource?.label ? [descriptionSource.label, descriptionSource.symbol].filter(Boolean).join(' · ') : ''
  const priceRows = prices.data?.prices ?? []
  const hasPrices = priceRows.length > 1
  const hasPrice = metrics.LatestPrice != null || numberOrNull(market.latest_price) !== null
  const downloadReport = () => {
    if (!history.data) return
    downloadTextFile(`${safeFileName(name)}-report.md`, buildCompanyReport({
      name,
      ticker,
      companyCode: canonicalCode || undefined,
      industry: company.industry ? String(company.industry) : undefined,
      market: company.market ? String(company.market) : undefined,
      description: businessDescription ? [businessDescription.slice(0, 4000), descriptionProvenance && `Source: ${descriptionProvenance}`].filter(Boolean).join('\n\n') : undefined,
      snapshotPeriod: metricPeriod || undefined,
      snapshotGroups,
      metrics,
      formatSnapshotMetric: formatMetric,
      history: history.data,
    }), 'text/markdown;charset=utf-8')
  }
  const refresh = canRefreshPrice && ticker
    ? <button type="button" className="icon-button quote__refresh" disabled={updatePrice.isPending} onClick={() => updatePrice.mutate()} title="Fetch the latest price from the provider" aria-label="Refresh price"><RefreshCw className={updatePrice.isPending ? 'spin' : undefined} /></button>
    : undefined
  const facts: Array<[string, ReactNode, string?]> = [
    ['Industry', String(company.industry ?? '')],
    ['Market', String(company.market ?? '')],
    ['EDINET code', canonicalCode, 'Identifier the FSA’s EDINET disclosure system assigns to every filer.'],
    ['Securities code', ticker, 'Exchange code used for prices (five digits including the check digit).'],
    ['Reporting currency', currencies.reporting ?? ''],
    ['Latest fiscal year', metricPeriod ? `Ends ${formatDay(metricPeriod)}` : '', 'Statement-based metrics use the latest annual filing with values.'],
    ['Price data', hasPrice ? [stringOrNull(market.price_provider), market.price_basis === 'adjusted' ? 'split-adjusted' : stringOrNull(market.price_basis), formatDay(market.latest_price_date)].filter(Boolean).join(' · ') : 'No stored prices'],
  ]
  const links = [
    ...(canonicalCode ? [
      { label: 'Filing Explorer', href: `/filings?company=${encodeURIComponent(canonicalCode)}&from=analysis`, hint: 'Read the original reports in Japanese and English' },
      { label: 'Compare with peers', href: `/compare?companies=${encodeURIComponent(canonicalCode)}`, hint: 'Open Comparison with this company preselected (P)' },
    ] : []),
    ...(researchCode && ticker ? [{ label: 'Option pricing', href: `/research?tab=options&company=${encodeURIComponent(researchCode)}`, hint: 'Black–Scholes prices and strategies from this company’s price and volatility' }] : []),
    ...(canonicalCode ? [{ label: 'Credit and bonds', href: `/research?tab=bonds&company=${encodeURIComponent(canonicalCode)}`, hint: 'Default risk (Merton, Altman Z) and a default-adjusted bond price' }] : []),
    ...(yahooSymbol ? [{ label: 'Yahoo Finance', href: `https://finance.yahoo.com/quote/${encodeURIComponent(yahooSymbol)}/`, external: true, hint: 'Quote, news, and profile' }] : []),
    ...(exchangeCode ? [
      { label: 'Yahoo! Finance Japan', href: `https://finance.yahoo.co.jp/quote/${encodeURIComponent(yahooSymbol)}`, external: true, hint: 'Japanese quote page with disclosures and forum' },
      { label: 'Kabutan', href: `https://kabutan.jp/stock/?code=${encodeURIComponent(exchangeCode)}`, external: true, hint: 'Japanese earnings summaries and news' },
    ] : []),
  ]
  return <div className="analysis">
    <div ref={header} className="analysis-header">
      <div className="analysis-header__id">
        <span className="eyebrow">Company analysis</span>
        <h1>{name}</h1>
        <p className="analysis-header__meta">
          {[ticker && <Tip key="ticker" content="Securities code">{ticker}</Tip>, canonicalCode && <Tip key="code" content="EDINET code">{canonicalCode}</Tip>, company.industry ? String(company.industry) : null, company.market ? String(company.market) : null].filter(Boolean).map((part, index) => <span key={index}>{part}</span>)}
          {researchCode && <ResearchBadge code={researchCode} price={metrics.LatestPrice ?? numberOrNull(market.latest_price)} priceCurrency={currencies.price} onClick={() => { const panel = document.getElementById('your-research'); panel?.scrollIntoView({ behavior: 'smooth', block: 'center' }) }} />}
        </p>
      </div>
      <Quote market={market} metrics={metrics} formatMetric={formatMetric} refresh={refresh} />
    </div>
    {updatePrice.isError && <div className="callout callout--warning" role="alert">Price refresh failed: {updatePrice.error instanceof Error ? updatePrice.error.message : 'unknown error'}</div>}
    {tickerOnly && <div className="callout callout--warning" role="status"><strong>“{tickerParam}” has no EDINET filings, so only its stored prices are shown{market.price_currency ? ` (in ${String(market.price_currency)})` : ''}.</strong> Companies and funds listed outside Japan do not file with EDINET. For a Japanese company, search by its name or EDINET code (/) to see statements and filings.</div>}

    <nav className="analysis-nav" aria-label="Analysis sections">
      <span className={headerHidden ? 'analysis-nav__id is-visible' : 'analysis-nav__id'} aria-hidden={!headerHidden}>
        <strong>{name}</strong>
        {hasPrice && <span className="mono">{formatMetric('LatestPrice', metrics.LatestPrice ?? numberOrNull(market.latest_price))}</span>}
      </span>
      {SECTIONS.map((section, index) => <a key={section.id} href={`#${section.id}`} className={activeSection === section.id ? 'active' : undefined} aria-current={activeSection === section.id ? 'location' : undefined} onClick={event => { event.preventDefault(); jumpTo(section.id) }}><kbd>{index + 1}</kbd>{section.label}</a>)}
      <span className="analysis-nav__spacer" />
      <div className="analysis-nav__actions">
        {params.get('from') === 'screen' && <ScreenTrailNav current={canonicalCode} />}
        {params.get('from') === 'portfolio' && <PortfolioTrailNav current={canonicalCode || tickerParam} />}
        <button type="button" className="button button--secondary button--small" disabled={!history.data} onClick={downloadReport} title={history.data ? 'Download a Markdown report with the snapshot and full financial history' : 'Financial history is still loading'}><Download aria-hidden="true" />Report</button>
        {canonicalCode && <Link className="button button--secondary button--small" to={`/compare?companies=${encodeURIComponent(canonicalCode)}`} title="Compare with peers (P)"><GitCompare aria-hidden="true" />Compare</Link>}
        {ticker && <Link className="button button--primary button--small" to={`/backtest?symbol=${encodeURIComponent(ticker)}`} title="Backtest this ticker (B)"><BarChart3 aria-hidden="true" />Backtest</Link>}
        <button type="button" className="icon-button analysis-nav__keys" onClick={() => setShowShortcuts(true)} title="Keyboard shortcuts (?)" aria-label="Keyboard shortcuts"><Keyboard aria-hidden="true" /></button>
      </div>
    </nav>

    <Section id="overview" title="Overview" hideTitle>
      <div className={hasPrices ? 'overview-grid' : 'overview-grid overview-grid--no-price'}>
        {ticker && (hasPrices || prices.isLoading) && <section className="panel panel--price" aria-label="Price history">
          {prices.isLoading ? <LoadingState label="Loading prices" /> : <PricePanel rows={priceRows} formatPrice={formatPrice} />}
        </section>}
        <section className="panel panel--stats" aria-labelledby="key-stats-title">
          <header className="panel__header">
            <h3 id="key-stats-title">Key statistics</h3>
            {metricPeriod && <Tip content="Statement-based figures come from the latest annual filing with values; valuation uses the latest stored price." className="panel__meta">FY ending {formatDay(metricPeriod)}</Tip>}
          </header>
          {!hasPrice && <p className="panel__note">{ticker ? 'No stored prices for this ticker yet, so valuation ratios are unavailable.' : 'Not listed: there is no market price, so valuation ratios do not apply.'}</p>}
          <KeyStats metrics={metrics} definitions={metricDefinitions} format={formatMetric} period={metricPeriod} hasPrice={hasPrice} />
        </section>
        <section className="panel panel--profile" aria-label="Company profile">
          <Profile description={businessDescription} provenance={descriptionProvenance} facts={facts} links={links} />
        </section>
        {researchCode && <section id="your-research" className="panel panel--research" aria-labelledby="your-research-title">
          <header className="panel__header">
            <h3 id="your-research-title">Your research</h3>
            <span className="panel__meta">Private to your account · the same notes, tags, and targets as the Research page</span>
            <Link className="panel__link" to={`/research?company=${encodeURIComponent(researchCode)}`}>Open in Research</Link>
          </header>
          <CompanyResearchPanel key={researchCode} code={researchCode} price={metrics.LatestPrice ?? numberOrNull(market.latest_price)} priceCurrency={currencies.price} compact tagRef={tagInput} noteRef={noteInput} keys={{ tag: 'T', note: 'N' }} />
        </section>}
      </div>
    </Section>

    <Section id="financials" title="Financial statements" aside={<span className="analysis-section__note">Annual figures from XBRL filings. Click lines to chart them.</span>}>
      <FinancialHistoryWorkspace history={history.data} isLoading={history.isLoading} error={history.error} retry={() => { void history.refetch() }} downloadPrefix={canonicalCode || ticker} currency={currencies.reporting} />
    </Section>

    {canonicalCode && <Section id="filings" title="Filings" aside={<span className="analysis-section__note">Retained EDINET annual reports with their XBRL data.</span>}>
      <FilingsPanel companyCode={canonicalCode} />
    </Section>}

    {showShortcuts && <ShortcutsDialog groups={SHORTCUTS} onClose={closeShortcuts} />}
  </div>
}
