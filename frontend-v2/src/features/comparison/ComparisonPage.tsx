import { keepPreviousData, useQuery } from '@tanstack/react-query'
import { ArrowLeft, Check, Download, Keyboard, Link2 } from 'lucide-react'
import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { Link, useNavigate, useSearchParams } from 'react-router-dom'

import { apiPost, apiRequest, queryString } from '../../api/client'
import { searchCompanies } from '../../components/CompanyPicker'
import { ErrorState, LoadingState } from '../../components/Feedback'
import { PageHeader } from '../../components/Page'
import { ShortcutsDialog, type ShortcutGroup } from '../../components/ShortcutsDialog'
import { useHotkeys } from '../../hooks/useHotkeys'
import { usePersistentState } from '../../hooks/usePersistentState'
import { groupMetrics, type MetricDefinition } from '../../metrics'
import { downloadTextFile, safeFileName } from '../analysis/downloads'
import { RankingChart, ScatterChart, TrendChart } from './ComparisonCharts'
import { ComparisonMatrix } from './ComparisonMatrix'
import { companyName, comparisonCsv, currencyNote, describeColumnMetric, emptyMetrics, fiscalYearNote, formatPeriod, MAX_COMPANIES, orderMetrics, parseCodes, shortName, sortCompanies } from './comparisonModel'
import type { CompanyInfo, ComparisonResponse, MetricCatalogResponse, Peer, PeersResponse, TrendsResponse } from './comparisonTypes'
import { CompanyPanel } from './CompanyPanel'
import { ComparisonStart } from './ComparisonStart'
import { MetricBar } from './MetricBar'
import { PeersPanel } from './PeersPanel'
import { SavedComparisons } from './SavedComparisons'
import './comparison.css'

const SHORTCUTS: ShortcutGroup[] = [
  { title: 'Anywhere', shortcuts: [
    { keys: ['/'], label: 'Search companies' },
    { keys: ['?'], label: 'Show or hide this list' },
    { keys: ['Esc'], label: 'Close a menu or leave a field' },
    { keys: ['1', '2', '3', '4'], label: 'Jump to Companies, Metrics, Table, Charts' },
  ] },
  { title: 'Companies', shortcuts: [
    { keys: ['A'], label: 'Add a company' },
    { keys: ['P'], label: 'Go to the suggested peers (↑/↓, then Enter adds)' },
    { keys: ['Shift+P'], label: 'Add the closest peer' },
    { keys: ['[', ']'], label: 'Move the company earlier or later' },
    { keys: ['Shift+X'], label: 'Remove the company' },
    { keys: ['O'], label: 'Open a saved comparison' },
    { keys: ['Ctrl+S'], label: 'Save this comparison' },
  ] },
  { title: 'Table', shortcuts: [
    { keys: ['↑', '↓'], label: 'Move between metrics (also J, K)' },
    { keys: ['←', '→'], label: 'Move between companies (also H, L)' },
    { keys: ['Enter'], label: 'Open the company in Analysis (Shift: new tab)' },
    { keys: ['S'], label: 'Sort companies by the metric, best first' },
    { keys: ['X'], label: 'Hide the metric' },
    { keys: ['M'], label: 'Add a metric' },
    { keys: ['E'], label: 'Show or hide metrics no company reports' },
    { keys: ['R'], label: 'Show or hide ranks' },
    { keys: ['I'], label: 'Index the trend chart to 100' },
    { keys: ['D'], label: 'Download the table as CSV' },
  ] },
]

const DEFAULT_SCATTER = { x: 'PriceToBook', y: 'ReturnOnEquity' }
// Up to this many companies, the charts sit beside the table on wide screens.
const SIDE_BY_SIDE = 4

function sameList(a: string[], b: string[]) {
  return a.length === b.length && a.every((item, index) => item === b[index])
}

export default function ComparisonPage() {
  const navigate = useNavigate()
  const [params, setParams] = useSearchParams()
  const fromScreen = params.get('source') === 'screen'
  const codes = useMemo(() => parseCodes(params.get('companies')), [params])
  const sortedCodes = useMemo(() => [...codes].sort(), [codes])
  // Links carry only what differs from the standard metrics: ``hide=PERatio,…`` and one ``add=Table.Column`` each.
  const hidden = useMemo(() => new Set((params.get('hide') ?? '').split(',').filter(Boolean)), [params])
  const added = useMemo(() => [...new Set(params.getAll('add').filter(Boolean))], [params])

  const catalog = useQuery({
    queryKey: ['comparison-metrics'],
    queryFn: () => apiRequest<MetricCatalogResponse>('/api/comparison/metrics'),
    staleTime: 10 * 60_000,
  })
  const standard = useMemo(() => catalog.data?.default_metrics ?? Object.keys(catalog.data?.definitions ?? {}), [catalog.data])
  const selectedMetrics = useMemo(() => [...standard.filter(metric => !hidden.has(metric)), ...added.filter(metric => !standard.includes(metric))], [added, hidden, standard])
  const custom = useMemo(() => selectedMetrics.filter(metric => !standard.includes(metric)), [selectedMetrics, standard])

  // Every standard metric is fetched, so showing or hiding one needs no round trip.
  const snapshot = useQuery({
    queryKey: ['comparison-snapshot', sortedCodes, custom],
    queryFn: () => apiPost<ComparisonResponse>('/api/comparison/snapshot', { company_codes: codes, metrics: [...standard, ...custom] }),
    enabled: codes.length >= 2 && !catalog.isLoading,
    placeholderData: keepPreviousData,
    staleTime: 5 * 60_000,
  })
  const peers = useQuery({
    queryKey: ['comparison-peers', sortedCodes],
    queryFn: () => apiRequest<PeersResponse>(`/api/comparison/peers${queryString({ codes: codes.join(','), limit: 24 })}`),
    enabled: codes.length > 0,
    placeholderData: keepPreviousData,
    staleTime: 5 * 60_000,
  })
  const trends = useQuery({
    queryKey: ['comparison-trends', sortedCodes, custom],
    queryFn: () => apiPost<TrendsResponse>('/api/comparison/trends', { company_codes: codes, metrics: custom }),
    enabled: codes.length >= 2,
    placeholderData: keepPreviousData,
    staleTime: 5 * 60_000,
  })

  const [known, setKnown] = useState<Record<string, CompanyInfo>>({})
  const remember = useCallback((items: CompanyInfo[]) => setKnown(previous => {
    const next = { ...previous }
    for (const item of items) next[item.company_code] = { ...previous[item.company_code], ...item }
    return next
  }), [])
  const result = snapshot.data
  const fromSnapshot = useMemo(() => Object.fromEntries((result?.companies ?? []).map(company => [company.company_code, {
    company_code: company.company_code,
    company_name: company.company.company_name,
    ticker: company.company.ticker,
    industry: company.company.industry,
  }])), [result])
  // Codes from a link (Analysis sends one) are named by a search until a comparison names them.
  const unnamed = codes.filter(code => !known[code] && !fromSnapshot[code])
  const lookup = useQuery({
    queryKey: ['comparison-lookup', unnamed],
    queryFn: async () => Object.fromEntries((await Promise.all(unnamed.map(async code => {
      const found = (await searchCompanies(code, 8).catch(() => ({ results: [] }))).results.find(company => company.company_code === code)
      return found ? [[code, { company_code: code, company_name: found.company_name, ticker: found.ticker, industry: found.industry }]] : []
    }))).flat()) as Record<string, CompanyInfo>,
    enabled: unnamed.length > 0 && (codes.length < 2 || !snapshot.isFetching),
    staleTime: Infinity,
  })
  const info: Record<string, CompanyInfo | undefined> = { ...lookup.data, ...known, ...fromSnapshot }

  const update = useCallback((next: { codes?: string[]; metrics?: string[] }) => {
    setParams(current => {
      const updated = new URLSearchParams(current)
      if (next.codes) {
        if (next.codes.length) updated.set('companies', next.codes.join(','))
        else updated.delete('companies')
      }
      if (next.metrics) {
        updated.delete('hide')
        updated.delete('add')
        const hide = standard.filter(metric => !next.metrics?.includes(metric))
        if (hide.length) updated.set('hide', hide.join(','))
        orderMetrics(next.metrics, standard).filter(metric => !standard.includes(metric)).forEach(metric => updated.append('add', metric))
      }
      return updated
    }, { replace: true })
  }, [setParams, standard])

  const definitions = useMemo<Record<string, MetricDefinition>>(() => ({
    ...result?.metric_definitions,
    ...catalog.data?.definitions,
    ...Object.fromEntries(custom.map(metric => [metric, describeColumnMetric(metric)])),
  }), [catalog.data, custom, result])

  const [sortMetric, setSortMetric] = useState<string | null>(null)
  const [showRanks, setShowRanks] = usePersistentState('comparison.ranks', false, [true, false])
  const [hideEmpty, setHideEmpty] = usePersistentState('comparison.hideEmpty', true, [true, false])
  const [indexed, setIndexed] = usePersistentState('comparison.indexTrend', false, [true, false])
  const [scatter, setScatter] = usePersistentState('comparison.scatter', DEFAULT_SCATTER)
  const [cursor, setCursor] = useState<{ metric: string | null; code: string | null }>({ metric: null, code: null })
  const [focusRequest, setFocusRequest] = useState(0)
  const [trendPick, setTrendPick] = useState<{ metric: string; atCursor: string | null } | null>(null)
  const [showShortcuts, setShowShortcuts] = useState(false)
  const [saved, setSaved] = useState<{ open: boolean; saving: boolean }>({ open: false, saving: false })
  const [copied, setCopied] = useState(false)

  const byCode = useMemo(() => new Map((result?.companies ?? []).map(company => [company.company_code, company])), [result])
  const ordered = codes.map(code => byCode.get(code)).filter((company): company is NonNullable<typeof company> => Boolean(company))
  const colorIndex = Object.fromEntries(codes.map((code, index) => [code, index]))
  const activeSort = sortMetric && selectedMetrics.includes(sortMetric) ? sortMetric : null
  const viewCompanies = activeSort ? sortCompanies(ordered, activeSort, definitions[activeSort]?.direction) : ordered
  const empty = ordered.length ? emptyMetrics(selectedMetrics, ordered) : new Set<string>()
  const visible = hideEmpty ? selectedMetrics.filter(metric => !empty.has(metric)) : selectedMetrics
  // The table groups rows, so the cursor moves in the grouped order.
  const rows = groupMetrics(visible, definitions).flatMap(group => group.metrics)
  const viewCodes = viewCompanies.map(company => company.company_code)
  // The cursor starts on the first metric where better and worse mean something, so the ranking chart opens on it.
  const cursorMetric = cursor.metric && rows.includes(cursor.metric) ? cursor.metric : rows.find(metric => definitions[metric]?.direction) ?? rows[0] ?? null
  const cursorCode = cursor.code && codes.includes(cursor.code) ? cursor.code : viewCodes[0] ?? codes[0] ?? null
  const trendMetrics = trends.data?.metrics ?? []
  const trendMetric = trendPick && trendPick.atCursor === cursorMetric ? trendPick.metric
    : cursorMetric && trendMetrics.includes(cursorMetric) ? cursorMetric
      : trendPick?.metric ?? 'Revenue'
  const scatterChoices = [...standard, ...custom]
  const scatterAxes = { x: scatterChoices.includes(scatter.x) ? scatter.x : DEFAULT_SCATTER.x, y: scatterChoices.includes(scatter.y) ? scatter.y : DEFAULT_SCATTER.y }

  const addInput = useRef<HTMLInputElement>(null)
  const metricInput = useRef<HTMLInputElement>(null)
  const peersBody = useRef<HTMLTableSectionElement>(null)
  const companiesSection = useRef<HTMLDivElement>(null)
  const metricsSection = useRef<HTMLDivElement>(null)
  const tableSection = useRef<HTMLElement>(null)
  const chartsSection = useRef<HTMLDivElement>(null)

  const addCompanies = (items: CompanyInfo[]) => {
    const fresh = items.filter(item => !codes.includes(item.company_code)).slice(0, MAX_COMPANIES - codes.length)
    if (!fresh.length) return
    remember(fresh)
    update({ codes: [...codes, ...fresh.map(item => item.company_code)] })
  }
  const addPeers = (items: Peer[]) => addCompanies(items.map(peer => ({ company_code: peer.company_code, company_name: peer.company_name, ticker: peer.ticker, industry: peer.industry })))
  const removeCompany = (code: string | null) => {
    if (!code) return
    const index = viewCodes.indexOf(code)
    setCursor(current => ({ ...current, code: viewCodes[index + 1] ?? viewCodes[index - 1] ?? null }))
    update({ codes: codes.filter(item => item !== code) })
  }
  const moveCompany = (code: string | null, offset: number) => {
    if (!code) return
    const index = codes.indexOf(code)
    const target = index + offset
    if (index < 0 || target < 0 || target >= codes.length) return
    const next = [...codes]
    next.splice(index, 1)
    next.splice(target, 0, code)
    setSortMetric(null)
    setCursor(current => ({ ...current, code }))
    update({ codes: next })
  }
  const setMetrics = (metrics: string[]) => update({ metrics })
  const hideMetric = (metric: string | null) => {
    if (!metric) return
    const index = rows.indexOf(metric)
    setCursor(current => ({ ...current, metric: rows[index + 1] ?? rows[index - 1] ?? null }))
    setMetrics(selectedMetrics.filter(item => item !== metric))
  }
  const focusMetric = (metric: string) => setCursor(current => ({ ...current, metric }))
  const moveRow = (offset: number | 'first' | 'last', focus = false) => {
    if (!rows.length) return
    const index = Math.max(0, rows.indexOf(cursorMetric ?? ''))
    const next = offset === 'first' ? 0 : offset === 'last' ? rows.length - 1 : Math.max(0, Math.min(rows.length - 1, index + offset))
    setCursor(current => ({ ...current, metric: rows[next] }))
    if (focus) setFocusRequest(count => count + 1)
  }
  const moveColumn = (offset: number, focus = false) => {
    if (!viewCodes.length) return
    const index = Math.max(0, viewCodes.indexOf(cursorCode ?? ''))
    setCursor(current => ({ ...current, code: viewCodes[Math.max(0, Math.min(viewCodes.length - 1, index + offset))] }))
    if (focus) setFocusRequest(count => count + 1)
  }
  const toggleSort = (metric: string | null) => { if (metric) setSortMetric(activeSort === metric ? null : metric) }
  const openCompany = (code: string, newTab = false) => {
    const path = `/analyze/${encodeURIComponent(code)}`
    if (newTab) window.open(path, '_blank', 'noopener')
    else navigate(path)
  }
  const download = () => {
    if (!viewCompanies.length) return
    const name = viewCompanies.slice(0, 3).map(company => shortName(companyName(company))).join(' vs ')
    downloadTextFile(`${safeFileName(`comparison ${name}`)}.csv`, comparisonCsv(viewCompanies, rows, definitions), 'text/csv;charset=utf-8')
  }
  const copyLink = async () => {
    try {
      await navigator.clipboard.writeText(window.location.href)
      setCopied(true)
      window.setTimeout(() => setCopied(false), 1800)
    } catch { /* clipboard unavailable: the address bar still has the link */ }
  }
  const jump = (section: HTMLElement | null, focus?: () => void) => {
    section?.scrollIntoView?.({ block: 'start', behavior: 'smooth' })
    focus?.()
  }
  const focusPeers = () => {
    const row = peersBody.current?.querySelector<HTMLElement>('tr[tabindex="0"]')
    row?.focus({ preventScroll: true })
    row?.scrollIntoView?.({ block: 'nearest' })
  }
  const loadSaved = (savedCodes: string[], metrics: string[]) => {
    setSortMetric(null)
    setCursor({ metric: null, code: null })
    update({ codes: parseCodes(savedCodes.join(',')), metrics: metrics.length ? metrics : standard })
  }
  const setSavedOpen = useCallback((open: boolean) => setSaved({ open, saving: false }), [])

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (!(event.ctrlKey || event.metaKey) || event.altKey || event.shiftKey || event.key.toLowerCase() !== 's') return
      event.preventDefault()
      setSaved({ open: true, saving: true })
    }
    window.addEventListener('keydown', onKeyDown)
    return () => window.removeEventListener('keydown', onKeyDown)
  }, [])

  useHotkeys({
    '?': () => setShowShortcuts(true),
    1: () => jump(companiesSection.current, () => addInput.current?.focus({ preventScroll: true })),
    2: () => jump(metricsSection.current),
    3: () => jump(tableSection.current, () => setFocusRequest(count => count + 1)),
    4: () => jump(chartsSection.current),
    a: () => addInput.current?.focus(),
    m: () => metricInput.current?.focus(),
    p: focusPeers,
    P: () => { const closest = peers.data?.peers.find(peer => !codes.includes(peer.company_code)); if (closest) addPeers([closest]) },
    j: () => moveRow(1, true),
    k: () => moveRow(-1, true),
    l: () => moveColumn(1, true),
    h: () => moveColumn(-1, true),
    s: () => toggleSort(cursorMetric),
    x: () => hideMetric(cursorMetric),
    X: () => removeCompany(cursorCode),
    '[': () => moveCompany(cursorCode, -1),
    ']': () => moveCompany(cursorCode, 1),
    e: () => setHideEmpty(!hideEmpty),
    r: () => setShowRanks(!showRanks),
    i: () => setIndexed(!indexed),
    d: download,
    o: () => setSaved({ open: true, saving: false }),
  }, !showShortcuts && !saved.open)

  const industries = [...new Set(codes.map(code => info[code]?.industry).filter(Boolean))]
  const notes = [fiscalYearNote(ordered), currencyNote(ordered), result?.missing.length ? `No financial data for ${result.missing.join(', ')}.` : null].filter(Boolean)
  const loadingFirst = codes.length >= 2 && !result && (snapshot.isLoading || catalog.isLoading)
  const pendingCodes = codes.filter(code => result && !byCode.has(code) && !result.missing.includes(code))
  const sideBySide = viewCompanies.length <= SIDE_BY_SIDE
  const periods = new Set(ordered.map(company => company.period_end ?? ''))
  const commonPeriod = periods.size === 1 ? ordered[0]?.period_end : null

  return <div className="stack dense-page cmp-page">
    <PageHeader
      eyebrow="Company research"
      title="Compare companies"
      description={codes.length ? [`${codes.length} ${codes.length === 1 ? 'company' : 'companies'}`, ordered.length ? `${rows.length} metrics` : '', industries.slice(0, 2).join(', ') + (industries.length > 2 ? ` +${industries.length - 2}` : '')].filter(Boolean).join(' · ') : 'Line up two to twelve companies metric by metric, with peers, rankings, and trends.'}
      actions={<>
        {fromScreen && <Link className="button button--ghost button--small" to="/screen"><ArrowLeft aria-hidden="true" />Return to Screening</Link>}
        <SavedComparisons open={saved.open} saving={saved.saving} codes={codes} metrics={sameList(selectedMetrics, standard) ? [] : selectedMetrics} defaultName={codes.map(code => shortName(info[code]?.company_name || code)).slice(0, 4).join(' vs ')} onOpenChange={setSavedOpen} onLoad={loadSaved} />
        <button type="button" className="button button--secondary button--small" disabled={!codes.length} onClick={() => void copyLink()} title="Copy a link to this comparison">{copied ? <Check aria-hidden="true" /> : <Link2 aria-hidden="true" />}{copied ? 'Copied' : 'Link'}</button>
        <button type="button" className="button button--secondary button--small" disabled={!viewCompanies.length} onClick={download} title="Download the table as CSV (D)"><Download aria-hidden="true" />CSV</button>
        <button type="button" className="icon-button" onClick={() => setShowShortcuts(true)} title="Keyboard shortcuts (?)" aria-label="Keyboard shortcuts"><Keyboard aria-hidden="true" /></button>
      </>}
    />
    <div className="cmp-top">
      <div ref={companiesSection} className="cmp-section">
        <CompanyPanel
          codes={codes}
          info={info}
          periods={Object.fromEntries(ordered.map(company => [company.company_code, company.period_end]))}
          cursorCode={ordered.length ? cursorCode : null}
          inputRef={addInput}
          actions={codes.length > 0 && <button type="button" className="button button--ghost button--small" onClick={() => { setSortMetric(null); update({ codes: [] }) }}>Clear</button>}
          onAdd={company => company.company_code && addCompanies([{ company_code: company.company_code, company_name: company.company_name, ticker: company.ticker, industry: company.industry }])}
          onRemove={removeCompany}
          onMove={moveCompany}
          onCursor={code => setCursor(current => ({ ...current, code }))}
        />
      </div>
      <PeersPanel codes={codes} info={info} colorIndex={colorIndex} data={codes.length ? peers.data && { ...peers.data, peers: peers.data.peers.filter(peer => !codes.includes(peer.company_code)) } : undefined} loading={peers.isFetching} error={peers.error} listRef={peersBody} onAdd={addPeers} />
    </div>
    <div ref={metricsSection} className="cmp-section">
      <MetricBar standard={standard} definitions={definitions} selected={selectedMetrics} catalog={catalog.data?.tables ?? {}} searchRef={metricInput} onChange={setMetrics} onFocusMetric={focusMetric} />
      {catalog.error && <p className="form-error">Could not load the metric catalog; the comparison uses the standard metrics.</p>}
    </div>
    {codes.length === 0 && <ComparisonStart onLoad={loadSaved} />}
    {codes.length === 1 && <p className="cmp-start">Add one more company to see the table, rankings, a scatter plot, and trends.</p>}
    {loadingFirst && <LoadingState label="Comparing companies" />}
    {snapshot.error && !result && <ErrorState error={snapshot.error} retry={() => void snapshot.refetch()} />}
    {codes.length >= 2 && ordered.length > 0 && <div className={sideBySide ? 'cmp-results cmp-results--side' : 'cmp-results'}>
      <section ref={tableSection} className="panel cmp-panel cmp-table-panel" aria-labelledby="cmp-table-title">
        <header className="cmp-panel__header">
          <h2 id="cmp-table-title">Comparison <kbd aria-hidden="true">3</kbd></h2>
          <span className="cmp-panel__meta">{commonPeriod ? `FY ending ${formatPeriod(commonPeriod)} · ` : ''}<span className="cmp-legend cmp-legend--best" />best <span className="cmp-legend cmp-legend--worst" />worst{activeSort ? ` · sorted by ${definitions[activeSort]?.label ?? activeSort}` : ''}</span>
          {(snapshot.isFetching || pendingCodes.length > 0) && <span className="cmp-panel__meta cmp-updating" role="status">Updating…</span>}
          <span className="cmp-panel__spacer" />
          {activeSort && <button type="button" className="button button--ghost button--small" onClick={() => setSortMetric(null)}>Original order</button>}
          <label className="inline-toggle" title="Show each company's rank in every row (R)"><input type="checkbox" checked={showRanks} onChange={event => setShowRanks(event.target.checked)} />Ranks</label>
          <label className="inline-toggle" title="Hide metrics no company reports (E)"><input type="checkbox" checked={hideEmpty} onChange={event => setHideEmpty(event.target.checked)} />Hide empty{empty.size ? ` (${empty.size})` : ''}</label>
        </header>
        {notes.length > 0 && <ul className="cmp-notes">{notes.map(note => <li key={note}>{note}</li>)}</ul>}
        {rows.length ? <ComparisonMatrix
          companies={viewCompanies}
          colorIndex={colorIndex}
          metrics={rows}
          definitions={definitions}
          cursorMetric={cursorMetric}
          cursorCode={cursorCode}
          showRanks={showRanks}
          sortMetric={activeSort}
          focusRequest={focusRequest}
          onCursor={next => setCursor(current => ({ metric: next.metric ?? current.metric, code: next.code ?? current.code }))}
          onMoveRow={offset => moveRow(offset)}
          onMoveColumn={offset => moveColumn(offset)}
          onOpen={openCompany}
          onSort={toggleSort}
          onRemoveMetric={hideMetric}
        /> : <p className="cmp-panel__empty">No metrics shown. Turn some on above, or press M to search.</p>}
      </section>
      <div ref={chartsSection} className="cmp-charts" aria-label="Charts">
        <RankingChart companies={viewCompanies} metric={cursorMetric} definitions={definitions} colorIndex={colorIndex} />
        <ScatterChart companies={viewCompanies} axes={scatterAxes} choices={scatterChoices} definitions={definitions} colorIndex={colorIndex} onAxes={setScatter} />
        <TrendChart companies={viewCompanies} trends={trends.data} loading={trends.isLoading} metric={trendMetric} indexed={indexed} colorIndex={colorIndex} onMetric={metric => setTrendPick({ metric, atCursor: cursorMetric })} onIndexed={setIndexed} />
      </div>
    </div>}
    {showShortcuts && <ShortcutsDialog groups={SHORTCUTS} onClose={() => setShowShortcuts(false)} />}
  </div>
}
