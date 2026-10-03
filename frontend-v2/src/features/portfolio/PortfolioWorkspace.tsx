import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { AlertTriangle, CheckCircle2, CloudDownload, FileUp, Keyboard, RefreshCw, Upload } from 'lucide-react'
import { useCallback, useMemo, useRef, useState } from 'react'
import { useNavigate } from 'react-router-dom'

import { apiRequest, queryString } from '../../api/client'
import { Card } from '../../components/Page'
import { ShortcutsDialog, type ShortcutGroup } from '../../components/ShortcutsDialog'
import { Tip } from '../../components/Tooltip'
import { useHotkeys } from '../../hooks/useHotkeys'
import { usePersistentState } from '../../hooks/usePersistentState'
import { useAuth } from '../auth/authContext'
import { PortfolioActivity } from './PortfolioActivity'
import { PortfolioData } from './PortfolioData'
import { PortfolioDetailContent } from './PortfolioDetailContent'
import { PortfolioDrawer } from './PortfolioDrawer'
import { PortfolioHoldings } from './PortfolioHoldings'
import { PortfolioIncome, type IncomeView } from './PortfolioIncome'
import { PortfolioOverview } from './PortfolioOverview'
import { PortfolioPerformance } from './PortfolioPerformance'
import { buildPortfolioSummary, compactMoney, decimal, formatDay, holdingAnalysisHref, holdingName, isCash, money, percent, performanceStart, RANGES, signedPercent } from './portfolioFormat'
import { StatButton } from './PortfolioPrimitives'
import { readPortfolioTrail, trailEntry, writePortfolioTrail } from './portfolioTrail'
import type {
  BenchmarkChoice,
  ContributionData,
  DataQuality,
  Holding,
  HoldingHistoryPoint,
  IncomeData,
  Performance,
  PerformanceRange,
  PieData,
  PortfolioDetail,
  PortfolioTab,
  RefreshResult,
  Transaction,
} from './portfolioTypes'

const TABS: Array<{ id: PortfolioTab; label: string }> = [
  { id: 'overview', label: 'Overview' },
  { id: 'holdings', label: 'Holdings' },
  { id: 'performance', label: 'Performance' },
  { id: 'income', label: 'Income' },
  { id: 'activity', label: 'Activity' },
  { id: 'data', label: 'Data & method' },
]
const TAB_IDS = TABS.map(tab => tab.id)
const RANGE_KEYS = RANGES.map(range => range.key)

const SHORTCUTS: ShortcutGroup[] = [
  { title: 'Anywhere on this page', shortcuts: [
    { keys: ['1', '2', '3', '4', '5', '6'], label: 'Overview, Holdings, Performance, Income, Activity, Data & method' },
    { keys: ['-', '='], label: 'Longer or shorter period' },
    { keys: ['C'], label: 'Choose the display currency' },
    { keys: ['B'], label: 'Choose the benchmark' },
    { keys: ['F'], label: 'Find a holding, record, or paying company' },
    { keys: ['R'], label: 'Rebuild the portfolio from your activity' },
    { keys: ['Shift+R'], label: 'Refresh market prices, then rebuild (operators)' },
    { keys: ['I'], label: 'Import an IBKR Flex Query file' },
    { keys: ['?'], label: 'Show or hide this list' },
  ] },
  { title: 'Holdings and activity lists', shortcuts: [
    { keys: ['↓', 'J'], label: 'Enter the list, then move down' },
    { keys: ['↑', 'K'], label: 'Move up' },
    { keys: ['Home', 'End'], label: 'First or last row' },
    { keys: ['Enter'], label: 'Open the details' },
    { keys: ['A'], label: 'Open the holding in Analysis' },
    { keys: ['[', ']'], label: 'Previous or next page' },
  ] },
  { title: 'Income', shortcuts: [
    { keys: ['F'], label: 'Choose companies (type, ↑/↓, Enter)' },
    { keys: ['X'], label: 'Show every payer again' },
    { keys: ['Enter'], label: 'In the payers list: show only that company' },
    { keys: ['A'], label: 'In the payers list: add or remove it' },
  ] },
  { title: 'Holding details', shortcuts: [
    { keys: ['Shift+J', 'Shift+K'], label: 'Next or previous holding' },
    { keys: ['A'], label: 'Open in Analysis (Shift+J/K step on from there)' },
    { keys: ['Esc'], label: 'Close' },
  ] },
]

function currencyOptions(data?: Array<{ code?: string } | string>) {
  const codes = data?.map(item => typeof item === 'string' ? item : item.code ?? '').filter(Boolean)
  return codes?.length ? codes : ['EUR', 'USD', 'JPY']
}

function detailMeta(detail: PortfolioDetail | null) {
  if (!detail) return { title: '', eyebrow: '' }
  if (detail.kind === 'holding') {
    const holding = detail.holding
    return { title: holding.symbol, eyebrow: isCash(holding) ? 'Cash balance' : 'Holding', description: holdingName(holding) || holding.asset_category || 'Portfolio position' }
  }
  if (detail.kind === 'transaction') return { title: detail.transaction.symbol || detail.transaction.activity_type || 'Transaction', eyebrow: 'Ledger record', description: formatDay(detail.transaction.trade_date) }
  return { title: '', eyebrow: '' }
}

function DataStatus({ quality, onOpen }: { quality?: DataQuality; onOpen: () => void }) {
  if (!quality) return null
  const errors = quality.issues.filter(issue => issue.level === 'error').length
  const warnings = quality.issues.filter(issue => issue.level === 'warning').length
  const label = errors ? `${errors} data problem${errors === 1 ? '' : 's'}` : warnings ? `${warnings} data warning${warnings === 1 ? '' : 's'}` : 'Data checks pass'
  const Icon = errors || warnings ? AlertTriangle : CheckCircle2
  return <button type="button" className={`pf-status ${errors ? 'is-error' : warnings ? 'is-warning' : 'is-ok'}`} onClick={onOpen} title="Open Data & method (6)">
    <Icon aria-hidden="true" />{label}
  </button>
}

export default function PortfolioWorkspace() {
  const [tab, setTab] = usePersistentState<PortfolioTab>('portfolio.tab', 'overview', TAB_IDS)
  const [currency, setCurrency] = usePersistentState('portfolio.currency', 'EUR')
  const [range, setRange] = usePersistentState<PerformanceRange>('portfolio.range', 'all', RANGE_KEYS)
  const [storedBenchmark, setBenchmark] = usePersistentState('portfolio.benchmark', 'VWCE')
  // Until a benchmark is chosen, the default falls back to the first one with stored prices.
  const [benchmarkChosen] = useState(() => {
    try { return localStorage.getItem('portfolio.benchmark') !== null } catch { return false }
  })
  const [riskFreeOverride, setRiskFreeOverride] = usePersistentState<number | null>('portfolio.riskFreeOverride', null)
  const [includeClosed, setIncludeClosed] = useState(false)
  const [incomeSymbols, setIncomeSymbols] = usePersistentState<string[]>('portfolio.incomeCompanies', [])
  const [incomeView, setIncomeView] = usePersistentState<IncomeView>('portfolio.incomeView', { grouping: 'quarterly', measure: 'net', stack: 'company' })
  const [detail, setDetail] = useState<PortfolioDetail | null>(null)
  const [status, setStatus] = useState('')
  const [showShortcuts, setShowShortcuts] = useState(false)
  const [holdingOrder, setHoldingOrder] = useState<Holding[]>([])
  // Coming back from Analysis lands on the holding last opened.
  const [returnTo] = useState(() => {
    const trail = readPortfolioTrail()
    if (!trail?.refocus || !trail.current) return undefined
    writePortfolioTrail({ ...trail, refocus: false })
    return trail.entries.find(entry => entry.key === trail.current)?.symbol
  })
  const queryClient = useQueryClient()
  const navigate = useNavigate()
  const auth = useAuth()
  const canRefresh = auth.status?.mode === 'disabled' || auth.user?.role === 'admin' || auth.user?.role === 'operator'
  const currencySelect = useRef<HTMLSelectElement>(null)
  const benchmarkSelect = useRef<HTMLSelectElement>(null)
  const importInput = useRef<HTMLInputElement>(null)
  const closeDetail = useCallback(() => setDetail(null), [])
  const openDetail = useCallback((next: PortfolioDetail) => setDetail(next), [])
  const closeShortcuts = useCallback(() => setShowShortcuts(false), [])
  const suffix = queryString({ display_currency: currency })

  const currencies = useQuery({ queryKey: ['portfolio-currencies'], queryFn: () => apiRequest<Array<{ code?: string } | string>>('/api/portfolio/display-currencies'), retry: false })
  const benchmarks = useQuery({ queryKey: ['portfolio-benchmarks'], queryFn: () => apiRequest<BenchmarkChoice[]>('/api/portfolio/benchmarks'), retry: false })
  const quality = useQuery({ queryKey: ['portfolio-data-quality', currency], queryFn: () => apiRequest<DataQuality>(`/api/portfolio/data-quality${suffix}`), retry: false })
  const dateRange = useQuery({ queryKey: ['portfolio-date-range'], queryFn: () => apiRequest<{ min_date?: string | null; max_date?: string | null }>('/api/portfolio/date-range'), retry: false })
  const activity = useQuery({ queryKey: ['portfolio-activity'], queryFn: () => apiRequest<{ by_activity: Record<string, number> }>('/api/portfolio/activity-summary'), retry: false })
  const transactions = useQuery({ queryKey: ['portfolio-transactions'], queryFn: () => apiRequest<Transaction[]>('/api/portfolio/transactions?limit=10000'), retry: false })
  const holdings = useQuery({
    queryKey: ['portfolio-holdings', currency, includeClosed],
    queryFn: () => apiRequest<Holding[]>(`/api/portfolio/holdings/performance${queryString({ display_currency: currency, include_closed: includeClosed })}`),
    retry: false,
  })
  const defaultUnavailable = !benchmarkChosen && benchmarks.data?.some(choice => choice.ticker === storedBenchmark && !choice.available)
  const benchmark = defaultUnavailable ? benchmarks.data?.find(choice => choice.available)?.ticker ?? storedBenchmark : storedBenchmark
  const valuationDate = quality.data?.valuation_date ?? dateRange.data?.max_date ?? undefined
  const rangeStart = performanceStart(range, valuationDate)
  const performance = useQuery({
    queryKey: ['portfolio-performance', currency, rangeStart, valuationDate, benchmark, riskFreeOverride],
    enabled: !quality.isLoading && !benchmarks.isLoading,
    queryFn: () => apiRequest<Performance>(`/api/portfolio/performance${queryString({
      base_currency: currency,
      start_date: rangeStart,
      end_date: rangeStart ? valuationDate : undefined,
      benchmark_ticker: benchmark || undefined,
      risk_free_rate: riskFreeOverride ?? undefined,
    })}`),
    retry: false,
  })
  const allocation = useQuery({ queryKey: ['portfolio-allocation', currency], queryFn: () => apiRequest<PieData>(`/api/portfolio/charts/holdings-by-value${suffix}`), retry: false })
  const currencyExposure = useQuery({ queryKey: ['portfolio-currency-chart', currency], queryFn: () => apiRequest<PieData>(`/api/portfolio/charts/holdings-by-currency${suffix}`), retry: false })

  const income = useQuery({ queryKey: ['portfolio-income', currency], queryFn: () => apiRequest<IncomeData>(`/api/portfolio/income${suffix}`), retry: false, enabled: tab === 'income' })
  const contribution = useQuery({ queryKey: ['portfolio-contribution', currency], queryFn: () => apiRequest<ContributionData>(`/api/portfolio/returns/contribution${queryString({ base_currency: currency })}`), retry: false, enabled: tab === 'performance' })

  const detailHolding = detail?.kind === 'holding' ? detail.holding : undefined
  const holdingHistory = useQuery({
    queryKey: ['portfolio-holding-history', detailHolding?.symbol],
    queryFn: () => apiRequest<HoldingHistoryPoint[]>(`/api/portfolio/holdings/${encodeURIComponent(detailHolding?.symbol ?? '')}/history`),
    enabled: Boolean(detailHolding?.symbol) && !(detailHolding && isCash(detailHolding)),
    retry: false,
  })

  const invalidate = useCallback(() => queryClient.invalidateQueries({ predicate: query => String(query.queryKey[0]).startsWith('portfolio') }), [queryClient])
  const rebuild = useMutation({
    mutationFn: () => apiRequest<{ daily_rows?: number; holdings_count?: number }>(`/api/portfolio/rebuild${queryString({ base_currency: currency })}`, { method: 'POST' }),
    onMutate: () => setStatus('Rebuilding the portfolio from your activity…'),
    onSuccess: async result => {
      setStatus(`Rebuilt ${result.holdings_count ?? 0} holdings over ${(result.daily_rows ?? 0).toLocaleString()} days.`)
      await invalidate()
    },
    onError: error => setStatus(error instanceof Error ? error.message : 'Rebuild failed'),
  })
  const refresh = useMutation({
    mutationFn: () => apiRequest<RefreshResult>(`/api/portfolio/refresh-market-data${queryString({ base_currency: currency, benchmark: benchmark || undefined })}`, { method: 'POST' }),
    onMutate: () => setStatus('Fetching the latest prices, exchange rates, and interest rates — this takes a minute or two…'),
    onSuccess: async result => {
      const parts = [`Updated prices for ${result.updated.length} of ${result.tickers} tickers`]
      if (result.refetched_daily_history?.length) parts.push(`fetched daily history for ${result.refetched_daily_history.join(', ')}`)
      if (result.relabelled_prices) parts.push(`corrected the currency of ${result.relabelled_prices.toLocaleString()} stored prices`)
      if (result.failed.length) parts.push(`could not update ${result.failed.join(', ')}`)
      setStatus(`${parts.join('; ')}. Rebuilt the portfolio.`)
      await invalidate()
    },
    onError: error => setStatus(error instanceof Error ? error.message : 'Price refresh failed'),
  })
  const busy = rebuild.isPending || refresh.isPending
  const uploadFiles = async (files: FileList | null) => {
    if (!files?.length) return
    setStatus(`Importing ${files.length} file${files.length === 1 ? '' : 's'}…`)
    try {
      for (const file of Array.from(files)) {
        const form = new FormData()
        form.set('file', file)
        await apiRequest('/api/portfolio/upload', { method: 'POST', body: form })
      }
      setStatus(`${files.length} file${files.length === 1 ? '' : 's'} imported. Rebuilding…`)
      await rebuild.mutateAsync()
    } catch (error) {
      setStatus(error instanceof Error ? error.message : 'Import failed')
    }
  }

  const summary = useMemo(() => buildPortfolioSummary(holdings.data ?? [], allocation.data), [allocation.data, holdings.data])
  const openHoldings = useMemo(() => (holdings.data ?? []).filter(holding => holding.is_open !== false && !isCash(holding)), [holdings.data])
  const stepList = holdingOrder.length ? holdingOrder.filter(holding => !isCash(holding)) : openHoldings
  const benchmarkLabel = performance.data?.benchmark?.available ? performance.data.benchmark.ticker : undefined
  const unavailable = holdings.isError && activity.isError

  const openAnalysis = useCallback((holding: Holding) => {
    if (isCash(holding)) return
    const list = (holdingOrder.length ? holdingOrder : openHoldings).filter(item => !isCash(item))
    const entries = list.map(trailEntry)
    if (!entries.some(entry => entry.symbol === holding.symbol)) entries.push(trailEntry(holding))
    writePortfolioTrail({ entries, current: trailEntry(holding).key, refocus: true })
    navigate(holdingAnalysisHref(holding))
  }, [holdingOrder, navigate, openHoldings])
  const stepHolding = (offset: number) => {
    if (!detailHolding) return
    const index = stepList.findIndex(holding => holding.symbol === detailHolding.symbol)
    const next = stepList[index + offset]
    if (next) setDetail({ kind: 'holding', holding: next })
  }
  const stepRange = (offset: number) => {
    const index = RANGE_KEYS.indexOf(range)
    setRange(RANGE_KEYS[Math.max(0, Math.min(RANGE_KEYS.length - 1, index + offset))])
  }

  const pageKeysActive = !detail && !showShortcuts
  useHotkeys({
    ...Object.fromEntries(TABS.map((item, index) => [String(index + 1), () => setTab(item.id)])),
    '-': () => stepRange(1),
    '=': () => stepRange(-1),
    '+': () => stepRange(-1),
    c: () => currencySelect.current?.focus(),
    b: () => benchmarkSelect.current?.focus(),
    f: () => {
      if (tab === 'income') return document.querySelector<HTMLInputElement>('[data-income-find]')?.focus()
      if (tab !== 'holdings' && tab !== 'activity') setTab('holdings')
      window.setTimeout(() => document.querySelector<HTMLInputElement>('[data-portfolio-find]')?.focus(), 0)
    },
    x: () => { if (tab === 'income') setIncomeSymbols([]) },
    r: () => { if (!busy) rebuild.mutate() },
    R: () => { if (!busy && canRefresh) refresh.mutate() },
    i: () => importInput.current?.click(),
    '?': () => setShowShortcuts(true),
  }, pageKeysActive)
  useHotkeys({
    a: () => { if (detailHolding) openAnalysis(detailHolding) },
    J: () => stepHolding(1),
    K: () => stepHolding(-1),
  }, Boolean(detailHolding) && !showShortcuts)

  const metadata = detailMeta(detail)
  const performanceData = performance.data
  const period = performanceData?.period
  const lastPoint = performanceData?.series?.at(-1)
  const bench = performanceData?.benchmark?.available ? performanceData.benchmark : undefined
  const stepIndex = detailHolding ? stepList.findIndex(holding => holding.symbol === detailHolding.symbol) : -1

  return <div className="stack dense-page portfolio-workspace">
    <header className="pf-header">
      <div className="pf-header__title">
        <span className="eyebrow">Portfolio</span>
        <h1>Portfolio</h1>
        <p className="pf-header__meta">
          {quality.data?.valuation_date ? <Tip content="The last day the ledger was valued. Rebuild (R) after importing activity; refresh prices (Shift+R) to extend it to today.">Valued as of {formatDay(quality.data.valuation_date)}</Tip> : 'Not valued yet'}
          {period && <span>{formatDay(period.start)} – {formatDay(period.end)}</span>}
          <DataStatus quality={quality.data} onOpen={() => setTab('data')} />
          <button type="button" className="icon-button" onClick={() => setShowShortcuts(true)} title="Keyboard shortcuts (?)" aria-label="Keyboard shortcuts"><Keyboard aria-hidden="true" /></button>
        </p>
      </div>
      <div className="pf-header__controls">
        <div className="range-buttons" role="group" aria-label="Performance period">
          {RANGES.map(option => <button key={option.key} type="button" className={range === option.key ? 'active' : ''} aria-pressed={range === option.key} title={option.title} onClick={() => setRange(option.key)}>{option.label}</button>)}
          <span className="range-buttons__keys" aria-hidden="true"><kbd>-</kbd><kbd>=</kbd></span>
        </div>
        <label className="pf-select" title="Display currency (C)">
          <span>Currency</span>
          <select ref={currencySelect} className="select" value={currency} onChange={event => setCurrency(event.target.value)} aria-label="Display currency">{currencyOptions(currencies.data).map(code => <option key={code}>{code}</option>)}</select>
          <kbd aria-hidden="true">C</kbd>
        </label>
        <label className="pf-select" title="Compare against (B)">
          <span>Benchmark</span>
          <select ref={benchmarkSelect} className="select" value={benchmark} onChange={event => setBenchmark(event.target.value)} aria-label="Benchmark">
            <option value="">None</option>
            {(benchmarks.data ?? []).map(choice => <option key={choice.ticker} value={choice.ticker} disabled={!choice.available && !canRefresh}>{choice.ticker} · {choice.label}{choice.available ? '' : ' (no prices yet)'}</option>)}
            {benchmark && !(benchmarks.data ?? []).some(choice => choice.ticker === benchmark) && <option value={benchmark}>{benchmark}</option>}
          </select>
          <kbd aria-hidden="true">B</kbd>
        </label>
        <div className="pf-header__actions">
          {canRefresh && <button type="button" className="button button--secondary button--small" disabled={busy} onClick={() => refresh.mutate()} title="Fetch the latest prices for every holding and the benchmark, plus exchange and interest rates, then rebuild (Shift+R)"><CloudDownload className={refresh.isPending ? 'spin' : undefined} />Refresh prices</button>}
          <button type="button" className="button button--ghost button--small" disabled={busy} onClick={() => rebuild.mutate()} title="Revalue every day from your activity and the stored prices (R)"><RefreshCw className={rebuild.isPending ? 'spin' : undefined} />Rebuild</button>
          <label className="button button--ghost button--small file-button" title="Import an IBKR Flex Query XML file (I)"><Upload />Import<input ref={importInput} type="file" accept=".xml,text/xml" multiple onChange={event => void uploadFiles(event.target.files)} /></label>
        </div>
      </div>
    </header>
    {status && <div className="inline-status" role="status">{status}</div>}
    {unavailable ? <Card title="Connect portfolio activity"><label className="file-drop"><FileUp /><strong>Import IBKR FlexQuery XML</strong><input type="file" accept=".xml,text/xml" multiple onChange={event => void uploadFiles(event.target.files)} /></label></Card> : <>
      <div className="portfolio-pulse">
        <StatButton label="Portfolio value" tip="Market value of every holding and cash balance on the valuation date, in the display currency." value={holdings.isLoading ? '—' : money(summary.totalValue, currency)} detail={lastPoint ? `${signedPercent(lastPoint.invested ? lastPoint.value / lastPoint.invested - 1 : null, 0)} on ${compactMoney(lastPoint.invested, currency)} put in` : `${summary.positionCount} holdings`} onClick={() => setTab('holdings')} />
        <StatButton label="Total return" tip="Time-weighted: daily returns with deposits and withdrawals removed, chained over the period. It measures the investments, not the timing of your deposits." value={signedPercent(performanceData?.total_return)} detail={bench ? `${bench.ticker} ${signedPercent(bench.total_return)}` : 'Time-weighted'} tone={Number(performanceData?.total_return) >= 0 ? 'positive' : 'negative'} onClick={() => setTab('performance')} />
        <StatButton label="Annual return" tip="The compound annual growth rate of the time-weighted return. Periods shorter than a year are not annualized. Money-weighted is the internal rate of return of your actual deposits and withdrawals." value={performanceData?.annualized_return != null ? signedPercent(performanceData.annualized_return) : '—'} detail={performanceData?.annualized_return == null && period ? 'Under a year: not annualized' : `Money-weighted ${percent(performanceData?.money_weighted_return)}`} tone={Number(performanceData?.annualized_return) >= 0 ? 'positive' : 'negative'} onClick={() => setTab('performance')} />
        <StatButton label="Volatility" tip="Standard deviation of weekday returns, annualized (× √261)." value={percent(performanceData?.volatility)} detail={bench ? `${bench.ticker} ${percent(bench.volatility)}` : 'Annualized'} onClick={() => setTab('performance')} />
        <StatButton label="Max drawdown" tip="The largest fall from a previous high on the time-weighted path, so deposits and withdrawals do not count." value={percent(performanceData?.max_drawdown)} detail={performanceData?.max_dd_peak_date ? `${formatDay(performanceData.max_dd_peak_date)} → ${formatDay(performanceData.max_dd_trough_date)}` : 'Peak to trough'} tone="negative" onClick={() => setTab('performance')} />
        <StatButton label="Sharpe ratio" tip="Annualized return above cash, divided by volatility. Cash is the short-term government rate in the display currency (see Data & method)." value={decimal(performanceData?.sharpe_ratio)} detail={performanceData?.risk_free?.kind === 'missing' ? 'No rate data: 0% cash' : `Cash ${percent(performanceData?.risk_free_rate)} a year`} onClick={() => setTab('data')} />
      </div>
      <nav className="step-tabs portfolio-tabs" aria-label="Portfolio sections">{TABS.map((item, index) => <button key={item.id} type="button" className={tab === item.id ? 'active' : ''} aria-pressed={tab === item.id} onClick={() => setTab(item.id)} title={`${item.label} (${index + 1})`}><kbd aria-hidden="true">{index + 1}</kbd>{item.label}</button>)}</nav>
      {tab === 'overview' && <PortfolioOverview performance={performanceData} isLoading={performance.isLoading} summary={summary} holdings={openHoldings} allocation={allocation.data} currencies={currencyExposure.data} transactions={transactions.data ?? []} currency={currency} benchmarkLabel={benchmarkLabel} onOpenDetail={openDetail} onTab={setTab} />}
      {tab === 'holdings' && <PortfolioHoldings data={holdings.data ?? []} summary={summary} currency={currency} includeClosed={includeClosed} isLoading={holdings.isLoading} error={holdings.error} hotkeys={pageKeysActive} returnTo={returnTo} onIncludeClosed={setIncludeClosed} onOpenDetail={openDetail} onAnalyze={openAnalysis} onOrderChange={setHoldingOrder} />}
      {tab === 'performance' && <PortfolioPerformance performance={performanceData} isLoading={performance.isLoading} contribution={contribution.data} currency={currency} benchmarkLabel={benchmarkLabel} />}
      {tab === 'income' && <PortfolioIncome
        data={income.data}
        isLoading={income.isLoading}
        currency={currency}
        start={rangeStart}
        periodLabel={rangeStart ? `From ${formatDay(rangeStart)}` : 'All history'}
        selected={incomeSymbols}
        onSelect={setIncomeSymbols}
        view={incomeView}
        onView={setIncomeView}
        hotkeys={pageKeysActive}
        onAnalyze={company => navigate(holdingAnalysisHref({ symbol: company.symbol, performance: { edinet_code: company.edinet_code } }))}
      />}
      {tab === 'activity' && <PortfolioActivity data={transactions.data ?? []} activity={activity.data?.by_activity ?? {}} dateRange={dateRange.data} isLoading={transactions.isLoading} error={transactions.error} hotkeys={pageKeysActive} onOpenDetail={openDetail} />}
      {tab === 'data' && <PortfolioData quality={quality.data} isLoading={quality.isLoading} performance={performanceData} benchmarks={benchmarks.data ?? []} currency={currency} canRefresh={canRefresh} busy={busy} riskFreeOverride={riskFreeOverride} onRiskFreeOverride={setRiskFreeOverride} onRefresh={() => refresh.mutate()} onRebuild={() => rebuild.mutate()} />}
    </>}
    <PortfolioDrawer open={Boolean(detail)} eyebrow={metadata.eyebrow} title={metadata.title} description={metadata.description} onClose={closeDetail}>
      {detail && <PortfolioDetailContent
        detail={detail}
        summary={summary}
        holdingHistory={holdingHistory.data}
        holdingHistoryLoading={holdingHistory.isLoading}
        currency={currency}
        position={stepIndex >= 0 ? { index: stepIndex, count: stepList.length } : undefined}
        onStep={stepHolding}
        onAnalyze={openAnalysis}
        onIncome={symbol => { setIncomeSymbols([symbol]); setTab('income'); setDetail(null) }}
      />}
    </PortfolioDrawer>
    {showShortcuts && <ShortcutsDialog groups={SHORTCUTS} onClose={closeShortcuts} />}
  </div>
}
