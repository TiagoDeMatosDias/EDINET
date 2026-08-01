import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { FileUp, RefreshCw, Upload } from 'lucide-react'
import { useCallback, useMemo, useState } from 'react'
import { useNavigate } from 'react-router-dom'

import { apiRequest, queryString } from '../../api/client'
import { Card, Field, PageHeader } from '../../components/Page'
import { PortfolioActivity } from './PortfolioActivity'
import { PortfolioDetailContent } from './PortfolioDetailContent'
import { PortfolioDrawer } from './PortfolioDrawer'
import { PortfolioHoldings } from './PortfolioHoldings'
import { PortfolioIncome } from './PortfolioIncome'
import { PortfolioOverview } from './PortfolioOverview'
import { PortfolioPerformance } from './PortfolioPerformance'
import { buildPortfolioSummary, money, percent, performanceStart, sliceValueHistory } from './portfolioFormat'
import { StatButton } from './PortfolioPrimitives'
import type {
  ContributionData,
  DividendCurrencyHistory,
  DividendGrowthData,
  DividendHistory,
  HeatmapData,
  Holding,
  HoldingHistoryPoint,
  Performance,
  PerformanceRange,
  PieData,
  PortfolioDetail,
  PortfolioTab,
  ScatterPoint,
  Transaction,
  ValueHistory,
} from './portfolioTypes'

const RANGE_LABELS: Record<PerformanceRange, string> = {
  all: 'All history',
  '5y': '5 years',
  '3y': '3 years',
  '1y': '1 year',
  ytd: 'Year to date',
}

const TABS: Array<{ id: PortfolioTab; label: string }> = [
  { id: 'overview', label: 'Overview' },
  { id: 'holdings', label: 'Holdings' },
  { id: 'performance', label: 'Performance' },
  { id: 'income', label: 'Income' },
  { id: 'activity', label: 'Activity' },
]

function detailMeta(detail?: PortfolioDetail | null) {
  if (!detail) return { title: '', eyebrow: '' }
  if (detail.kind === 'value') return { title: 'Portfolio value', eyebrow: 'Valuation detail', description: 'Current wealth, cash flows, and the complete value history.' }
  if (detail.kind === 'performance') return { title: 'Performance methodology', eyebrow: 'Return detail', description: 'Return composition, consistency, and the assumptions behind each statistic.' }
  if (detail.kind === 'risk') return { title: 'Risk diagnostics', eyebrow: 'Risk detail', description: 'Drawdown, volatility, tail loss, and the daily return distribution.' }
  if (detail.kind === 'allocation') return { title: 'Exposure map', eyebrow: 'Allocation detail', description: 'Position and native-currency concentration at current market values.' }
  if (detail.kind === 'income') return { title: 'Income detail', eyebrow: 'Dividend analysis', description: 'Gross income, withholding, net cash received, and payer concentration.' }
  if (detail.kind === 'activity') return { title: 'Activity summary', eyebrow: 'Ledger detail', description: 'Imported records by type and the latest portfolio events.' }
  if (detail.kind === 'holding') return { title: detail.holding.symbol, eyebrow: 'Position detail', description: detail.holding.performance?.name || detail.holding.asset_category || 'Portfolio position' }
  return { title: detail.transaction.symbol || detail.transaction.activity_type || 'Transaction', eyebrow: 'Transaction detail', description: detail.transaction.trade_date || 'Imported activity record' }
}

function PortfolioPulse({ summary, performance, currency, rangeLabel, isLoading, onOpenDetail }: {
  summary: ReturnType<typeof buildPortfolioSummary>
  performance?: Performance
  currency: string
  rangeLabel: string
  isLoading: boolean
  onOpenDetail: (detail: PortfolioDetail) => void
}) {
  return <div className="portfolio-pulse">
    <StatButton label="Portfolio value" value={isLoading ? '—' : money(summary.totalValue, currency)} detail={isLoading ? 'Loading positions' : `${summary.positionCount} positions`} onClick={() => onOpenDetail({ kind: 'value' })} />
    <StatButton label="Total return" value={percent(performance?.total_return)} detail="Time-weighted" tone={Number(performance?.total_return) >= 0 ? 'positive' : 'negative'} onClick={() => onOpenDetail({ kind: 'performance' })} />
    <StatButton label="Annualized return" value={percent(performance?.annualized_return)} detail={rangeLabel} tone={Number(performance?.annualized_return) >= 0 ? 'positive' : 'negative'} onClick={() => onOpenDetail({ kind: 'performance' })} />
    <StatButton label="Max drawdown" value={percent(performance?.max_drawdown)} detail="Peak-to-trough" tone="negative" onClick={() => onOpenDetail({ kind: 'risk' })} />
    <StatButton label="Net dividends" value={money(performance?.dividend_breakdown?.total_net, currency)} detail={percent(performance?.return_attribution?.dividend_yield) + ' contribution'} onClick={() => onOpenDetail({ kind: 'income' })} />
    <StatButton label="Cash reserve" value={isLoading ? '—' : money(summary.cashValue, currency)} detail={isLoading ? 'Loading balances' : percent(summary.cashWeight)} onClick={() => onOpenDetail({ kind: 'allocation' })} />
  </div>
}

function filterHeatmap(data?: HeatmapData, startDate?: string) {
  if (!data || !startDate) return data
  const startYear = Number(startDate.slice(0, 4))
  const indexes = data.years.map((year, index) => ({ year, index })).filter(row => row.year >= startYear)
  return { ...data, years: indexes.map(row => row.year), values: indexes.map(row => data.values[row.index]) }
}

function currencyOptions(data?: Array<{ code?: string } | string>) {
  return data?.map(item => typeof item === 'string' ? item : item.code ?? '').filter(Boolean) ?? ['EUR', 'USD', 'JPY']
}

export default function PortfolioWorkspace() {
  const [tab, setTab] = useState<PortfolioTab>('overview')
  const [currency, setCurrency] = useState('EUR')
  const [range, setRange] = useState<PerformanceRange>('all')
  const [includeClosed, setIncludeClosed] = useState(false)
  const [dividendPeriod, setDividendPeriod] = useState<'monthly' | 'quarterly' | 'yearly'>('quarterly')
  const [detail, setDetail] = useState<PortfolioDetail | null>(null)
  const [status, setStatus] = useState('')
  const queryClient = useQueryClient()
  const navigate = useNavigate()
  const closeDetail = useCallback(() => setDetail(null), [])
  const openDetail = useCallback((next: PortfolioDetail) => setDetail(next), [])
  const suffix = queryString({ display_currency: currency })

  const currencies = useQuery({ queryKey: ['portfolio-currencies'], queryFn: () => apiRequest<Array<{ code?: string } | string>>('/api/portfolio/display-currencies'), retry: false })
  const dateRange = useQuery({ queryKey: ['portfolio-date-range'], queryFn: () => apiRequest<{ min_date?: string | null; max_date?: string | null }>('/api/portfolio/date-range'), retry: false })
  const activity = useQuery({ queryKey: ['portfolio-activity'], queryFn: () => apiRequest<{ by_activity: Record<string, number> }>('/api/portfolio/activity-summary'), retry: false })
  const transactions = useQuery({ queryKey: ['portfolio-transactions'], queryFn: () => apiRequest<Transaction[]>('/api/portfolio/transactions?limit=1000'), retry: false })
  const holdings = useQuery({
    queryKey: ['portfolio-holdings', currency, includeClosed],
    queryFn: () => apiRequest<Holding[]>(`/api/portfolio/holdings/performance${queryString({ display_currency: currency, include_closed: includeClosed })}`),
    retry: false,
  })
  const rangeStart = performanceStart(range, dateRange.data?.max_date ?? undefined)
  const performance = useQuery({
    queryKey: ['portfolio-performance', currency, rangeStart, dateRange.data?.max_date],
    queryFn: () => apiRequest<Performance>(`/api/portfolio/performance${queryString({ base_currency: currency, start_date: rangeStart, end_date: rangeStart ? dateRange.data?.max_date : undefined })}`),
    retry: false,
  })
  const valueHistory = useQuery({ queryKey: ['portfolio-value-history', currency], queryFn: () => apiRequest<ValueHistory>(`/api/portfolio/charts/portfolio-value-history${suffix}`), retry: false })
  const allocation = useQuery({ queryKey: ['portfolio-allocation', currency], queryFn: () => apiRequest<PieData>(`/api/portfolio/charts/holdings-by-value${suffix}`), retry: false })
  const currencyExposure = useQuery({ queryKey: ['portfolio-currency-chart', currency], queryFn: () => apiRequest<PieData>(`/api/portfolio/charts/holdings-by-currency${suffix}`), retry: false })

  const incomeEnabled = tab === 'income' || detail?.kind === 'income'
  const performanceEnabled = tab === 'performance'
  const dividendsByCurrency = useQuery({ queryKey: ['portfolio-dividends-currency', currency, dividendPeriod], queryFn: () => apiRequest<DividendCurrencyHistory>(`/api/portfolio/charts/dividends-by-currency${suffix}&period=${dividendPeriod}`), retry: false, enabled: incomeEnabled })
  const dividendsByCompany = useQuery({ queryKey: ['portfolio-dividends-by-company', currency], queryFn: () => apiRequest<DividendHistory>(`/api/portfolio/charts/dividends-by-company${suffix}&period=yearly`), retry: false, enabled: incomeEnabled })
  const dividendGrowth = useQuery({ queryKey: ['portfolio-dividend-growth'], queryFn: () => apiRequest<DividendGrowthData>('/api/portfolio/dividends/yoy/per-company'), retry: false, enabled: incomeEnabled })
  const heatmap = useQuery({ queryKey: ['portfolio-return-heatmap', currency], queryFn: () => apiRequest<HeatmapData>(`/api/portfolio/charts/returns-heatmap${suffix}`), retry: false, enabled: performanceEnabled })
  const scatter = useQuery({ queryKey: ['portfolio-return-cost', currency], queryFn: () => apiRequest<ScatterPoint[]>(`/api/portfolio/charts/return-vs-cost${suffix}`), retry: false, enabled: performanceEnabled })
  const contribution = useQuery({ queryKey: ['portfolio-contribution', currency], queryFn: () => apiRequest<ContributionData>(`/api/portfolio/returns/contribution${queryString({ base_currency: currency })}`), retry: false, enabled: performanceEnabled })

  const detailHolding = detail?.kind === 'holding' ? detail.holding : undefined
  const detailIsCash = detailHolding?.asset_category === 'CASH' || detailHolding?.symbol.startsWith('CASH')
  const holdingHistory = useQuery({
    queryKey: ['portfolio-holding-history', detailHolding?.symbol],
    queryFn: () => apiRequest<HoldingHistoryPoint[]>(`/api/portfolio/holdings/${encodeURIComponent(detailHolding?.symbol ?? '')}/history`),
    enabled: Boolean(detailHolding?.symbol) && !detailIsCash,
    retry: false,
  })

  const invalidate = useCallback(() => queryClient.invalidateQueries({ predicate: query => String(query.queryKey[0]).startsWith('portfolio') }), [queryClient])
  const rebuild = useMutation({
    mutationFn: () => apiRequest<{ daily_rows?: number; holdings_count?: number }>(`/api/portfolio/rebuild${queryString({ base_currency: currency })}`, { method: 'POST' }),
    onMutate: () => setStatus('Rebuilding portfolio state…'),
    onSuccess: async result => {
      setStatus(`Rebuilt ${result.holdings_count ?? 0} holdings across ${(result.daily_rows ?? 0).toLocaleString()} daily rows.`)
      await invalidate()
    },
    onError: error => setStatus(error instanceof Error ? error.message : 'Rebuild failed'),
  })
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
  const rangedHistory = useMemo(() => sliceValueHistory(valueHistory.data, rangeStart), [rangeStart, valueHistory.data])
  const rangedHeatmap = useMemo(() => filterHeatmap(heatmap.data, rangeStart), [heatmap.data, rangeStart])
  const metadata = detailMeta(detail)
  const unavailable = holdings.isError && activity.isError

  return <div className="stack dense-page portfolio-workspace">
    <PageHeader eyebrow="Portfolio intelligence" title="Portfolio" description="A decision-ready view of wealth, exposures, performance, income, and portfolio activity." actions={<div className="portfolio-header-actions">
      <Field label="Performance period"><select className="select" value={range} onChange={event => setRange(event.target.value as PerformanceRange)}>{Object.entries(RANGE_LABELS).map(([value, label]) => <option key={value} value={value}>{label}</option>)}</select></Field>
      <Field label="Display currency"><select className="select" value={currency} onChange={event => setCurrency(event.target.value)}>{currencyOptions(currencies.data).map(code => <option key={code}>{code}</option>)}</select></Field>
      <button className="button button--secondary" disabled={rebuild.isPending} onClick={() => rebuild.mutate()}><RefreshCw />Rebuild</button>
      <label className="button button--ghost file-button"><Upload />Import<input type="file" accept=".xml,text/xml" multiple onChange={event => void uploadFiles(event.target.files)} /></label>
    </div>} />
    {status && <div className="inline-status" role="status">{status}</div>}
    {unavailable ? <Card title="Connect portfolio activity"><label className="file-drop"><FileUp /><strong>Import IBKR FlexQuery XML</strong><input type="file" accept=".xml,text/xml" multiple onChange={event => void uploadFiles(event.target.files)} /></label></Card> : <>
      <PortfolioPulse summary={summary} performance={performance.data} currency={currency} rangeLabel={RANGE_LABELS[range]} isLoading={holdings.isLoading} onOpenDetail={openDetail} />
      <nav className="step-tabs portfolio-tabs" aria-label="Portfolio sections">{TABS.map(item => <button key={item.id} className={tab === item.id ? 'active' : ''} aria-pressed={tab === item.id} onClick={() => setTab(item.id)}>{item.label}</button>)}</nav>
      {tab === 'overview' && <PortfolioOverview summary={summary} performance={performance.data} valueHistory={rangedHistory} allocation={allocation.data} currencies={currencyExposure.data} activity={activity.data?.by_activity ?? {}} transactions={transactions.data ?? []} currency={currency} onOpenDetail={openDetail} />}
      {tab === 'holdings' && <PortfolioHoldings data={holdings.data ?? []} summary={summary} currency={currency} includeClosed={includeClosed} isLoading={holdings.isLoading} error={holdings.error} onIncludeClosed={setIncludeClosed} onOpenDetail={openDetail} />}
      {tab === 'performance' && <PortfolioPerformance performance={performance.data} valueHistory={rangedHistory} heatmap={rangedHeatmap} scatter={scatter.data} contribution={contribution.data} allocation={allocation.data} holdings={(holdings.data ?? []).filter(holding => holding.is_open !== false)} currency={currency} onOpenDetail={openDetail} />}
      {tab === 'income' && <PortfolioIncome performance={performance.data} byCurrency={dividendsByCurrency.data} byCompany={dividendsByCompany.data} growth={dividendGrowth.data} currency={currency} period={dividendPeriod} onPeriod={setDividendPeriod} onOpenDetail={openDetail} />}
      {tab === 'activity' && <PortfolioActivity data={transactions.data ?? []} activity={activity.data?.by_activity ?? {}} dateRange={dateRange.data} isLoading={transactions.isLoading} error={transactions.error} onOpenDetail={openDetail} />}
    </>}
    <PortfolioDrawer open={Boolean(detail)} eyebrow={metadata.eyebrow} title={metadata.title} description={metadata.description} onClose={closeDetail}>
      {detail && <PortfolioDetailContent detail={detail} performance={performance.data} summary={summary} valueHistory={rangedHistory} allocation={allocation.data} currencies={currencyExposure.data} dividends={dividendsByCompany.data} activity={activity.data?.by_activity ?? {}} transactions={transactions.data ?? []} holdingHistory={holdingHistory.data} holdingHistoryLoading={holdingHistory.isLoading} currency={currency} onAnalyze={symbol => navigate(`/analyze?q=${encodeURIComponent(symbol)}`)} />}
    </PortfolioDrawer>
  </div>
}
