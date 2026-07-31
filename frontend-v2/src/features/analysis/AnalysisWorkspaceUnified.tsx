import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { CategoryScale, Chart as ChartJS, Filler, Legend, LinearScale, LineElement, PointElement, Tooltip } from 'chart.js'
import { ArrowLeft, BarChart3, ExternalLink, Plus, RefreshCw, Star, X } from 'lucide-react'
import { useState } from 'react'
import { Line } from 'react-chartjs-2'
import { Link, useParams, useSearchParams } from 'react-router-dom'

import { apiPost, apiRequest, queryString } from '../../api/client'
import type { SecurityHistory, SecurityOverview } from '../../api/types'
import { BRAND_COLORS } from '../../brand'
import { EmptyState, ErrorState, LoadingState } from '../../components/Feedback'
import { Card, Metric, PageHeader } from '../../components/Page'
import { FinancialHistoryWorkspace } from './FinancialHistoryWorkspace'
import { filterPriceHistory, PRICE_RANGE_OPTIONS, type PriceHistoryRow, type PriceRangeKey } from './priceHistoryRanges'

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, Filler, Legend, Tooltip)
const PRICE_COLOR = BRAND_COLORS.midnight
const SNAPSHOT_GROUPS: Array<{ title: string; metrics: Array<[string, string]> }> = [
  { title: 'Market', metrics: [['LatestPrice', 'Price'], ['MarketCap', 'Market cap']] },
  { title: 'Valuation', metrics: [['PERatio', 'P/E'], ['PriceToBook', 'P/B'], ['PriceToSales', 'P/S'], ['EnterpriseValueToSales', 'EV/Sales'], ['DividendsYield', 'Dividend yield'], ['PayoutRatio', 'Payout ratio']] },
  { title: 'Quality', metrics: [['ReturnOnEquity', 'Return on equity'], ['ReturnOnAssets', 'Return on assets'], ['DebtToEquity', 'Debt/equity'], ['CurrentRatio', 'Current ratio'], ['GrossMargin', 'Gross margin'], ['OperatingMargin', 'Operating margin'], ['NetMargin', 'Net margin']] },
  { title: 'Income', metrics: [['Revenue', 'Revenue'], ['OperatingIncome', 'Operating income'], ['NetIncome', 'Net income']] },
  { title: 'Balance sheet', metrics: [['TotalAssets', 'Total assets'], ['TotalEquity', "Shareholders' equity"], ['SharesOutstanding', 'Shares outstanding']] },
]

interface FilingSummary {
  doc_id: string
  submitted_at?: string | null
  period_end?: string | null
  status: string
}

function compactNumber(value: number) {
  if (Math.abs(value) < 1_000) return value.toLocaleString(undefined, { maximumFractionDigits: 2 })
  const units: Array<[number, string]> = [[1e12, 'Trillion'], [1e9, 'Billion'], [1e6, 'Million'], [1e3, 'Thousand']]
  const [scale, label] = units.find(([threshold]) => Math.abs(value) >= threshold) ?? [1, '']
  return `${(value / scale).toLocaleString(undefined, { maximumFractionDigits: 2 })} ${label}`
}

function formatOverview(key: string, value: number | null | undefined) {
  if (value == null || Number.isNaN(value)) return '—'
  if (key === 'LatestPrice') return `¥${value.toLocaleString()}`
  if (['MarketCap', 'Revenue', 'OperatingIncome', 'NetIncome', 'TotalAssets', 'TotalEquity'].includes(key)) return `¥${compactNumber(value)}`
  if (['DividendsYield', 'PayoutRatio', 'ReturnOnAssets', 'ReturnOnEquity', 'GrossMargin', 'NetMargin', 'OperatingMargin'].includes(key)) return `${(value * 100).toFixed(1)}%`
  return value.toLocaleString(undefined, { maximumFractionDigits: 2 })
}

export function yahooFinanceSymbol(ticker: string) {
  const trimmed = ticker.trim()
  const japanese = /^\d{5}$/.test(trimmed) && trimmed.endsWith('0') ? trimmed.slice(0, -1) : trimmed
  return /^\d{4}$/.test(japanese) ? `${japanese}.T` : japanese
}

function PriceChart({ ticker }: { ticker: string }) {
  const [range, setRange] = useState<PriceRangeKey>('all')
  const prices = useQuery({
    queryKey: ['security-prices', ticker],
    enabled: Boolean(ticker),
    queryFn: () => apiRequest<{ prices: PriceHistoryRow[] }>(`/api/security/price-history${queryString({ ticker })}`),
  })
  const rows = prices.data?.prices ?? []
  if (prices.isLoading) return <LoadingState label="Loading prices" />
  if (!rows.length) return <EmptyState title="No price history" description="No prices found." />
  const visible = filterPriceHistory(rows, range)
  const data = {
    labels: visible.map(row => row.trade_date ?? row.Date ?? row.date ?? ''),
    datasets: [{ data: visible.map(row => row.Price ?? row.price ?? null), borderColor: PRICE_COLOR, backgroundColor: `${BRAND_COLORS.midnight}18`, fill: true, pointRadius: 0, tension: .2 }],
  }
  const options = { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } }, scales: { x: { grid: { display: false }, ticks: { autoSkip: true, maxTicksLimit: 9, maxRotation: 0 } }, y: { position: 'right' as const } } }
  return <div className="price-history-workspace"><div className="price-range-toolbar"><div className="price-range-buttons" role="group" aria-label="Price history range">{PRICE_RANGE_OPTIONS.map(option => <button key={option.key} type="button" className={range === option.key ? 'active' : ''} aria-pressed={range === option.key} onClick={() => setRange(option.key)}>{option.label}</button>)}</div><span>{visible.length.toLocaleString()} prices</span></div><div className="price-chart"><Line data={data} options={options} /></div></div>
}

function FilingSummaryCard({ companyCode }: { companyCode: string }) {
  const filings = useQuery({ queryKey: ['company-filings', companyCode], queryFn: () => apiRequest<{ filings: FilingSummary[] }>(`/api/filings/company/${encodeURIComponent(companyCode)}`) })
  return <Card title="XBRL filings" description="Retained EDINET type-1 reports for this company.">{filings.isLoading && <LoadingState label="Loading filings" />}{filings.data?.filings.slice(0, 8).map(filing => <Link className="filing-row" key={filing.doc_id} to={`/filings?doc=${encodeURIComponent(filing.doc_id)}&company=${encodeURIComponent(companyCode)}`}><span><strong>{filing.period_end || 'Period unavailable'}</strong><small>{filing.doc_id}</small></span><span><small>{filing.submitted_at || 'Submission unavailable'} · {filing.status}</small></span></Link>)}{filings.data && !filings.data.filings.length && <EmptyState title="No archived XBRL reports" description="Type-1 filing packages will appear after acquisition." />}<Link className="button button--ghost" to={`/filings?company=${encodeURIComponent(companyCode)}`}>Open Filing Explorer</Link></Card>
}

function SnapshotMetrics({ metrics }: { metrics: Record<string, number | null> }) {
  return <div className="company-snapshot-groups">{SNAPSHOT_GROUPS.map(group => <section className="company-snapshot-group" key={group.title}><h3>{group.title}</h3><dl className="company-snapshot-metrics">{group.metrics.map(([key, label]) => <div key={key}><dt>{label}</dt><dd>{formatOverview(key, metrics[key])}</dd></div>)}</dl></section>)}</div>
}

export default function AnalysisWorkspaceUnified() {
  const { companyCode: routeCompanyCode } = useParams()
  const [params] = useSearchParams()
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
  const canonicalCode = String(company.edinet_code ?? company.company_code ?? companyCode ?? '')
  const name = String(company.company_name ?? lookup ?? 'Company')
  const ticker = String(company.ticker ?? '')
  const history = useQuery({
    queryKey: ['security-history', canonicalCode],
    enabled: Boolean(canonicalCode),
    queryFn: () => apiRequest<SecurityHistory>(`/api/security/history${queryString({ company_code: canonicalCode, periods: 16 })}`),
    retry: false,
  })
  const metricPeriod = String(overview.data?.metadata?.comparison_metric_period_end ?? overview.data?.metadata?.last_financial_period_end ?? '')
  const updatePrice = useMutation({ mutationFn: () => apiPost('/api/security/update-price', { ticker }), onSuccess: () => queryClient.invalidateQueries({ queryKey: ['security-overview', lookupKey] }) })
  const [newTag, setNewTag] = useState('')
  const tags = useQuery({ queryKey: ['company-tags', canonicalCode], enabled: Boolean(canonicalCode), queryFn: () => apiRequest<{ tags: string[] }>(`/api/tags/${encodeURIComponent(canonicalCode)}`) })
  const addTag = useMutation({ mutationFn: (tag: string) => apiRequest(`/api/tags/${encodeURIComponent(canonicalCode)}/${encodeURIComponent(tag)}`, { method: 'POST' }), onSuccess: () => { void queryClient.invalidateQueries({ queryKey: ['company-tags', canonicalCode] }); void queryClient.invalidateQueries({ queryKey: ['research-tags'] }) } })
  const removeTag = useMutation({ mutationFn: (tag: string) => apiRequest(`/api/tags/${encodeURIComponent(canonicalCode)}/${encodeURIComponent(tag)}`, { method: 'DELETE' }), onSuccess: () => { void queryClient.invalidateQueries({ queryKey: ['company-tags', canonicalCode] }); void queryClient.invalidateQueries({ queryKey: ['research-tags'] }) } })
  const isFavorite = tags.data?.tags.includes('Favorite') ?? false
  const favorite = useMutation({
    mutationFn: () => apiRequest(`/api/tags/${encodeURIComponent(canonicalCode)}/Favorite`, { method: isFavorite ? 'DELETE' : 'POST' }),
    onSuccess: () => { void queryClient.invalidateQueries({ queryKey: ['company-tags', canonicalCode] }); void queryClient.invalidateQueries({ queryKey: ['research-tags'] }) },
  })

  if (!lookup) return <div className="stack dense-page analysis-empty-page"><PageHeader eyebrow="Company research" title="Analyze a company" description="Use the company search above to open price, statements, ratios, and trends." /><EmptyState title="Search for a company above" description="Enter a name, ticker, EDINET code, or industry and choose a result." /></div>
  if (overview.isLoading) return <LoadingState label="Loading company analysis" />
  if (overview.isError) return <ErrorState error={overview.error} retry={() => overview.refetch()} />
  const metricKeys = [['LatestPrice', 'Price'], ['MarketCap', 'Market cap'], ['PERatio', 'P/E'], ['PriceToBook', 'P/B'], ['PriceToSales', 'P/S'], ['ReturnOnEquity', 'ROE'], ['ReturnOnAssets', 'ROA'], ['DividendsYield', 'Dividend'], ['CurrentRatio', 'Current ratio'], ['DebtToEquity', 'Debt/equity'], ['OperatingMargin', 'Operating margin'], ['PayoutRatio', 'Payout']]
  const yahooSymbol = yahooFinanceSymbol(ticker)
  return <div className="stack dense-page analysis-workspace"><PageHeader eyebrow="Company analysis" title={name} description={[ticker, canonicalCode, company.industry, company.market].filter(Boolean).join(' · ')} actions={<div className="button-row">{params.get('from') === 'screen' && <Link className="button button--ghost" to="/screen"><ArrowLeft />Screen</Link>}{yahooSymbol && <a className="button button--secondary" href={`https://finance.yahoo.com/quote/${encodeURIComponent(yahooSymbol)}/`} target="_blank" rel="noreferrer"><ExternalLink />Yahoo Finance</a>}{canonicalCode && <button className="button button--secondary" disabled={favorite.isPending} onClick={() => favorite.mutate()} aria-pressed={isFavorite}><Star />{favorite.isPending ? 'Saving…' : isFavorite ? 'Favorited' : 'Favorite'}</button>}<Link className="button button--primary" to={`/backtest?symbol=${ticker}`}><BarChart3 />Backtest</Link></div>} /><div className="metric-strip analysis-metric-strip">{metricKeys.map(([key, label]) => <Metric key={key} label={label} value={formatOverview(key, metrics[key])} detail={key === 'LatestPrice' ? <button className="text-button" onClick={() => updatePrice.mutate()}><RefreshCw />Refresh</button> : undefined} />)}</div><div className="analysis-top-grid"><Card title="Price history"><PriceChart ticker={ticker} /></Card><Card title="Company snapshot"><dl className="company-facts"><div><dt>Industry</dt><dd>{String(company.industry ?? '—')}</dd></div><div><dt>Market</dt><dd>{String(company.market ?? '—')}</dd></div><div><dt>Code</dt><dd>{canonicalCode || '—'}</dd></div><div><dt>Ticker</dt><dd>{ticker || '—'}</dd></div></dl>{metricPeriod && <p className="company-snapshot-period">Financial metrics: {metricPeriod}</p>}<SnapshotMetrics metrics={metrics} /><div className="company-tags"><div className="tag-list">{(tags.data?.tags ?? []).map(tag => <span className="tag" key={tag}>{tag}<button className="icon-button" onClick={() => removeTag.mutate(tag)} aria-label={`Remove tag ${tag}`}><X /></button></span>)}</div><div className="tag-add"><input className="input" placeholder="Add tag…" value={newTag} onChange={e => setNewTag(e.target.value)} onKeyDown={e => { if (e.key === 'Enter' && newTag.trim()) { addTag.mutate(newTag.trim()); setNewTag('') } }} /><button className="button button--ghost" disabled={!newTag.trim() || !canonicalCode} onClick={() => { addTag.mutate(newTag.trim()); setNewTag('') }} aria-label="Add tag"><Plus /></button></div></div><p className="company-description company-description--compact">{String(company.description_summary ?? company.description ?? 'No business description available.')}</p></Card></div><Card className="analysis-history-card" title="Financial history" description="Select metrics in the table to chart them alongside the underlying values."><FinancialHistoryWorkspace history={history.data} isLoading={history.isLoading} error={history.error} retry={() => { void history.refetch() }} /></Card>{canonicalCode && <FilingSummaryCard companyCode={canonicalCode} />}</div>
}
