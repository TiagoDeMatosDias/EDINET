import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { CategoryScale, Chart as ChartJS, Filler, Legend, LinearScale, LineElement, PointElement, Tooltip } from 'chart.js'
import { ArrowLeft, BarChart3, Download, ExternalLink, Plus, RefreshCw, X } from 'lucide-react'
import { useState } from 'react'
import { Line } from 'react-chartjs-2'
import { Link, useParams, useSearchParams } from 'react-router-dom'

import { ApiError, apiPost, apiRequest, authenticatedFetch, queryString } from '../../api/client'
import type { SecurityHistory, SecurityOverview } from '../../api/types'
import { BRAND_COLORS } from '../../brand'
import { useAuth } from '../auth/authContext'
import { formatMetricValue, groupMetrics, metricDefinition } from '../../metrics'
import { EmptyState, ErrorState, LoadingState } from '../../components/Feedback'
import { Card, Metric, PageHeader } from '../../components/Page'
import { downloadBlob } from '../../api/download'
import { downloadTextFile, safeFileName } from './downloads'
import { FinancialHistoryWorkspace } from './FinancialHistoryWorkspace'
import { buildCompanyReport, type SnapshotGroup } from './markdownReport'
import { filterPriceHistory, PRICE_RANGE_OPTIONS, type PriceHistoryRow, type PriceRangeKey } from './priceHistoryRanges'

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, Filler, Legend, Tooltip)
const PRICE_COLOR = BRAND_COLORS.ink
// The headline strip is a curated pick; labels and formats come from the
// server's metric definitions like the grouped snapshot below it.
const HEADLINE_METRICS = ['LatestPrice', 'MarketCap', 'PERatio', 'PriceToBook', 'PriceToSales', 'ReturnOnEquity', 'ReturnOnAssets', 'DividendsYield', 'CurrentRatio', 'DebtToEquity', 'OperatingMargin', 'PayoutRatio']

function stringOrNull(value: unknown) {
  return typeof value === 'string' && value ? value : null
}

interface FilingSummary {
  doc_id: string
  submitted_at?: string | null
  period_end?: string | null
  status: string
}

function PriceChart({ ticker }: { ticker: string }) {
  const [range, setRange] = useState<PriceRangeKey>('all')
  const prices = useQuery({
    queryKey: ['security-prices', ticker],
    enabled: Boolean(ticker),
    queryFn: () => apiRequest<{ prices: PriceHistoryRow[] }>(`/api/security/price-history${queryString({ ticker, adjusted: true })}`),
  })
  const rows = prices.data?.prices ?? []
  if (prices.isLoading) return <LoadingState label="Loading prices" />
  if (!rows.length) return <EmptyState title="No price history" description="No prices found." />
  const visible = filterPriceHistory(rows, range)
  const basisLabels = [...new Set(visible.map(row => row.price_basis).filter(Boolean))]
  const providerLabels = [...new Set(visible.map(row => row.provider).filter(Boolean))]
  const data = {
    labels: visible.map(row => row.trade_date ?? row.Date ?? row.date ?? ''),
    datasets: [{ data: visible.map(row => row.Price ?? row.price ?? null), borderColor: PRICE_COLOR, fill: false, pointRadius: 0, tension: .2 }],
  }
  const options = { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } }, scales: { x: { grid: { display: false }, ticks: { autoSkip: true, maxTicksLimit: 9, maxRotation: 0 } }, y: { position: 'right' as const } } }
  return <div className="price-history-workspace"><div className="price-range-toolbar"><div className="price-range-buttons" role="group" aria-label="Price history range">{PRICE_RANGE_OPTIONS.map(option => <button key={option.key} type="button" className={range === option.key ? 'active' : ''} aria-pressed={range === option.key} onClick={() => setRange(option.key)}>{option.label}</button>)}</div><span>{visible.length.toLocaleString()} prices</span>{basisLabels.length > 0 && <span title="Price basis recorded with each source row">Basis: {basisLabels.join(', ')}</span>}{providerLabels.length > 0 && <span title="Price provider recorded with each source row">Source: {providerLabels.join(', ')}</span>}</div><div className="price-chart"><Line data={data} options={options} /></div></div>
}

function FilingSummaryCard({ companyCode }: { companyCode: string }) {
  const filings = useQuery({ queryKey: ['company-filings', companyCode], queryFn: () => apiRequest<{ filings: FilingSummary[] }>(`/api/filings/company/${encodeURIComponent(companyCode)}`) })
  const [exporting, setExporting] = useState(false)
  const [exportError, setExportError] = useState('')
  const exportAll = async () => {
    setExporting(true)
    setExportError('')
    try {
      const response = await authenticatedFetch(`/api/filings/company/${encodeURIComponent(companyCode)}/export`)
      if (!response.ok) throw new Error(response.status === 404 ? 'No filing archives are available to export.' : `Export failed (${response.status})`)
      const blob = await response.blob()
      downloadBlob(`${safeFileName(companyCode)}-filings.zip`, blob)
    } catch (error) {
      setExportError(error instanceof Error ? error.message : 'Export failed')
    } finally {
      setExporting(false)
    }
  }
  return <Card
    title="XBRL filings"
    description="Retained EDINET type-1 reports for this company."
    actions={filings.data && filings.data.filings.length > 0
      ? <button className="button button--secondary" disabled={exporting} onClick={() => void exportAll()} title="Download a ZIP containing every retained filing archive"><Download />{exporting ? 'Preparing…' : 'Export all filings'}</button>
      : undefined}
  >{filings.isLoading && <LoadingState label="Loading filings" />}{filings.data?.filings.slice(0, 8).map(filing => <Link className="filing-row" key={filing.doc_id} to={`/filings/${encodeURIComponent(filing.doc_id)}?from=analysis&company=${encodeURIComponent(companyCode)}`}><span><strong>{filing.period_end || 'Period unavailable'}</strong><small>{filing.doc_id}</small></span><span><small>{filing.submitted_at || 'Submission unavailable'} · {filing.status}</small></span></Link>)}{filings.data && !filings.data.filings.length && <EmptyState title="No archived XBRL reports" description="Type-1 filing packages will appear after acquisition." />}{exportError && <p className="form-error" role="alert" style={{ margin: '8px 0 0' }}>{exportError}</p>}<Link className="button button--ghost" to={`/filings?company=${encodeURIComponent(companyCode)}&from=analysis`}>Open Filing Explorer</Link></Card>
}

function SnapshotMetrics({ metrics, groups, format }: { metrics: Record<string, number | null>; groups: SnapshotGroup[]; format: (key: string, value: number | null | undefined) => string }) {
  return <div className="company-snapshot-groups">{groups.map(group => <section className="company-snapshot-group" key={group.title}><h3>{group.title}</h3><dl className="company-snapshot-metrics">{group.metrics.map(([key, label]) => <div key={key}><dt>{label}</dt><dd>{format(key, metrics[key])}</dd></div>)}</dl></section>)}</div>
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
  const auth = useAuth()
  // Provider refreshes write shared market data; the API allows operators and admins only.
  // With authentication disabled the server treats every request as the local admin.
  const canRefreshPrice = auth.status?.mode === 'disabled' || auth.user?.role === 'admin' || auth.user?.role === 'operator'
  const updatePrice = useMutation({ mutationFn: () => apiPost('/api/security/update-price', { ticker }), onSuccess: () => queryClient.invalidateQueries({ queryKey: ['security-overview', lookupKey] }) })
  const [newTag, setNewTag] = useState('')
  const tags = useQuery({ queryKey: ['company-tags', canonicalCode], enabled: Boolean(canonicalCode), queryFn: () => apiRequest<{ tags: string[] }>(`/api/tags/${encodeURIComponent(canonicalCode)}`) })
  const addTag = useMutation({ mutationFn: (tag: string) => apiRequest(`/api/tags/${encodeURIComponent(canonicalCode)}/${encodeURIComponent(tag)}`, { method: 'POST' }), onSuccess: () => { void queryClient.invalidateQueries({ queryKey: ['company-tags', canonicalCode] }); void queryClient.invalidateQueries({ queryKey: ['research-tags'] }) } })
  const removeTag = useMutation({ mutationFn: (tag: string) => apiRequest(`/api/tags/${encodeURIComponent(canonicalCode)}/${encodeURIComponent(tag)}`, { method: 'DELETE' }), onSuccess: () => { void queryClient.invalidateQueries({ queryKey: ['company-tags', canonicalCode] }); void queryClient.invalidateQueries({ queryKey: ['research-tags'] }) } })

  if (!lookup) return <div className="stack dense-page analysis-empty-page"><PageHeader eyebrow="Company research" title="Analyze a company" description="Use the company search above to open price, statements, ratios, and trends." /><EmptyState title="Search for a company above" description="Enter a name, ticker, EDINET code, or industry and choose a result." /></div>
  if (overview.isLoading) return <LoadingState label="Loading company analysis" />
  if (overview.isError && overview.error instanceof ApiError && overview.error.status === 404) {
    return <div className="stack dense-page analysis-empty-page"><PageHeader eyebrow="Company research" title="Company not found" description={`No company in the research database matches “${lookup}”.`} /><EmptyState title="Search for the company above" description="Enter a name, ticker, EDINET code, or industry and choose a result." /></div>
  }
  if (overview.isError) return <ErrorState error={overview.error} retry={() => overview.refetch()} />
  const metricDefinitions = overview.data?.metric_definitions ?? {}
  const currencies = { price: stringOrNull(overview.data?.market?.price_currency), reporting: stringOrNull(overview.data?.metadata?.reporting_currency) }
  const formatMetric = (key: string, value: number | null | undefined) => formatMetricValue(metricDefinitions[key], value, currencies)
  const snapshotGroups: SnapshotGroup[] = groupMetrics(Object.keys(metricDefinitions), metricDefinitions)
    .map(({ group, metrics: keys }) => ({ title: group, metrics: keys.map(key => [key, metricDefinitions[key].label] as [string, string]) }))
  const metricKeys = HEADLINE_METRICS.map(key => [key, metricDefinition(key, metricDefinitions).label])
  const qualityFlags = overview.data?.metadata?.data_quality_flags
  const tickerOnly = Array.isArray(qualityFlags) && qualityFlags.includes('ticker_only_no_company_record')
  const yahooSymbol = String(company.yahoo_symbol ?? '')
  // The filing's own business description comes first; the external profile is a labelled fallback.
  const descriptionSource = company.description_source as { kind?: string; label?: string; symbol?: string } | null | undefined
  const businessDescription = String((descriptionSource?.kind === 'external' ? company.yahoo_description : company.description_summary || company.description) ?? '').trim()
  const descriptionProvenance = businessDescription && descriptionSource?.label ? [descriptionSource.label, descriptionSource.symbol].filter(Boolean).join(' · ') : ''
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
  return <div className="stack dense-page analysis-workspace"><PageHeader eyebrow="Company analysis" title={name} description={[ticker, canonicalCode, company.industry, company.market].filter(Boolean).join(' · ')} actions={<div className="button-row">{params.get('from') === 'screen' && <Link className="button button--ghost" to="/screen"><ArrowLeft />Return to Screening</Link>}<button className="button button--secondary" disabled={!history.data} onClick={downloadReport} title={history.data ? 'Download a markdown report with the snapshot and financial history' : 'Financial history is still loading'}><Download />Export report</button>{yahooSymbol && <a className="button button--secondary" href={`https://finance.yahoo.com/quote/${encodeURIComponent(yahooSymbol)}/`} target="_blank" rel="noreferrer"><ExternalLink />Yahoo Finance</a>}<Link className="button button--primary" to={`/backtest?symbol=${ticker}`}><BarChart3 />Backtest</Link></div>} />{tickerOnly && <div className="callout callout--warning" role="status"><strong>No company record matches “{tickerParam}”.</strong> Showing stored price data only. Broker and portfolio symbols do not always match the exchange ticker used in EDINET data; search by company name or EDINET code for statements and filings.</div>}<div className="metric-strip analysis-metric-strip">{metricKeys.map(([key, label]) => <Metric key={key} label={label} value={formatMetric(key, metrics[key])} detail={key === 'LatestPrice' && canRefreshPrice ? <button className="text-button" onClick={() => updatePrice.mutate()}><RefreshCw />Refresh</button> : undefined} />)}</div><div className="analysis-top-grid"><Card title="Price history"><PriceChart ticker={ticker} /></Card><Card title="Company snapshot"><dl className="company-facts"><div><dt>Industry</dt><dd>{String(company.industry ?? '—')}</dd></div><div><dt>Market</dt><dd>{String(company.market ?? '—')}</dd></div><div><dt>Code</dt><dd>{canonicalCode || '—'}</dd></div><div><dt>Ticker</dt><dd>{ticker || '—'}</dd></div></dl>{metricPeriod && <p className="company-snapshot-period">Financial metrics: {metricPeriod}</p>}<SnapshotMetrics metrics={metrics} groups={snapshotGroups} format={formatMetric} /><div className="company-tags"><div className="tag-list">{(tags.data?.tags ?? []).map(tag => <span className="tag" key={tag}>{tag}<button className="icon-button" onClick={() => removeTag.mutate(tag)} aria-label={`Remove tag ${tag}`}><X /></button></span>)}</div><div className="tag-add"><input className="input" placeholder="Add tag…" value={newTag} onChange={e => setNewTag(e.target.value)} onKeyDown={e => { if (e.key === 'Enter' && newTag.trim()) { addTag.mutate(newTag.trim()); setNewTag('') } }} /><button className="button button--ghost" disabled={!newTag.trim() || !canonicalCode} onClick={() => { addTag.mutate(newTag.trim()); setNewTag('') }} aria-label="Add tag"><Plus /></button></div></div><div className="company-description-block"><strong>Business description</strong><p className="company-description company-description--compact">{businessDescription || 'No business description available.'}</p>{descriptionProvenance && <small className="company-description-source">Source: {descriptionProvenance}</small>}</div></Card></div><Card className="analysis-history-card" title="Financial history" description="Select metrics in the table to chart them alongside the underlying values."><FinancialHistoryWorkspace history={history.data} isLoading={history.isLoading} error={history.error} retry={() => { void history.refetch() }} downloadPrefix={canonicalCode || ticker} /></Card>{canonicalCode && <FilingSummaryCard companyCode={canonicalCode} />}</div>
}
