import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import type { ColumnDef } from '@tanstack/react-table'
import { ArcElement, BarElement, CategoryScale, Chart as ChartJS, Filler, Legend, LinearScale, LineElement, PointElement, Tooltip } from 'chart.js'
import { Building2, FileUp, RefreshCw, Upload } from 'lucide-react'
import { useMemo, useState } from 'react'
import { Bar, Doughnut, Line, Scatter } from 'react-chartjs-2'
import { useNavigate } from 'react-router-dom'

import { apiRequest, queryString } from '../../api/client'
import { DataTable } from '../../components/DataTable'
import { EmptyState, ErrorState, LoadingState } from '../../components/Feedback'
import { Card, Field, Metric, PageHeader } from '../../components/Page'
import { PortfolioAdvancedAnalytics } from './PortfolioAdvancedAnalytics'

ChartJS.register(ArcElement, BarElement, CategoryScale, Filler, Legend, LinearScale, LineElement, PointElement, Tooltip)
const COLORS = ['#146ef5', '#12a56c', '#8b5cf6', '#e58b16', '#e14d5a', '#0e9fbe', '#64748b', '#d946ef', '#84cc16', '#f97316']

type Holding = { symbol: string; asset_category?: string; quantity?: number; avg_cost?: number | null; market_price?: number | null; market_value?: number | null; currency?: string; performance?: Record<string, unknown> }
type Transaction = { id?: number; trade_date?: string; activity_type?: string; symbol?: string; description?: string; quantity?: number; amount?: number; currency?: string; source_file?: string }
type Performance = Record<string, unknown> & { dividend_breakdown?: Record<string, unknown>; return_distribution?: Record<string, unknown>; return_attribution?: Record<string, unknown> }
type PieData = { labels: string[]; values: number[]; total: number; currency: string }
type ValueHistory = { dates: string[]; holdings: Record<string, Array<number | null>>; currency: string; portfolio_values?: Array<number | null>; daily_returns?: Array<number | null>; cumulative_returns?: Array<number | null> }
type DividendHistory = { periods: string[]; companies: Record<string, number[]>; currency: string }
type DividendCurrencyHistory = { periods: string[]; currencies: Record<string, number[]>; currency: string }
type DividendGrowthData = { years: number[]; companies: Record<string, { currency: string; dps: Array<number | null>; yoy_growth: Array<number | null>; avg_market_value_eur: Array<number | null> }>; weighted_average_growth: Array<number | null> }
type HeatmapData = { years: number[]; months: number[]; values: Array<Array<number | null>> }
type ScatterPoint = { symbol: string; cost_basis_display: number; annualized_return: number; is_open: boolean }

function money(value: unknown, currency = 'EUR') { const number = Number(value); return Number.isFinite(number) ? new Intl.NumberFormat(undefined, { style: 'currency', currency, maximumFractionDigits: 0 }).format(number) : '—' }
function percent(value: unknown) { const number = Number(value); return Number.isFinite(number) ? `${(number * 100).toFixed(1)}%` : '—' }
function number(value: unknown, digits = 2) { const parsed = Number(value); return Number.isFinite(parsed) ? parsed.toFixed(digits) : '—' }

function compactPie(data?: PieData, limit = 8) {
  if (!data) return { labels: [], values: [] }
  const rows = data.labels.map((label, index) => ({ label, value: data.values[index] ?? 0 })).sort((a, b) => b.value - a.value)
  const shown = rows.slice(0, limit)
  const other = rows.slice(limit).reduce((sum, row) => sum + row.value, 0)
  return { labels: [...shown.map(row => row.label), ...(other ? ['Other'] : [])], values: [...shown.map(row => row.value), ...(other ? [other] : [])] }
}

function ValueChart({ data, currency }: { data?: ValueHistory; currency: string }) {
  const totals = data?.portfolio_values?.length === data?.dates.length ? data?.portfolio_values?.map(value => value ?? 0) ?? [] : data?.dates.map((_, index) => Object.values(data?.holdings ?? {}).reduce((sum, values) => sum + Number(values[index] ?? 0), 0)) ?? []
  const chart = { labels: data?.dates ?? [], datasets: [{ label: `Portfolio value (${currency})`, data: totals, borderColor: COLORS[0], backgroundColor: '#146ef518', fill: true, pointRadius: 0, tension: .18 }] }
  return <div className="portfolio-value-chart"><Line data={chart} options={{ responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } }, scales: { x: { display: false }, y: { position: 'right', ticks: { callback: value => Number(value).toLocaleString(undefined, { notation: 'compact' }) } } } }} /></div>
}

function AllocationChart({ data, title }: { data?: PieData; title: string }) {
  const compact = compactPie(data)
  const chart = { labels: compact.labels, datasets: [{ data: compact.values, backgroundColor: COLORS, borderWidth: 0 }] }
  return <div className="mini-chart"><strong>{title}</strong><Doughnut data={chart} options={{ responsive: true, maintainAspectRatio: false, cutout: '64%', plugins: { legend: { position: 'right', labels: { boxWidth: 9, font: { size: 10 } } } } }} /></div>
}

function ReturnHeatmap({ data }: { data?: HeatmapData }) {
  if (!data?.years.length) return <EmptyState title="No monthly returns" description="Return history is not available." />
  const color = (value: number | null) => { if (value == null) return undefined; const strength = Math.min(Math.abs(value) / 10, 1); return value >= 0 ? `rgba(18,165,108,${.12 + strength * .72})` : `rgba(225,77,90,${.12 + strength * .72})` }
  return <div className="return-heatmap"><div className="heatmap-row heatmap-head"><span>Year</span>{data.months.map(month => <span key={month}>{new Date(2020, month - 1).toLocaleString(undefined, { month: 'short' })}</span>)}</div>{data.years.map((year, row) => <div className="heatmap-row" key={year}><strong>{year}</strong>{data.values[row].map((value, column) => <span key={column} style={{ background: color(value) }} title={value == null ? 'No data' : `${value.toFixed(2)}%`}>{value == null ? '·' : value.toFixed(1)}</span>)}</div>)}</div>
}

function DividendsByCurrencyChart({ data }: { data?: DividendCurrencyHistory }) {
  const currencies = Object.entries(data?.currencies ?? {}).map(([ccy, values]) => ({ ccy, values, total: values.reduce((sum, v) => sum + v, 0) })).sort((a, b) => b.total - a.total)
  const chart = { labels: data?.periods ?? [], datasets: currencies.map((row, i) => ({ label: row.ccy, data: row.values, backgroundColor: COLORS[i % COLORS.length] })) }
  return <div className="analytics-chart"><Bar data={chart} options={{ responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'bottom', labels: { boxWidth: 9, font: { size: 10 } } } }, scales: { x: { stacked: true }, y: { stacked: true, position: 'right' } } }} /></div>
}

function DividendsByCompanyBar({ data }: { data?: DividendHistory }) {
  const companies = Object.entries(data?.companies ?? {}).map(([symbol, values], i) => ({ symbol, values, total: values.reduce((sum, v) => sum + v, 0), color: COLORS[i % COLORS.length] })).sort((a, b) => b.total - a.total)
  const [selected, setSelected] = useState<string[]>(() => companies.map(c => c.symbol))
  const filtered = companies.filter(c => selected.includes(c.symbol))
  const chart = { labels: data?.periods ?? [], datasets: filtered.map(row => ({ label: row.symbol, data: row.values, backgroundColor: row.color })) }
  return <div>
    <CompanyFilter companies={companies} selected={selected} onChange={setSelected} />
    <div className="analytics-chart"><Bar data={chart} options={{ responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } }, scales: { x: { stacked: true }, y: { stacked: true, position: 'right' } } }} /></div>
  </div>
}

function DividendsByCompanyPie({ data }: { data?: DividendHistory }) {
  const companies = Object.entries(data?.companies ?? {}).map(([symbol, values], i) => ({ symbol, values, total: values.reduce((sum, v) => sum + v, 0), color: COLORS[i % COLORS.length] })).sort((a, b) => b.total - a.total)
  const [selected, setSelected] = useState<string[]>(() => companies.map(c => c.symbol))
  const filtered = companies.filter(c => selected.includes(c.symbol))
  const total = filtered.reduce((sum, c) => sum + c.total, 0)
  const compact = compactPie({ labels: filtered.map(c => c.symbol), values: filtered.map(c => c.total), total, currency: data?.currency ?? 'EUR' })
  const chart = { labels: compact.labels, datasets: [{ data: compact.values, backgroundColor: filtered.map(c => c.color).concat(compact.labels.length > filtered.length ? [COLORS[6]] : []), borderWidth: 0 }] }
  return <div className="mini-chart">
    <strong>Total dividends by company</strong>
    <CompanyFilter companies={companies} selected={selected} onChange={setSelected} />
    <Doughnut data={chart} options={{ responsive: true, maintainAspectRatio: false, cutout: '64%', plugins: { legend: { display: false } } }} />
  </div>
}

function CompanyFilter({ companies, selected, onChange }: { companies: { symbol: string; color: string }[]; selected: string[]; onChange: (s: string[]) => void }) {
  const [open, setOpen] = useState(false)
  return <div className="company-filter">
    <button className="company-filter-toggle" onClick={() => setOpen(o => !o)}>{selected.length} of {companies.length}</button>
    {open && <div className="company-filter-dropdown">
      <label className="company-filter-item"><input type="checkbox" checked={selected.length === companies.length} onChange={() => onChange(selected.length === companies.length ? [] : companies.map(c => c.symbol))} /> All</label>
      {companies.map(c => <label key={c.symbol} className="company-filter-item"><input type="checkbox" checked={selected.includes(c.symbol)} onChange={() => onChange(selected.includes(c.symbol) ? selected.filter(s => s !== c.symbol) : [...selected, c.symbol])} /><span className="company-filter-dot" style={{ background: c.color }} />{c.symbol}</label>)}
    </div>}
  </div>
}

function DividendGrowthChart({ data }: { data?: DividendGrowthData }) {
  const companies = Object.entries(data?.companies ?? {})
    .map(([symbol, c], i) => ({ symbol, currency: c.currency, yoy: c.yoy_growth, mv: c.avg_market_value_eur ?? [], total: c.dps.reduce((sum, v) => sum + (v ?? 0), 0), color: COLORS[i % COLORS.length] }))
    .filter(c => c.yoy.some(v => v != null))
    .sort((a, b) => b.total - a.total)
  const [selected, setSelected] = useState<string[]>(() => companies.map(c => c.symbol))
  if (!companies.length) return <EmptyState title="No dividend growth data" description="No companies with dividend history found." />
  const filtered = companies.filter(c => selected.includes(c.symbol))
  const allMv = filtered.flatMap(c => c.mv.filter((v): v is number => v != null))
  const maxMv = Math.max(...allMv, 1)
  const scale = (mv: number | null) => mv ? Math.max(3, Math.sqrt((mv || 1) / maxMv) * 16) : 3
  const datasets: any[] = filtered.map(c => ({
    label: c.symbol,
    data: c.yoy.map((growth, j) => growth != null ? { x: (data?.years ?? [])[j], y: growth } : null).filter(Boolean),
    pointRadius: c.yoy.map((_, j) => scale(c.mv[j])),
    backgroundColor: c.color + 'aa',
    borderColor: c.color,
    borderWidth: 1,
  }))
  if (data?.weighted_average_growth?.some((v): v is number => v != null)) {
    datasets.push({
      type: 'line' as const,
      label: 'Portfolio avg',
      data: data.weighted_average_growth.map((v, j) => v != null ? { x: (data?.years ?? [])[j], y: v } : null).filter(Boolean),
      borderColor: '#1e293b',
      borderWidth: 2.5,
      pointRadius: 4,
      pointBackgroundColor: '#1e293b',
      backgroundColor: 'transparent',
    })
  }
  return <div>
    <CompanyFilter companies={companies} selected={selected} onChange={setSelected} />
    <div className="analytics-chart">
      <Scatter data={{ datasets }} options={{ responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false }, tooltip: { callbacks: { label: ctx => `${ctx.dataset.label}: ${(ctx.parsed.y ?? 0).toFixed(1)}%` } } }, scales: { x: { type: 'linear', grid: { display: false }, title: { display: true, text: 'Year' }, ticks: { stepSize: 1, callback: v => String(v) } }, y: { position: 'right', title: { display: true, text: 'YoY growth %' }, ticks: { callback: v => v.toFixed(0) + '%' } } } }} />
    </div>
  </div>
}

function CostReturnChart({ data }: { data?: ScatterPoint[] }) {
  const symbols = [...new Set((data ?? []).map(p => p.symbol))].sort()
  const colorMap = Object.fromEntries(symbols.map((s, i) => [s, COLORS[i % COLORS.length]]))
  const companies = symbols.map(s => ({ symbol: s, color: colorMap[s] }))
  const [selected, setSelected] = useState<string[]>(() => symbols)
  const filtered = (data ?? []).filter(p => selected.includes(p.symbol))
  const chart = { datasets: [
    { label: 'Open', data: filtered.filter(p => p.is_open).map(p => ({ x: p.cost_basis_display, y: p.annualized_return, symbol: p.symbol })), backgroundColor: filtered.filter(p => p.is_open).map(p => colorMap[p.symbol] + 'cc'), borderColor: filtered.filter(p => p.is_open).map(p => colorMap[p.symbol]), borderWidth: 1 },
    { label: 'Closed', data: filtered.filter(p => !p.is_open).map(p => ({ x: p.cost_basis_display, y: p.annualized_return, symbol: p.symbol })), backgroundColor: filtered.filter(p => !p.is_open).map(p => colorMap[p.symbol] + '66'), borderColor: filtered.filter(p => !p.is_open).map(p => colorMap[p.symbol]), borderWidth: 1, pointStyle: 'triangle' },
  ]}
  return <div>
    <CompanyFilter companies={companies} selected={selected} onChange={setSelected} />
    <div className="analytics-chart"><Scatter data={chart} options={{ responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false }, tooltip: { callbacks: { label: context => { const raw = context.raw as { x: number; y: number; symbol: string }; return `${raw.symbol}: ${raw.y.toFixed(1)}% on ${raw.x.toLocaleString()}` } } } }, scales: { x: { title: { display: true, text: 'Cost basis' } }, y: { position: 'right', title: { display: true, text: 'Annualized return %' } } } }} /></div>
  </div>
}

function PerformanceMetrics({ data, currency }: { data?: Performance; currency: string }) {
  const distribution = data?.return_distribution ?? {}
  const attribution = data?.return_attribution ?? {}
  return <div className="metric-strip metric-strip--wrap"><Metric label="Total return" value={percent(data?.total_return)} /><Metric label="Annualized" value={percent(data?.annualized_return)} /><Metric label="Volatility" value={percent(data?.volatility)} /><Metric label="Sharpe" value={number(data?.sharpe_ratio)} /><Metric label="Sortino" value={number(data?.sortino_ratio)} /><Metric label="Max drawdown" value={percent(data?.max_drawdown)} /><Metric label="Calmar" value={number(data?.calmar_ratio)} /><Metric label="Win rate" value={percent(data?.win_rate)} /><Metric label="Profit factor" value={number(data?.profit_factor)} /><Metric label="VaR 95%" value={percent(data?.var_95)} /><Metric label="CVaR 95%" value={percent(data?.cvar_95)} /><Metric label="Dividends" value={money(data?.total_dividend_income, currency)} /><Metric label="Capital appreciation" value={percent(attribution.capital_appreciation)} /><Metric label="Dividend yield" value={percent(attribution.dividend_yield)} /><Metric label="Best day" value={percent(distribution.max)} /><Metric label="Worst day" value={percent(distribution.min)} /></div>
}

function HoldingsTable({ data, currency, navigate }: { data: Holding[]; currency: string; navigate: ReturnType<typeof useNavigate> }) {
  const columns = useMemo<ColumnDef<Holding>[]>(() => [{ accessorKey: 'symbol', header: 'Holding', cell: ({ row }) => <button className="company-link" onClick={() => navigate(`/analyze?q=${encodeURIComponent(row.original.symbol)}`)}><Building2 /><span><strong>{row.original.symbol}</strong><small>{String(row.original.performance?.name ?? row.original.asset_category ?? '')}</small></span></button> }, { accessorKey: 'quantity', header: 'Qty' }, { accessorKey: 'market_price', header: 'Price', cell: info => money(info.getValue(), String(info.row.original.currency ?? currency)) }, { accessorKey: 'market_value', header: `Value (${currency})`, cell: info => money(info.getValue(), currency) }, { id: 'cost', header: 'Cost basis', accessorFn: row => row.performance?.cost_basis_display, cell: info => money(info.getValue(), currency) }, { id: 'pnl', header: 'P&L', accessorFn: row => row.performance?.pnl_display, cell: info => money(info.getValue(), currency) }, { id: 'return', header: 'Return', accessorFn: row => row.performance?.total_return_display ?? row.performance?.total_return_native, cell: info => percent(info.getValue()) }, { id: 'annualized', header: 'Ann. return', accessorFn: row => row.performance?.annualized_return, cell: info => percent(info.getValue()) }], [currency, navigate])
  return <DataTable data={data} columns={columns} emptyText="No holdings." dense />
}

function TransactionsTable({ data, currency }: { data: Transaction[]; currency: string }) {
  const columns = useMemo<ColumnDef<Transaction>[]>(() => [{ accessorKey: 'trade_date', header: 'Date' }, { accessorKey: 'activity_type', header: 'Activity' }, { accessorKey: 'symbol', header: 'Symbol' }, { accessorKey: 'description', header: 'Description' }, { accessorKey: 'quantity', header: 'Qty' }, { accessorKey: 'amount', header: 'Amount', cell: info => money(info.getValue(), String(info.row.original.currency ?? currency)) }, { accessorKey: 'source_file', header: 'Source' }], [currency])
  return <DataTable data={data} columns={columns} emptyText="No transactions." dense />
}

export default function PortfolioWorkspace() {
  const [tab, setTab] = useState<'overview' | 'holdings' | 'analytics' | 'transactions'>('overview')
  const [currency, setCurrency] = useState('EUR')
  const [dividendPeriod, setDividendPeriod] = useState<'monthly' | 'quarterly' | 'yearly'>('quarterly')
  const [uploadStatus, setUploadStatus] = useState('')
  const queryClient = useQueryClient()
  const navigate = useNavigate()
  const suffix = queryString({ display_currency: currency })
  const currencies = useQuery({ queryKey: ['portfolio-currencies'], queryFn: () => apiRequest<Array<{ code?: string } | string>>('/api/portfolio/display-currencies'), retry: false })
  const activity = useQuery({ queryKey: ['portfolio-activity'], queryFn: () => apiRequest<{ by_activity: Record<string, number> }>('/api/portfolio/activity-summary'), retry: false })
  const holdings = useQuery({ queryKey: ['portfolio-holdings', currency], queryFn: () => apiRequest<Holding[]>(`/api/portfolio/holdings/performance${queryString({ display_currency: currency, include_closed: false })}`), retry: false })
  const transactions = useQuery({ queryKey: ['portfolio-transactions'], queryFn: () => apiRequest<Transaction[]>('/api/portfolio/transactions?limit=1000'), retry: false })
  const performance = useQuery({ queryKey: ['portfolio-performance', currency], queryFn: () => apiRequest<Performance>(`/api/portfolio/performance${queryString({ base_currency: currency })}`), retry: false })
  const valueHistory = useQuery({ queryKey: ['portfolio-value-history', currency], queryFn: () => apiRequest<ValueHistory>(`/api/portfolio/charts/portfolio-value-history${suffix}`), retry: false })
  const allocation = useQuery({ queryKey: ['portfolio-allocation', currency], queryFn: () => apiRequest<PieData>(`/api/portfolio/charts/holdings-by-value${suffix}`), retry: false })
  const currenciesChart = useQuery({ queryKey: ['portfolio-currency-chart', currency], queryFn: () => apiRequest<PieData>(`/api/portfolio/charts/holdings-by-currency${suffix}`), retry: false })
  const dividendsByCurrency = useQuery({ queryKey: ['portfolio-dividends-currency', currency, dividendPeriod], queryFn: () => apiRequest<DividendCurrencyHistory>(`/api/portfolio/charts/dividends-by-currency${suffix}&period=${dividendPeriod}`), retry: false })
  const dividendsByCompany = useQuery({ queryKey: ['portfolio-dividends-by-company', currency], queryFn: () => apiRequest<DividendHistory>(`/api/portfolio/charts/dividends-by-company${suffix}&period=yearly`), retry: false })
  const dividendsByCompanyPie = useQuery({ queryKey: ['portfolio-dividends-company-pie', currency], queryFn: () => apiRequest<DividendHistory>(`/api/portfolio/charts/dividends-by-company${suffix}&period=yearly`), retry: false })
  const dividendGrowth = useQuery({ queryKey: ['portfolio-dividend-growth', currency], queryFn: () => apiRequest<DividendGrowthData>('/api/portfolio/dividends/yoy/per-company'), retry: false })
  const heatmap = useQuery({ queryKey: ['portfolio-return-heatmap', currency], queryFn: () => apiRequest<HeatmapData>(`/api/portfolio/charts/returns-heatmap${suffix}`), retry: false })
  const scatter = useQuery({ queryKey: ['portfolio-return-cost', currency], queryFn: () => apiRequest<ScatterPoint[]>(`/api/portfolio/charts/return-vs-cost${suffix}`), retry: false })
  const invalidate = () => queryClient.invalidateQueries({ predicate: query => String(query.queryKey[0]).startsWith('portfolio') })
  const rebuild = useMutation({ mutationFn: () => apiRequest(`/api/portfolio/rebuild${queryString({ base_currency: currency })}`, { method: 'POST' }), onSuccess: invalidate })
  const totalValue = (holdings.data ?? []).reduce((sum, item) => sum + Number(item.market_value ?? 0), 0)
  const uploadFiles = async (files: FileList | null) => { if (!files?.length) return; setUploadStatus('Importing…'); try { for (const file of Array.from(files)) { const form = new FormData(); form.set('file', file); await apiRequest('/api/portfolio/upload', { method: 'POST', body: form }) } setUploadStatus(`${files.length} imported`); await rebuild.mutateAsync(); await invalidate() } catch (error) { setUploadStatus(error instanceof Error ? error.message : 'Import failed') } }
  const unavailable = holdings.isError && activity.isError

  return <div className="stack dense-page portfolio-workspace"><PageHeader eyebrow="Portfolio monitoring" title="Portfolio" description="Performance, exposures, holdings, income, and risk in one dense workspace." actions={<div className="button-row"><Field label="Currency"><select className="select" value={currency} onChange={event => setCurrency(event.target.value)}>{(currencies.data ?? ['EUR', 'USD', 'JPY']).map(item => { const code = typeof item === 'string' ? item : item.code ?? ''; return <option key={code}>{code}</option> })}</select></Field><button className="button button--secondary" disabled={rebuild.isPending} onClick={() => rebuild.mutate()}><RefreshCw />Rebuild</button><label className="button button--ghost file-button"><Upload />Import<input type="file" accept=".xml,text/xml" multiple onChange={event => void uploadFiles(event.target.files)} /></label></div>} />
    {uploadStatus && <div className="inline-status" role="status">{uploadStatus}</div>}
    {unavailable ? <Card title="Connect portfolio activity"><label className="file-drop"><FileUp /><strong>Import IBKR FlexQuery XML</strong><input type="file" accept=".xml,text/xml" multiple onChange={event => void uploadFiles(event.target.files)} /></label></Card> : <><PerformanceMetrics data={performance.data} currency={currency} /><div className="step-tabs"><button className={tab === 'overview' ? 'active' : ''} onClick={() => setTab('overview')}>Overview</button><button className={tab === 'holdings' ? 'active' : ''} onClick={() => setTab('holdings')}>Holdings</button><button className={tab === 'analytics' ? 'active' : ''} onClick={() => setTab('analytics')}>Analytics</button><button className={tab === 'transactions' ? 'active' : ''} onClick={() => setTab('transactions')}>Transactions</button></div>
      {tab === 'overview' && <div className="portfolio-overview-grid"><Card title="Portfolio value" description={`${money(totalValue, currency)} · ${performance.data?.start_date ?? '—'} to ${performance.data?.end_date ?? '—'}`}>{valueHistory.isLoading ? <LoadingState label="Loading value history" /> : <ValueChart data={valueHistory.data} currency={currency} />}</Card><Card title="Current exposure"><div className="allocation-grid"><AllocationChart data={allocation.data} title="Holdings" /><AllocationChart data={currenciesChart.data} title="Currencies" /></div></Card><Card title="Activity" description="Imported records by type"><div className="activity-grid activity-grid--dense">{Object.entries(activity.data?.by_activity ?? {}).map(([name, count]) => <Metric key={name} label={name.replaceAll('_', ' ')} value={count.toLocaleString()} />)}</div></Card></div>}
      {tab === 'holdings' && <Card title={`${holdings.data?.length ?? 0} open holdings`} description="Value, cost, P&L, and native/display-currency returns.">{holdings.isLoading ? <LoadingState label="Loading holdings" /> : holdings.isError ? <ErrorState error={holdings.error} /> : <div className="fixed-table"><HoldingsTable data={holdings.data ?? []} currency={currency} navigate={navigate} /></div>}</Card>}
      {tab === 'analytics' && <div className="analytics-grid"><Card title="Monthly return heatmap"><ReturnHeatmap data={heatmap.data} /></Card><Card title="Dividends by currency" style={{ gridColumn: '1 / -1' }}><div className="period-toolbar"><span className="period-label">Aggregation</span><div className="period-tabs">{['monthly','quarterly','yearly'].map(p => <button key={p} className={`period-tab${dividendPeriod === p ? ' active' : ''}`} onClick={() => setDividendPeriod(p as typeof dividendPeriod)}>{p}</button>)}</div></div><DividendsByCurrencyChart data={dividendsByCurrency.data} /></Card><Card title="Dividend per share growth" style={{ gridColumn: '1 / -1' }}><DividendGrowthChart data={dividendGrowth.data} /></Card><Card title="Dividends by company (total)"><DividendsByCompanyPie data={dividendsByCompanyPie.data} /></Card><Card title="Yearly dividends by company"><DividendsByCompanyBar data={dividendsByCompany.data} /></Card><Card title="Return versus cost basis"><CostReturnChart data={scatter.data} /></Card><PortfolioAdvancedAnalytics valueHistory={valueHistory.data} allocation={allocation.data} holdings={holdings.data ?? []} /></div>}
      {tab === 'transactions' && <Card title="Transactions" description="Latest 1,000 imported activity records.">{transactions.isLoading ? <LoadingState label="Loading transactions" /> : transactions.isError ? <ErrorState error={transactions.error} /> : <div className="fixed-table"><TransactionsTable data={transactions.data ?? []} currency={currency} /></div>}</Card>}</>}
  </div>
}

