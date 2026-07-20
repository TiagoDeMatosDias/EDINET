import { ArrowDown, ArrowUp } from 'lucide-react'
import { useMemo, useState } from 'react'
import { Line } from 'react-chartjs-2'

import type { HistoryMetric, HistoryTable, SecurityHistory } from '../../api/types'
import { EmptyState, ErrorState, LoadingState } from '../../components/Feedback'

const COLORS = ['#146ef5', '#12a56c', '#8b5cf6', '#e58b16', '#e14d5a', '#0e9fbe', '#64748b', '#d946ef']
const INCOME_STATEMENT_DEFAULTS = [
  'Net Sales',
  'Operating Income - Operating Profit (loss)',
  'Profit (loss)',
]
export type FinancialUnit = { scale: number; label: string }

function finiteNumber(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value)
}

function numericMetrics(metrics: HistoryMetric[]) {
  return metrics.filter(metric => metric.values.some(finiteNumber))
}

export function defaultMetricFields(sourceKey: string, table: HistoryTable | undefined, metrics: HistoryMetric[]) {
  if (sourceKey !== 'IncomeStatement' && table?.display_name !== 'Income Statement') return metrics.slice(0, 4).map(metric => metric.field)
  return INCOME_STATEMENT_DEFAULTS.flatMap(name => {
    const metric = metrics.find(candidate => candidate.display_name === name)
    return metric ? [metric.field] : []
  })
}
export function chooseFinancialUnit(metrics: HistoryMetric[]): FinancialUnit {
  const maximum = Math.max(0, ...metrics.flatMap(metric => metric.values.filter(finiteNumber).map(Math.abs)))
  if (maximum >= 1e12) return { scale: 1e12, label: 'Trillion' }
  if (maximum >= 1e9) return { scale: 1e9, label: 'Billion' }
  if (maximum >= 1e6) return { scale: 1e6, label: 'Million' }
  if (maximum >= 1e3) return { scale: 1e3, label: 'Thousand' }
  return { scale: 1, label: 'Units' }
}

export function completeMetricFields(metrics: HistoryMetric[], periods: string[]) {
  const relevant = periods.map((_, index) => index).filter(index => metrics.some(metric => finiteNumber(metric.values[index])))
  if (!relevant.length) return []
  return metrics.filter(metric => relevant.every(index => finiteNumber(metric.values[index]))).map(metric => metric.field)
}

function orderedRows(table: HistoryTable, search: string, order: string, manual: string[]) {
  const query = search.trim().toLowerCase()
  const rows = table.metrics.filter(metric => !query || `${metric.display_name} ${metric.field}`.toLowerCase().includes(query))
  if (order === 'alpha') return [...rows].sort((a, b) => a.display_name.localeCompare(b.display_name))
  if (order === 'coverage') return [...rows].sort((a, b) => b.values.filter(value => value != null).length - a.values.filter(value => value != null).length)
  if (order === 'manual' && manual.length) return [...rows].sort((a, b) => manual.indexOf(a.field) - manual.indexOf(b.field))
  return rows
}

function formatValue(value: unknown, compact: boolean, unit: FinancialUnit) {
  if (!finiteNumber(value)) return String(value ?? '—')
  const scaled = compact ? value / unit.scale : value
  return scaled.toLocaleString(undefined, { maximumFractionDigits: compact ? 2 : 0 })
}

type TableProps = {
  table: HistoryTable; periods: string[]; search: string; order: string; compact: boolean; unit: FinancialUnit
  manual: string[]; selected: string[]; onMove: (field: string, delta: number) => void; onSelect: (fields: string[]) => void
}

function FinancialTable({ table, periods, search, order, compact, unit, manual, selected, onMove, onSelect }: TableProps) {
  const [page, setPage] = useState(0)
  const rows = orderedRows(table, search, order, manual)
  const pageSize = 22
  const pages = Math.max(1, Math.ceil(rows.length / pageSize))
  const safePage = Math.min(page, pages - 1)
  const shown = rows.slice(safePage * pageSize, safePage * pageSize + pageSize)
  const toggle = (field: string, checked: boolean) => onSelect(checked ? [...selected, field] : selected.filter(item => item !== field))
  return <div className="financial-table-panel"><div className="financial-table-wrap"><table className="financial-data-table"><colgroup><col className="financial-metric-col" />{periods.map(period => <col key={period} />)}</colgroup><thead><tr><th>Metric{compact && <small> · {unit.label}</small>}</th>{periods.map(period => <th key={period} title={period}>{period.slice(0, 4)}</th>)}</tr></thead><tbody>{shown.map(metric => <tr key={metric.field}><th><label className="financial-metric-label"><input type="checkbox" checked={selected.includes(metric.field)} onChange={event => toggle(metric.field, event.target.checked)} /><span title={metric.field}>{metric.display_name}</span></label>{order === 'manual' && <span className="row-order"><button onClick={() => onMove(metric.field, -1)} aria-label={`Move ${metric.display_name} up`}><ArrowUp /></button><button onClick={() => onMove(metric.field, 1)} aria-label={`Move ${metric.display_name} down`}><ArrowDown /></button></span>}</th>{metric.values.map((value, index) => <td key={periods[index] ?? index} title={String(value ?? '')}>{formatValue(value, compact, unit)}</td>)}</tr>)}</tbody></table></div><div className="table-pager"><span>{rows.length.toLocaleString()} metrics · page {safePage + 1} of {pages}</span><button disabled={safePage === 0} onClick={() => setPage(Math.max(0, safePage - 1))}>Previous</button><button disabled={safePage >= pages - 1} onClick={() => setPage(Math.min(pages - 1, safePage + 1))}>Next</button></div></div>
}

function SelectedChart({ metrics, periods, selected, compact, unit }: { metrics: HistoryMetric[]; periods: string[]; selected: string[]; compact: boolean; unit: FinancialUnit }) {
  const active = metrics.filter(metric => selected.includes(metric.field))
  const datasets = active.map((metric, index) => ({ label: metric.display_name, data: metric.values.map(value => finiteNumber(value) ? value / (compact ? unit.scale : 1) : null), borderColor: COLORS[index % COLORS.length], pointRadius: active.length > 8 ? 0 : 2, tension: .18 }))
  if (!active.length) return <EmptyState title="No metrics selected" description="Select metrics using the table checkboxes or selection buttons." />
  return <Line data={{ labels: periods, datasets }} options={{ responsive: true, maintainAspectRatio: false, interaction: { mode: 'index', intersect: false }, plugins: { legend: { position: 'bottom', labels: { boxWidth: 9, usePointStyle: true, font: { size: 10 } } } }, scales: { x: { grid: { display: false } }, y: { position: 'right', title: { display: compact, text: unit.label }, ticks: { callback: value => Number(value).toLocaleString() } } } }} />
}

export function FinancialHistoryWorkspace({ history, isLoading, error, retry }: { history?: SecurityHistory; isLoading: boolean; error: unknown; retry: () => void }) {
  const [source, setSource] = useState('')
  const [search, setSearch] = useState('')
  const [selected, setSelected] = useState<string[] | null>(null)
  const [compact, setCompact] = useState(true)
  const [order, setOrder] = useState('coverage')
  const [manualOrders, setManualOrders] = useState<Record<string, string[]>>({})
  const tables = useMemo(() => history?.tables ?? {}, [history?.tables])
  const sourceKey = source && tables[source] ? source : Object.keys(tables)[0] ?? ''
  const table = tables[sourceKey]
  const numeric = numericMetrics(table?.metrics ?? [])
  const defaults = defaultMetricFields(sourceKey, table, numeric)
  const active = (selected === null ? defaults : selected).filter(field => numeric.some(metric => metric.field === field))
  const complete = completeMetricFields(numeric, history?.periods ?? [])
  const unit = chooseFinancialUnit(numeric)
  const move = (field: string, delta: number) => { const fields = manualOrders[sourceKey] ?? (table?.metrics ?? []).map(metric => metric.field); const index = fields.indexOf(field); const next = [...fields]; const target = Math.max(0, Math.min(fields.length - 1, index + delta)); [next[index], next[target]] = [next[target], next[index]]; setManualOrders(current => ({ ...current, [sourceKey]: next })); setOrder('manual') }
  if (isLoading) return <LoadingState label="Loading financial history" />
  if (error) return <ErrorState error={error} retry={retry} />
  if (!table) return <EmptyState title="No financial history" description="No compatible statement tables were found." />
  return <div className="financial-workspace financial-workspace--unified"><div className="financial-toolbar financial-toolbar--unified"><select className="select" value={sourceKey} onChange={event => { setSource(event.target.value); setSelected(null) }}>{Object.entries(tables).map(([key, value]) => <option key={key} value={key}>{value.display_name}</option>)}</select><input className="input" value={search} onChange={event => setSearch(event.target.value)} placeholder="Filter metrics by name or field" /><select className="select" aria-label="Metric order" value={order} onChange={event => setOrder(event.target.value)}><option value="source">Source order</option><option value="alpha">A–Z</option><option value="coverage">Most Data</option><option value="manual">Manual order</option></select><div className="segmented"><button className={!compact ? 'active' : ''} onClick={() => setCompact(false)}>Raw</button><button className={compact ? 'active' : ''} onClick={() => setCompact(true)}>Compact · {unit.label}</button></div></div><div className="selection-actions"><span>{active.length} selected</span><button onClick={() => setSelected(defaults)}>Default</button><button onClick={() => setSelected(numeric.map(metric => metric.field))}>All</button><button onClick={() => setSelected([])}>None</button><button onClick={() => setSelected(complete)}>Complete data ({complete.length})</button></div><div className="financial-history-split"><FinancialTable table={table} periods={history?.periods ?? []} search={search} order={order} compact={compact} unit={unit} manual={manualOrders[sourceKey] ?? []} selected={active} onMove={move} onSelect={setSelected} /><section className="financial-chart-panel"><strong>Selected metrics</strong><div className="financial-chart financial-chart--unified"><SelectedChart metrics={numeric} periods={history?.periods ?? []} selected={active} compact={compact} unit={unit} /></div></section></div></div>
}
