import { BarElement, CategoryScale, Chart as ChartJS, LinearScale, LineElement, PointElement, Tooltip, type ChartOptions, type TooltipItem } from 'chart.js'
import { BarChart3, Download, LineChart, Search, X } from 'lucide-react'
import { useMemo, useRef, useState, type KeyboardEvent as ReactKeyboardEvent } from 'react'
import { Bar, Line } from 'react-chartjs-2'

import type { SecurityHistory } from '../../api/types'
import { SERIES_COLORS } from '../../brand'
import { EmptyState, ErrorState, LoadingState } from '../../components/Feedback'
import { Tip } from '../../components/Tooltip'
import { useHotkeys } from '../../hooks/useHotkeys'
import { usePersistentState } from '../../hooks/usePersistentState'
import { crosshairPlugin } from './chartPlugins'
import { downloadTextFile, selectedMetricsCsv } from './downloads'
import { formatGranularNumber } from './numberFormat'
import {
  chooseFinancialUnit,
  commonSize,
  commonSizeBase,
  compoundGrowth,
  defaultChartFields,
  finiteNumber,
  formatCell,
  formatChange,
  formatShare,
  isRollingTable,
  layoutStatement,
  moneyDigits,
  orderTableKeys,
  periodChanges,
  shortTableName,
  type FinancialUnit,
  type StatementRow,
  type StatementView,
} from './statementLayout'

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, BarElement, Tooltip)

const VIEWS: Array<{ key: StatementView; label: string; hint: string }> = [
  { key: 'values', label: 'Values', hint: 'Reported values' },
  { key: 'yoy', label: 'YoY', hint: 'Change against the prior fiscal year (percentage points for ratios)' },
  { key: 'common', label: 'Common size', hint: 'Each line as a share of revenue (income statement) or total assets (balance sheet)' },
]
const VIEW_KEYS = VIEWS.map(view => view.key)
const CHART_TYPES = ['bar', 'line'] as const
type ChartType = typeof CHART_TYPES[number]
const MAX_SERIES = SERIES_COLORS.length

/** Slots hold chart colours: a series keeps its colour while others come and go. */
type Slots = Array<string | null>

function addToSlots(slots: Slots, field: string): Slots | null {
  const hole = slots.indexOf(null)
  if (hole !== -1) return slots.map((value, index) => index === hole ? field : value)
  return slots.length < MAX_SERIES ? [...slots, field] : null
}

function periodLabel(period: string) {
  return period.slice(0, 7)
}

function unitCaption(unit: FinancialUnit, currency: string | null) {
  const scale = unit.label === 'Units' ? '' : unit.label.toLowerCase()
  return [currency, scale].filter(Boolean).join(' ') || 'Values'
}

function Sparkline({ values, color }: { values: Array<number | null>; color?: string }) {
  const points = values.map((value, index) => ({ value, index })).filter((point): point is { value: number; index: number } => finiteNumber(point.value))
  if (points.length < 2) return null
  const width = 56
  const height = 16
  const min = Math.min(...points.map(point => point.value))
  const max = Math.max(...points.map(point => point.value))
  const x = (index: number) => 1 + (index / Math.max(1, values.length - 1)) * (width - 2)
  const y = (value: number) => max === min ? height / 2 : 1 + (1 - (value - min) / (max - min)) * (height - 2)
  // Gaps in the history break the line rather than bridging missing years.
  const segments: string[] = []
  let current: string[] = []
  values.forEach((value, index) => {
    if (finiteNumber(value)) current.push(`${x(index).toFixed(1)},${y(value).toFixed(1)}`)
    else if (current.length) { segments.push(current.join(' ')); current = [] }
  })
  if (current.length) segments.push(current.join(' '))
  const last = points[points.length - 1]
  return <svg className="sparkline" viewBox={`0 0 ${width} ${height}`} width={width} height={height} aria-hidden="true" style={color ? { color } : undefined}>
    {segments.map(segment => segment.includes(' ') ? <polyline key={segment} points={segment} /> : null)}
    <circle cx={x(last.index)} cy={y(last.value)} r="1.8" />
  </svg>
}

function seriesValues(row: StatementRow, view: StatementView, base: StatementRow | undefined, unit: FinancialUnit) {
  if (view === 'yoy') return periodChanges(row).map(value => finiteNumber(value) ? value * 100 : null)
  if (view === 'common') return commonSize(row, base).map(value => finiteNumber(value) ? value * 100 : null)
  if (row.kind === 'percent') return row.values.map(value => finiteNumber(value) ? value * 100 : null)
  if (row.kind === 'money') return row.values.map(value => finiteNumber(value) ? value / unit.scale : null)
  return row.values
}

function axisFor(row: StatementRow, view: StatementView, unit: FinancialUnit, currency: string | null) {
  if (view === 'yoy') return row.kind === 'percent' ? { key: 'pp', title: 'Change, percentage points', suffix: ' pp' } : { key: 'pct', title: 'Change, %', suffix: '%' }
  if (view === 'common') return { key: 'share', title: 'Share of base line, %', suffix: '%' }
  if (row.kind === 'money') return { key: 'money', title: unitCaption(unit, currency), suffix: '' }
  if (row.kind === 'percent') return { key: 'percent', title: '%', suffix: '%' }
  return { key: 'number', title: 'Value', suffix: '' }
}

interface Series { row: StatementRow; color: string; values: Array<number | null> }

function SeriesChart({ periods, series, type, title, suffix }: { periods: string[]; series: Series[]; type: ChartType; title: string; suffix: string }) {
  const format = (value: number) => `${value.toLocaleString('en-US', { maximumFractionDigits: Math.abs(value) >= 100 ? 0 : Math.abs(value) >= 10 ? 1 : 2 })}${suffix}`
  const options: ChartOptions<'bar' | 'line'> = {
    responsive: true,
    maintainAspectRatio: false,
    animation: { duration: 150 },
    interaction: { mode: 'index', intersect: false },
    plugins: {
      legend: { display: false },
      tooltip: {
        callbacks: {
          label: (item: TooltipItem<'bar' | 'line'>) => ` ${finiteNumber(item.parsed.y) ? format(item.parsed.y) : '—'}  ${item.dataset.label ?? ''}`,
        },
      },
    },
    scales: {
      x: { grid: { display: false }, ticks: { maxRotation: 0, autoSkip: true } },
      y: {
        position: 'right',
        title: { display: true, text: title, font: { size: 10 } },
        grid: { color: context => context.tick.value === 0 ? 'rgb(28 27 25 / 45%)' : 'rgb(228 223 211 / 90%)' },
        ticks: { callback: value => format(Number(value)), maxTicksLimit: 6 },
      },
    },
  }
  const labels = periods.map(periodLabel)
  if (type === 'line') {
    const data = { labels, datasets: series.map(item => ({ label: item.row.label, data: item.values, borderColor: item.color, backgroundColor: item.color, borderWidth: 2, pointRadius: 2.5, pointHoverRadius: 4, spanGaps: false, tension: 0 })) }
    return <Line data={data} options={options as ChartOptions<'line'>} plugins={[crosshairPlugin]} />
  }
  const data = { labels, datasets: series.map(item => ({ label: item.row.label, data: item.values, backgroundColor: item.color, borderRadius: 2, maxBarThickness: 26, categoryPercentage: .72, barPercentage: .92 })) }
  return <Bar data={data} options={options as ChartOptions<'bar'>} />
}

function cellText(row: StatementRow, index: number, view: StatementView, unit: FinancialUnit, changes: Array<number | null>, shares: Array<number | null>) {
  if (view === 'yoy') return { text: formatChange(changes[index], row.kind), value: changes[index] }
  if (view === 'common') return { text: formatShare(shares[index]), value: shares[index] }
  return { text: formatCell(row.values[index], row.kind, unit), value: row.values[index] }
}

function rowTooltip(row: StatementRow, periods: string[]) {
  return <span className="tip-lines">
    <strong>{row.label}</strong>
    <span>Reported as “{row.fullName}”</span>
    {row.mergedFrom.length > 0 && <span>Combined with earlier names: {row.mergedFrom.join(', ')}</span>}
    <span>{row.coverage} of {periods.length} fiscal years reported</span>
  </span>
}

export function FinancialHistoryWorkspace({ history, isLoading, error, retry, downloadPrefix, currency = null }: { history?: SecurityHistory; isLoading: boolean; error: unknown; retry: () => void; downloadPrefix?: string; currency?: string | null }) {
  const tables = useMemo(() => history?.tables ?? {}, [history?.tables])
  const periods = useMemo(() => history?.periods ?? [], [history?.periods])
  const tableKeys = useMemo(() => orderTableKeys(Object.keys(tables)), [tables])
  const layouts = useMemo(() => Object.fromEntries(tableKeys.map(key => [key, layoutStatement(key, tables[key].metrics)])), [tableKeys, tables])
  const populated = tableKeys.filter(key => layouts[key].length > 0)
  const [source, setSource] = useState('')
  const [view, setView] = usePersistentState<StatementView>('analysis.financials.view', 'values', VIEW_KEYS)
  const [chartType, setChartType] = usePersistentState<ChartType>('analysis.financials.chart', 'bar', CHART_TYPES)
  const [showEmpty, setShowEmpty] = useState(false)
  const [filter, setFilter] = useState('')
  const [slotsByTable, setSlotsByTable] = useState<Record<string, Slots>>({})
  const [notice, setNotice] = useState('')
  const [focusIndex, setFocusIndex] = useState(0)
  const filterInput = useRef<HTMLInputElement>(null)
  const rowRefs = useRef<Array<HTMLTableRowElement | null>>([])
  const tabRefs = useRef<Record<string, HTMLButtonElement | null>>({})

  const sourceKey = source && layouts[source]?.length ? source : populated[0] ?? ''
  const table = tables[sourceKey]
  const baseRows = useMemo(() => layouts[sourceKey] ?? [], [layouts, sourceKey])
  const rows = useMemo(() => showEmpty && table ? layoutStatement(sourceKey, table.metrics, true) : baseRows, [showEmpty, table, sourceKey, baseRows])
  const emptyCount = useMemo(() => table?.metrics.filter(metric => !metric.values.some(finiteNumber)).length ?? 0, [table])
  const query = filter.trim().toLowerCase()
  const visibleRows = query ? rows.filter(row => `${row.label} ${row.fullName}`.toLowerCase().includes(query)) : rows
  const unit = useMemo(() => {
    const money = baseRows.filter(row => row.kind === 'money')
    const chosen = chooseFinancialUnit(money)
    return { ...chosen, digits: moneyDigits(money, chosen) }
  }, [baseRows])
  const base = commonSizeBase(sourceKey, baseRows)
  const viewAvailable = (key: StatementView) => key !== 'common' || Boolean(base)
  const activeView: StatementView = viewAvailable(view) ? view : 'values'
  const slots: Slots = slotsByTable[sourceKey] ?? defaultChartFields(sourceKey, baseRows)
  const selected = slots.filter((field): field is string => field !== null)
  const colorOf = (field: string) => { const index = slots.indexOf(field); return index === -1 ? undefined : SERIES_COLORS[index] }

  const selectTable = (key: string) => {
    setSource(key)
    setFilter('')
    setFocusIndex(0)
    setNotice('')
  }
  const cycleTable = (step: number) => {
    if (!populated.length) return
    const next = populated[(populated.indexOf(sourceKey) + step + populated.length) % populated.length]
    selectTable(next)
    tabRefs.current[next]?.focus()
  }
  const setSlots = (next: Slots) => setSlotsByTable(current => ({ ...current, [sourceKey]: next }))
  const toggle = (field: string) => {
    setNotice('')
    if (slots.includes(field)) { setSlots(slots.map(value => value === field ? null : value)); return }
    const next = addToSlots(slots, field)
    if (next) setSlots(next)
    else setNotice(`Charts hold up to ${MAX_SERIES} lines. Remove one to add another.`)
  }
  const cycleView = () => {
    const available = VIEWS.filter(item => viewAvailable(item.key))
    setView(available[(available.findIndex(item => item.key === activeView) + 1) % available.length].key)
  }
  const exportCsv = () => {
    if (!visibleRows.length) return
    const prefix = downloadPrefix?.trim() ? downloadPrefix.trim() : 'financial-history'
    const metrics = visibleRows.map(row => ({ field: row.field, display_name: row.label, values: row.values }))
    downloadTextFile(`${prefix}-${sourceKey}.csv`, selectedMetricsCsv(metrics, periods, metrics.map(metric => metric.field)), 'text/csv;charset=utf-8')
  }

  useHotkeys({
    '[': () => cycleTable(-1),
    ']': () => cycleTable(1),
    v: cycleView,
    e: () => setShowEmpty(!showEmpty),
    f: () => filterInput.current?.focus(),
    c: () => setChartType(chartType === 'bar' ? 'line' : 'bar'),
    x: () => { setSlots([]); setNotice('') },
  }, populated.length > 0)

  const moveFocus = (index: number) => {
    const next = Math.max(0, Math.min(visibleRows.length - 1, index))
    setFocusIndex(next)
    rowRefs.current[next]?.focus()
    rowRefs.current[next]?.scrollIntoView({ block: 'nearest' })
  }
  const onTableKeyDown = (event: ReactKeyboardEvent<HTMLTableSectionElement>) => {
    const keys: Record<string, () => void> = {
      ArrowDown: () => moveFocus(focusIndex + 1), j: () => moveFocus(focusIndex + 1),
      ArrowUp: () => moveFocus(focusIndex - 1), k: () => moveFocus(focusIndex - 1),
      Home: () => moveFocus(0), End: () => moveFocus(visibleRows.length - 1),
      PageDown: () => moveFocus(focusIndex + 10), PageUp: () => moveFocus(focusIndex - 10),
      ' ': () => { const row = visibleRows[focusIndex]; if (row) toggle(row.field) },
      Enter: () => { const row = visibleRows[focusIndex]; if (row) toggle(row.field) },
    }
    const action = keys[event.key]
    if (!action || event.ctrlKey || event.metaKey || event.altKey) return
    event.preventDefault()
    event.stopPropagation()
    action()
  }
  const onTabKeyDown = (event: ReactKeyboardEvent<HTMLDivElement>) => {
    if (event.key !== 'ArrowRight' && event.key !== 'ArrowLeft') return
    event.preventDefault()
    cycleTable(event.key === 'ArrowRight' ? 1 : -1)
  }

  if (isLoading) return <LoadingState label="Loading financial statements" />
  if (error) return <ErrorState error={error} retry={retry} />
  if (!table) return <EmptyState title="No financial statements" description="No statement values were found in this company's stored filings." />

  const selectedRows = selected.map(field => baseRows.find(row => row.field === field) ?? rows.find(row => row.field === field)).filter((row): row is StatementRow => Boolean(row))
  const chartGroups = new Map<string, { title: string; suffix: string; series: Series[] }>()
  for (const row of selectedRows) {
    const axis = axisFor(row, activeView, unit, currency)
    const group = chartGroups.get(axis.key) ?? { title: axis.title, suffix: axis.suffix, series: [] }
    group.series.push({ row, color: colorOf(row.field) ?? SERIES_COLORS[0], values: seriesValues(row, activeView, base, unit) })
    chartGroups.set(axis.key, group)
  }
  const safeFocus = Math.min(focusIndex, Math.max(0, visibleRows.length - 1))
  const statementTabs = tableKeys.filter(key => !isRollingTable(key))
  const rollingTabs = tableKeys.filter(key => isRollingTable(key))
  const tab = (key: string) => {
    const count = layouts[key].length
    const label = shortTableName(key, tables[key].display_name)
    const active = key === sourceKey
    return <button
      key={key}
      ref={element => { tabRefs.current[key] = element }}
      role="tab"
      type="button"
      className={active ? 'statement-tab active' : 'statement-tab'}
      aria-selected={active}
      tabIndex={active ? 0 : -1}
      disabled={!count}
      title={count ? `${tables[key].display_name}: ${count} reported lines` : `${tables[key].display_name}: no values in the stored filings`}
      onClick={() => selectTable(key)}
    >{label}<small>{count || '—'}</small></button>
  }

  return <div className="statements">
    <div className="statement-tabs" role="tablist" aria-label="Financial statements" onKeyDown={onTabKeyDown}>
      {statementTabs.map(tab)}
      {rollingTabs.length > 0 && <span className="statement-tabs__divider"><Tip content="Rolling tables hold multi-year averages and growth rates derived from the annual statements." focusable={false}>Rolling</Tip></span>}
      {rollingTabs.map(tab)}
      <span className="statement-tabs__keys" aria-hidden="true"><kbd>[</kbd><kbd>]</kbd></span>
    </div>
    <div className="statement-toolbar">
      <div className="segmented segmented--small" role="group" aria-label="Statement view">
        {VIEWS.map(item => <button key={item.key} type="button" className={activeView === item.key ? 'active' : ''} aria-pressed={activeView === item.key} disabled={!viewAvailable(item.key)} title={viewAvailable(item.key) ? item.hint : 'Common size needs a revenue or total assets line'} onClick={() => setView(item.key)}>{item.label}</button>)}
      </div>
      <kbd className="toolbar-key" title="Cycle views">V</kbd>
      <label className="statement-filter">
        <Search aria-hidden="true" />
        <input ref={filterInput} value={filter} onChange={event => { setFilter(event.target.value); setFocusIndex(0) }} onKeyDown={event => { if (event.key === 'Escape') { setFilter(''); event.currentTarget.blur() } if (event.key === 'ArrowDown') { event.preventDefault(); moveFocus(0) } }} placeholder="Filter lines" aria-label="Filter statement lines" />
        <kbd>F</kbd>
      </label>
      {emptyCount > 0 && <button type="button" className="text-button" aria-pressed={showEmpty} onClick={() => setShowEmpty(!showEmpty)} title="Lines the taxonomy defines but this company never reported">
        {showEmpty ? 'Hide empty lines' : `Show ${emptyCount.toLocaleString()} empty lines`} <kbd>E</kbd>
      </button>}
      <span className="statement-toolbar__spacer" />
      <div className="segmented segmented--small" role="group" aria-label="Chart type">
        <button type="button" className={chartType === 'bar' ? 'active' : ''} aria-pressed={chartType === 'bar'} onClick={() => setChartType('bar')} title="Bar chart (C toggles)"><BarChart3 aria-hidden="true" />Bars</button>
        <button type="button" className={chartType === 'line' ? 'active' : ''} aria-pressed={chartType === 'line'} onClick={() => setChartType('line')} title="Line chart (C toggles)"><LineChart aria-hidden="true" />Lines</button>
      </div>
      <button type="button" className="button button--ghost button--small" onClick={exportCsv} title="Download the lines shown as CSV, with unrounded values"><Download aria-hidden="true" />CSV</button>
    </div>

    <section className="statement-chart" aria-label="Charted lines">
      <div className="series-legend">
        {selectedRows.map(row => <button key={row.field} type="button" className="series-chip" onClick={() => toggle(row.field)} title={`Remove ${row.label} from the chart`}><span className="series-chip__key" style={{ background: colorOf(row.field) }} />{row.label}<X aria-hidden="true" /></button>)}
        {selectedRows.length === 0 && <span className="series-legend__hint">Click a line in the table, or focus it and press <kbd>Space</kbd>, to chart it.</span>}
        {selectedRows.length > 0 && <button type="button" className="text-button series-legend__clear" onClick={() => { setSlots([]); setNotice('') }}>Clear <kbd>X</kbd></button>}
        {notice && <span className="series-legend__notice" role="status">{notice}</span>}
      </div>
      {chartGroups.size > 0 && <div className={`statement-chart__plots statement-chart__plots--${Math.min(chartGroups.size, 3)}`}>
        {[...chartGroups.values()].map(group => <div className="statement-chart__plot" key={group.title}><SeriesChart periods={periods} series={group.series} type={chartType} title={group.title} suffix={group.suffix} /></div>)}
      </div>}
    </section>

    <div className="statement-table-wrap" role="tabpanel" aria-label={tables[sourceKey].display_name}>
      <table className="statement-table">
        <caption className="sr-only">{tables[sourceKey].display_name}, {unitCaption(unit, currency)}. Use the arrow keys to move between lines and Space to chart one.</caption>
        <thead>
          <tr>
            <th scope="col" className="col-metric">{unitCaption(unit, currency)}{activeView !== 'values' && <small> · {VIEWS.find(item => item.key === activeView)?.label}</small>}</th>
            <th scope="col" className="col-trend">Trend</th>
            {periods.map((period, index) => <th scope="col" key={period} title={`Fiscal year ending ${period}`} className={index === periods.length - 1 ? 'is-latest' : undefined}>{periodLabel(period)}</th>)}
            {activeView === 'values' && <>
              <th scope="col" className="col-stat"><Tip content="Change in the latest fiscal year against the one before (percentage points for ratios)." focusable={false}>YoY</Tip></th>
              <th scope="col" className="col-stat"><Tip content="Compound annual growth rate between the first and last reported years, when both are positive." focusable={false}>CAGR</Tip></th>
            </>}
          </tr>
        </thead>
        <tbody onKeyDown={onTableKeyDown}>
          {visibleRows.map((row, rowIndex) => {
            const color = colorOf(row.field)
            const changes = periodChanges(row)
            const shares = activeView === 'common' ? commonSize(row, base) : []
            // Only a line reported in the latest year has a latest change.
            const latestChange = changes[changes.length - 1] ?? null
            const growth = activeView === 'values' ? compoundGrowth(row, periods) : null
            const classes = ['statement-row', row.total ? 'is-total' : '', color ? 'is-charted' : '', row.coverage === 0 ? 'is-empty' : ''].filter(Boolean).join(' ')
            return <tr
              key={row.field}
              ref={element => { rowRefs.current[rowIndex] = element }}
              className={classes}
              tabIndex={rowIndex === safeFocus ? 0 : -1}
              aria-selected={Boolean(color)}
              onFocus={() => setFocusIndex(rowIndex)}
              onClick={() => { if (!window.getSelection()?.toString()) toggle(row.field) }}
            >
              <th scope="row" className="col-metric" style={{ paddingLeft: `${8 + row.depth * 14}px` }}>
                <span className="chart-key" style={color ? { background: color, borderColor: color } : undefined} aria-hidden="true" />
                <Tip content={rowTooltip(row, periods)} focusable={false}><span className="row-label">{row.label}</span></Tip>
              </th>
              <td className="col-trend"><Sparkline values={row.values} color={color} /></td>
              {periods.map((period, index) => {
                const cell = cellText(row, index, activeView, unit, changes, shares)
                const raw = row.values[index]
                return <td key={period} title={finiteNumber(raw) ? formatGranularNumber(raw) : undefined} className={[finiteNumber(cell.value) && cell.value < 0 ? 'neg' : '', index === periods.length - 1 ? 'is-latest' : ''].filter(Boolean).join(' ') || undefined}>{cell.text}</td>
              })}
              {activeView === 'values' && <>
                <td className={`col-stat${finiteNumber(latestChange) && latestChange < 0 ? ' neg' : ''}`}>{formatChange(latestChange, row.kind)}</td>
                <td className={`col-stat${growth && growth.rate < 0 ? ' neg' : ''}`} title={growth ? `Over ${growth.years} years` : undefined}>{growth ? formatChange(growth.rate, 'money') : ''}</td>
              </>}
            </tr>
          })}
        </tbody>
      </table>
      {visibleRows.length === 0 && <p className="statement-empty">No lines match “{filter}”.</p>}
    </div>
    <p className="statement-footnote">
      {visibleRows.length.toLocaleString()} lines · fiscal years ending {periods.length ? `${periodLabel(periods[0])} to ${periodLabel(periods[periods.length - 1])}` : '—'} · values from EDINET XBRL filings
      <span className="statement-footnote__keys"><kbd>↑</kbd><kbd>↓</kbd> move · <kbd>Space</kbd> chart · <kbd>?</kbd> all shortcuts</span>
    </p>
  </div>
}
