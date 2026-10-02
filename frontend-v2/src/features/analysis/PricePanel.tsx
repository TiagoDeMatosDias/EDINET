import { CategoryScale, Chart as ChartJS, Filler, LinearScale, LineElement, PointElement, Tooltip, type ChartOptions } from 'chart.js'
import { useMemo } from 'react'
import { Line } from 'react-chartjs-2'

import { BRAND_COLORS } from '../../brand'
import { Tip } from '../../components/Tooltip'
import { useHotkeys } from '../../hooks/useHotkeys'
import { usePersistentState } from '../../hooks/usePersistentState'
import { crosshairPlugin, referenceLinePlugin } from './chartPlugins'
import { filterPriceHistory, PRICE_RANGES, priceDate, priceTicks, priceValue, type PriceHistoryRow, type PriceRangeKey } from './priceHistoryRanges'

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, Filler, Tooltip)

const RANGE_KEYS = PRICE_RANGES.map(range => range.key)

const tooltipDate = new Intl.DateTimeFormat('en-GB', { weekday: 'short', day: 'numeric', month: 'short', year: 'numeric', timeZone: 'UTC' })

export function PricePanel({ rows, formatPrice }: { rows: PriceHistoryRow[]; formatPrice: (value: number) => string }) {
  const [range, setRange] = usePersistentState<PriceRangeKey>('analysis.price.range', '5y', RANGE_KEYS)
  const step = (delta: number) => {
    const index = RANGE_KEYS.indexOf(range)
    setRange(RANGE_KEYS[Math.max(0, Math.min(RANGE_KEYS.length - 1, index + delta))])
  }
  useHotkeys({ '-': () => step(1), '=': () => step(-1), '+': () => step(-1) })
  const visible = useMemo(() => filterPriceHistory(rows, range).filter(row => priceValue(row) !== null), [rows, range])
  const labels = useMemo(() => visible.map(priceDate), [visible])
  const values = visible.map(row => priceValue(row) as number)
  const first = values[0]
  const last = values[values.length - 1]
  const change = first && last ? last / first - 1 : null
  const high = values.length ? Math.max(...values) : null
  const low = values.length ? Math.min(...values) : null
  const basis = [...new Set(visible.map(row => row.price_basis).filter((value): value is string => Boolean(value) && value !== 'unknown'))]
  const providers = [...new Set(visible.map(row => row.provider).filter((value): value is string => Boolean(value)))]
  const ticks = useMemo(() => priceTicks(labels), [labels])
  const options: ChartOptions<'line'> = {
    responsive: true,
    maintainAspectRatio: false,
    animation: false,
    interaction: { mode: 'index', intersect: false },
    plugins: {
      legend: { display: false },
      tooltip: {
        displayColors: false,
        callbacks: {
          title: items => {
            const time = Date.parse(`${items[0]?.label ?? ''}T00:00:00Z`)
            return Number.isFinite(time) ? tooltipDate.format(time) : items[0]?.label ?? ''
          },
          label: item => {
            const value = item.parsed.y
            if (value == null || !first) return ''
            const sinceStart = value / first - 1
            return `${formatPrice(value)}   ${sinceStart >= 0 ? '+' : '−'}${Math.abs(sinceStart * 100).toFixed(1)}% in range`
          },
        },
      },
      ...({ referenceLine: { value: first ?? null } } as object),
    },
    scales: {
      x: { grid: { display: false }, ticks: { autoSkip: false, maxRotation: 0, callback: (_value, index) => ticks.get(index) ?? null } },
      y: { position: 'right', grid: { color: 'rgb(228 223 211 / 80%)' }, ticks: { maxTicksLimit: 6, callback: value => formatPrice(Number(value)) } },
    },
  }
  const data = {
    labels,
    datasets: [{
      data: values,
      borderColor: BRAND_COLORS.ink,
      backgroundColor: 'rgb(28 27 25 / 5%)',
      fill: 'start' as const,
      borderWidth: 1.5,
      pointRadius: 0,
      pointHoverRadius: 3,
      pointHoverBackgroundColor: BRAND_COLORS.ink,
      tension: 0,
    }],
  }
  return <div className="price-panel">
    <div className="price-panel__toolbar">
      <div className="range-buttons" role="group" aria-label="Price range">
        {PRICE_RANGES.map(option => <button key={option.key} type="button" className={range === option.key ? 'active' : ''} aria-pressed={range === option.key} title={option.title} onClick={() => setRange(option.key)}>{option.label}</button>)}
        <span className="range-buttons__keys" aria-hidden="true"><kbd>-</kbd><kbd>=</kbd></span>
      </div>
      {change !== null && <dl className="price-panel__stats">
        <div><dt>Return</dt><dd className={change < 0 ? 'neg' : 'pos'}>{change >= 0 ? '+' : '−'}{Math.abs(change * 100).toFixed(1)}%</dd></div>
        {high !== null && <div><dt>High</dt><dd>{formatPrice(high)}</dd></div>}
        {low !== null && <div><dt>Low</dt><dd>{formatPrice(low)}</dd></div>}
      </dl>}
    </div>
    <div className="price-panel__chart">
      {visible.length > 1
        ? <Line data={data} options={options} plugins={[crosshairPlugin, referenceLinePlugin]} />
        : <p className="price-panel__empty">No prices in this range.</p>}
    </div>
    <p className="price-panel__source">
      {visible.length.toLocaleString()} closes
      {providers.length > 0 && <> · Source: {providers.join(', ')}</>}
      {basis.length > 0 && <> · <Tip content="Adjusted prices are restated for share splits so the history is comparable over time; raw prices are as traded.">{basis.includes('adjusted') ? 'split-adjusted' : basis.join(', ')}</Tip></>}
      <span className="price-panel__hint">Dashed line: price at the start of the range</span>
    </p>
  </div>
}
