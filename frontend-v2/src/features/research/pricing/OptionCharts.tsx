import { useMemo, useState } from 'react'
import { Line } from 'react-chartjs-2'

import { BRAND_COLORS, SERIES_COLORS } from '../../../brand'
import { formatMetricValue } from '../../../metrics'
import { formatDay, formatPercent } from '../researchModel'
import { blackScholes, strategyProfit, type Leg, type Market, type OptionInputs } from './optionsModel'
import { baseOptions, markersPlugin, shortAmount, type MarkerOptions } from './pricingCharts'
import { PRICE_FORMAT } from '../researchQueries'

const POINTS = 161

function grid(from: number, to: number, count = POINTS) {
  return Array.from({ length: count }, (_, index) => from + (to - from) * index / (count - 1))
}

/** Profit or loss across share prices: at expiry, today, and halfway there. */
export function PayoffChart({ legs, years, market, fees = 0, breakevens, move, currency }: {
  legs: Leg[]
  years: number
  market: Market
  /** Trading fees for the whole position, per share. */
  fees?: number
  breakevens: number[]
  move: { low: number; high: number }
  currency: string | null
}) {
  const data = useMemo(() => {
    const strikes = legs.filter(leg => leg.kind !== 'stock').map(leg => leg.strike)
    const low = Math.max(0, Math.min(market.spot * 0.6, ...strikes.map(strike => strike * 0.85), move.low * 0.9))
    const high = Math.max(market.spot * 1.4, ...strikes.map(strike => strike * 1.15), move.high * 1.1)
    const xs = grid(low, high)
    const line = (remaining: number) => xs.map(x => ({ x, y: strategyProfit(legs, x, remaining, years, market, fees) }))
    return {
      datasets: [
        { label: 'At expiry', data: line(0), borderColor: SERIES_COLORS[0], backgroundColor: SERIES_COLORS[0], borderWidth: 2, pointRadius: 0 },
        { label: 'Today', data: line(years), borderColor: SERIES_COLORS[1], backgroundColor: SERIES_COLORS[1], borderWidth: 1.5, pointRadius: 0 },
        { label: 'Halfway', data: line(years / 2), borderColor: SERIES_COLORS[3], backgroundColor: SERIES_COLORS[3], borderWidth: 1.25, borderDash: [4, 3], pointRadius: 0 },
      ],
    }
  }, [legs, years, market, fees, move])
  const money = (value: number) => formatMetricValue(PRICE_FORMAT, value, { price: currency })
  const options = useMemo(() => {
    const base = baseOptions({ x: 'Share price', y: 'Profit or loss', xTicks: shortAmount, yTicks: shortAmount })
    const markers: MarkerOptions = {
      zeroLine: true,
      band: { from: move.low, to: move.high },
      markers: [
        { value: market.spot, label: 'Spot', color: BRAND_COLORS.ink, dashed: false },
        ...breakevens.map(value => ({ value, label: 'Breakeven', color: BRAND_COLORS.vermilion })),
      ],
    }
    return {
      ...base,
      plugins: {
        ...base.plugins,
        pricingMarkers: markers,
        tooltip: { ...base.plugins?.tooltip, callbacks: { title: (items: Array<{ parsed: { x: number | null } }>) => `Price ${money(items[0]?.parsed.x ?? 0)}`, label: (item: { dataset: { label?: string }; parsed: { y: number | null } }) => `${item.dataset.label}: ${money(item.parsed.y ?? 0)}` } },
      },
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [market.spot, breakevens, move, currency])
  return <figure className="rs-chart">
    <figcaption className="rs-chart__caption"><span>Profit or loss by share price</span><span className="rs-dim">shaded: one standard deviation by expiry</span></figcaption>
    <div className="rs-chart__canvas" role="img" aria-label={`Profit or loss of the strategy by share price; breakevens ${breakevens.map(money).join(', ') || 'none'}`}>
      <Line data={data} options={options as never} plugins={[markersPlugin]} />
    </div>
  </figure>
}

/** Call and put value as volatility or time to expiry changes, all else equal. */
export function SensitivityChart({ input, implied, currency }: { input: OptionInputs; implied: number | null; currency: string | null }) {
  const [mode, setMode] = useState<'volatility' | 'time'>('volatility')
  const days = Math.round(input.years * 365)
  const data = useMemo(() => {
    const xs = mode === 'volatility' ? grid(0.05, Math.max(1, input.volatility * 2), 96) : grid(0, days, Math.min(Math.max(days + 1, 2), 120))
    const value = (kind: 'call' | 'put', x: number) => blackScholes(kind, mode === 'volatility' ? { ...input, volatility: x } : { ...input, years: x / 365 }).price
    return {
      datasets: (['call', 'put'] as const).map((kind, index) => ({
        label: kind === 'call' ? 'Call' : 'Put',
        data: xs.map(x => ({ x: mode === 'volatility' ? x * 100 : x, y: value(kind, x) })),
        borderColor: SERIES_COLORS[index * 2],
        backgroundColor: SERIES_COLORS[index * 2],
        borderWidth: 1.75,
        pointRadius: 0,
      })),
    }
  }, [input, mode, days])
  const options = useMemo(() => {
    const base = baseOptions({
      x: mode === 'volatility' ? 'Volatility (%)' : 'Days left to expiry',
      y: 'Option value',
      yTicks: shortAmount,
    })
    const markers: MarkerOptions = {
      markers: mode === 'volatility'
        ? [{ value: input.volatility * 100, label: 'Used', color: BRAND_COLORS.ink, dashed: false }, ...(implied != null ? [{ value: implied * 100, label: 'Implied', color: BRAND_COLORS.vermilion }] : [])]
        : [{ value: days, label: 'Today', color: BRAND_COLORS.ink, dashed: false }],
    }
    return {
      ...base,
      scales: { ...base.scales, x: { ...base.scales?.x, reverse: mode === 'time' } },
      plugins: {
        ...base.plugins,
        pricingMarkers: markers,
        tooltip: { ...base.plugins?.tooltip, callbacks: { title: (items: Array<{ parsed: { x: number | null } }>) => mode === 'volatility' ? `Volatility ${(items[0]?.parsed.x ?? 0).toFixed(1)}%` : `${Math.round(items[0]?.parsed.x ?? 0)} days left`, label: (item: { dataset: { label?: string }; parsed: { y: number | null } }) => `${item.dataset.label}: ${formatMetricValue(PRICE_FORMAT, item.parsed.y, { price: currency })}` } },
      },
    }
  }, [mode, input.volatility, implied, days, currency])
  return <figure className="rs-chart">
    <figcaption className="rs-chart__caption">
      <span>Value as</span>
      <div className="segmented segmented--small" role="group" aria-label="Sensitivity to">
        <button type="button" className={mode === 'volatility' ? 'active' : ''} aria-pressed={mode === 'volatility'} onClick={() => setMode('volatility')}>Volatility changes</button>
        <button type="button" className={mode === 'time' ? 'active' : ''} aria-pressed={mode === 'time'} onClick={() => setMode('time')}>Time passes</button>
      </div>
    </figcaption>
    <div className="rs-chart__canvas" role="img" aria-label={mode === 'volatility' ? 'Call and put value by volatility' : 'Call and put value as expiry approaches'}>
      <Line data={data} options={options as never} plugins={[markersPlugin]} />
    </div>
  </figure>
}

/** Three-month realised volatility over time, against the volatility used. */
export function VolatilityHistoryChart({ history, chosen }: { history: Array<{ date: string; value: number }>; chosen: number }) {
  const data = useMemo(() => ({
    labels: history.map(point => point.date),
    datasets: [
      { label: '3-month realised', data: history.map(point => point.value * 100), borderColor: SERIES_COLORS[0], backgroundColor: 'rgb(52 96 168 / 10%)', fill: 'origin', borderWidth: 1.5, pointRadius: 0 },
      { label: 'Used', data: history.map(() => chosen * 100), borderColor: BRAND_COLORS.vermilion, borderDash: [4, 3], borderWidth: 1.25, pointRadius: 0 },
    ],
  }), [history, chosen])
  const options = useMemo(() => ({
    responsive: true,
    maintainAspectRatio: false,
    animation: false as const,
    interaction: { mode: 'index' as const, intersect: false },
    plugins: {
      legend: { position: 'bottom' as const, labels: { boxWidth: 12, boxHeight: 2, font: { size: 10 } } },
      tooltip: { callbacks: { title: (items: Array<{ label: string }>) => formatDay(items[0]?.label), label: (item: { dataset: { label?: string }; parsed: { y: number | null } }) => `${item.dataset.label}: ${(item.parsed.y ?? 0).toFixed(1)}%` } },
    },
    scales: {
      x: { ticks: { font: { size: 10 }, maxTicksLimit: 6, callback: (_value: unknown, index: number) => history[index]?.date.slice(0, 7) ?? '' }, grid: { display: false } },
      y: { min: 0, title: { display: true, text: 'Volatility (%)', font: { size: 10 } }, ticks: { font: { size: 10 } } },
    },
  }), [history])
  const latest = history[history.length - 1]
  return <figure className="rs-chart">
    <figcaption className="rs-chart__caption"><span>Realised volatility, 3-month window</span><span className="rs-dim">now {formatPercent(latest?.value)}</span></figcaption>
    <div className="rs-chart__canvas" role="img" aria-label="Three-month realised volatility over the last three years">
      <Line data={data} options={options} />
    </div>
  </figure>
}
