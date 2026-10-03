import { useMemo } from 'react'
import { Bar, Line } from 'react-chartjs-2'

import { BRAND_COLORS, CHART_GRID_COLOR, SERIES_COLORS } from '../../../brand'
import { formatNumber, formatPercent } from '../researchModel'
import { cleanPrice, dirtyPrice, riskMeasures, survival, yieldFromPrice, type BondTerms, type Outcome } from './bondModel'
import { baseOptions, markersPlugin, type MarkerOptions } from './pricingCharts'

/** Price against yield: the curve, the duration tangent, and duration plus convexity. */
export function PriceYieldChart({ terms, yieldRate, marketPrice }: { terms: BondTerms; yieldRate: number; marketPrice: number | null }) {
  const data = useMemo(() => {
    const low = Math.max(-0.01, yieldRate - 0.05)
    const high = yieldRate + 0.05
    const xs = Array.from({ length: 81 }, (_, index) => low + (high - low) * index / 80)
    const base = cleanPrice(terms, yieldRate)
    const risk = riskMeasures(terms, yieldRate)
    // Clean and dirty prices differ by a constant accrual, so both move by the dirty price's sensitivity.
    const dirty = dirtyPrice(terms, yieldRate)
    const approx = (y: number, curvature: boolean) => base - risk.modified * dirty * (y - yieldRate) + (curvature ? 0.5 * risk.convexity * dirty * (y - yieldRate) ** 2 : 0)
    return {
      datasets: [
        { label: 'Price', data: xs.map(y => ({ x: y * 100, y: cleanPrice(terms, y) })), borderColor: SERIES_COLORS[0], backgroundColor: SERIES_COLORS[0], borderWidth: 2, pointRadius: 0 },
        { label: 'Duration estimate', data: xs.map(y => ({ x: y * 100, y: approx(y, false) })), borderColor: BRAND_COLORS.stone, backgroundColor: BRAND_COLORS.stone, borderWidth: 1, borderDash: [4, 3], pointRadius: 0 },
        { label: '+ convexity', data: xs.map(y => ({ x: y * 100, y: approx(y, true) })), borderColor: SERIES_COLORS[3], backgroundColor: SERIES_COLORS[3], borderWidth: 1, borderDash: [2, 2], pointRadius: 0 },
      ],
    }
  }, [terms, yieldRate])
  const options = useMemo(() => {
    const base = baseOptions({ x: 'Yield to maturity (%)', y: 'Clean price', xTicks: value => value.toFixed(1), yTicks: value => formatNumber(value, 0) })
    const market = marketPrice != null ? yieldFromPrice(terms, marketPrice) : null
    const markers: MarkerOptions = { markers: [
      { value: yieldRate * 100, label: 'Model', color: BRAND_COLORS.ink, dashed: false },
      ...(market != null ? [{ value: market * 100, label: 'Market', color: BRAND_COLORS.vermilion }] : []),
    ] }
    return {
      ...base,
      plugins: {
        ...base.plugins,
        pricingMarkers: markers,
        tooltip: { ...base.plugins?.tooltip, callbacks: { title: (items: Array<{ parsed: { x: number | null } }>) => `Yield ${(items[0]?.parsed.x ?? 0).toFixed(2)}%`, label: (item: { dataset: { label?: string }; parsed: { y: number | null } }) => `${item.dataset.label}: ${formatNumber(item.parsed.y, 3)}` } },
      },
    }
  }, [terms, yieldRate, marketPrice])
  return <figure className="rs-chart">
    <figcaption className="rs-chart__caption"><span>Price and yield</span><span className="rs-dim">the gap between the lines is convexity</span></figcaption>
    <div className="rs-chart__canvas" role="img" aria-label="Bond price by yield to maturity, with duration and convexity estimates">
      <Line data={data} options={options as never} plugins={[markersPlugin]} />
    </div>
  </figure>
}

/** The chance of default in each year, and of surviving to each year end. */
export function DefaultTimelineChart({ hazard, years }: { hazard: number; years: number }) {
  const count = Math.max(1, Math.ceil(years - 1e-9))
  const labels = Array.from({ length: count }, (_, index) => `Y${index + 1}`)
  const data = useMemo(() => ({
    labels,
    datasets: [
      { type: 'bar' as const, label: 'Default in the year', data: labels.map((_, index) => (survival(hazard, index) - survival(hazard, Math.min(index + 1, years))) * 100), backgroundColor: 'rgb(196 70 44 / 55%)', yAxisID: 'y' },
      { type: 'line' as const, label: 'Survival', data: labels.map((_, index) => survival(hazard, Math.min(index + 1, years)) * 100), borderColor: SERIES_COLORS[0], backgroundColor: SERIES_COLORS[0], borderWidth: 1.75, pointRadius: 2, yAxisID: 'y2' },
    ],
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }), [hazard, years, count])
  const options = useMemo(() => ({
    responsive: true,
    maintainAspectRatio: false,
    animation: false as const,
    interaction: { mode: 'index' as const, intersect: false },
    plugins: {
      legend: { position: 'bottom' as const, labels: { boxWidth: 12, boxHeight: 6, font: { size: 10 } } },
      tooltip: { callbacks: { label: (item: { dataset: { label?: string }; parsed: { y: number | null } }) => `${item.dataset.label}: ${(item.parsed.y ?? 0).toFixed(2)}%` } },
    },
    scales: {
      x: { grid: { display: false }, ticks: { font: { size: 10 } } },
      y: { beginAtZero: true, title: { display: true, text: 'Default in year (%)', font: { size: 10 } }, grid: { color: CHART_GRID_COLOR }, ticks: { font: { size: 10 } } },
      y2: { position: 'right' as const, min: 0, max: 100, title: { display: true, text: 'Survival (%)', font: { size: 10 } }, grid: { display: false }, ticks: { font: { size: 10 } } },
    },
  }), [])
  return <figure className="rs-chart">
    <figcaption className="rs-chart__caption"><span>Default risk by year</span><span className="rs-dim">annual rate {formatPercent(1 - Math.exp(-hazard), 2)}</span></figcaption>
    <div className="rs-chart__canvas" role="img" aria-label={`Chance of default in each year at an annual rate of ${formatPercent(1 - Math.exp(-hazard), 2)}`}>
      <Bar data={data as never} options={options} />
    </div>
  </figure>
}

/** Every way the bond can end: its probability and everything received against the price paid, fees included. */
export function OutcomesChart({ outcomes, frequency }: { outcomes: Outcome[]; frequency: number }) {
  const label = (item: Outcome) => item.defaulted ? `Default by ${formatNumber(item.time, frequency === 1 ? 0 : 1)}y` : 'Repaid'
  const data = useMemo(() => ({
    labels: outcomes.map(label),
    datasets: [
      // Repayment is far likelier than any one default date, so its bar is left out and its chance is in the caption.
      { type: 'bar' as const, label: 'Chance of default by then', data: outcomes.map(item => item.defaulted ? item.probability * 100 : null), backgroundColor: 'rgb(196 70 44 / 55%)', yAxisID: 'y' },
      { type: 'line' as const, label: 'Total return', data: outcomes.map(item => item.totalReturn * 100), borderColor: BRAND_COLORS.ink, backgroundColor: BRAND_COLORS.ink, borderWidth: 1.25, pointRadius: 2, yAxisID: 'y2' },
    ],
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }), [outcomes, frequency])
  const options = useMemo(() => ({
    responsive: true,
    maintainAspectRatio: false,
    animation: false as const,
    interaction: { mode: 'index' as const, intersect: false },
    plugins: {
      legend: { position: 'bottom' as const, labels: { boxWidth: 12, boxHeight: 6, font: { size: 10 } } },
      tooltip: { callbacks: { label: (item: { dataset: { label?: string }; parsed: { y: number | null } }) => `${item.dataset.label}: ${(item.parsed.y ?? 0).toFixed(2)}%` } },
    },
    scales: {
      x: { grid: { display: false }, ticks: { font: { size: 9 }, autoSkip: true, maxRotation: 0 } },
      y: { beginAtZero: true, title: { display: true, text: 'Probability (%)', font: { size: 10 } }, grid: { color: CHART_GRID_COLOR }, ticks: { font: { size: 10 } } },
      y2: { position: 'right' as const, title: { display: true, text: 'Total return (%)', font: { size: 10 } }, grid: { display: false }, ticks: { font: { size: 10 } } },
    },
  }), [])
  const repaid = outcomes[outcomes.length - 1]
  return <figure className="rs-chart">
    <figcaption className="rs-chart__caption"><span>Outcomes</span><span className="rs-dim">repaid in full {formatPercent(repaid?.probability, 1)}</span></figcaption>
    <div className="rs-chart__canvas" role="img" aria-label={`Probability and return of each outcome; repaid in full with probability ${formatPercent(repaid?.probability, 1)}`}>
      <Bar data={data as never} options={options} />
    </div>
  </figure>
}
