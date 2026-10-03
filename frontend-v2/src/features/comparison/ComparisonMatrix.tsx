import { ArrowDownWideNarrow, X } from 'lucide-react'
import { Fragment, useEffect, useRef, type KeyboardEvent } from 'react'
import { Link } from 'react-router-dom'

import { Tip } from '../../components/Tooltip'
import { formatMetricValue, groupMetrics, metricDefinition, type MetricCurrencies, type MetricDefinition } from '../../metrics'
import { abbreviate, companyName, formatPeriod, heatColor, median, rankValues, shortName } from './comparisonModel'
import type { ComparisonCompany } from './comparisonTypes'
import { Swatch } from './CompanyPanel'

function currencies(company: ComparisonCompany): MetricCurrencies {
  return { price: company.market?.price_currency, reporting: company.reporting_currency }
}

/** A median only means something when every value is in the same currency. */
function sharedCurrencies(companies: ComparisonCompany[], definition: MetricDefinition): MetricCurrencies | null {
  if (definition.format !== 'money') return {}
  const key = definition.currency ?? 'reporting'
  const codes = new Set(companies.map(company => currencies(company)[key]).filter(Boolean))
  return codes.size === 1 ? currencies(companies[0]) : null
}

const DIRECTION_TIP = { higher: 'Higher is better', lower: 'Lower is better (negative values reflect losses and rank last)' }

/**
 * Metrics down, companies across. Cells are tinted from vermilion (worst) to
 * indigo (best) where a direction is meaningful; size metrics get a bar
 * relative to the largest company. One row and one company are under the
 * cursor: ↑/↓ and ←/→ (or J/K and H/L) move it, Enter opens the company.
 */
export function ComparisonMatrix({ companies, colorIndex, metrics, definitions, cursorMetric, cursorCode, showRanks, sortMetric, focusRequest, onCursor, onMoveRow, onMoveColumn, onOpen, onSort, onRemoveMetric }: {
  companies: ComparisonCompany[]
  colorIndex: Record<string, number>
  metrics: string[]
  definitions: Record<string, MetricDefinition>
  cursorMetric: string | null
  cursorCode: string | null
  showRanks: boolean
  sortMetric: string | null
  /** Changes whenever the page wants keyboard focus on the cursor row. */
  focusRequest: number
  onCursor: (cursor: { metric?: string; code?: string }) => void
  onMoveRow: (offset: number | 'first' | 'last') => void
  onMoveColumn: (offset: number) => void
  onOpen: (code: string, newTab?: boolean) => void
  onSort: (metric: string) => void
  onRemoveMetric: (metric: string) => void
}) {
  const rows = useRef(new Map<string, HTMLTableRowElement>())
  const body = useRef<HTMLTableSectionElement | null>(null)
  const showMedian = companies.length >= 3
  const periods = new Set(companies.map(company => company.period_end ?? ''))
  const handledRequest = useRef(focusRequest)
  // Focus follows the cursor while the table has it, or when the page asks (J/K from elsewhere).
  useEffect(() => {
    const requested = focusRequest !== handledRequest.current
    handledRequest.current = focusRequest
    const row = cursorMetric ? rows.current.get(cursorMetric) : undefined
    if (!row || !(requested || body.current?.contains(document.activeElement))) return
    row.focus({ preventScroll: true })
    row.scrollIntoView?.({ block: 'nearest' })
  }, [cursorMetric, focusRequest])

  const onKeyDown = (event: KeyboardEvent<HTMLTableSectionElement>) => {
    if (event.ctrlKey || event.metaKey || event.altKey) return
    const keys: Record<string, () => void> = {
      ArrowDown: () => onMoveRow(1),
      ArrowUp: () => onMoveRow(-1),
      ArrowRight: () => onMoveColumn(1),
      ArrowLeft: () => onMoveColumn(-1),
      Home: () => onMoveRow('first'),
      End: () => onMoveRow('last'),
      Enter: () => { if (cursorCode) onOpen(cursorCode, event.shiftKey) },
    }
    const action = keys[event.key]
    if (!action) return
    event.preventDefault()
    action()
  }

  return <div className="cmp-matrix-scroll">
    <table className="cmp-table cmp-matrix" aria-label="Comparison by metric">
      <thead>
        <tr>
          <th scope="col" className="cmp-matrix__corner">Metric</th>
          {companies.map(company => <th
            key={company.company_code}
            scope="col"
            className={company.company_code === cursorCode ? 'num is-cursor' : 'num'}
            title={[companyName(company), company.company.industry, company.period_end && `Fiscal year ending ${formatPeriod(company.period_end)}`, company.price_date && `Price ${company.price_date}`].filter(Boolean).join('\n')}
            onClick={() => onCursor({ code: company.company_code })}
          >
            <span className="cmp-matrix__company"><Swatch index={colorIndex[company.company_code] ?? 0} /><Link to={`/analyze/${encodeURIComponent(company.company_code)}`}>{shortName(companyName(company))}</Link></span>
            <small>{[company.company.ticker, periods.size > 1 && company.period_end && `FY ${formatPeriod(company.period_end)}`].filter(Boolean).join(' · ')}</small>
          </th>)}
          {showMedian && <th scope="col" className="num cmp-matrix__median"><Tip content="The middle value among these companies.">Median</Tip></th>}
        </tr>
      </thead>
      <tbody ref={body} onKeyDown={onKeyDown}>
        {groupMetrics(metrics, definitions).map(({ group, metrics: keys }) => <Fragment key={group}>
          <tr className="cmp-matrix__group"><th colSpan={companies.length + (showMedian ? 2 : 1)} scope="colgroup">{group}</th></tr>
          {keys.map(metric => {
            const definition = metricDefinition(metric, definitions)
            const values = companies.map(company => company.metrics[metric])
            const ranks = rankValues(definition.direction, values)
            const largest = Math.max(0, ...values.filter((value): value is number => value != null && Number.isFinite(value)).map(Math.abs))
            // Size bars compare scale (market cap, revenue, assets); a share price is not a size.
            const bars = !definition.direction && definition.format === 'money' && metric !== 'LatestPrice' && largest > 0
            const shared = sharedCurrencies(companies, definition)
            const middle = median(values)
            const isCursor = metric === cursorMetric
            return <tr
              key={metric}
              ref={element => { if (element) rows.current.set(metric, element); else rows.current.delete(metric) }}
              tabIndex={isCursor ? 0 : -1}
              className={isCursor ? 'is-cursor' : undefined}
              aria-selected={isCursor}
              onFocus={() => { if (!isCursor) onCursor({ metric }) }}
            >
              <th scope="row">
                <span className="cmp-matrix__label">
                  <Tip content={<span className="tip-lines"><strong>{definition.label}</strong>{definition.description && <span>{definition.description}</span>}{definition.direction && <span>{DIRECTION_TIP[definition.direction]}.</span>}</span>}>{definition.label}</Tip>
                  {definition.direction && <span className="cmp-matrix__dir" aria-label={DIRECTION_TIP[definition.direction]}>{definition.direction === 'higher' ? '↑' : '↓'}</span>}
                </span>
                <span className="cmp-matrix__actions">
                  <button type="button" className={sortMetric === metric ? 'icon-button is-on' : 'icon-button'} tabIndex={-1} aria-pressed={sortMetric === metric} aria-label={`Sort companies by ${definition.label}`} title="Sort companies by this metric, best first (S)" onClick={event => { event.stopPropagation(); onSort(metric) }}><ArrowDownWideNarrow /></button>
                  <button type="button" className="icon-button" tabIndex={-1} aria-label={`Hide ${definition.label}`} title="Hide this metric (X)" onClick={event => { event.stopPropagation(); onRemoveMetric(metric) }}><X /></button>
                </span>
              </th>
              {companies.map((company, index) => {
                const value = values[index]
                const rank = ranks[index]
                const text = formatMetricValue(definition, value, currencies(company))
                const classes = ['num', company.company_code === cursorCode && isCursor ? 'is-cursor-cell' : '', rank?.rank === 1 && rank.score === 1 ? 'is-best' : '', value == null ? 'is-empty' : ''].filter(Boolean).join(' ')
                return <td key={company.company_code} className={classes} style={{ background: heatColor(rank?.score) }} title={rank ? `${text} · ${rank.rank} of ${rank.of}` : text} onClick={() => onCursor({ metric, code: company.company_code })}>
                  {abbreviate(text)}
                  {showRanks && rank && <small className="cmp-matrix__rank">#{rank.rank}</small>}
                  {bars && value != null && <span className={value < 0 ? 'cmp-matrix__bar is-negative' : 'cmp-matrix__bar'} style={{ width: `${(Math.abs(value) / largest) * 100}%` }} aria-hidden="true" />}
                </td>
              })}
              {showMedian && <td className="num cmp-matrix__median">{shared ? abbreviate(formatMetricValue(definition, middle, shared)) : '—'}</td>}
            </tr>
          })}
        </Fragment>)}
      </tbody>
    </table>
  </div>
}
