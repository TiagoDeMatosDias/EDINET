import { ArrowUpRight, Check } from 'lucide-react'
import { useMemo } from 'react'

import { LoadingState } from '../../components/Feedback'
import { Metric } from '../../components/Page'
import { IncomeCompanyPicker } from './IncomeCompanyPicker'
import { indexedPerShare, MEASURE_LABEL, selectPayments, stackIncome, summarizeIncome, weightedGrowthByYear, withheld, type IncomeGrouping, type IncomeMeasure, type IncomeStack } from './incomeModel'
import { AnnualPerShareChart, GrowthByYearChart, IncomeStackChart, IndexedPerShareChart, PerShareChart } from './PortfolioCharts'
import { formatDay, money, percent, quantity, signedPercent } from './portfolioFormat'
import { SectionCard } from './PortfolioPrimitives'
import { PortfolioTable, type TableColumn } from './PortfolioTable'
import type { IncomeCompany, IncomeData, IncomePayment } from './portfolioTypes'

export type IncomeView = { grouping: IncomeGrouping; measure: IncomeMeasure; stack: IncomeStack }

type Props = {
  data?: IncomeData
  isLoading: boolean
  currency: string
  /** The page period's base date; payments after it are shown. */
  start?: string
  periodLabel: string
  selected: string[]
  onSelect: (symbols: string[]) => void
  view: IncomeView
  onView: (view: IncomeView) => void
  hotkeys?: boolean
  onAnalyze: (company: IncomeCompany) => void
}

const FREQUENCY: Record<number, string> = { 1: 'Yearly', 2: 'Twice a year', 4: 'Quarterly', 12: 'Monthly' }

/** Years from the first payment to the valuation date, at least one (so a short span is not scaled up). */
function yearsSpanned(first: string | undefined, last: string) {
  if (!first) return 1
  return Math.max(1, (Date.parse(last) - Date.parse(first)) / 31_557_600_000)
}

function Segmented<T extends string>({ label, value, options, onChange }: { label: string; value: T; options: Array<[T, string]>; onChange: (value: T) => void }) {
  return <div className="pf-segment"><span>{label}</span><div className="period-tabs" role="group" aria-label={label}>
    {options.map(([key, text]) => <button key={key} type="button" className={`period-tab${value === key ? ' active' : ''}`} aria-pressed={value === key} onClick={() => onChange(key)}>{text}</button>)}
  </div></div>
}

function payerColumns(currency: string, chosen: Set<string>, onToggle: (symbol: string) => void): TableColumn<IncomeCompany>[] {
  return [
    { id: 'pick', header: '', cell: row => <button type="button" className={`pf-check${chosen.has(row.symbol) ? ' is-on' : ''}`} role="checkbox" aria-checked={chosen.has(row.symbol)} aria-label={`Show ${row.symbol}`} onClick={() => onToggle(row.symbol)}>{chosen.has(row.symbol) && <Check aria-hidden="true" />}</button> },
    { id: 'symbol', header: 'Company', rowHeader: true, sortValue: row => row.symbol, cell: row => <span className="pf-holding"><span><strong>{row.symbol}{!row.is_held && <em> sold</em>}</strong><small>{row.name}</small></span></span> },
    { id: 'net', header: `Net (${currency})`, numeric: true, tip: 'Dividends after withholding tax in the period, converted on each payment date.', sortValue: row => row.net, cell: row => money(row.net, currency) },
    { id: 'gross', header: 'Gross', numeric: true, sortValue: row => row.gross, cell: row => money(row.gross, currency) },
    { id: 'tax', header: 'Withheld', numeric: true, tip: 'Withholding tax and the share of gross dividends it took.', sortValue: row => -row.tax, cell: row => <span>{money(withheld(row.tax), currency)}<small className="pf-cell-note">{percent(row.withholding_rate)}</small></span> },
    { id: 'share', header: 'Share', numeric: true, tip: 'Share of all the net dividends shown.', sortValue: row => row.share_of_income, cell: row => percent(row.share_of_income) },
    { id: 'payments', header: 'Payments', numeric: true, sortValue: row => row.payments, cell: row => <span>{row.payments}<small className="pf-cell-note">{FREQUENCY[row.frequency] ?? `${row.frequency}× a year`}</small></span> },
    { id: 'ttm', header: 'Last 12 mo', numeric: true, tip: 'Net dividends received in the year to the valuation date.', sortValue: row => row.ttm_net, cell: row => row.ttm_net ? money(row.ttm_net, currency) : '—' },
    { id: 'dps', header: 'Per share', numeric: true, tip: 'The latest payment per share, in the currency it was declared in.', sortValue: row => row.latest_per_share, cell: row => row.latest_per_share ? <span>{money(row.latest_per_share, row.currency, row.latest_per_share < 10 ? 3 : 0)}<small className="pf-cell-note">{formatDay(row.latest_date)}</small></span> : '—' },
    { id: 'growth', header: 'Growth', numeric: true, tip: 'The latest amount per share against the payment about a year earlier.', sortValue: row => row.per_share_growth_1y, cell: row => <span className={Number(row.per_share_growth_1y) < 0 ? 'number-negative' : undefined}>{signedPercent(row.per_share_growth_1y)}</span> },
    { id: 'yoc', header: 'Yield on cost', numeric: true, tip: 'The last twelve months’ dividends per share against your average cost per share (held companies).', sortValue: row => row.yield_on_cost, cell: row => percent(row.yield_on_cost, 2) },
    { id: 'yield', header: 'Yield', numeric: true, tip: 'The last twelve months’ dividends per share against the latest price.', sortValue: row => row.current_yield, cell: row => percent(row.current_yield, 2) },
  ]
}

function rangeCompany(company: IncomeCompany, payments: IncomePayment[], total: number): IncomeCompany {
  // Totals for the chosen period; the per-share history stays complete.
  const gross = payments.reduce((sum, row) => sum + row.gross, 0)
  const tax = payments.reduce((sum, row) => sum + row.tax, 0)
  return { ...company, gross, tax, net: gross + tax, withholding_rate: gross > 0 ? -tax / gross : null, share_of_income: total ? (gross + tax) / total : null, payments: payments.filter(row => row.type !== 'Tax adjustment').length }
}

function CompanyDetail({ company, payments, currency, onAnalyze }: { company: IncomeCompany; payments: IncomePayment[]; currency: string; onAnalyze: (company: IncomeCompany) => void }) {
  const dividends = payments.filter(row => row.type !== 'Tax adjustment')
  const columns: TableColumn<IncomePayment>[] = [
    { id: 'date', header: 'Paid', sortValue: row => row.date, cell: row => <span className="mono">{row.date}</span> },
    { id: 'type', header: 'Type', cell: row => row.type + (row.in_lieu_native ? ' (part in lieu)' : '') },
    { id: 'per_share', header: 'Per share', numeric: true, sortValue: row => row.per_share, cell: row => row.per_share ? money(row.per_share, row.currency, 4) : '—' },
    { id: 'shares', header: 'Shares', numeric: true, tip: 'Shares the payment was made on (gross ÷ amount per share).', sortValue: row => row.shares, cell: row => row.shares ? quantity(row.shares) : '—' },
    { id: 'gross', header: `Gross (${company.currency})`, numeric: true, sortValue: row => row.gross_native, cell: row => money(row.gross_native, row.currency, 2) },
    { id: 'tax', header: 'Withheld', numeric: true, sortValue: row => -row.tax_native, cell: row => <span>{money(withheld(row.tax_native), row.currency, 2)}<small className="pf-cell-note">{percent(row.withholding_rate)}</small></span> },
    { id: 'net_native', header: 'Net', numeric: true, sortValue: row => row.net_native, cell: row => money(row.net_native, row.currency, 2) },
    { id: 'net', header: `Net (${currency})`, numeric: true, sortValue: row => row.net, cell: row => money(row.net, currency, 2) },
  ]
  return <SectionCard
    title={`${company.symbol} · ${company.name}`}
    description={[company.is_held ? `${quantity(company.shares_held)} shares held` : 'No longer held', FREQUENCY[company.frequency] ?? `${company.frequency} payments a year`, `paid in ${company.currency}`, company.edinet_code].filter(Boolean).join(' · ')}
    actions={<button type="button" className="card-explore" onClick={() => onAnalyze(company)}>Open in Analysis<ArrowUpRight /></button>}
  >
    <div className="portfolio-section-metrics pf-income-company">
      <Metric label="Latest per share" value={company.latest_per_share ? money(company.latest_per_share, company.currency, 4) : '—'} detail={formatDay(company.latest_date)} />
      <Metric label="Growth on a year ago" value={signedPercent(company.per_share_growth_1y)} detail="Per payment" />
      <Metric label="Last 12 months per share" value={company.ttm_per_share ? money(company.ttm_per_share, company.currency, 4) : '—'} />
      <Metric label="Yield on cost" value={percent(company.yield_on_cost, 2)} detail="Last 12 months ÷ average cost" />
      <Metric label="Current yield" value={percent(company.current_yield, 2)} detail="Last 12 months ÷ latest price" />
      <Metric label="Withholding" value={percent(company.withholding_rate)} detail="Of gross, all payments" />
    </div>
    <div className="pf-income-grid pf-income-grid--even">
      <section><h3 className="pf-subhead">Per share, each payment <span>{company.currency}</span></h3><PerShareChart payments={dividends} currency={company.currency} /></section>
      <section><h3 className="pf-subhead">Per share, each calendar year <span>* part of a year</span></h3><AnnualPerShareChart years={company.annual} currency={company.currency} /></section>
    </div>
    <h3 className="pf-subhead">Payments <span>{payments.length} in the period</span></h3>
    <PortfolioTable label={`${company.symbol} payments`} rows={payments} columns={columns} rowKey={row => `${row.date}-${row.type}-${row.per_share}`} initialSort={{ column: 'date', direction: 'desc' }} pageSize={25} hotkeys={false} />
  </SectionCard>
}

export function PortfolioIncome(props: Props) {
  const { data, currency, selected, view } = props
  const chosen = useMemo(() => new Set(selected), [selected])
  const inRange = useMemo(() => selectPayments(data?.payments ?? [], [], props.start), [data, props.start])
  const shown = useMemo(() => selectPayments(inRange, selected), [inRange, selected])
  const total = useMemo(() => inRange.reduce((sum, row) => sum + row.net, 0), [inRange])
  const payers = useMemo(() => (data?.companies ?? [])
    .map(company => rangeCompany(company, inRange.filter(row => row.symbol === company.symbol), total))
    .filter(company => company.payments > 0 || Math.abs(company.net) > 0.005), [data, inRange, total])
  const summary = summarizeIncome(shown, inRange, data?.valuation_date ?? '', selected.length > 0)
  const stacked = useMemo(() => stackIncome(shown, view.grouping, view.measure, view.stack), [shown, view])
  const focusCompanies = selected.length ? payers.filter(company => chosen.has(company.symbol)) : payers
  const fromYear = props.start ? Number(props.start.slice(0, 4)) + 1 : undefined
  const growth = useMemo(() => weightedGrowthByYear((data?.companies ?? []).filter(company => !selected.length || chosen.has(company.symbol)), fromYear), [chosen, data, fromYear, selected.length])
  const indexed = useMemo(() => indexedPerShare((data?.companies ?? []).filter(company => chosen.has(company.symbol))), [chosen, data])
  const single = selected.length === 1 ? (data?.companies ?? []).find(company => company.symbol === selected[0]) : undefined
  const toggle = (symbol: string) => props.onSelect(chosen.has(symbol) ? selected.filter(item => item !== symbol) : [...selected, symbol])

  if (props.isLoading) return <LoadingState label="Loading dividends" />
  if (!data?.payments.length) return <p className="portfolio-empty-copy">No dividend payments have been imported.</p>
  return <div className="portfolio-section-stack">
    <div className="pf-income-controls">
      <IncomeCompanyPicker companies={data.companies} selected={selected} currency={currency} onChange={props.onSelect} />
      <div className="pf-income-controls__view">
        <Segmented label="Show" value={view.measure} options={[['net', 'Net'], ['gross', 'Gross'], ['tax', 'Withholding']]} onChange={measure => props.onView({ ...view, measure })} />
        <Segmented label="By" value={view.grouping} options={[['monthly', 'Month'], ['quarterly', 'Quarter'], ['yearly', 'Year']]} onChange={grouping => props.onView({ ...view, grouping })} />
        <Segmented label="Stack" value={view.stack} options={[['company', 'Company'], ['currency', 'Currency']]} onChange={stack => props.onView({ ...view, stack })} />
      </div>
    </div>
    <div className="portfolio-section-metrics">
      <Metric label="Net dividends" value={money(summary.net, currency)} detail={`${summary.payments} payments · ${summary.companies} compan${summary.companies === 1 ? 'y' : 'ies'}`} />
      <Metric label="Gross dividends" value={money(summary.gross, currency)} />
      <Metric label="Withholding tax" value={money(withheld(summary.tax), currency)} detail={summary.rate != null ? `${percent(summary.rate)} of gross` : undefined} />
      <Metric label="Last 12 months" value={money(summary.lastTwelveMonths, currency)} detail="Net, to the valuation date" />
      <Metric label={selected.length ? 'Share of all dividends' : 'Average a year'} value={selected.length ? percent(summary.shareOfAll) : money(summary.net / yearsSpanned(shown[0]?.date, data.valuation_date), currency)} detail={selected.length ? 'Of every payer in the period' : 'Net, since the first payment shown'} />
      <Metric label="Dividend growth" value={growth.length ? signedPercent(growth[growth.length - 1].growth) : '—'} detail={growth.length ? `${growth[growth.length - 1].year}, per share, income-weighted` : 'Needs two complete years'} />
    </div>
    <SectionCard title={`${MEASURE_LABEL[view.measure]} by ${view.grouping === 'monthly' ? 'month' : view.grouping === 'quarterly' ? 'quarter' : 'year'}`} description={`${props.periodLabel} · ${selected.length ? selected.join(', ') : 'all payers'} · converted to ${currency} on each payment date`}>
      <IncomeStackChart periods={stacked.periods} series={stacked.series} currency={currency} measureLabel={MEASURE_LABEL[view.measure]} />
    </SectionCard>
    {single && <CompanyDetail company={payers.find(company => company.symbol === single.symbol) ?? single} payments={shown} currency={currency} onAnalyze={props.onAnalyze} />}
    {!single && <div className="pf-income-grid pf-income-grid--even">
      <SectionCard title="Dividend-per-share growth" description={`Year on year, ${selected.length ? 'for the chosen companies' : 'across every payer'}, weighted by the income each paid`}>
        <GrowthByYearChart rows={growth} />
      </SectionCard>
      {selected.length > 1 && selected.length <= 6
        ? <SectionCard title="Per share, indexed" description="Each company’s dividend per share by year, 100 in its first complete year">
          <IndexedPerShareChart years={indexed.years} series={indexed.series} />
          {indexed.series.length < selected.length && <p className="pf-footnote">Not shown: {selected.filter(symbol => !indexed.series.some(series => series.symbol === symbol)).join(', ')}, with fewer than two complete years of payments.</p>}
        </SectionCard>
        : <SectionCard title="Withholding by company" description="Share of gross dividends withheld, largest payers first">
          <ul className="pf-withholding">{focusCompanies.slice(0, 10).map(company => <li key={company.symbol}><strong>{company.symbol}</strong><span className="pf-bars__track" aria-hidden="true"><i style={{ width: `${Math.min(100, Math.max(0, company.withholding_rate ?? 0) / 0.3 * 100)}%` }} /></span><b>{percent(company.withholding_rate)}<small>{money(withheld(company.tax), currency)}</small></b></li>)}</ul>
          <p className="pf-footnote">Bars run to 30%. Treaty rates are typically 15% for US dividends and 15.315% for Japanese ones.</p>
        </SectionCard>}
    </div>}
    <SectionCard title="Payers" description={`${payers.length} companies paid in the period. Enter shows one company; A adds or removes it from the selection.`}>
      <PortfolioTable
        label="Paying companies"
        rows={payers}
        columns={payerColumns(currency, chosen, toggle)}
        rowKey={row => row.symbol}
        initialSort={{ column: 'net', direction: 'desc' }}
        hotkeys={props.hotkeys && !single}
        onOpen={row => props.onSelect([row.symbol])}
        openLabel="show only this"
        onSecondary={row => toggle(row.symbol)}
        secondaryLabel="add or remove"
        rowClassName={row => chosen.has(row.symbol) ? 'is-chosen' : undefined}
      />
    </SectionCard>
  </div>
}
