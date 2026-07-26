import { Fragment, useState } from 'react'
import { useMutation } from '@tanstack/react-query'
import { ArrowDown, ArrowUp, X } from 'lucide-react'

import { apiPost } from '../../api/client'
import type { SecuritySearchResult } from '../../api/types'
import { CompanyPicker } from '../../components/CompanyPicker'
import { EmptyState, LoadingState } from '../../components/Feedback'
import { Card, PageHeader } from '../../components/Page'

interface ComparisonCompany {
  company_code: string
  company: { company_name?: string; ticker?: string; industry?: string; market?: string }
  metrics: Record<string, number | null>
  common_size_income: Record<string, number | null>
  common_size_balance: Record<string, number | null>
  percentiles: Record<string, number | null>
  period_end?: string | null
  price_date?: string | null
  data_quality_flags?: string[]
}

interface ComparisonResponse {
  companies: ComparisonCompany[]
  requested: string[]
  missing: string[]
  metrics: string[]
}

const METRIC_DEFINITIONS: Record<string, { label: string; group: string }> = {
  LatestPrice: { label: 'Price', group: 'Market' },
  MarketCap: { label: 'Market cap', group: 'Market' },
  PERatio: { label: 'P/E', group: 'Valuation' },
  PriceToBook: { label: 'P/B', group: 'Valuation' },
  PriceToSales: { label: 'P/S', group: 'Valuation' },
  EnterpriseValueToSales: { label: 'EV/Sales', group: 'Valuation' },
  DividendsYield: { label: 'Dividend yield', group: 'Valuation' },
  ReturnOnEquity: { label: 'ROE', group: 'Quality' },
  DebtToEquity: { label: 'Debt/equity', group: 'Quality' },
  CurrentRatio: { label: 'Current ratio', group: 'Quality' },
  GrossMargin: { label: 'Gross margin', group: 'Quality' },
  OperatingMargin: { label: 'Operating margin', group: 'Quality' },
  NetMargin: { label: 'Net margin', group: 'Quality' },
  PayoutRatio: { label: 'Payout ratio', group: 'Valuation' },
  ReturnOnAssets: { label: 'Return on assets', group: 'Quality' },
  Revenue: { label: 'Revenue', group: 'Income' },
  OperatingIncome: { label: 'Operating income', group: 'Income' },
  NetIncome: { label: 'Net income', group: 'Income' },
  TotalAssets: { label: 'Total assets', group: 'Balance sheet' },
  TotalEquity: { label: "Shareholders' equity", group: 'Balance sheet' },
  SharesOutstanding: { label: 'Shares outstanding', group: 'Balance sheet' },
}

const DEFAULT_METRICS = Object.keys(METRIC_DEFINITIONS)
const GROUPS = ['Market', 'Valuation', 'Income', 'Balance sheet', 'Quality']
const PERCENT_METRICS = new Set(['DividendsYield', 'ReturnOnEquity', 'GrossMargin', 'OperatingMargin', 'NetMargin', 'PayoutRatio', 'ReturnOnAssets'])
const CURRENCY_METRICS = new Set(['LatestPrice', 'MarketCap', 'Revenue', 'OperatingIncome', 'NetIncome', 'TotalAssets', 'TotalEquity'])

function formatMetric(metric: string, value: number | null | undefined) {
  if (value == null || Number.isNaN(value)) return '—'
  if (PERCENT_METRICS.has(metric)) return `${(value * 100).toFixed(1)}%`
  const formatted = Math.abs(value) >= 1_000_000
    ? `${(value / 1_000_000).toLocaleString(undefined, { maximumFractionDigits: 2 })}m`
    : value.toLocaleString(undefined, { maximumFractionDigits: 2 })
  return CURRENCY_METRICS.has(metric) ? `¥${formatted}` : formatted
}

function formatPercent(value: number | null | undefined) {
  return value == null ? '—' : `${(value * 100).toFixed(0)}%`
}

function companyLabel(company: ComparisonCompany) {
  return company.company.company_name || company.company.ticker || company.company_code
}

function bestValue(companies: ComparisonCompany[], metric: string) {
  const values = companies.map(company => company.metrics[metric]).filter((value): value is number => value != null)
  return values.length > 1 ? Math.max(...values) : null
}

function CompanySet({ companies, onChange }: { companies: SecuritySearchResult[]; onChange: (companies: SecuritySearchResult[]) => void }) {
  const addCompany = (company: SecuritySearchResult | null) => {
    if (!company?.company_code || companies.some(item => item.company_code === company.company_code)) return
    onChange([...companies, company])
  }
  const removeCompany = (code: string | null) => onChange(companies.filter(company => company.company_code !== code))
  const moveCompany = (index: number, offset: number) => {
    const next = [...companies]
    const target = index + offset
    if (target < 0 || target >= next.length) return
    const [item] = next.splice(index, 1)
    next.splice(target, 0, item)
    onChange(next)
  }
  return (
    <div className="stack">
      <CompanyPicker selected={null} onSelect={addCompany} clearOnSelect requireCompanyCode disabled={companies.length >= 12} label="Add company" />
      <div className="comparison-company-list">
        {companies.map((company, index) => (
          <div className="comparison-company-chip" key={company.company_code}>
            <span><strong>{company.company_name}</strong><small>{[company.ticker, company.company_code].filter(Boolean).join(' · ')}</small></span>
            <div className="button-row">
              <button className="icon-button" type="button" disabled={index === 0} onClick={() => moveCompany(index, -1)} aria-label={`Move ${company.company_name} up`}><ArrowUp /></button>
              <button className="icon-button" type="button" disabled={index === companies.length - 1} onClick={() => moveCompany(index, 1)} aria-label={`Move ${company.company_name} down`}><ArrowDown /></button>
              <button className="icon-button" type="button" onClick={() => removeCompany(company.company_code)} aria-label={`Remove ${company.company_name}`}><X /></button>
            </div>
          </div>
        ))}
      </div>
      {!companies.length && <p className="muted">Add at least two companies to build a comparison.</p>}
    </div>
  )
}

function MetricMatrix({ result, showPercentiles }: { result: ComparisonResponse; showPercentiles: boolean }) {
  return (
    <Card title="Financial comparison" description="Values use each company's latest available price and reported financial period.">
      <div className="table-scroll">
        <table className="data-grid comparison-matrix">
          <thead><tr><th>Metric</th>{result.companies.map(company => <th key={company.company_code}><strong>{companyLabel(company)}</strong><small>{[company.company.ticker, company.company_code].filter(Boolean).join(' · ')}</small><small>{company.period_end ? `Period ${company.period_end}` : 'Period unavailable'}</small></th>)}</tr></thead>
          <tbody>
            {GROUPS.map(group => <Fragment key={group}>
              <tr className="comparison-group" key={`${group}-heading`}><th colSpan={result.companies.length + 1}>{group}</th></tr>
              {result.metrics.filter(metric => METRIC_DEFINITIONS[metric]?.group === group).map(metric => {
                const best = bestValue(result.companies, metric)
                return <tr key={metric}><th>{METRIC_DEFINITIONS[metric]?.label ?? metric}{showPercentiles && <small>Peer percentile</small>}</th>{result.companies.map(company => <td key={company.company_code} className={best != null && company.metrics[metric] === best ? 'comparison-best' : ''}>{formatMetric(metric, company.metrics[metric])}{showPercentiles && <small>{formatPercent(company.percentiles[metric])}</small>}</td>)}</tr>
              })}
            </Fragment>)}
          </tbody>
        </table>
      </div>
    </Card>
  )
}

function CommonSizeTable({ title, companies, field, rows }: { title: string; companies: ComparisonCompany[]; field: 'common_size_income' | 'common_size_balance'; rows: Array<[string, string]> }) {
  return <Card title={title} description="Each row is shown as a percentage of its statement base."><div className="table-scroll"><table className="data-grid"><thead><tr><th>Company</th>{rows.map(([, label]) => <th key={label}>{label}</th>)}</tr></thead><tbody>{companies.map(company => <tr key={company.company_code}><th>{companyLabel(company)}</th>{rows.map(([key]) => <td key={key}>{formatPercent(company[field][key])}</td>)}</tr>)}</tbody></table></div></Card>
}

export default function ComparisonPage() {
  const [companies, setCompanies] = useState<SecuritySearchResult[]>([])
  const [showPercentiles, setShowPercentiles] = useState(false)
  const compare = useMutation({
    mutationFn: (selected: SecuritySearchResult[]) => apiPost<ComparisonResponse>('/api/comparison/snapshot', {
      company_codes: selected.map(company => company.company_code),
      metrics: DEFAULT_METRICS,
    }),
  })
  const run = () => { if (companies.length >= 2) compare.mutate(companies) }
  const result = compare.data
  return (
    <div className="stack dense-page">
      <PageHeader eyebrow="Company research" title="Compare companies" description="Select two to twelve companies by name, ticker, EDINET code, industry, or market." />
      <Card title="Company set" description="The same company search used by analysis, filings, and research is used here.">
        <CompanySet companies={companies} onChange={setCompanies} />
        <div className="button-row comparison-actions">
          <button className="button button--primary" disabled={compare.isPending || companies.length < 2} onClick={run}>{compare.isPending ? 'Comparing…' : 'Compare'}</button>
          {companies.length > 0 && <button className="button button--ghost" onClick={() => { setCompanies([]); compare.reset() }}>Clear</button>}
          <label className="inline-toggle"><input type="checkbox" checked={showPercentiles} onChange={event => setShowPercentiles(event.target.checked)} />Show peer percentiles</label>
        </div>
      </Card>
      {compare.isPending && <LoadingState label="Calculating comparison" />}
      {compare.error && <p className="form-error">{(compare.error as Error).message}</p>}
      {result?.missing.length ? <p className="form-error">Could not find: {result.missing.join(', ')}</p> : null}
      {result && !result.companies.length && <EmptyState title="No companies found" description="Choose companies with available EDINET financial records and try again." />}
      {result && result.companies.length > 0 && <>
        <MetricMatrix result={result} showPercentiles={showPercentiles} />
        <div className="two-column">
          <CommonSizeTable title="Income structure" companies={result.companies} field="common_size_income" rows={[["Revenue", "Revenue"], ["OperatingIncome", "Operating income"], ["NetIncome", "Net income"]]} />
          <CommonSizeTable title="Balance-sheet structure" companies={result.companies} field="common_size_balance" rows={[["TotalAssets", "Total assets"], ["TotalEquity", "Shareholders' equity"]]} />
        </div>
      </>}
    </div>
  )
}
