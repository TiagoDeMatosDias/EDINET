import { Fragment, useEffect, useMemo, useState } from 'react'
import { useMutation, useQuery } from '@tanstack/react-query'
import { ArrowDown, ArrowLeft, ArrowUp, Plus, X } from 'lucide-react'
import { Link, useSearchParams } from 'react-router-dom'

import { apiPost, apiRequest } from '../../api/client'
import type { SecuritySearchResult } from '../../api/types'
import { CompanyPicker, searchCompanies } from '../../components/CompanyPicker'
import { EmptyState, LoadingState } from '../../components/Feedback'
import { Card, PageHeader } from '../../components/Page'
import { formatMetricValue, groupMetrics, metricDefinition, type MetricDefinition } from '../../metrics'
import { bestValue } from './bestValue'

interface ComparisonCompany {
  company_code: string
  company: { company_name?: string; ticker?: string; industry?: string; market?: string }
  metrics: Record<string, number | null>
  common_size_income: Record<string, number | null>
  common_size_balance: Record<string, number | null>
  percentiles: Record<string, number | null>
  market?: { price_currency?: string | null }
  reporting_currency?: string | null
  period_end?: string | null
  price_date?: string | null
  data_quality_flags?: string[]
}

interface ComparisonResponse {
  companies: ComparisonCompany[]
  requested: string[]
  missing: string[]
  metrics: string[]
  metric_definitions?: Record<string, MetricDefinition>
}

interface MetricCatalogResponse {
  tables: Record<string, string[]>
  definitions?: Record<string, MetricDefinition>
  default_metrics?: string[]
}

function formatPercent(value: number | null | undefined) {
  return value == null ? '—' : `${(value * 100).toFixed(0)}%`
}

function companyLabel(company: ComparisonCompany) {
  return company.company.company_name || company.company.ticker || company.company_code
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

export function MetricPicker({
  catalog,
  definitions,
  isLoading,
  selected,
  onChange,
}: {
  catalog: Record<string, string[]>
  definitions?: Record<string, MetricDefinition>
  isLoading: boolean
  selected: string[]
  onChange: (metrics: string[]) => void
}) {
  const [open, setOpen] = useState(false)
  const [tableSearch, setTableSearch] = useState('')
  const [columnSearch, setColumnSearch] = useState('')
  const [table, setTable] = useState('')
  const [column, setColumn] = useState('')
  const tables = useMemo(() => Object.keys(catalog).sort((a, b) => a.localeCompare(b)), [catalog])
  const filteredTables = useMemo(() => {
    const query = tableSearch.trim().toLowerCase()
    return tables.filter(item => !query || item.toLowerCase().includes(query))
  }, [tableSearch, tables])
  const activeTable = filteredTables.includes(table) ? table : filteredTables[0] ?? ''
  const columns = useMemo(() => catalog[activeTable] ?? [], [activeTable, catalog])
  const filteredColumns = useMemo(() => {
    const query = columnSearch.trim().toLowerCase()
    return columns.filter(item => !query || item.toLowerCase().includes(query))
  }, [columnSearch, columns])
  const activeColumn = filteredColumns.includes(column) ? column : filteredColumns[0] ?? ''
  const selectedRef = activeTable && activeColumn ? `${activeTable}.${activeColumn}` : ''
  const alreadySelected = selectedRef !== '' && selected.includes(selectedRef)
  const addMetric = () => {
    if (!selectedRef || alreadySelected) return
    onChange([...selected, selectedRef])
  }

  return (
    <div className="comparison-metric-picker">
      <div className="comparison-metric-picker-header">
        <div><strong>{selected.length} metrics selected</strong><small>Choose standard or table-based metrics to compare.</small></div>
        <button className="button button--secondary" type="button" onClick={() => setOpen(value => !value)}><Plus />Add metric</button>
      </div>
      <div className="comparison-metric-list">
        {selected.map(metric => {
          const definition = metricDefinition(metric, definitions)
          return <span className="comparison-metric-chip" key={metric}><span><strong>{definition.label}</strong><small>{metric.includes('.') ? metric : definition.group}</small></span><button className="icon-button" type="button" onClick={() => onChange(selected.filter(item => item !== metric))} aria-label={`Remove metric ${definition.label}`}><X /></button></span>
        })}
        {!selected.length && <span className="muted">No metrics selected.</span>}
      </div>
      {open && <div className="comparison-metric-picker-panel">
        <div className="comparison-metric-picker-fields">
          <label className="field-label">Find table<input className="input" value={tableSearch} onChange={event => setTableSearch(event.target.value)} placeholder="Search tables" /></label>
          <label className="field-label">Table<select className="select" aria-label="Metric table" value={activeTable} onChange={event => { setTable(event.target.value); setColumn('') }} disabled={isLoading || !filteredTables.length}><option value="">{isLoading ? 'Loading tables…' : 'Select table…'}</option>{filteredTables.map(item => <option key={item} value={item}>{item}</option>)}</select></label>
          <label className="field-label">Find column<input className="input" value={columnSearch} onChange={event => setColumnSearch(event.target.value)} placeholder="Search columns" disabled={!activeTable} /></label>
          <label className="field-label">Column<select className="select" aria-label="Metric column" value={activeColumn} onChange={event => setColumn(event.target.value)} disabled={!activeTable || !filteredColumns.length}><option value="">Select column…</option>{filteredColumns.map(item => <option key={item} value={item}>{item}</option>)}</select></label>
        </div>
        <div className="button-row comparison-metric-picker-actions"><button className="button button--primary" type="button" disabled={!selectedRef || alreadySelected} onClick={addMetric}>{alreadySelected ? 'Already added' : 'Add selected metric'}</button><button className="button button--ghost" type="button" onClick={() => setOpen(false)}>Done</button></div>
      </div>}
    </div>
  )
}

function MetricMatrix({ result, showPercentiles }: { result: ComparisonResponse; showPercentiles: boolean }) {
  const definitions = result.metric_definitions ?? {}
  return (
    <Card title="Financial comparison" description="Values use each company's latest available price and reported financial period. Highlights mark the most favourable value where direction is meaningful: lower valuation multiples and leverage, higher returns, margins, yield, and liquidity.">
      <div className="table-scroll">
        <table className="data-grid comparison-matrix">
          <thead><tr><th>Metric</th>{result.companies.map(company => <th key={company.company_code}><strong>{companyLabel(company)}</strong><small>{[company.company.ticker, company.company_code].filter(Boolean).join(' · ')}</small><small>{company.period_end ? `Period ${company.period_end}` : 'Period unavailable'}</small></th>)}</tr></thead>
          <tbody>
            {groupMetrics(result.metrics, definitions).map(({ group, metrics }) => <Fragment key={group}>
              <tr className="comparison-group" key={`${group}-heading`}><th colSpan={result.companies.length + 1}>{group}</th></tr>
              {metrics.map(metric => {
                const definition = metricDefinition(metric, definitions)
                const best = bestValue(definition.direction, result.companies.map(company => company.metrics[metric]))
                return <tr key={metric}><th>{definition.label}{showPercentiles && <small>Peer percentile</small>}</th>{result.companies.map(company => <td key={company.company_code} className={best != null && company.metrics[metric] === best ? 'comparison-best' : ''}>{formatMetricValue(definition, company.metrics[metric], { price: company.market?.price_currency, reporting: company.reporting_currency })}{showPercentiles && <small>{formatPercent(company.percentiles[metric])}</small>}</td>)}</tr>
              })}
            </Fragment>)}
          </tbody>
        </table>
      </div>
    </Card>
  )
}

function CommonSizeTable({ title, companies, field, definitions }: { title: string; companies: ComparisonCompany[]; field: 'common_size_income' | 'common_size_balance'; definitions?: Record<string, MetricDefinition> }) {
  const keys = Object.keys(companies[0]?.[field] ?? {})
  return <Card title={title} description="Each row is shown as a percentage of its statement base."><div className="table-scroll"><table className="data-grid"><thead><tr><th>Company</th>{keys.map(key => <th key={key}>{metricDefinition(key, definitions).label}</th>)}</tr></thead><tbody>{companies.map(company => <tr key={company.company_code}><th>{companyLabel(company)}</th>{keys.map(key => <td key={key}>{formatPercent(company[field][key])}</td>)}</tr>)}</tbody></table></div></Card>
}

export default function ComparisonPage() {
  const [params] = useSearchParams()
  const fromScreen = params.get('source') === 'screen'
  const initialCodes = useMemo(() => [...new Set((params.get('companies') ?? '').split(',').map(code => code.trim()).filter(Boolean))].slice(0, 12), [params])
  const [companies, setCompanies] = useState<SecuritySearchResult[]>([])
  const [hydrating, setHydrating] = useState(Boolean(initialCodes.length))
  // ``null`` until the reader changes the selection: the server's standard metrics apply.
  const [chosenMetrics, setChosenMetrics] = useState<string[] | null>(null)
  const [showPercentiles, setShowPercentiles] = useState(false)
  const metricCatalog = useQuery({
    queryKey: ['comparison-metrics'],
    queryFn: () => apiRequest<MetricCatalogResponse>('/api/comparison/metrics'),
  })
  useEffect(() => {
    if (!initialCodes.length) return
    let cancelled = false
    void Promise.all(initialCodes.map(async code => {
      const response = await searchCompanies(code, 8)
      return response.results.find(company => company.company_code === code) ?? null
    })).then(results => {
      if (cancelled) return
      setCompanies(results.filter((company): company is SecuritySearchResult => Boolean(company)))
      setHydrating(false)
    }).catch(() => {
      if (!cancelled) {
        setCompanies([])
        setHydrating(false)
      }
    })
    return () => { cancelled = true }
  }, [initialCodes])
  const compare = useMutation({
    mutationFn: (selection: { companies: SecuritySearchResult[]; metrics: string[] }) => apiPost<ComparisonResponse>('/api/comparison/snapshot', {
      company_codes: selection.companies.map(company => company.company_code),
      metrics: selection.metrics,
    }),
  })
  const selectedMetrics = chosenMetrics ?? metricCatalog.data?.default_metrics ?? []
  // An empty request asks the server for its standard metrics (used if the catalog failed to load).
  const canRun = companies.length >= 2 && (chosenMetrics === null || chosenMetrics.length > 0)
  const run = () => { if (canRun) compare.mutate({ companies, metrics: chosenMetrics ?? selectedMetrics }) }
  const result = compare.data
  return (
    <div className="stack dense-page">
      <PageHeader eyebrow="Company research" title="Compare companies" description="Select two to twelve companies by name, ticker, EDINET code, industry, or market." actions={fromScreen && <Link className="button button--ghost" to="/screen"><ArrowLeft />Return to Screening</Link>} />
      {initialCodes.length > 0 && <p className="callout callout--success">Screen matches were preloaded. Remove or reorder companies before comparing.</p>}
      <Card title="Company set" description="The same company search used by analysis, filings, and research is used here.">
        <CompanySet companies={companies} onChange={setCompanies} />
        <div className="button-row comparison-actions">
          <button className="button button--primary" disabled={hydrating || compare.isPending || !canRun} onClick={run}>{hydrating ? 'Loading screen matches…' : compare.isPending ? 'Comparing…' : 'Compare'}</button>
          {companies.length > 0 && <button className="button button--ghost" onClick={() => { setCompanies([]); compare.reset() }}>Clear</button>}
          <label className="inline-toggle"><input type="checkbox" checked={showPercentiles} onChange={event => setShowPercentiles(event.target.checked)} />Show peer percentiles</label>
        </div>
      </Card>
      <Card title="Metrics" description="Start with the standard metrics, or add any numeric column from a statement table.">
        <MetricPicker catalog={metricCatalog.data?.tables ?? {}} definitions={metricCatalog.data?.definitions} isLoading={metricCatalog.isLoading} selected={selectedMetrics} onChange={setChosenMetrics} />
        {metricCatalog.error && <p className="form-error">Could not load the metric catalog; comparisons use the standard metrics.</p>}
      </Card>
      {compare.isPending && <LoadingState label="Calculating comparison" />}
      {compare.error && <p className="form-error">{(compare.error as Error).message}</p>}
      {result?.missing.length ? <p className="form-error">Could not find: {result.missing.join(', ')}</p> : null}
      {result && !result.companies.length && <EmptyState title="No companies found" description="Choose companies with available EDINET financial records and try again." />}
      {result && result.companies.length > 0 && <>
        <MetricMatrix result={result} showPercentiles={showPercentiles} />
        <div className="two-column">
          <CommonSizeTable title="Income structure" companies={result.companies} field="common_size_income" definitions={{ ...metricCatalog.data?.definitions, ...result.metric_definitions }} />
          <CommonSizeTable title="Balance-sheet structure" companies={result.companies} field="common_size_balance" definitions={{ ...metricCatalog.data?.definitions, ...result.metric_definitions }} />
        </div>
      </>}
    </div>
  )
}
