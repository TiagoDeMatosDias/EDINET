import { useState } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'

import { apiPost } from '../../api/client'
import { EmptyState, LoadingState } from '../../components/Feedback'
import { PageHeader } from '../../components/Page'

interface ComparisonCompany {
  company_code: string
  company: { company_name?: string; edinet_code?: string; industry?: string }
  metrics: Record<string, number | null>
}

const METRIC_LABELS: Record<string, string> = {
  LatestPrice: 'Price',
  MarketCap: 'Market cap',
  PERatio: 'P/E',
  PriceToBook: 'P/B',
  ReturnOnEquity: 'ROE',
  DividendsYield: 'Div yield',
  OperatingMargin: 'Op margin',
  NetMargin: 'Net margin',
  RevenueGrowth: 'Rev growth',
  TotalAssets: 'Total assets',
  TotalEquity: 'Total equity',
}

const DEFAULT_METRICS = ['LatestPrice', 'MarketCap', 'PERatio', 'PriceToBook', 'ReturnOnEquity', 'DividendsYield', 'OperatingMargin', 'NetMargin']

export default function ComparisonPage() {
  const client = useQueryClient()
  const [input, setInput] = useState('')
  const [showPercentiles, setShowPercentiles] = useState(false)

  const compare = useMutation({
    mutationFn: (codes: string[]) =>
      apiPost<{ companies: ComparisonCompany[]; requested: string[] }>('/api/comparison/snapshot', {
        company_codes: codes,
        metrics: DEFAULT_METRICS,
      }),
  })

  const run = () => {
    const codes = input.split(',').map(v => v.trim()).filter(Boolean)
    if (codes.length >= 2) compare.mutate(codes)
  }

  const result = compare.data
  const metrics = result?.companies.length
    ? Array.from(new Set(result.companies.flatMap(c => Object.keys(c.metrics))))
        .filter(k => DEFAULT_METRICS.includes(k))
    : DEFAULT_METRICS

  return (
    <div className="stack dense-page">
      <PageHeader
        eyebrow="Company research"
        title="Compare companies"
        description="Compare financial metrics across two to twelve EDINET companies."
      />
      <div className="card">
        <div className="card-header">
          <div>
            <h2>Company set</h2>
            <p>Enter EDINET codes separated by commas, then click Compare.</p>
          </div>
          <div className="button-row">
            <input
              className="input"
              aria-label="Company codes"
              value={input}
              onChange={e => setInput(e.target.value)}
              onKeyDown={e => { if (e.key === 'Enter') run() }}
              placeholder="E00001, E00002"
            />
            <button
              className="button button--primary"
              disabled={compare.isPending || input.split(',').filter(Boolean).length < 2}
              onClick={run}
            >
              {compare.isPending ? 'Comparing…' : 'Compare'}
            </button>
          </div>
        </div>
      </div>

      {compare.isPending && <LoadingState label="Calculating comparison" />}
      {compare.error && <p className="form-error">{(compare.error as Error).message}</p>}
      {result && !result.companies.length && (
        <EmptyState title="No companies found" description="Check the EDINET codes and try again." />
      )}

      {result && result.companies.length > 0 && (
        <div className="card table-scroll">
          <div className="card-header">
            <h2>Metric matrix</h2>
            <label className="inline-toggle">
              <input type="checkbox" checked={showPercentiles} onChange={e => setShowPercentiles(e.target.checked)} />
              Show percentile ranks
            </label>
          </div>
          <table className="data-grid">
            <thead>
              <tr>
                <th>Metric</th>
                {result.companies.map(c => (
                  <th key={c.company_code}>
                    {String(c.company?.company_name ?? c.company_code)}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {metrics.map(metric => (
                <tr key={metric}>
                  <th>{METRIC_LABELS[metric] ?? metric}</th>
                  {result.companies.map(c => (
                    <td key={c.company_code}>
                      {c.metrics[metric] == null
                        ? '—'
                        : typeof c.metrics[metric] === 'number'
                          ? Number(c.metrics[metric]).toLocaleString(undefined, { maximumFractionDigits: 3 })
                          : String(c.metrics[metric])}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
