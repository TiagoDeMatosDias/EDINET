import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'

import { apiRequest, queryString } from '../../api/client'
import { EmptyState, LoadingState } from '../../components/Feedback'
import { PageHeader } from '../../components/Page'

interface CompanyRow { company_code: string; company: Record<string, unknown>; metrics: Record<string, number | null> }

export default function ComparisonPage() {
  const [input, setInput] = useState('')
  const [codes, setCodes] = useState<string[]>([])
  const comparison = useQuery({ queryKey: ['comparison', codes], enabled: codes.length >= 2, queryFn: () => apiRequest<{ companies: CompanyRow[] }>(`/api/security/compare${queryString({ company_codes: codes.join(',') })}`) })
  const metricKeys = ['LatestPrice', 'MarketCap', 'PERatio', 'PriceToBook', 'ReturnOnEquity', 'DividendsYield']
  return <div className="stack dense-page"><PageHeader eyebrow="Company research" title="Compare companies" description="Compare the same overview metrics across two to twelve EDINET companies." /><div className="card"><div className="card-header"><div><h2>Company set</h2><p>Enter EDINET codes separated by commas.</p></div><div className="button-row"><input className="input" aria-label="Company codes" value={input} onChange={event => setInput(event.target.value)} placeholder="E00001, E00002" /><button className="button button--primary" onClick={() => setCodes(input.split(',').map(value => value.trim()).filter(Boolean))}>Compare</button></div></div></div>{comparison.isLoading && <LoadingState label="Calculating comparison" />}{comparison.error && <p className="form-error">{comparison.error.message}</p>}{comparison.data && !comparison.data.companies.length && <EmptyState title="No companies found" description="Check the EDINET codes and try again." />}{comparison.data?.companies.length ? <div className="card table-scroll"><table className="data-grid"><thead><tr><th>Metric</th>{comparison.data.companies.map(company => <th key={company.company_code}>{String(company.company.company_name ?? company.company_code)}</th>)}</tr></thead><tbody>{metricKeys.map(metric => <tr key={metric}><th>{metric}</th>{comparison.data!.companies.map(company => <td key={company.company_code}>{company.metrics[metric] == null ? '—' : Number(company.metrics[metric]).toLocaleString(undefined, { maximumFractionDigits: 3 })}</td>)}</tr>)}</tbody></table></div> : null}</div>
}
