import { useEffect, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useSearchParams, useNavigate } from 'react-router-dom'

import { apiRequest } from '../../api/client'
import type { SecuritySearchResult } from '../../api/types'
import { CompanyPicker } from '../../components/CompanyPicker'
import { EmptyState, LoadingState } from '../../components/Feedback'
import { PageHeader } from '../../components/Page'

interface Filing {
  doc_id: string; edinet_code?: string | null; submitter_name?: string | null
  submitted_at?: string | null; period_end?: string | null; status: string
  form_code?: string | null
}

interface FilingCoverageSummary {
  unique_filings: number
  unique_companies: number
  unique_archives: number
  parsed_filings: number
  error_filings: number
  filings_with_issues: number
}

interface FilingCoverageResponse {
  summary: FilingCoverageSummary
}

function FilingStat({ label, value }: { label: string; value: number }) {
  return <div className="filing-stat"><span>{label}</span><strong>{value.toLocaleString()}</strong></div>
}

function FilingStats({ summary }: { summary: FilingCoverageSummary }) {
  return (
    <div className="filing-stats-grid">
      <FilingStat label="Unique filings" value={summary.unique_filings} />
      <FilingStat label="Companies with filings" value={summary.unique_companies} />
      <FilingStat label="Parsed filings" value={summary.parsed_filings} />
      <FilingStat label="Unique archive packages" value={summary.unique_archives} />
    </div>
  )
}

export default function FilingsPage() {
  const [searchParams] = useSearchParams()
  const navigate = useNavigate()
  const initialCode = searchParams.get('company') ?? ''
  const initialDoc = searchParams.get('doc') ?? ''
  const [selected, setSelected] = useState<SecuritySearchResult | null>(
    initialCode ? { company_code: initialCode, ticker: '', company_name: initialCode } : null,
  )
  const [searchedCode, setSearchedCode] = useState(initialCode)
  const hasSearch = Boolean(searchedCode.trim())

  useEffect(() => {
    if (!initialDoc) return
    const returnParams = new URLSearchParams()
    const from = searchParams.get('from')
    const company = searchParams.get('company')
    if (from) returnParams.set('from', from)
    if (company) returnParams.set('company', company)
    const suffix = returnParams.toString() ? `?${returnParams.toString()}` : ''
    navigate(`/filings/${encodeURIComponent(initialDoc)}${suffix}`, { replace: true })
  }, [initialDoc, navigate, searchParams])

  const coverage = useQuery({
    queryKey: ['filing-coverage'],
    queryFn: () => apiRequest<FilingCoverageResponse>('/api/filings/coverage'),
  })

  const filings = useQuery({
    queryKey: ['filings', searchedCode.trim()],
    enabled: hasSearch,
    queryFn: () => searchedCode.trim()
      ? apiRequest<{ filings: Filing[] }>(`/api/filings/company/${encodeURIComponent(searchedCode.trim())}`)
      : Promise.resolve({ filings: [] }),
  })

  const chooseCompany = (company: SecuritySearchResult | null) => {
    setSelected(company)
    setSearchedCode(company?.company_code ?? '')
  }

  return (
    <div className="stack dense-page">
      <PageHeader eyebrow="EDINET archive" title="XBRL filings" description="Browse retained type-1 packages. Click a filing to open the dedicated viewer." />
      <div className="card">
        <div className="card-header">
          <div>
            <h2>Find filings</h2>
            <p>Search by company name, ticker, EDINET code, industry, or market.</p>
          </div>
          <CompanyPicker selected={selected} onSelect={chooseCompany} label="Find filings" />
        </div>
        {!hasSearch ? (
          <div className="card-body filing-overview">
            <p className="muted">Choose a company to browse its retained filings.</p>
            {coverage.isLoading && <LoadingState label="Loading filing statistics" />}
            {coverage.error && <p className="form-error">{(coverage.error as Error).message}</p>}
            {coverage.data && <FilingStats summary={coverage.data.summary} />}
          </div>
        ) : (
          <div className="card-body">
            {filings.isLoading && <LoadingState label="Loading filings" />}
            {filings.error && <p className="form-error">{(filings.error as Error).message}</p>}
            {filings.data && !filings.data.filings.length && (
              <EmptyState title="No filings found" description={`No archived filings for ${searchedCode}.`} />
            )}
            <div className="filing-list">
              {filings.data?.filings.map(f => (
                <button
                  key={f.doc_id}
                  className="filing-row"
                  onClick={() => {
                    const returnParams = new URLSearchParams()
                    const from = searchParams.get('from')
                    const company = searchParams.get('company')
                    if (from) returnParams.set('from', from)
                    if (company) returnParams.set('company', company)
                    const suffix = returnParams.toString() ? `?${returnParams.toString()}` : ''
                    navigate(`/filings/${encodeURIComponent(f.doc_id)}${suffix}`)
                  }}
                >
                  <span>
                    <strong>{f.submitter_name || f.edinet_code || 'Unknown'}</strong>
                    <small>{f.doc_id} · {f.form_code || 'XBRL'} · {f.status}</small>
                  </span>
                  <span style={{ textAlign: 'right' }}>
                    <strong>{f.period_end || '—'}</strong>
                    <small>{f.submitted_at ? new Date(f.submitted_at).toLocaleDateString() : 'Unknown date'}</small>
                  </span>
                </button>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
