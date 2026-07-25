import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useSearchParams, useNavigate } from 'react-router-dom'

import { apiRequest } from '../../api/client'
import { EmptyState, LoadingState } from '../../components/Feedback'
import { PageHeader } from '../../components/Page'

interface Filing {
  doc_id: string; edinet_code?: string | null; submitter_name?: string | null
  submitted_at?: string | null; period_end?: string | null; status: string
  form_code?: string | null
}

export default function FilingsPage() {
  const [searchParams] = useSearchParams()
  const navigate = useNavigate()
  const [companyCode, setCompanyCode] = useState(searchParams.get('company') ?? '')
  const [searchedCode, setSearchedCode] = useState(searchParams.get('company') ?? '')

  const filings = useQuery({
    queryKey: ['filings', searchedCode || 'recent'],
    queryFn: () => {
      if (searchedCode.trim()) {
        return apiRequest<{ filings: Filing[] }>(`/api/filings/company/${encodeURIComponent(searchedCode.trim())}`)
      }
      return apiRequest<{ filings: Filing[] }>(`/api/filings?limit=100`)
    },
  })

  return (
    <div className="stack dense-page">
      <PageHeader eyebrow="EDINET archive" title="XBRL filings" description="Browse retained type-1 packages. Click a filing to open the dedicated viewer." />
      <div className="card">
        <div className="card-header">
          <div>
            <h2>Find filings</h2>
            <p>Enter an EDINET code to filter, or leave empty to see all recent filings.</p>
          </div>
          <div className="button-row">
            <input
              className="input"
              aria-label="EDINET code"
              placeholder="E00000"
              value={companyCode}
              onChange={e => setCompanyCode(e.target.value)}
              onKeyDown={e => { if (e.key === 'Enter') { setSearchedCode(companyCode.trim()); void filings.refetch() } }}
            />
            <button className="button button--primary" onClick={() => { setSearchedCode(companyCode.trim()); void filings.refetch() }}>
              {companyCode.trim() ? 'Search' : 'Show all'}
            </button>
          </div>
        </div>
        <div className="card-body">
          {filings.isLoading && <LoadingState label="Loading filings" />}
          {filings.error && <p className="form-error">{(filings.error as Error).message}</p>}
          {filings.data && !filings.data.filings.length && (
            <EmptyState title="No filings found" description={searchedCode ? `No archived filings for ${searchedCode}.` : 'No filings archived yet. Download XBRL packages first.'} />
          )}
          <div className="filing-list">
            {filings.data?.filings.map(f => (
              <button
                key={f.doc_id}
                className="filing-row"
                onClick={() => navigate(`/filings/${encodeURIComponent(f.doc_id)}`)}
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
      </div>
    </div>
  )
}
