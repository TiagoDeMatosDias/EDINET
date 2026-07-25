import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useSearchParams } from 'react-router-dom'

import { apiRequest, queryString } from '../../api/client'
import { EmptyState, LoadingState } from '../../components/Feedback'
import { PageHeader } from '../../components/Page'

interface Filing {
  doc_id: string
  edinet_code?: string | null
  submitter_name?: string | null
  submitted_at?: string | null
  period_end?: string | null
  status: string
}

interface FilingListResponse { filings: Filing[] }
interface Fact { fact_id: string; concept: string; context_id?: string; value_text?: string; numeric_value?: number | null; unit_id?: string }
interface Section { section_id: string; title?: string; text: string }
interface FilingDetail { filing: Filing; artifacts: Array<{ artifact_id: string; member_path: string; kind: string; size_bytes: number }> }

export default function FilingsPage() {
  const [searchParams] = useSearchParams()
  const [companyCode, setCompanyCode] = useState(searchParams.get('company') ?? '')
  const [selected, setSelected] = useState<string | null>(searchParams.get('doc'))
  const filings = useQuery({
    queryKey: ['filings', companyCode],
    enabled: companyCode.trim().length > 0,
    queryFn: () => apiRequest<FilingListResponse>(`/api/filings/company/${encodeURIComponent(companyCode.trim())}`),
  })
  const detail = useQuery({
    queryKey: ['filing', selected],
    enabled: Boolean(selected),
    queryFn: () => apiRequest<FilingDetail>(`/api/filings/${encodeURIComponent(selected ?? '')}`),
  })
  const facts = useQuery({
    queryKey: ['filing-facts', selected],
    enabled: Boolean(selected),
    queryFn: () => apiRequest<{ facts: Fact[] }>(`/api/filings/${encodeURIComponent(selected ?? '')}/facts${queryString({ limit: 100 })}`),
  })
  const sections = useQuery({
    queryKey: ['filing-sections', selected],
    enabled: Boolean(selected),
    queryFn: () => apiRequest<{ sections: Section[] }>(`/api/filings/${encodeURIComponent(selected ?? '')}/sections${queryString({ limit: 50 })}`),
  })

  return <div className="stack dense-page"><PageHeader eyebrow="EDINET archive" title="XBRL filings" description="Browse retained type-1 packages, structured facts, and sanitized narrative sections." /><div className="card"><div className="card-header"><div><h2>Find company filings</h2><p>Enter an EDINET code to list archived annual reports.</p></div><div className="button-row"><input className="input" aria-label="EDINET code" placeholder="E00000" value={companyCode} onChange={event => setCompanyCode(event.target.value)} /><button className="button button--primary" onClick={() => void filings.refetch()} disabled={!companyCode.trim()}>Search</button></div></div><div className="card-body">{filings.isLoading && <LoadingState label="Loading filings" />}{filings.error && <p className="form-error">Unable to load filings: {filings.error.message}</p>}{filings.data && !filings.data.filings.length && <EmptyState title="No archived filings" description="Acquire a type-1 EDINET package before it appears here." />}{filings.data?.filings.map(filing => <button className={selected === filing.doc_id ? 'filing-row filing-row--selected' : 'filing-row'} key={filing.doc_id} onClick={() => setSelected(filing.doc_id)}><span><strong>{filing.doc_id}</strong><small>{filing.submitter_name || filing.edinet_code || 'Unknown company'}</small></span><span><strong>{filing.period_end || 'Period unavailable'}</strong><small>{filing.status}</small></span></button>)}</div></div>{selected && <div className="two-column filing-detail-grid"><div className="card"><div className="card-header"><div><h2>{detail.data?.filing.submitter_name || selected}</h2><p>{detail.data?.filing.submitted_at || 'Submission date unavailable'}</p></div><span className="status-pill">{detail.data?.filing.status || 'Loading'}</span></div><div className="card-body"><h3>Archive members</h3>{detail.isLoading && <LoadingState label="Loading filing" />}{detail.data?.artifacts.map(artifact => <div className="artifact-row" key={artifact.artifact_id}><span>{artifact.member_path}</span><small>{artifact.kind} · {artifact.size_bytes.toLocaleString()} bytes</small></div>)}<h3>Structured facts</h3>{facts.isLoading && <LoadingState label="Parsing facts" />}{facts.data?.facts.map(fact => <div className="fact-row" key={fact.fact_id}><span>{fact.concept}</span><strong>{fact.value_text || '—'}</strong><small>{fact.context_id || 'No context'}{fact.unit_id ? ` · ${fact.unit_id}` : ''}</small></div>)}</div></div><div className="card"><div className="card-header"><div><h2>Filing narrative</h2><p>Active content is removed before indexing.</p></div></div><div className="card-body filing-sections">{sections.isLoading && <LoadingState label="Loading narrative" />}{sections.data?.sections.map(section => <article key={section.section_id}><h3>{section.title || 'Section'}</h3><p>{section.text}</p></article>)}{sections.data && !sections.data.sections.length && <EmptyState title="No narrative sections" description="This package did not contain indexable HTML." />}</div></div></div>}</div>
}
