import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useSearchParams } from 'react-router-dom'

import { apiRequest, queryString } from '../../api/client'
import { EmptyState, LoadingState } from '../../components/Feedback'
import { PageHeader } from '../../components/Page'

interface Filing {
  doc_id: string; edinet_code?: string | null; submitter_name?: string | null
  submitted_at?: string | null; period_end?: string | null; status: string
  form_code?: string | null; archive_sha256?: string | null
}
interface Fact {
  fact_id: string; concept: string; context_id?: string; value_text?: string
  numeric_value?: number | null; unit_id?: string; namespace_uri?: string
}
interface Section {
  section_id: string; title?: string; text: string; ordinal: number
}
interface FilingDetail {
  filing: Filing
  artifacts: Array<{ artifact_id: string; member_path: string; kind: string; size_bytes: number }>
}
interface QualityIssue { issue_id: string; severity: string; code: string; message: string; fact_id?: string | null }
interface TaxonomyEntry { namespace_uri?: string; concept?: string }

type ViewerTab = 'document' | 'statements' | 'audit' | 'taxonomy' | 'quality'

const TABS: { key: ViewerTab; label: string }[] = [
  { key: 'document', label: 'Document' },
  { key: 'statements', label: 'Facts' },
  { key: 'audit', label: 'Audit' },
  { key: 'taxonomy', label: 'Taxonomy' },
  { key: 'quality', label: 'Quality' },
]

const SEVERITY_COLORS: Record<string, string> = { error: 'status-pill--warn', warning: 'status-pill--warn', info: 'status-pill--ok' }

export default function FilingsPage() {
  const [searchParams] = useSearchParams()
  const [companyCode, setCompanyCode] = useState(searchParams.get('company') ?? '')
  const [selected, setSelected] = useState<string | null>(searchParams.get('doc'))
  const [tab, setTab] = useState<ViewerTab>('document')
  const [selectedFact, setSelectedFact] = useState<Fact | null>(null)
  const [factFilter, setFactFilter] = useState('')

  const filings = useQuery({
    queryKey: ['filings', companyCode],
    enabled: companyCode.trim().length > 0,
    queryFn: () => apiRequest<{ filings: Filing[] }>(`/api/filings/company/${encodeURIComponent(companyCode.trim())}`),
  })
  const detail = useQuery({
    queryKey: ['filing', selected],
    enabled: Boolean(selected),
    queryFn: () => apiRequest<FilingDetail>(`/api/filings/${encodeURIComponent(selected ?? '')}`),
  })
  const facts = useQuery({
    queryKey: ['filing-facts', selected, factFilter],
    enabled: Boolean(selected),
    queryFn: () => apiRequest<{ facts: Fact[]; count: number }>(
      `/api/filings/${encodeURIComponent(selected ?? '')}/facts${queryString({ concept: factFilter || undefined, limit: 500 })}`,
    ),
  })
  const sections = useQuery({
    queryKey: ['filing-sections', selected],
    enabled: Boolean(selected),
    queryFn: () => apiRequest<{ sections: Section[]; count: number }>(`/api/filings/${encodeURIComponent(selected ?? '')}/sections${queryString({ limit: 200 })}`),
  })
  const quality = useQuery({
    queryKey: ['filing-quality', selected],
    enabled: Boolean(selected),
    queryFn: () => apiRequest<{ issues: QualityIssue[] }>(`/api/filings/${encodeURIComponent(selected ?? '')}/quality`),
  })
  const taxonomy = useQuery({
    queryKey: ['filing-taxonomy', selected],
    enabled: Boolean(selected) && tab === 'taxonomy',
    queryFn: () => apiRequest<{ taxonomy: TaxonomyEntry[]; count: number }>(`/api/filings/${encodeURIComponent(selected ?? '')}/taxonomy`),
  })

  return (
    <div className="stack dense-page">
      <PageHeader eyebrow="EDINET archive" title="XBRL filings" description="Browse retained type-1 packages with structured facts, narrative sections, and quality evidence." />
      <div className="card">
        <div className="card-header">
          <div>
            <h2>Find company filings</h2>
            <p>Enter an EDINET code to list archived annual reports.</p>
          </div>
          <div className="button-row">
            <input className="input" aria-label="EDINET code" placeholder="E00000" value={companyCode} onChange={e => setCompanyCode(e.target.value)} onKeyDown={e => { if (e.key === 'Enter') void filings.refetch() }} />
            <button className="button button--primary" onClick={() => void filings.refetch()} disabled={!companyCode.trim()}>Search</button>
          </div>
        </div>
        <div className="card-body">
          {filings.isLoading && <LoadingState label="Loading filings" />}
          {filings.error && <p className="form-error">Unable to load filings: {(filings.error as Error).message}</p>}
          {filings.data && !filings.data.filings.length && <EmptyState title="No archived filings" description="Acquire a type-1 EDINET package before it appears here." />}
          <div className="filing-list">
            {filings.data?.filings.map(f => (
              <button
                className={selected === f.doc_id ? 'filing-row filing-row--selected' : 'filing-row'}
                key={f.doc_id}
                onClick={() => { setSelected(f.doc_id); setSelectedFact(null) }}
              >
                <span><strong>{f.doc_id}</strong><small>{f.submitter_name || f.edinet_code || 'Unknown'}</small></span>
                <span><strong>{f.period_end || '—'}</strong><small>{f.form_code ?? ''} · {f.status}</small></span>
              </button>
            ))}
          </div>
        </div>
      </div>

      {selected && (
        <div className="filing-viewer">
          {/* Left: outline/sections */}
          <div className="card filing-outline">
            <div className="card-header"><h2>Sections</h2></div>
            <div className="card-body">
              {sections.isLoading && <LoadingState label="Loading" />}
              {sections.data?.sections.map(s => (
                <button key={s.section_id} className="outline-item" onClick={() => setTab('document')}>
                  <small>{s.title || `Section ${s.ordinal}`}</small>
                </button>
              ))}
              {sections.data && !sections.data.sections.length && <EmptyState title="No sections" description="No indexable narrative found." />}
            </div>
          </div>

          {/* Center: main content */}
          <div className="card filing-main">
            <div className="tabs-bar">
              {TABS.map(t => (
                <button key={t.key} className={tab === t.key ? 'tab tab--active' : 'tab'} onClick={() => setTab(t.key)}>
                  {t.label}
                </button>
              ))}
            </div>
            <div className="card-body">
              {tab === 'document' && (
                <div className="filing-sections">
                  {sections.isLoading && <LoadingState label="Loading narrative" />}
                  {sections.data?.sections.map(s => (
                    <article key={s.section_id}>
                      <h3>{s.title || `Section ${s.ordinal}`}</h3>
                      <p>{s.text}</p>
                    </article>
                  ))}
                  {sections.data && !sections.data.sections.length && <EmptyState title="No narrative sections" description="This package did not contain indexable HTML." />}
                </div>
              )}

              {tab === 'statements' && (
                <div>
                  <div className="button-row" style={{ marginBottom: 12 }}>
                    <input className="input compact" placeholder="Filter by concept…" value={factFilter} onChange={e => setFactFilter(e.target.value)} />
                  </div>
                  {facts.isLoading && <LoadingState label="Loading facts" />}
                  {facts.data?.facts.map(f => (
                    <button
                      key={f.fact_id}
                      className={selectedFact?.fact_id === f.fact_id ? 'fact-row fact-row--selected' : 'fact-row'}
                      onClick={() => setSelectedFact(f)}
                    >
                      <span>{f.concept}</span>
                      <strong>{f.value_text ?? (f.numeric_value != null ? String(f.numeric_value) : '—')}</strong>
                      <small>{f.context_id ?? ''}{f.unit_id ? ` · ${f.unit_id}` : ''}</small>
                    </button>
                  ))}
                </div>
              )}

              {tab === 'audit' && (
                <div className="filing-sections">
                  {detail.data?.artifacts.filter(a => a.member_path.includes('AuditDoc')).map(a => (
                    <div key={a.artifact_id} className="artifact-row">
                      <span>{a.member_path}</span>
                      <small>{a.kind} · {a.size_bytes.toLocaleString()} bytes</small>
                    </div>
                  ))}
                  {detail.data && !detail.data.artifacts.some(a => a.member_path.includes('AuditDoc')) && <EmptyState title="No audit reports" description="This filing does not include separate audit documents." />}
                </div>
              )}

              {tab === 'taxonomy' && (
                <div>
                  {taxonomy.isLoading && <LoadingState label="Loading taxonomy" />}
                  {taxonomy.data?.taxonomy.map((t, i) => (
                    <div key={i} className="fact-row">
                      <span>{t.concept}</span>
                      <small>{t.namespace_uri ?? ''}</small>
                    </div>
                  ))}
                  {taxonomy.data && !taxonomy.data.taxonomy.length && <EmptyState title="No taxonomy" description="No concepts indexed for this filing." />}
                </div>
              )}

              {tab === 'quality' && (
                <div>
                  {quality.isLoading && <LoadingState label="Loading quality issues" />}
                  {quality.data?.issues.map(issue => (
                    <div key={issue.issue_id} className="fact-row">
                      <span className={`status-pill ${SEVERITY_COLORS[issue.severity] ?? ''}`}>{issue.severity}</span>
                      <strong>{issue.code}</strong>
                      <small>{issue.message}</small>
                    </div>
                  ))}
                  {quality.data && !quality.data.issues.length && <p>No quality issues detected.</p>}
                </div>
              )}
            </div>
          </div>

          {/* Right: fact inspector */}
          <div className="card filing-inspector">
            <div className="card-header"><h2>Inspector</h2></div>
            <div className="card-body">
              {selectedFact ? (
                <div className="stack">
                  <div className="metric"><span className="metric-label">Concept</span><span className="metric-value">{selectedFact.concept}</span></div>
                  <div className="metric"><span className="metric-label">Value</span><span className="metric-value">{selectedFact.value_text ?? String(selectedFact.numeric_value ?? '—')}</span></div>
                  <div className="metric"><span className="metric-label">Numeric</span><span className="metric-value">{selectedFact.numeric_value != null ? selectedFact.numeric_value.toLocaleString() : '—'}</span></div>
                  <div className="metric"><span className="metric-label">Context</span><span className="metric-value">{selectedFact.context_id || '—'}</span></div>
                  <div className="metric"><span className="metric-label">Unit</span><span className="metric-value">{selectedFact.unit_id || '—'}</span></div>
                  <div className="metric"><span className="metric-label">Namespace</span><span className="metric-value" style={{ fontSize: '.72rem' }}>{selectedFact.namespace_uri || '—'}</span></div>
                </div>
              ) : (
                <EmptyState title="Select a fact" description="Click a fact in the Facts tab to inspect its details." />
              )}
            </div>
            <div className="card-header"><h2>Filing info</h2></div>
            <div className="card-body">
              {detail.data ? (
                <div className="stack">
                  <div className="metric"><span className="metric-label">Status</span><span className="metric-value"><span className="status-pill">{detail.data.filing.status}</span></span></div>
                  <div className="metric"><span className="metric-label">Submitted</span><span className="metric-value">{detail.data.filing.submitted_at || '—'}</span></div>
                  <div className="metric"><span className="metric-label">Period end</span><span className="metric-value">{detail.data.filing.period_end || '—'}</span></div>
                  <div className="metric"><span className="metric-label">Members</span><span className="metric-value">{detail.data.artifacts.length}</span></div>
                </div>
              ) : <LoadingState label="Loading" />}
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
