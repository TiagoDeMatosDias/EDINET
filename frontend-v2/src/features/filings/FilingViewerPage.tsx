import { useMemo, useState } from 'react'
import { Link, useParams, useNavigate, useSearchParams } from 'react-router-dom'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { ArrowLeft, FileText, Globe, ShieldAlert, Table2, Tags } from 'lucide-react'

import { apiRequest, queryString } from '../../api/client'
import { DownloadButton } from '../../components/DownloadButton'
import { EmptyState, ErrorState, LoadingState } from '../../components/Feedback'
import { filterRows, lineItemCount, splitPeriods, type StatementPeriod, type StatementRow, type StatementTable, type StatementTablesResponse } from './statements'

interface Filing {
  doc_id: string; edinet_code?: string | null; submitter_name?: string | null
  submitted_at?: string | null; period_start?: string | null; period_end?: string | null
  status: string; form_code?: string | null; archive_sha256?: string | null
}
interface Section {
  section_id: string; title?: string; title_en?: string; text: string; text_en?: string; ordinal: number
}
interface FilingDetail {
  filing: Filing
  artifacts: Array<{ artifact_id: string; member_path: string; kind: string; size_bytes: number }>
}
interface QualityIssue { issue_id: string; severity: string; code: string; message: string }
interface TaxonomyEntry { namespace_uri?: string; concept?: string }

type Tab = 'original' | 'document' | 'facts' | 'audit' | 'taxonomy' | 'quality'

const TABS: { key: Tab; label: string; icon: typeof FileText }[] = [
  { key: 'original', label: 'Report', icon: Globe },
  { key: 'document', label: 'Sections', icon: FileText },
  { key: 'facts', label: 'Statements', icon: Table2 },
  { key: 'audit', label: 'Audit', icon: ShieldAlert },
  { key: 'taxonomy', label: 'Taxonomy', icon: Tags },
  { key: 'quality', label: 'Quality', icon: ShieldAlert },
]

const SEVERITY_CLASS: Record<string, string> = { error: 'status-pill--warn', warning: 'status-pill--warn', info: 'status-pill--ok' }

function fmtNum(n: number): string {
  if (Math.abs(n) >= 1e12) return (n / 1e12).toFixed(2) + 'T'
  if (Math.abs(n) >= 1e9) return (n / 1e9).toFixed(2) + 'B'
  if (Math.abs(n) >= 1e6) return (n / 1e6).toFixed(2) + 'M'
  if (Math.abs(n) >= 1e3) return (n / 1e3).toFixed(1) + 'K'
  return n.toLocaleString(undefined, { maximumFractionDigits: 2 })
}

function translationErrorMessage(error: unknown, fallback: string): string {
  return error instanceof Error && error.message ? error.message : fallback
}

function StatementGrid({ periods, rows, memberHeader }: { periods: StatementPeriod[]; rows: StatementRow[]; memberHeader: string }) {
  const columnCount = (memberHeader ? 3 : 2) + periods.length
  const indent = (depth: number) => ({ paddingLeft: `${10 + depth * 14}px` })
  return (
    <div className="table-scroll">
      <table className="facts-table statement-table">
        <thead>
          <tr>
            <th scope="col" className="concept-col">Line item</th>
            {memberHeader && <th scope="col">{memberHeader}</th>}
            <th scope="col" className="unit-col">Unit</th>
            {periods.map(period => (
              <th key={period.key} scope="col" className="num-col">
                {period.label}
                {period.detail && <small>{period.detail}</small>}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, index) => {
            if (row.kind === 'heading') {
              return <tr key={index} className="section-row"><td colSpan={columnCount} style={indent(row.depth)}><strong>{row.label}</strong></td></tr>
            }
            const isTotal = row.kind === 'total'
            return (
              <tr key={index} className={isTotal ? 'subtotal-row' : ''}>
                <td className="concept-cell" style={indent(row.depth)} title={row.concept}>{isTotal ? <strong>{row.label}</strong> : row.label}</td>
                {memberHeader && <td className="member-cell">{row.member}</td>}
                <td className="unit-cell">{row.unit ?? ''}</td>
                {periods.map(period => {
                  const value = row.values?.[period.key]
                  return (
                    <td key={period.key} className="num-col">
                      {value != null ? (isTotal ? <strong>{fmtNum(value)}</strong> : fmtNum(value)) : '—'}
                    </td>
                  )
                })}
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

function StatementView({ statement, rows }: { statement: StatementTable; rows: StatementRow[] }) {
  const [showSparse, setShowSparse] = useState(false)
  const { shown, sparse } = splitPeriods(statement.periods)
  return (
    <section className="statement-block" aria-label={statement.name}>
      <h2 className="statement-title">{statement.name}</h2>
      {sparse.length > 0 && (
        <p className="text-muted statement-note">
          {showSparse
            ? `Showing all ${statement.periods.length} reported periods.`
            : `${sparse.length} sparsely reported ${sparse.length === 1 ? 'period is' : 'periods are'} hidden.`}
          {' '}<button className="text-button" onClick={() => setShowSparse(value => !value)}>{showSparse ? 'Hide sparse periods' : 'Show all periods'}</button>
        </p>
      )}
      <StatementGrid periods={showSparse ? statement.periods : shown} rows={rows} memberHeader={statement.member_axes.join(' · ')} />
    </section>
  )
}

export default function FilingViewerPage() {
  const { docId } = useParams<{ docId: string }>()
  const navigate = useNavigate()
  const [searchParams] = useSearchParams()
  const returnToAnalysis = Boolean(searchParams.get('company'))
  const analysisHref = returnToAnalysis ? `/analyze/${encodeURIComponent(searchParams.get('company') ?? '')}` : ''
  const queryClient = useQueryClient()
  const [tab, setTab] = useState<Tab>('original')
  const [selectedHtm, setSelectedHtm] = useState<string | null>(null)
  const [htmContent, setHtmContent] = useState('')
  const [htmCache, setHtmCache] = useState<Map<string, { jp: string; en: string }>>(new Map())
  const htmContentEn = selectedHtm ? htmCache.get(selectedHtm)?.en || '' : ''
  const [showEn, setShowEn] = useState(false)
  const [translatingHtm, setTranslatingHtm] = useState(false)
  const [htmError, setHtmError] = useState('')
  const [conceptFilter, setConceptFilter] = useState('')
  const [sideBySide, setSideBySide] = useState(true)
  const showEnglishPane = showEn && Boolean(htmContentEn)

  const detail = useQuery({
    queryKey: ['filing', docId],
    enabled: Boolean(docId),
    queryFn: () => apiRequest<FilingDetail>(`/api/filings/${encodeURIComponent(docId ?? '')}`),
  })
  const statementTables = useQuery({
    queryKey: ['filing-statement-tables', docId],
    enabled: Boolean(docId) && tab === 'facts',
    queryFn: () => apiRequest<StatementTablesResponse>(`/api/filings/${encodeURIComponent(docId ?? '')}/statement-tables`),
  })
  const [selectedStatement, setSelectedStatement] = useState<string | null>(null)
  const [translatingSections, setTranslatingSections] = useState<Set<string>>(new Set())
  const [sectionTranslationErrors, setSectionTranslationErrors] = useState<Record<string, string>>({})
  const sections = useQuery({
    queryKey: ['filing-sections', docId],
    enabled: Boolean(docId),
    queryFn: () => apiRequest<{ sections: Section[]; count: number }>(`/api/filings/${encodeURIComponent(docId ?? '')}/sections${queryString({ limit: 500 })}`),
  })
  const sectionTranslations = useQuery({
    queryKey: ['filing-sections-en', docId],
    enabled: Boolean(docId) && tab === 'document' && sideBySide,
    retry: false,
    queryFn: () => apiRequest<{ sections: Section[]; count: number }>(`/api/filings/${encodeURIComponent(docId ?? '')}/sections-translated${queryString({ limit: 500, bodies: 'true' })}`),
  })
  const documentSections = useMemo(() => {
    const translatedById = new Map((sectionTranslations.data?.sections ?? []).map(section => [section.section_id, section]))
    return (sections.data?.sections ?? []).map(section => ({ ...section, ...(translatedById.get(section.section_id) ?? {}) }))
  }, [sectionTranslations.data, sections.data])
  const fetchBodyTranslation = async (sectionId: string, force = false) => {
    if (translatingSections.has(sectionId)) return
    setTranslatingSections(prev => new Set(prev).add(sectionId))
    setSectionTranslationErrors(prev => { const next = { ...prev }; delete next[sectionId]; return next })
    try {
      const params = force ? { section_id: sectionId, force: 'true' } : { section_id: sectionId }
      const data = await apiRequest<{ section: Section }>(`/api/filings/${encodeURIComponent(docId ?? '')}/translate-body${queryString(params)}`)
      if (data.section?.text_en) {
        queryClient.setQueryData(['filing-sections-en', docId], (old: { sections: Section[]; count: number } | undefined) => {
          const base = old ?? { sections: sections.data?.sections ?? [], count: sections.data?.count ?? 0 }
          return { ...base, sections: base.sections.map(s => s.section_id === sectionId ? { ...s, text_en: data.section.text_en, title_en: data.section.title_en } : s) }
        })
      }
    } catch (error) {
      setSectionTranslationErrors(prev => ({ ...prev, [sectionId]: translationErrorMessage(error, 'Translation request failed') }))
    }
    setTranslatingSections(prev => { const next = new Set(prev); next.delete(sectionId); return next })
  }
  const taxonomy = useQuery({
    queryKey: ['filing-taxonomy', docId],
    enabled: Boolean(docId) && tab === 'taxonomy',
    queryFn: () => apiRequest<{ taxonomy: TaxonomyEntry[] }>(`/api/filings/${encodeURIComponent(docId ?? '')}/taxonomy`),
  })
  const quality = useQuery({
    queryKey: ['filing-quality', docId],
    enabled: Boolean(docId),
    queryFn: () => apiRequest<{ issues: QualityIssue[] }>(`/api/filings/${encodeURIComponent(docId ?? '')}/quality`),
  })
  const htmFiles = useQuery({
    queryKey: ['filing-htm', docId],
    enabled: Boolean(docId),
    queryFn: () => apiRequest<{ files: Array<{ artifact_id: string; member_path: string; label: string; filename: string; size_bytes: number }> }>(`/api/filings/${encodeURIComponent(docId ?? '')}/htm-files`),
  })

  const loadHtm = async (artifactId: string) => {
    setSelectedHtm(artifactId)
    setHtmError('')
    const cached = htmCache.get(artifactId)
    if (cached) {
      setHtmContent(cached.jp)
      if (cached.en) setShowEn(true)
      return
    }
    setHtmContent('')
    setShowEn(false)
    try {
      const data = await apiRequest<{ html: string }>(`/api/filings/${encodeURIComponent(docId ?? '')}/html/${encodeURIComponent(artifactId)}`)
      setHtmContent(data.html)
      setHtmCache(prev => new Map(prev).set(artifactId, { jp: data.html, en: '' }))
    } catch { setHtmContent('<p>Failed to load report</p>'); return }
    // Start English in background
    setTranslatingHtm(true)
    try {
      const enData = await apiRequest<{ html: string; html_en?: string }>(`/api/filings/${encodeURIComponent(docId ?? '')}/html/${encodeURIComponent(artifactId)}?translate=true`)
      if (enData.html_en) {
        setHtmCache(prev => { const m = new Map(prev); const entry = m.get(artifactId) || { jp: '', en: '' }; m.set(artifactId, { jp: entry.jp || enData.html, en: enData.html_en! }); return m })
        setShowEn(true)
        setHtmError('')
      } else { setHtmError('Translation returned empty') }
    } catch (error) { setHtmError(translationErrorMessage(error, 'Translation request failed')) }
    setTranslatingHtm(false)
  }

  const toggleTranslation = async () => {
    if (!selectedHtm) return
    const currentEn = htmCache.get(selectedHtm)?.en
    if (currentEn) { setShowEn(!showEn); return }
    setTranslatingHtm(true)
    try {
      const enData = await apiRequest<{ html: string; html_en?: string }>(`/api/filings/${encodeURIComponent(docId ?? '')}/html/${encodeURIComponent(selectedHtm)}?translate=true`)
      if (enData.html_en) {
        setHtmCache(prev => { const m = new Map(prev); const entry = m.get(selectedHtm) || { jp: '', en: '' }; m.set(selectedHtm, { jp: entry.jp || enData.html, en: enData.html_en! }); return m })
        setShowEn(true)
        setHtmError('')
      } else { setHtmError('Translation returned empty') }
    } catch (error) { setHtmError(translationErrorMessage(error, 'Translation request failed')) }
    setTranslatingHtm(false)
  }

  const filing = detail.data?.filing

  const statementMatches = useMemo(() => (statementTables.data?.statements ?? [])
    .map(statement => ({ statement, rows: filterRows(statement.rows, conceptFilter) }))
    .filter(({ rows }) => lineItemCount(rows) > 0), [conceptFilter, statementTables.data])
  const activeStatement = statementMatches.find(({ statement }) => statement.id === selectedStatement) ?? statementMatches[0]

  return (
    <div className="filing-viewer-page">
      <header className="filing-viewer-header">
        <button className="button button--ghost" onClick={() => navigate('/filings')}><ArrowLeft size={16} /> Back</button>
        {returnToAnalysis && <Link className="button button--secondary" to={analysisHref}><ArrowLeft size={16} /> Return to Analysis</Link>}
        <div className="filing-viewer-title">
          <h1>{filing?.submitter_name || filing?.edinet_code || docId}</h1>
          <div className="filing-meta">
            <span className="status-pill">{filing?.status ?? 'Loading'}</span>
            <span>{docId}</span>
            {filing?.edinet_code && <span>{filing.edinet_code}</span>}
            {filing?.form_code && <span>{filing.form_code}</span>}
            {filing?.period_end && <span>Period: {filing.period_end}</span>}
            {filing?.submitted_at && <span>Filed: {filing.submitted_at}</span>}
          </div>
        </div>
        {filing?.archive_sha256 && (
          <DownloadButton path={`/api/filings/${encodeURIComponent(docId ?? '')}/artifact`} filename={`${docId}.zip`}>ZIP</DownloadButton>
        )}
      </header>

      <div className="tabs-bar">
        {TABS.map(t => (
          <button key={t.key} className={tab === t.key ? 'tab tab--active' : 'tab'} onClick={() => setTab(t.key)}>
            <t.icon size={14} /> {t.label}
          </button>
        ))}
      </div>

      <div className="filing-viewer-body">
        {detail.isLoading && <LoadingState label="Loading" />}

        {/* Original HTM report */}
        {tab === 'original' && (
          <div className="filing-report-layout">
            <div className="card filing-report-files">
              <div className="card-header"><h2>Report files</h2></div>
              <div className="card-body">
                {htmFiles.isLoading ? <LoadingState label="Loading" /> : (
                  htmFiles.data?.files.map(f => (
                    <button
                      key={f.artifact_id}
                      className={selectedHtm === f.artifact_id ? 'outline-item' : 'outline-item'}
                      style={selectedHtm === f.artifact_id ? { background: 'var(--primary-soft)', fontWeight: 600 } : {}}
                      onClick={() => { void loadHtm(f.artifact_id) }}
                    >
                      <strong>{f.label}</strong>
                      <small style={{ display: 'block', fontSize: '.68rem', color: 'var(--muted)' }}>{(f.size_bytes / 1024).toFixed(0)} KB</small>
                    </button>
                  ))
                )}
                {htmFiles.data && !htmFiles.data.files.length && <EmptyState title="No HTML files" description="This archive has no inline XBRL report files." />}
              </div>
            </div>
            <div className="filing-report-content">
              {!selectedHtm ? (
                <EmptyState title="Select a report file" description="Choose a report file to view the original EDINET report." />
              ) : !htmContent ? (
                <LoadingState label="Loading report" />
              ) : (
                <div className="card">
                  <div className="card-header" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <h2>{htmFiles.data?.files.find(f => f.artifact_id === selectedHtm)?.label || 'Report'}</h2>
                    <button
                      className={showEnglishPane ? 'button button--primary button--small' : 'button button--secondary button--small'}
                      disabled={translatingHtm || (!htmContentEn && showEn)}
                      onClick={() => toggleTranslation()}
                    >
                      {translatingHtm ? 'Translating with Argos…' : showEnglishPane ? 'Hide English' : htmContentEn ? 'Show English alongside' : 'Translate to English'}
                    </button>
                    <button
                      className="button button--small button--ghost"
                      disabled={translatingHtm || !selectedHtm}
                      onClick={async () => {
                        if (!selectedHtm) return
                        setTranslatingHtm(true)
                        setHtmError('')
                        try {
                          const enData = await apiRequest<{ html: string; html_en?: string }>(`/api/filings/${encodeURIComponent(docId ?? '')}/html/${encodeURIComponent(selectedHtm)}?translate=true&force=true`)
                          if (enData.html_en) {
                            setHtmCache(prev => { const m = new Map(prev); const entry = m.get(selectedHtm) || { jp: '', en: '' }; m.set(selectedHtm, { jp: entry.jp || enData.html, en: enData.html_en! }); return m })
                            setShowEn(true)
                            setHtmError('')
                          } else { setHtmError('Translation returned empty') }
                        } catch (error) { setHtmError(translationErrorMessage(error, 'Refresh failed')) }
                        setTranslatingHtm(false)
                      }}
                    >
                      ↻ Retranslate
                    </button>
                    {htmError && <small style={{ color: 'var(--danger)', marginLeft: 8 }}>{htmError}</small>}
                  </div>
                  <div className="card-body filing-report-body" style={{ padding: 0, position: 'relative' }}>
                    {translatingHtm && (
                      <div style={{ position: 'absolute', top: 8, right: 12, zIndex: 1, padding: '4px 10px', borderRadius: 6, background: 'var(--primary-soft)', color: 'var(--primary)', fontSize: '.78rem', fontWeight: 600 }}>
                        Translating with Argos…
                      </div>
                    )}
                    <div className={showEnglishPane ? 'filing-report-grid filing-report-grid--dual' : 'filing-report-grid'}>
                      <div className="filing-report-pane">
                        <span className="panel-label">Japanese original</span>
                        <iframe
                          className="filing-report-frame"
                          srcDoc={htmContent}
                          sandbox="allow-same-origin"
                          title="EDINET report Japanese original"
                        />
                      </div>
                      {showEnglishPane && (
                        <div className="filing-report-pane">
                          <span className="panel-label">English translation</span>
                          <iframe
                            className="filing-report-frame"
                            srcDoc={htmContentEn}
                            sandbox="allow-same-origin"
                            title="EDINET report English translation"
                          />
                        </div>
                      )}
                    </div>
                  </div>
                </div>
              )}
            </div>
          </div>
        )}

        {/* Document */}
        {tab === 'document' && (
          <div className="filing-document">
            <div className="facts-toolbar">
              <label className="inline-toggle">
                <input type="checkbox" checked={sideBySide} onChange={e => setSideBySide(e.target.checked)} /> Side-by-side English
              </label>
              {sideBySide && sectionTranslations.isFetching && <span className="text-muted">Translating the complete document…</span>}
              {sideBySide && sectionTranslations.isError && (
                <>
                  <span role="alert" style={{ color: 'var(--danger)' }}>
                    {translationErrorMessage(sectionTranslations.error, 'Document translation failed')}
                  </span>
                  <button className="button button--secondary button--small" onClick={() => { void sectionTranslations.refetch() }}>Retry</button>
                </>
              )}
            </div>
            {sections.isLoading ? <LoadingState label="Loading document" /> : (
              <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                {documentSections.map(s => {
                  const sectionIsTranslating = sectionTranslations.isFetching || translatingSections.has(s.section_id)
                  return (
                    <article key={s.section_id} className="filing-section">
                      <h2>{s.title || `Section ${s.ordinal}`}</h2>
                      {sideBySide && s.title_en && (
                        <h3 className="en-heading">{s.title_en}</h3>
                      )}
                      {sideBySide ? (
                        <div className="side-by-side">
                          <div className="side-panel jp-panel">
                            <span className="panel-label">日本語</span>
                            <p>{s.text}</p>
                          </div>
                          <div className="side-panel en-panel">
                            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 6 }}>
                              <span className="panel-label" style={{ marginBottom: 0 }}>English</span>
                              <button
                                className="text-button"
                                style={{ fontSize: '.7rem' }}
                                disabled={sectionIsTranslating}
                                onClick={() => { void fetchBodyTranslation(s.section_id, true) }}
                              >
                                {translatingSections.has(s.section_id) ? 'Translating…' : 'Retranslate'}
                              </button>
                            </div>
                            {s.text_en ? (
                              <p>{s.text_en}</p>
                            ) : sectionIsTranslating ? (
                              <p className="text-muted" style={{ fontStyle: 'italic' }}>Translating…</p>
                            ) : (
                              <>
                                <p className="text-muted" style={{ fontStyle: 'italic', cursor: 'pointer', textDecoration: 'underline' }}
                                   onClick={() => { void fetchBodyTranslation(s.section_id) }}>
                                  Click to translate
                                </p>
                                {sectionTranslationErrors[s.section_id] && <p role="alert" style={{ color: 'var(--danger)' }}>{sectionTranslationErrors[s.section_id]}</p>}
                              </>
                            )}
                          </div>
                        </div>
                      ) : (
                        <p>{s.text}</p>
                      )}
                    </article>
                  )
                })}
              </div>
            )}
            {sections.data && !sections.data.sections.length && <EmptyState title="No narrative" />}
          </div>
        )}

        {/* Facts / Statements */}
        {tab === 'facts' && (
          <div>
            <div className="facts-toolbar">
              <input className="input" aria-label="Filter line items" placeholder="Filter line items…" value={conceptFilter} onChange={e => setConceptFilter(e.target.value)} />
              {statementTables.data && <span className="text-muted">{statementTables.data.fact_count.toLocaleString()} facts · {statementTables.data.statements.length} tables</span>}
            </div>
            {statementTables.isLoading ? <LoadingState label="Loading statements" /> : statementTables.isError ? <ErrorState error={statementTables.error} retry={() => { void statementTables.refetch() }} /> : !activeStatement ? (
              <EmptyState title={conceptFilter ? 'No matching line items' : 'No numeric facts'} />
            ) : (
              <div className="filing-report-layout">
                <nav className="card filing-report-files" aria-label="Statements and notes">
                  <div className="card-body">
                    {statementMatches.map(({ statement, rows }) => (
                      <button
                        key={statement.id}
                        className="outline-item"
                        aria-current={statement.id === activeStatement.statement.id ? 'true' : undefined}
                        onClick={() => setSelectedStatement(statement.id)}
                      >
                        <strong>{statement.name}</strong>
                        <small>{lineItemCount(rows)} line items</small>
                      </button>
                    ))}
                  </div>
                </nav>
                <div className="filing-report-content">
                  <StatementView key={activeStatement.statement.id} statement={activeStatement.statement} rows={activeStatement.rows} />
                </div>
              </div>
            )}
          </div>
        )}

        {/* Audit */}
        {tab === 'audit' && (
          <div style={{ paddingTop: 20 }}>
            {detail.data?.artifacts.filter(a => a.member_path.includes('AuditDoc')).map(a => (
              <div key={a.artifact_id} style={{ display: 'flex', justifyContent: 'space-between', padding: '8px 0', borderBottom: '1px solid var(--border)' }}>
                <span>{a.member_path}</span>
                <small>{a.kind} · {a.size_bytes.toLocaleString()} bytes</small>
              </div>
            ))}
            {!detail.data?.artifacts.some(a => a.member_path.includes('AuditDoc')) && <EmptyState title="No audit reports" />}
          </div>
        )}

        {/* Taxonomy */}
        {tab === 'taxonomy' && (
          <div style={{ paddingTop: 20 }}>
            {taxonomy.isLoading ? <LoadingState label="Loading" /> : taxonomy.data?.taxonomy.length ? (
              <table className="facts-table"><thead><tr><th>Concept</th><th>Namespace</th></tr></thead><tbody>{taxonomy.data.taxonomy.map((t, i) => <tr key={i}><td>{t.concept}</td><td style={{ fontFamily: 'monospace', fontSize: '.72rem' }}>{t.namespace_uri || '—'}</td></tr>)}</tbody></table>
            ) : <EmptyState title="No taxonomy" />}
          </div>
        )}

        {/* Quality */}
        {tab === 'quality' && (
          <div style={{ paddingTop: 20 }}>
            {quality.isLoading ? <LoadingState label="Loading" /> : quality.data?.issues.length ? quality.data.issues.map(i => (
              <div key={i.issue_id} className="quality-row">
                <span className={`status-pill ${SEVERITY_CLASS[i.severity] ?? ''}`}>{i.severity}</span>
                <strong>{i.code}</strong>
                <small>{i.message}</small>
              </div>
            )) : <p className="text-muted">No quality issues detected.</p>}
          </div>
        )}
      </div>
    </div>
  )
}
