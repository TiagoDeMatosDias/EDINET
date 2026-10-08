import { useQuery } from '@tanstack/react-query'
import { ArrowLeft, ArrowRight, ChevronLeft, ChevronRight } from 'lucide-react'
import { Link, useNavigate, useParams, useSearchParams } from 'react-router-dom'

import { apiRequest, queryString } from '../../api/client'
import { DownloadButton } from '../../components/DownloadButton'
import { ErrorState, LoadingState } from '../../components/Feedback'
import { Tip } from '../../components/Tooltip'
import { HotkeyHelpButton } from '../../hotkeys/HotkeyHelpButton'
import { HotkeyKbd } from '../../hotkeys/HotkeyKbd'
import { useHotkeyScope } from '../../hotkeys/useHotkeyScope'
import { companyName, fiscalLabel, formatBytes, formatDay, formLabel, periodSpan } from './filingFormat'
import { FilingStatus, type FilingRow } from './FilingsTable'
import { DetailsTab } from './viewer/DetailsTab'
import { ReportTab } from './viewer/ReportTab'
import { SectionsTab } from './viewer/SectionsTab'
import { StatementsTab } from './viewer/StatementsTab'
import { filingViewerScope } from './filingsHotkeys'
import { VIEWER_TABS, type FilingDetail, type QualityIssue, type ReportFile, type Section, type ViewerTab } from './viewer/viewerTypes'
import './filings.css'

const TAB_KEYS = VIEWER_TABS.map(tab => tab.key) as ViewerTab[]

export default function FilingViewerPage() {
  const { docId = '' } = useParams<{ docId: string }>()
  const navigate = useNavigate()
  const [searchParams, setSearchParams] = useSearchParams()
  const requestedTab = searchParams.get('tab') as ViewerTab | null
  const tab: ViewerTab = requestedTab && TAB_KEYS.includes(requestedTab) ? requestedTab : 'report'
  const item = searchParams.get('item')
  const from = searchParams.get('from')

  const detail = useQuery({
    queryKey: ['filing', docId],
    enabled: Boolean(docId),
    queryFn: () => apiRequest<FilingDetail>(`/api/filings/${encodeURIComponent(docId)}`),
  })
  const filing = detail.data?.filing
  const companyCode = filing?.edinet_code ?? searchParams.get('company') ?? ''
  // The company's other filings give the English name and older/newer navigation.
  const siblings = useQuery({
    queryKey: ['filings', 'company', companyCode],
    enabled: Boolean(companyCode),
    queryFn: () => apiRequest<{ filings: FilingRow[] }>(`/api/filings${queryString({ company_code: companyCode, limit: 500 })}`),
  })
  const files = useQuery({
    queryKey: ['filing-htm', docId],
    enabled: Boolean(docId),
    queryFn: () => apiRequest<{ files: ReportFile[] }>(`/api/filings/${encodeURIComponent(docId)}/htm-files`),
  })
  const sections = useQuery({
    queryKey: ['filing-sections', docId],
    enabled: Boolean(docId),
    queryFn: () => apiRequest<{ sections: Section[]; count: number }>(`/api/filings/${encodeURIComponent(docId)}/sections${queryString({ limit: 500 })}`),
  })
  const quality = useQuery({
    queryKey: ['filing-quality', docId],
    enabled: Boolean(docId),
    queryFn: () => apiRequest<{ issues: QualityIssue[] }>(`/api/filings/${encodeURIComponent(docId)}/quality`),
  })

  const others = siblings.data?.filings ?? []
  const self = others.find(row => row.doc_id === docId)
  const position = others.findIndex(row => row.doc_id === docId)
  const newer = position > 0 ? others[position - 1] : undefined
  const older = position >= 0 ? others[position + 1] : undefined
  const name = companyName({ ...filing, company_name: self?.company_name })
  const ticker = self?.ticker
  const issues = quality.data?.issues ?? []
  // Stepping between years keeps the open tab, so a statement can be compared year to year.
  const hrefFor = (row: FilingRow) => {
    const params = new URLSearchParams()
    if (from) params.set('from', from)
    if (companyCode) params.set('company', companyCode)
    if (tab !== 'report') params.set('tab', tab)
    return `/filings/${encodeURIComponent(row.doc_id)}${params.size ? `?${params}` : ''}`
  }

  const update = (changes: Record<string, string | null>) => {
    const next = new URLSearchParams(searchParams)
    for (const [key, value] of Object.entries(changes)) {
      if (value) next.set(key, value)
      else next.delete(key)
    }
    setSearchParams(next, { replace: 'item' in changes && !('tab' in changes) })
  }
  const selectTab = (key: ViewerTab) => update({ tab: key === 'report' ? null : key, item: null })
  const selectItem = (id: string) => update({ item: id })

  useHotkeyScope(filingViewerScope, {
    'tab-report': () => selectTab('report'),
    'tab-sections': () => selectTab('sections'),
    'tab-statements': () => selectTab('statements'),
    'tab-details': () => selectTab('details'),
    older: () => { if (older) navigate(hrefFor(older)) },
    newer: () => { if (newer) navigate(hrefFor(newer)) },
    analyze: () => { if (companyCode) navigate(`/analyze/${encodeURIComponent(companyCode)}`) },
    list: () => { if (companyCode) navigate(`/filings?company=${encodeURIComponent(companyCode)}`) },
  })

  if (detail.isLoading) return <LoadingState label="Loading the filing" />
  if (detail.isError) return <ErrorState error={detail.error} retry={() => void detail.refetch()} />

  const fileStem = [companyCode || name, filing?.period_end?.slice(0, 7), docId].filter(Boolean).join('-')
  return <div className="filing-viewer">
    <nav className="viewer-breadcrumbs" aria-label="Breadcrumb">
      {from === 'analysis' && companyCode
        ? <Link to={`/analyze/${encodeURIComponent(companyCode)}`}><ArrowLeft aria-hidden="true" />Back to analysis</Link>
        : <Link to="/filings">Filings</Link>}
      {companyCode && <><span aria-hidden="true">/</span><Link to={`/filings?company=${encodeURIComponent(companyCode)}`} title="All retained filings from this company (L)">{name}</Link></>}
      <span aria-hidden="true">/</span><span aria-current="page">{fiscalLabel(filing?.period_end)}</span>
    </nav>

    <header className="viewer-header">
      <div className="viewer-header__id">
        <h1>{name}</h1>
        <p className="viewer-header__meta">
          {filing?.submitter_name && filing.submitter_name !== name && <span lang="ja">{filing.submitter_name}</span>}
          {ticker && <Tip content="Securities code">{ticker}</Tip>}
          {companyCode && <Tip content="EDINET code">{companyCode}</Tip>}
        </p>
        <p className="viewer-header__report">
          <Tip content={`EDINET form ${filing?.form_code ?? 'unknown'}`}><strong>{formLabel(filing?.form_code)}</strong></Tip>
          <span>{periodSpan(filing?.period_start, filing?.period_end) || fiscalLabel(filing?.period_end)}</span>
          {filing?.submitted_at && <span>Filed {formatDay(filing.submitted_at)} {filing.submitted_at.slice(11, 16)}</span>}
          <span className="mono">{docId}</span>
          {self?.archive_size != null && <span>{formatBytes(self.archive_size)}</span>}
          {filing && <FilingStatus filing={filing} />}
          {issues.length > 0 && <button type="button" className="text-button viewer-header__issues" onClick={() => selectTab('details')}>{issues.length} quality {issues.length === 1 ? 'note' : 'notes'}</button>}
        </p>
      </div>
      <div className="viewer-header__nav">
        <div className="viewer-stepper" aria-label="Other reports from this company">
          {older ? <Link to={hrefFor(older)} title={`Older report: ${fiscalLabel(older.period_end)} ([)`}><ChevronLeft aria-hidden="true" />{fiscalLabel(older.period_end)}</Link> : <span className="viewer-stepper__none"><ChevronLeft aria-hidden="true" />Oldest</span>}
          {position >= 0 && <span className="viewer-stepper__count">{others.length - position} of {others.length}</span>}
          {newer ? <Link to={hrefFor(newer)} title={`Newer report: ${fiscalLabel(newer.period_end)} (])`}>{fiscalLabel(newer.period_end)}<ChevronRight aria-hidden="true" /></Link> : <span className="viewer-stepper__none">Latest<ChevronRight aria-hidden="true" /></span>}
        </div>
        <div className="viewer-header__actions">
          {companyCode && <Link className="button button--secondary button--small" to={`/analyze/${encodeURIComponent(companyCode)}`} title="Open the company analysis (A)">Analysis<ArrowRight aria-hidden="true" /></Link>}
          {filing?.archive_sha256 && <DownloadButton path={`/api/filings/${encodeURIComponent(docId)}/artifact`} filename={`${docId}.zip`}>ZIP</DownloadButton>}
          <HotkeyHelpButton />
        </div>
      </div>
    </header>

    <div className="viewer-tabs" role="tablist" aria-label="Filing views">
      {VIEWER_TABS.map(option => <button key={option.key} type="button" role="tab" aria-selected={tab === option.key} className={tab === option.key ? 'viewer-tab active' : 'viewer-tab'} title={option.hint} onClick={() => selectTab(option.key)}>
        <HotkeyKbd hotkey={filingViewerScope.byId[`tab-${option.key}`]} />{option.label}
        {option.key === 'details' && issues.length > 0 && <small className="viewer-tab__badge">{issues.length}</small>}
        {option.key === 'report' && files.data && <small>{files.data.files.length}</small>}
        {option.key === 'sections' && sections.data && <small>{sections.data.count}</small>}
      </button>)}
    </div>

    <div className="viewer-body" role="tabpanel">
      {tab === 'report' && <ReportTab docId={docId} files={files.data?.files ?? []} filesLoading={files.isLoading} active={item} onSelect={selectItem} />}
      {tab === 'sections' && <SectionsTab docId={docId} files={files.data?.files ?? []} sections={sections.data?.sections ?? []} loading={sections.isLoading} />}
      {tab === 'statements' && <StatementsTab docId={docId} fileStem={fileStem} active={item} onSelect={selectItem} />}
      {tab === 'details' && <DetailsTab docId={docId} artifacts={detail.data?.artifacts ?? []} issues={issues} issuesLoading={quality.isLoading} />}
    </div>
  </div>
}
