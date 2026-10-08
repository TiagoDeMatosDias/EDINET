import { useQuery, useQueryClient } from '@tanstack/react-query'
import { RefreshCw } from 'lucide-react'
import { useState } from 'react'

import { apiRequest, queryString } from '../../../api/client'
import { EmptyState, ErrorState, LoadingState } from '../../../components/Feedback'
import { HotkeyKbd } from '../../../hotkeys/HotkeyKbd'
import { useHotkeyScope } from '../../../hotkeys/useHotkeyScope'
import { reportTabScope } from '../filingsHotkeys'
import { usePersistentState } from '../../../hooks/usePersistentState'
import { formatBytes } from '../filingFormat'
import { FILE_GROUP_LABELS, type ReportFile } from './viewerTypes'

type Language = 'ja' | 'both' | 'en'
const LANGUAGES: Array<{ key: Language; label: string }> = [
  { key: 'ja', label: '日本語' },
  { key: 'both', label: 'Side by side' },
  { key: 'en', label: 'English' },
]
const LANGUAGE_KEYS = LANGUAGES.map(language => language.key)

// The archive's own <style> blocks are stripped server-side; this keeps the report legible.
const READER_CSS = `<style>
html{background:#fbfaf6}
body{margin:0;padding:18px 26px 40px;color:#1c1b19;font-family:"Hiragino Sans","Yu Gothic","Noto Sans JP","Zen Kaku Gothic New",system-ui,sans-serif;font-size:14px;line-height:1.75}
h1,h2,h3,h4{line-height:1.4}
table{border-collapse:collapse;max-width:100%}
td,th{vertical-align:top}
img{max-width:100%;height:auto}
::selection{background:#f1d9d4}
</style>`

function readerDocument(html: string) {
  return html.includes('<head>') ? html.replace('<head>', `<head>${READER_CSS}`) : `${READER_CSS}${html}`
}

interface HtmlResponse { html: string; html_en?: string }

export function ReportTab({ docId, files, filesLoading, active, onSelect }: { docId: string; files: ReportFile[]; filesLoading: boolean; active: string | null; onSelect: (artifactId: string) => void }) {
  const queryClient = useQueryClient()
  const [language, setLanguage] = usePersistentState<Language>('filings.report.language', 'ja', LANGUAGE_KEYS)
  const [refreshing, setRefreshing] = useState(false)
  const [refreshError, setRefreshError] = useState('')
  const selected = files.find(file => file.artifact_id === active) ?? files.find(file => file.group === 'business') ?? files[0]
  const artifactId = selected?.artifact_id ?? ''
  const wantsEnglish = language !== 'ja'
  const original = useQuery({
    queryKey: ['filing-html', docId, artifactId],
    enabled: Boolean(artifactId),
    staleTime: Infinity,
    queryFn: () => apiRequest<HtmlResponse>(`/api/filings/${encodeURIComponent(docId)}/html/${encodeURIComponent(artifactId)}`),
  })
  const english = useQuery({
    queryKey: ['filing-html-en', docId, artifactId],
    enabled: Boolean(artifactId) && wantsEnglish,
    staleTime: Infinity,
    retry: false,
    queryFn: () => apiRequest<HtmlResponse>(`/api/filings/${encodeURIComponent(docId)}/html/${encodeURIComponent(artifactId)}${queryString({ translate: true })}`),
  })
  const index = files.findIndex(file => file.artifact_id === artifactId)
  const step = (delta: number) => {
    const next = files[Math.max(0, Math.min(files.length - 1, index + delta))]
    if (next) onSelect(next.artifact_id)
  }
  useHotkeyScope(reportTabScope, {
    next: () => step(1),
    previous: () => step(-1),
    language: () => setLanguage(LANGUAGE_KEYS[(LANGUAGE_KEYS.indexOf(language) + 1) % LANGUAGE_KEYS.length]),
  }, { enabled: files.length > 0 })
  const retranslate = async () => {
    setRefreshing(true)
    setRefreshError('')
    try {
      const data = await apiRequest<HtmlResponse>(`/api/filings/${encodeURIComponent(docId)}/html/${encodeURIComponent(artifactId)}${queryString({ translate: true, force: true })}`)
      queryClient.setQueryData(['filing-html-en', docId, artifactId], data)
    } catch (error) {
      setRefreshError(error instanceof Error ? error.message : 'Retranslation failed')
    } finally {
      setRefreshing(false)
    }
  }

  if (filesLoading) return <LoadingState label="Loading report files" />
  if (!files.length) return <EmptyState title="No report documents" description="This archive has no inline XBRL report files to display." />

  const groups = [...new Set(files.map(file => file.group))]
  const englishHtml = english.data?.html_en ?? ''
  const showJapanese = language !== 'en' || !englishHtml
  const showEnglish = wantsEnglish && Boolean(englishHtml)
  return <div className="viewer-split">
    <nav className="viewer-sidebar" aria-label="Report documents">
      {groups.map(group => <section key={group}>
        <h3>{FILE_GROUP_LABELS[group]}</h3>
        <ul>{files.filter(file => file.group === group).map(file => <li key={file.artifact_id}>
          <button type="button" className={file.artifact_id === artifactId ? 'viewer-item active' : 'viewer-item'} aria-current={file.artifact_id === artifactId ? 'true' : undefined} onClick={() => onSelect(file.artifact_id)} title={file.filename}>
            <span>{file.label}</span>
            <small>{file.heading && file.heading !== file.label ? `${file.heading} · ` : ''}{formatBytes(file.size_bytes)}</small>
          </button>
        </li>)}</ul>
      </section>)}
      <p className="viewer-sidebar__keys"><HotkeyKbd hotkey={reportTabScope.byId.next} /><HotkeyKbd hotkey={reportTabScope.byId.previous} /> next and previous document</p>
    </nav>
    <section className="viewer-main" aria-label={selected?.label ?? 'Report'}>
      <header className="viewer-toolbar">
        <div className="viewer-toolbar__title">
          <h2>{selected?.label}</h2>
          {selected?.heading && selected.heading !== selected.label && <span lang="ja">{selected.heading}</span>}
        </div>
        <div className="segmented segmented--small" role="group" aria-label="Report language">
          {LANGUAGES.map(option => <button key={option.key} type="button" className={language === option.key ? 'active' : ''} aria-pressed={language === option.key} onClick={() => setLanguage(option.key)}>{option.label}</button>)}
        </div>
        <span title="Cycle the report language"><HotkeyKbd hotkey={reportTabScope.byId.language} /></span>
        {wantsEnglish && englishHtml && <button type="button" className="text-button" disabled={refreshing} onClick={() => void retranslate()} title="Discard the cached translation of this document and translate it again"><RefreshCw aria-hidden="true" className={refreshing ? 'spin' : undefined} />Retranslate</button>}
      </header>
      {wantsEnglish && english.isFetching && !englishHtml && <p className="viewer-note" role="status">Translating this document with the local Argos model. Long sections can take a minute; the Japanese original stays readable meanwhile.</p>}
      {wantsEnglish && english.isError && <p className="viewer-note viewer-note--error" role="alert">{english.error instanceof Error ? english.error.message : 'Translation failed'} <button type="button" className="text-button" onClick={() => void english.refetch()}>Try again</button></p>}
      {refreshError && <p className="viewer-note viewer-note--error" role="alert">{refreshError}</p>}
      {original.isLoading ? <LoadingState label="Loading the report" /> : original.isError ? <ErrorState error={original.error} retry={() => void original.refetch()} /> : <div className={showJapanese && showEnglish ? 'report-frames report-frames--dual' : 'report-frames'}>
        {showJapanese && <figure className="report-frame">
          {showEnglish && <figcaption>Japanese original</figcaption>}
          <iframe srcDoc={readerDocument(original.data?.html ?? '')} sandbox="allow-same-origin" title={`${selected?.label ?? 'Report'} (Japanese original)`} />
        </figure>}
        {showEnglish && <figure className="report-frame">
          <figcaption>English · machine translation</figcaption>
          <iframe srcDoc={readerDocument(englishHtml)} sandbox="allow-same-origin" title={`${selected?.label ?? 'Report'} (English translation)`} />
        </figure>}
      </div>}
    </section>
  </div>
}
