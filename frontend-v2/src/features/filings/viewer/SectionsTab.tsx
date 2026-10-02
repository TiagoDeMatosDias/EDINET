import { useQuery, useQueryClient } from '@tanstack/react-query'
import { Search } from 'lucide-react'
import { useMemo, useRef, useState, type ReactNode } from 'react'

import { apiRequest, queryString } from '../../../api/client'
import { EmptyState, LoadingState } from '../../../components/Feedback'
import { useHotkeys } from '../../../hooks/useHotkeys'
import { usePersistentState } from '../../../hooks/usePersistentState'
import type { ReportFile, Section } from './viewerTypes'

function errorMessage(error: unknown, fallback: string) {
  return error instanceof Error && error.message ? error.message : fallback
}

function highlight(text: string, needle: string): ReactNode {
  if (!needle) return text
  const pattern = new RegExp(`(${needle.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')})`, 'gi')
  return text.split(pattern).map((part, index) => index % 2 ? <mark key={index}>{part}</mark> : part)
}

export function SectionsTab({ docId, files, sections, loading }: { docId: string; files: ReportFile[]; sections: Section[]; loading: boolean }) {
  const queryClient = useQueryClient()
  const [sideBySide, setSideBySide] = usePersistentState('filings.sections.english', true, [true, false])
  const [query, setQuery] = useState('')
  const [current, setCurrent] = useState(0)
  const [translating, setTranslating] = useState<Set<string>>(new Set())
  const [errors, setErrors] = useState<Record<string, string>>({})
  const search = useRef<HTMLInputElement>(null)
  const translations = useQuery({
    queryKey: ['filing-sections-en', docId],
    enabled: sideBySide && sections.length > 0,
    retry: false,
    queryFn: () => apiRequest<{ sections: Section[]; count: number }>(`/api/filings/${encodeURIComponent(docId)}/sections-translated${queryString({ limit: 500, bodies: 'true' })}`),
  })
  const merged = useMemo(() => {
    const byId = new Map((translations.data?.sections ?? []).map(section => [section.section_id, section]))
    return sections.map(section => ({ ...section, ...(byId.get(section.section_id) ?? {}) }))
  }, [sections, translations.data])
  const needle = query.trim()
  const lower = needle.toLowerCase()
  const visible = needle ? merged.filter(section => [section.title, section.title_en, section.text, section.text_en].some(value => (value ?? '').toLowerCase().includes(lower))) : merged
  const fileLabels = new Map(files.map(file => [file.artifact_id, file.label]))
  const outline: Array<{ label: string; sections: typeof visible }> = []
  for (const section of visible) {
    const label = fileLabels.get(section.artifact_id ?? '') ?? 'Other text'
    const last = outline[outline.length - 1]
    if (last?.label === label) last.sections.push(section)
    else outline.push({ label, sections: [section] })
  }

  const jump = (index: number) => {
    const next = Math.max(0, Math.min(visible.length - 1, index))
    setCurrent(next)
    document.getElementById(`section-${visible[next]?.section_id}`)?.scrollIntoView({ behavior: 'smooth', block: 'start' })
  }
  useHotkeys({
    f: () => search.current?.focus(),
    t: () => setSideBySide(!sideBySide),
    j: () => jump(current + 1),
    k: () => jump(current - 1),
  }, sections.length > 0)

  const translateSection = async (sectionId: string, force = false) => {
    if (translating.has(sectionId)) return
    setTranslating(previous => new Set(previous).add(sectionId))
    setErrors(previous => { const next = { ...previous }; delete next[sectionId]; return next })
    try {
      const data = await apiRequest<{ section: Section }>(`/api/filings/${encodeURIComponent(docId)}/translate-body${queryString(force ? { section_id: sectionId, force: 'true' } : { section_id: sectionId })}`)
      if (data.section?.text_en) {
        queryClient.setQueryData(['filing-sections-en', docId], (old: { sections: Section[]; count: number } | undefined) => {
          const base = old ?? { sections, count: sections.length }
          return { ...base, sections: base.sections.map(section => section.section_id === sectionId ? { ...section, text_en: data.section.text_en, title_en: data.section.title_en } : section) }
        })
      }
    } catch (error) {
      setErrors(previous => ({ ...previous, [sectionId]: errorMessage(error, 'Translation request failed') }))
    }
    setTranslating(previous => { const next = new Set(previous); next.delete(sectionId); return next })
  }

  if (loading) return <LoadingState label="Loading the document text" />
  if (!sections.length) return <EmptyState title="No narrative text" description="This filing has no narrative sections." />
  return <div className="viewer-split">
    <nav className="viewer-sidebar" aria-label="Document outline">
      <label className="viewer-search">
        <Search aria-hidden="true" />
        <input ref={search} value={query} onChange={event => { setQuery(event.target.value); setCurrent(0) }} onKeyDown={event => { if (event.key === 'Escape') { setQuery(''); event.currentTarget.blur() } }} placeholder="Search the text" aria-label="Search the document text" />
        <kbd>F</kbd>
      </label>
      {needle && <p className="viewer-sidebar__count">{visible.length} of {merged.length} sections match</p>}
      {outline.map(group => <section key={`${group.label}-${group.sections[0]?.section_id}`}>
        <h3>{group.label}</h3>
        <ul>{group.sections.map(section => {
          const index = visible.indexOf(section)
          return <li key={section.section_id}>
            <button type="button" className={index === current ? 'viewer-item active' : 'viewer-item'} onClick={() => jump(index)}>
              <span lang="ja">{section.title || `Section ${section.ordinal}`}</span>
              {sideBySide && section.title_en && <small>{section.title_en}</small>}
            </button>
          </li>
        })}</ul>
      </section>)}
    </nav>
    <section className="viewer-main" aria-label="Document text">
      <header className="viewer-toolbar">
        <div className="viewer-toolbar__title"><h2>{needle ? `Sections matching “${needle}”` : 'Narrative text'}</h2><span>{visible.length} sections</span></div>
        <label className="inline-toggle"><input type="checkbox" checked={sideBySide} onChange={event => setSideBySide(event.target.checked)} /> English alongside</label>
        <kbd title="Toggle English">T</kbd>
      </header>
      {sideBySide && translations.isFetching && <p className="viewer-note" role="status">Translating the complete document with the local Argos model…</p>}
      {sideBySide && translations.isError && <p className="viewer-note viewer-note--error" role="alert">{errorMessage(translations.error, 'Document translation failed')} Sections can still be translated one at a time below. <button type="button" className="text-button" onClick={() => void translations.refetch()}>Retry the whole document</button></p>}
      <div className="sections-reader">
        {visible.map(section => {
          const busy = translations.isFetching || translating.has(section.section_id)
          return <article key={section.section_id} id={`section-${section.section_id}`} className="reader-section">
            <h3 lang="ja">{highlight(section.title || `Section ${section.ordinal}`, needle)}</h3>
            {sideBySide && section.title_en && <p className="reader-section__en-title">{highlight(section.title_en, needle)}</p>}
            <div className={sideBySide ? 'reader-columns reader-columns--dual' : 'reader-columns'}>
              <p lang="ja">{highlight(section.text, needle)}</p>
              {sideBySide && <div className="reader-english">
                {section.text_en
                  ? <p>{highlight(section.text_en, needle)}</p>
                  : busy
                    ? <p className="muted">Translating…</p>
                    : <button type="button" className="text-button" onClick={() => void translateSection(section.section_id)}>Translate this section</button>}
                {errors[section.section_id] && <p role="alert" className="form-error">{errors[section.section_id]}</p>}
                {section.text_en && <button type="button" className="text-button reader-english__again" disabled={busy} onClick={() => void translateSection(section.section_id, true)}>{translating.has(section.section_id) ? 'Translating…' : 'Retranslate'}</button>}
              </div>}
            </div>
          </article>
        })}
        {!visible.length && <EmptyState title="No sections match" description={`Nothing in this filing mentions “${needle}”.`} />}
      </div>
    </section>
  </div>
}
