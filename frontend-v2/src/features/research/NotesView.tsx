import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Download, Search, X } from 'lucide-react'
import { useMemo, useRef, useState, type KeyboardEvent } from 'react'

import { apiPost, apiRequest } from '../../api/client'
import type { SecuritySearchResult } from '../../api/types'
import { CompanyPicker } from '../../components/CompanyPicker'
import { ErrorState, LoadingState } from '../../components/Feedback'
import { useHotkeys } from '../../hooks/useHotkeys'
import { downloadTextFile, safeFileName } from '../analysis/downloads'
import { NoteItem } from './CompanyResearchPanel'
import { notesMarkdown, noteTitle } from './researchModel'
import { invalidateResearch } from './researchQueries'
import type { BookCompany, Note } from './researchTypes'
import { moveCursorKey, useListCursor } from './useListCursor'

export function NotesView({ companies, companyCode, onCompany, active, today }: {
  companies: BookCompany[]
  companyCode: string
  onCompany: (code: string) => void
  active: boolean
  today: string
}) {
  const client = useQueryClient()
  const notes = useQuery({ queryKey: ['research-notes'], queryFn: () => apiRequest<{ notes: Note[] }>('/api/research/notes'), retry: false })
  const names = useMemo(() => new Map(companies.map(company => [company.company_code, company.company_name])), [companies])
  const [query, setQuery] = useState('')
  const [cursor, setCursor] = useState(0)
  const [focusRequest, setFocusRequest] = useState(0)
  const [editing, setEditing] = useState<string | null>(null)
  const [armed, setArmed] = useState<string | null>(null)
  const [draftCompany, setDraftCompany] = useState<SecuritySearchResult | null>(null)
  const [body, setBody] = useState('')
  const filterRef = useRef<HTMLInputElement>(null)
  const noteRef = useRef<HTMLTextAreaElement>(null)

  const all = useMemo(() => notes.data?.notes ?? [], [notes.data])
  const shown = useMemo(() => {
    const words = query.trim().toLocaleLowerCase().split(/\s+/).filter(Boolean)
    return all.filter(note => {
      if (companyCode && note.edinet_code !== companyCode) return false
      const haystack = `${note.title}\n${note.body}\n${note.edinet_code ? names.get(note.edinet_code) ?? note.edinet_code : ''}`.toLocaleLowerCase()
      return words.every(word => haystack.includes(word))
    })
  }, [all, companyCode, names, query])
  const index = Math.min(cursor, Math.max(shown.length - 1, 0))
  const current = shown[index]
  const list = useListCursor<HTMLDivElement>(index, focusRequest)
  const targetCompany = draftCompany?.company_code ?? companyCode

  const create = useMutation({
    mutationFn: () => apiPost<Note>('/api/research/notes', { title: noteTitle('', body), body: body.trim(), edinet_code: targetCompany || undefined }),
    onSuccess: () => { setBody(''); setCursor(0); invalidateResearch(client) },
  })
  const remove = useMutation({
    mutationFn: (id: string) => apiRequest(`/api/research/notes/${encodeURIComponent(id)}`, { method: 'DELETE' }),
    onSuccess: () => invalidateResearch(client),
  })
  const step = (delta: number) => { setCursor(Math.max(0, Math.min(shown.length - 1, index + delta))); setFocusRequest(value => value + 1) }
  const focus = (element: HTMLElement | null) => { element?.focus(); element?.scrollIntoView?.({ block: 'center', behavior: 'smooth' }) }
  const download = () => downloadTextFile(`${safeFileName(companyCode ? names.get(companyCode) ?? companyCode : 'research')}-notes.md`, notesMarkdown(shown, names), 'text/markdown;charset=utf-8')

  useHotkeys({
    j: () => step(1),
    k: () => step(-1),
    n: () => focus(noteRef.current),
    f: () => focus(filterRef.current),
    e: () => { if (current) setEditing(current.note_id) },
    x: () => {
      if (!current) return
      if (armed === current.note_id) { setArmed(null); remove.mutate(current.note_id) } else setArmed(current.note_id)
    },
    d: download,
  }, active && !editing)

  const onListKeyDown = (event: KeyboardEvent<HTMLDivElement>) => {
    if ((event.target as HTMLElement).closest('.is-editing')) return
    moveCursorKey(event, index, shown.length, setCursor)
  }
  // X arms the note's delete button and a second X deletes; a click on the armed button does too.
  const onArmed = (id: string) => (next: boolean) => setArmed(next ? id : null)

  if (notes.isLoading) return <LoadingState label="Loading notes" />
  if (notes.error) return <ErrorState error={notes.error} retry={() => { void notes.refetch() }} />
  const filterName = companyCode ? names.get(companyCode) ?? companyCode : ''

  return <div className="rs-notes-view">
    <section className="panel rs-compose" aria-label="New note">
      <div className="rs-compose__company">
        <CompanyPicker selected={draftCompany ?? (companyCode ? { company_code: companyCode, ticker: '', company_name: filterName } : null)} onSelect={setDraftCompany} label="Company (optional)" placeholder="General, or choose a company…" />
      </div>
      <textarea
        ref={noteRef}
        className="input"
        rows={3}
        aria-label="New note"
        placeholder="Write a note — the first line is its title. Ctrl+Enter saves."
        value={body}
        onChange={event => setBody(event.target.value)}
        onKeyDown={event => {
          if (event.key === 'Enter' && (event.ctrlKey || event.metaKey) && body.trim()) { event.preventDefault(); create.mutate() }
          if (event.key === 'Escape') event.currentTarget.blur()
        }}
      />
      <div className="rs-compose__actions">
        <button type="button" className="button button--primary button--small" disabled={!body.trim() || create.isPending} onClick={() => create.mutate()}>{create.isPending ? 'Saving…' : 'Save note'} <kbd aria-hidden="true">N</kbd></button>
        {create.error && <span className="form-error">{(create.error as Error).message}</span>}
      </div>
    </section>
    <section className="panel" aria-label="Notes">
      <div className="rs-toolbar">
        <label className="rs-search">
          <Search aria-hidden="true" />
          <input ref={filterRef} className="input" value={query} placeholder="Search notes" aria-label="Search notes" onChange={event => { setQuery(event.target.value); setCursor(0) }} onKeyDown={event => { if (event.key === 'Escape') { setQuery(''); event.currentTarget.blur() } if (event.key === 'ArrowDown') { event.preventDefault(); setFocusRequest(value => value + 1) } }} />
          <kbd aria-hidden="true">F</kbd>
        </label>
        {companyCode && <span className="rs-chip" aria-pressed="true">{filterName}<button type="button" className="icon-button" aria-label={`Show notes on every company, not just ${filterName}`} onClick={() => onCompany('')}><X /></button></span>}
        <span className="rs-count">{shown.length === all.length ? `${all.length} notes` : `${shown.length} of ${all.length} notes`}</span>
        <span className="rs-toolbar__spacer" />
        <button type="button" className="button button--ghost button--small" disabled={!shown.length} onClick={download} title="Download the notes listed as Markdown (D)"><Download aria-hidden="true" />Markdown</button>
      </div>
      <div ref={list} className="rs-note-list" role="listbox" aria-label="Notes" onKeyDown={onListKeyDown}>
        {shown.map((note, position) => <div
          key={note.note_id}
          role="option"
          aria-selected={position === index}
          data-cursor={position === index}
          tabIndex={position === index ? 0 : -1}
          onFocus={event => { if (event.target === event.currentTarget) setCursor(position) }}
          onClick={() => setCursor(position)}
        >
          <NoteItem
            note={note}
            today={today}
            active={position === index}
            company={note.edinet_code ? { code: note.edinet_code, name: names.get(note.edinet_code) ?? note.edinet_code } : null}
            editing={editing === note.note_id}
            onEditingChange={next => { setEditing(next ? note.note_id : null); if (!next) setFocusRequest(value => value + 1) }}
            armed={armed === note.note_id}
            onArmedChange={onArmed(note.note_id)}
          />
        </div>)}
        {!shown.length && <p className="rs-empty">{all.length ? 'No notes match.' : 'No notes yet. Write one above (N), or from a company in the Companies tab or on its Analysis page.'}</p>}
      </div>
    </section>
  </div>
}
