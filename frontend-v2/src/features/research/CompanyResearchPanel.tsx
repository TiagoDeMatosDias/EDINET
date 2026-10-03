import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Bell, Briefcase, Plus, X } from 'lucide-react'
import { useId, useState, type Ref } from 'react'
import { Link } from 'react-router-dom'

import { apiPost, apiRequest } from '../../api/client'
import { formatMetricValue } from '../../metrics'
import { ConfirmButton } from './ConfirmButton'
import { invalidateResearch, useResearchBook } from './researchQueries'
import { ALERT_OPERATORS, alertCondition, formatDay, formatSignedPercent, isPositionTag, noteTitle, relativeDay, reviewState, THESIS_STATUSES, todayIso, upside } from './researchModel'
import type { CompanyResearch, Note, ResearchBook, ThesisStatus } from './researchTypes'
import './research.css'


interface Draft { target: string; currency: string; review: string; thesis: string }

const CURRENCIES = ['JPY', 'USD', 'EUR', 'GBP']

/**
 * A company's research in one place — status, target, review date, thesis,
 * tags, notes, and alerts — shared by the Research page and company Analysis
 * so both read and edit the same records.
 */
export function CompanyResearchPanel({ code, price, priceCurrency, compact = false, tagRef, noteRef, thesisRef, statusRef, keys = {}, today = todayIso() }: {
  code: string
  price?: number | null
  priceCurrency?: string | null
  compact?: boolean
  /** Let page shortcuts focus the tag, note, thesis, and status controls. */
  tagRef?: Ref<HTMLInputElement>
  noteRef?: Ref<HTMLTextAreaElement>
  thesisRef?: Ref<HTMLTextAreaElement>
  statusRef?: Ref<HTMLDivElement>
  /** The page's shortcut keys for these controls, shown beside them. */
  keys?: { thesis?: string; tag?: string; note?: string }
  today?: string
}) {
  const client = useQueryClient()
  const ids = useId()
  const research = useQuery({ queryKey: ['company-research', code], queryFn: () => apiRequest<CompanyResearch>(`/api/research/companies/${encodeURIComponent(code)}`), retry: false })
  const tags = useQuery({ queryKey: ['company-tags', code], queryFn: () => apiRequest<{ tags: string[] }>(`/api/tags/${encodeURIComponent(code)}`), retry: false })
  const allTags = useQuery({ queryKey: ['research-tags'], queryFn: () => apiRequest<{ tags: Array<{ name: string; member_count: number }> }>('/api/tags'), retry: false })
  const notes = useQuery({ queryKey: ['research-notes', code], queryFn: () => apiRequest<{ notes: Note[] }>(`/api/research/notes?edinet_code=${encodeURIComponent(code)}`), retry: false })
  const book = useResearchBook()
  const record = research.data
  const serverDraft: Draft = {
    target: record?.target_value == null ? '' : String(record.target_value),
    currency: record?.target_currency ?? priceCurrency ?? 'JPY',
    review: record?.review_on ?? '',
    thesis: record?.thesis ?? '',
  }
  const [draft, setDraft] = useState<Draft | null>(null)
  const values = draft ?? serverDraft

  const save = useMutation({
    mutationFn: (next: { status?: ThesisStatus | null; draft?: Draft }) => {
      const source = next.draft ?? values
      const target = source.target.trim() === '' ? null : Number(source.target.replace(/,/g, ''))
      return apiRequest<CompanyResearch>(`/api/research/companies/${encodeURIComponent(code)}`, {
        method: 'PATCH',
        body: JSON.stringify({
          thesis_status: (next.status === undefined ? record?.thesis_status : next.status) || null,
          target_value: target != null && Number.isFinite(target) ? target : null,
          target_currency: source.currency || null,
          review_on: source.review || null,
          thesis: source.thesis.trim() || null,
        }),
      })
    },
    onSuccess: saved => {
      client.setQueryData(['company-research', code], saved)
      setDraft(null)
      invalidateResearch(client)
    },
  })
  const dirty = draft != null && (draft.target !== serverDraft.target || draft.currency !== serverDraft.currency || draft.review !== serverDraft.review || draft.thesis !== serverDraft.thesis)
  const commit = () => { if (dirty) save.mutate({ draft: values }) }

  const [newTag, setNewTag] = useState('')
  const setTags = useMutation({
    mutationFn: (next: string[]) => apiRequest<{ tags: Array<{ tag: string }> }>(`/api/research/tags/${encodeURIComponent(code)}`, { method: 'PUT', body: JSON.stringify({ tags: next }) }),
    onSuccess: () => invalidateResearch(client, code),
  })
  // Position tags follow the portfolio; the server keeps them whatever is sent.
  const positionTags = (tags.data?.tags ?? []).filter(isPositionTag)
  const currentTags = (tags.data?.tags ?? []).filter(tag => !isPositionTag(tag))
  const addTag = () => {
    const tag = newTag.trim()
    if (!tag || isPositionTag(tag)) return
    setNewTag('')
    if (!currentTags.includes(tag)) setTags.mutate([...currentTags, tag])
  }
  const suggestions = (allTags.data?.tags ?? []).map(tag => tag.name).filter(name => !currentTags.includes(name) && !isPositionTag(name))

  const [noteBody, setNoteBody] = useState('')
  const addNote = useMutation({
    mutationFn: (body: string) => apiPost<Note>('/api/research/notes', { title: noteTitle('', body), body, edinet_code: code }),
    onSuccess: () => { setNoteBody(''); invalidateResearch(client) },
  })

  const definitions = book.data?.metric_definitions ?? {}
  const alerts = (book.data?.alerts ?? []).filter(alert => alert.edinet_code === code)
  const company = book.data?.companies.find(item => item.company_code === code)
  const livePrice = price ?? company?.LatestPrice ?? null
  const liveCurrency = priceCurrency ?? company?.price_currency ?? null
  const gap = upside({ target_value: record?.target_value, target_currency: record?.target_currency, LatestPrice: livePrice, price_currency: liveCurrency })
  const review = reviewState(record?.review_on, today)
  const noteList = notes.data?.notes ?? []
  const shownNotes = compact ? noteList.slice(0, 3) : noteList

  return <div className={compact ? 'rs-company rs-company--compact' : 'rs-company'}>
    <div className="rs-company__row">
      <div className="rs-status" role="group" aria-label="Thesis status" ref={statusRef}>
        {THESIS_STATUSES.map(status => <button
          key={status.key}
          type="button"
          className={`rs-status__option rs-status__option--${status.key}`}
          aria-pressed={record?.thesis_status === status.key}
          title={status.hint}
          disabled={save.isPending || research.isLoading}
          onClick={() => save.mutate({ status: record?.thesis_status === status.key ? null : status.key })}
        >{status.label}</button>)}
      </div>
      <label className="rs-inline-field">
        <span>Target</span>
        <input className="input" inputMode="decimal" aria-label="Target price" value={values.target} placeholder="—" onChange={event => setDraft({ ...values, target: event.target.value })} onBlur={commit} onKeyDown={event => { if (event.key === 'Enter') commit() }} />
        <select className="select" aria-label="Target currency" value={values.currency} onChange={event => { const next = { ...values, currency: event.target.value }; setDraft(next); save.mutate({ draft: next }) }}>
          {[...new Set([values.currency, ...CURRENCIES])].map(currency => <option key={currency}>{currency}</option>)}
        </select>
      </label>
      {gap != null && <span className={gap >= 0 ? 'rs-upside is-up' : 'rs-upside is-down'} title={`Target against the latest price of ${formatMetricValue({ label: 'Price', group: '', format: 'money', currency: 'price' }, livePrice, { price: liveCurrency })}`}>{formatSignedPercent(gap)}</span>}
      <label className="rs-inline-field">
        <span>Review</span>
        <input className="input" type="date" aria-label="Review on" value={values.review} onChange={event => { const next = { ...values, review: event.target.value }; setDraft(next); save.mutate({ draft: next }) }} />
      </label>
      {review && review !== 'later' && <span className={`rs-badge rs-badge--${review}`}>{review === 'overdue' ? `Review overdue · ${relativeDay(record?.review_on, today)}` : `Review ${relativeDay(record?.review_on, today)}`}</span>}
      <span className="rs-save-state" aria-live="polite">{save.isPending ? 'Saving…' : save.isError ? 'Not saved' : dirty ? 'Unsaved' : ''}</span>
    </div>

    <label className="rs-thesis">
      <span className="rs-label">Thesis {keys.thesis && <kbd aria-hidden="true">{keys.thesis}</kbd>}</span>
      <textarea
        ref={thesisRef}
        className="input"
        rows={compact ? 2 : 3}
        value={values.thesis}
        placeholder="Why you own it or would buy it, and what would change your mind"
        onChange={event => setDraft({ ...values, thesis: event.target.value })}
        onBlur={commit}
        onKeyDown={event => {
          if (event.key === 'Enter' && (event.ctrlKey || event.metaKey)) { event.preventDefault(); commit() }
          if (event.key === 'Escape') { setDraft(null); event.currentTarget.blur() }
        }}
      />
    </label>
    {save.error && <p className="form-error">{(save.error as Error).message}</p>}

    <div className="rs-tags">
      <span className="rs-label">Tags {keys.tag && <kbd aria-hidden="true">{keys.tag}</kbd>}</span>
      {positionTags.map(tag => <span className="tag rs-tag--position" key={tag} title="Follows your portfolio: it changes when you open or close a position">
        <Briefcase aria-hidden="true" /><Link to={`/research?tag=${encodeURIComponent(tag)}`}>{tag}</Link>
      </span>)}
      {currentTags.map(tag => <span className="tag" key={tag}>
        <Link to={`/research?tag=${encodeURIComponent(tag)}`} title={`Companies tagged ${tag}`}>{tag}</Link>
        <button type="button" className="icon-button" aria-label={`Remove tag ${tag}`} onClick={() => setTags.mutate(currentTags.filter(item => item !== tag))}><X /></button>
      </span>)}
      <form className="rs-tags__add" onSubmit={event => { event.preventDefault(); addTag() }}>
        <input ref={tagRef} className="input" list={`${ids}-tags`} aria-label="Add a tag" placeholder="Add tag" value={newTag} onChange={event => setNewTag(event.target.value)} onKeyDown={event => { if (event.key === 'Escape') { setNewTag(''); event.currentTarget.blur() } }} />
        <datalist id={`${ids}-tags`}>{suggestions.map(name => <option key={name} value={name} />)}</datalist>
        <button type="submit" className="icon-button" aria-label="Add tag" disabled={!newTag.trim()}><Plus /></button>
      </form>
      {!currentTags.includes('Favorite') && <button type="button" className="text-button rs-tags__favorite" onClick={() => setTags.mutate([...currentTags, 'Favorite'])}>+ Favorite</button>}
    </div>

    <section className="rs-notes" aria-label="Notes">
      <form className="rs-note-form" onSubmit={event => { event.preventDefault(); if (noteBody.trim()) addNote.mutate(noteBody.trim()) }}>
        <textarea
          ref={noteRef}
          className="input"
          rows={2}
          aria-label="New note"
          placeholder="Add a note — the first line is its title. Ctrl+Enter saves."
          value={noteBody}
          onChange={event => setNoteBody(event.target.value)}
          onKeyDown={event => {
            if (event.key === 'Enter' && (event.ctrlKey || event.metaKey) && noteBody.trim()) { event.preventDefault(); addNote.mutate(noteBody.trim()) }
            if (event.key === 'Escape') event.currentTarget.blur()
          }}
        />
        <button type="submit" className="button button--secondary button--small" disabled={!noteBody.trim() || addNote.isPending}>{addNote.isPending ? 'Saving…' : 'Add note'}{keys.note && <kbd aria-hidden="true">{keys.note}</kbd>}</button>
      </form>
      {addNote.error && <p className="form-error">{(addNote.error as Error).message}</p>}
      {shownNotes.map(note => <NoteItem key={note.note_id} note={note} today={today} />)}
      {compact && noteList.length > shownNotes.length && <Link className="rs-more" to={`/research?tab=notes&company=${encodeURIComponent(code)}`}>All {noteList.length} notes in Research</Link>}
      {!noteList.length && !notes.isLoading && <p className="rs-empty">No notes yet.</p>}
    </section>

    <AlertsForCompany code={code} alerts={alerts} definitions={definitions} priceCurrency={liveCurrency} />
  </div>
}

/** A note that edits in place. */
export function NoteItem({ note, today, company, onDeleted, active = false, editing: editingProp, onEditingChange, armed, onArmedChange }: {
  note: Note
  today: string
  company?: { code: string; name: string } | null
  onDeleted?: () => void
  active?: boolean
  editing?: boolean
  onEditingChange?: (editing: boolean) => void
  armed?: boolean
  onArmedChange?: (armed: boolean) => void
}) {
  const client = useQueryClient()
  const [editingState, setEditingState] = useState(false)
  const editing = editingProp ?? editingState
  const setEditing = (next: boolean) => { setEditingState(next); onEditingChange?.(next) }
  const remove = useMutation({
    mutationFn: () => apiRequest(`/api/research/notes/${encodeURIComponent(note.note_id)}`, { method: 'DELETE' }),
    onSuccess: () => { onDeleted?.(); invalidateResearch(client) },
  })
  const firstLine = note.body.trim().split('\n', 1)[0].replace(/^#+\s*/, '').trim()
  const detail = firstLine === note.title.trim() ? note.body.trim().split('\n').slice(1).join('\n').trim() : note.body.trim()
  if (editing) return <NoteEditor note={note} onDone={() => setEditing(false)} />
  return <article className={active ? 'rs-note is-active' : 'rs-note'}>
    <header>
      <strong>{note.title}</strong>
      {company && <Link className="rs-note__company" to={`/research?company=${encodeURIComponent(company.code)}`}>{company.name}</Link>}
      <small title={note.updated_at ? `Updated ${formatDay(note.updated_at)}${note.version && note.version > 1 ? ` · version ${note.version}` : ''}` : undefined}>{relativeDay(note.updated_at, today)}</small>
      <span className="rs-note__actions">
        <button type="button" className="text-button" onClick={() => setEditing(true)}>Edit</button>
        <ConfirmButton label="Delete note" confirmLabel="Delete?" armed={armed} onArmedChange={onArmedChange} onConfirm={() => remove.mutate()}>Delete</ConfirmButton>
      </span>
    </header>
    {detail && <p>{detail}</p>}
  </article>
}

/** Editing starts from the note as saved; a stale version is refused rather than overwritten. */
function NoteEditor({ note, onDone }: { note: Note; onDone: () => void }) {
  const client = useQueryClient()
  const [title, setTitle] = useState(note.title)
  const [body, setBody] = useState(note.body)
  const update = useMutation({
    mutationFn: () => apiRequest<Note>(`/api/research/notes/${encodeURIComponent(note.note_id)}`, { method: 'PATCH', body: JSON.stringify({ title: noteTitle(title, body), body, edinet_code: note.edinet_code || undefined, expected_version: note.version }) }),
    onSuccess: () => { onDone(); invalidateResearch(client) },
  })
  return <article className="rs-note is-editing">
    <input className="input" aria-label="Note title" value={title} onChange={event => setTitle(event.target.value)} />
    <textarea
      className="input"
      rows={Math.min(14, Math.max(4, body.split('\n').length + 1))}
      aria-label="Note text"
      value={body}
      autoFocus
      onChange={event => setBody(event.target.value)}
      onKeyDown={event => {
        if (event.key === 'Enter' && (event.ctrlKey || event.metaKey) && body.trim()) { event.preventDefault(); update.mutate() }
        if (event.key === 'Escape') { event.stopPropagation(); onDone() }
      }}
    />
    <div className="rs-note__actions">
      <button type="button" className="button button--primary button--small" disabled={!body.trim() || update.isPending} onClick={() => update.mutate()}>{update.isPending ? 'Saving…' : 'Save'}</button>
      <button type="button" className="text-button" onClick={onDone}>Cancel</button>
      <small>Ctrl+Enter saves · Esc cancels</small>
    </div>
    {update.error && <p className="form-error">{(update.error as Error).message.includes('modified') ? 'This note changed in another window. Copy your text, then reload to see the latest version.' : (update.error as Error).message}</p>}
  </article>
}

function AlertsForCompany({ code, alerts, definitions, priceCurrency }: {
  code: string
  alerts: ResearchBook['alerts']
  definitions: ResearchBook['metric_definitions']
  priceCurrency?: string | null
}) {
  const client = useQueryClient()
  const metrics = Object.keys(definitions)
  const [metric, setMetric] = useState('LatestPrice')
  const [operator, setOperator] = useState<string>('>')
  const [value, setValue] = useState('')
  const create = useMutation({
    mutationFn: () => {
      const threshold = Number(value.replace(/,/g, '')) / (definitions[metric]?.format === 'percent' ? 100 : 1)
      return apiPost('/api/research/alerts', { name: alertCondition({ metric, operator, value: threshold, price_currency: priceCurrency }, definitions), edinet_code: code, metric, operator, value: threshold })
    },
    onSuccess: () => { setValue(''); invalidateResearch(client) },
  })
  const remove = useMutation({
    mutationFn: (id: string) => apiRequest(`/api/research/alerts/${encodeURIComponent(id)}`, { method: 'DELETE' }),
    onSuccess: () => invalidateResearch(client),
  })
  const valid = value.trim() !== '' && Number.isFinite(Number(value.replace(/,/g, '')))
  return <section className="rs-alerts" aria-label="Alerts">
    <span className="rs-label"><Bell aria-hidden="true" />Alerts</span>
    {alerts.map(alert => <span key={alert.alert_id} className={alert.triggered ? 'rs-alert-chip is-triggered' : 'rs-alert-chip'} title={alert.current_value == null ? 'No current value' : `Now ${formatMetricValue(definitions[alert.metric], alert.current_value, { price: alert.price_currency })}`}>
      {alert.triggered && <strong>Triggered · </strong>}{alertCondition(alert, definitions)}
      <button type="button" className="icon-button" aria-label={`Remove alert ${alert.name}`} onClick={() => remove.mutate(alert.alert_id)}><X /></button>
    </span>)}
    {metrics.length > 0 && <form className="rs-alerts__add" onSubmit={event => { event.preventDefault(); if (valid) create.mutate() }}>
      <select className="select" aria-label="Alert metric" value={metric} onChange={event => setMetric(event.target.value)}>{metrics.map(key => <option key={key} value={key}>{definitions[key].label}</option>)}</select>
      <select className="select" aria-label="Alert condition" value={operator} onChange={event => setOperator(event.target.value)}>{ALERT_OPERATORS.map(item => <option key={item}>{item}</option>)}</select>
      <input className="input" inputMode="decimal" aria-label="Alert threshold" placeholder={definitions[metric]?.format === 'percent' ? '%' : 'Value'} value={value} onChange={event => setValue(event.target.value)} />
      <button type="submit" className="icon-button" aria-label="Add alert" disabled={!valid || create.isPending}><Plus /></button>
    </form>}
    {create.error && <p className="form-error">{(create.error as Error).message}</p>}
  </section>
}
