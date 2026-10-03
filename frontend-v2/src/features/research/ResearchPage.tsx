import { useQuery } from '@tanstack/react-query'
import { Keyboard } from 'lucide-react'
import { useCallback, useMemo, useRef, useState, type KeyboardEvent } from 'react'
import { useNavigate, useSearchParams } from 'react-router-dom'

import { apiRequest } from '../../api/client'
import { PageHeader } from '../../components/Page'
import { ShortcutsDialog, type ShortcutGroup } from '../../components/ShortcutsDialog'
import { useHotkeys } from '../../hooks/useHotkeys'
import { AlertsView } from './AlertsView'
import { BookView } from './BookView'
import { NotesView } from './NotesView'
import { BondsView } from './pricing/BondsView'
import { OptionsView } from './pricing/OptionsView'
import { reviewState, todayIso } from './researchModel'
import { useResearchBook } from './researchQueries'
import { ALERTS_SHORTCUTS, BONDS_SHORTCUTS, BOOK_SHORTCUTS, NOTES_SHORTCUTS, OPTIONS_SHORTCUTS } from './researchShortcuts'
import type { Note } from './researchTypes'

const TABS = [
  { key: 'companies', label: 'Companies', shortcuts: BOOK_SHORTCUTS },
  { key: 'notes', label: 'Notes', shortcuts: NOTES_SHORTCUTS },
  { key: 'alerts', label: 'Alerts', shortcuts: ALERTS_SHORTCUTS },
  { key: 'options', label: 'Options', shortcuts: OPTIONS_SHORTCUTS },
  { key: 'bonds', label: 'Bonds & credit', shortcuts: BONDS_SHORTCUTS },
] as const

type TabKey = typeof TABS[number]['key']

const ANYWHERE: ShortcutGroup = { title: 'Research', shortcuts: [
  { keys: ['1', '2', '3', '4', '5'], label: 'Companies, Notes, Alerts, Options, Bonds' },
  { keys: ['[', ']'], label: 'In the tab list: previous or next tab (also ← →)' },
  { keys: ['?'], label: 'Show or hide this list' },
] }

export default function ResearchPage() {
  const [params, setParams] = useSearchParams()
  const navigate = useNavigate()
  const tabParam = params.get('tab')
  const tab: TabKey = TABS.some(item => item.key === tabParam) ? tabParam as TabKey : 'companies'
  const company = params.get('company') ?? ''
  const tag = params.get('tag') ?? ''
  const status = params.get('status') ?? ''
  const today = useMemo(() => todayIso(), [])
  const [showShortcuts, setShowShortcuts] = useState(false)
  const closeShortcuts = useCallback(() => setShowShortcuts(false), [])
  const tabRefs = useRef<Array<HTMLButtonElement | null>>([])

  const book = useResearchBook()
  const notes = useQuery({ queryKey: ['research-notes'], queryFn: () => apiRequest<{ notes: Note[] }>('/api/research/notes'), retry: false })

  const update = (patch: Record<string, string>) => {
    const next = new URLSearchParams(params)
    for (const [key, value] of Object.entries(patch)) {
      if (value && !(key === 'tab' && value === 'companies')) next.set(key, value)
      else next.delete(key)
    }
    setParams(next, { replace: true })
  }
  const selectTab = (key: TabKey) => update({ tab: key })
  useHotkeys(Object.fromEntries([
    ['?', () => setShowShortcuts(true)],
    ...TABS.map((item, index) => [String(index + 1), () => selectTab(item.key)]),
  ]), !showShortcuts)

  const companies = book.data?.companies ?? []
  const triggered = (book.data?.alerts ?? []).filter(alert => alert.triggered).length
  const due = companies.filter(item => { const state = reviewState(item.review_on, today); return state === 'overdue' || state === 'due' }).length
  const counts: Record<TabKey, string> = {
    companies: book.data ? String(companies.length) : '',
    notes: notes.data ? String(notes.data.notes.length) : '',
    alerts: book.data ? (triggered ? `${triggered} triggered` : String(book.data.alerts.length)) : '',
    options: '',
    bonds: '',
  }
  const description = [
    `${companies.length} ${companies.length === 1 ? 'company' : 'companies'}`,
    notes.data && `${notes.data.notes.length} ${notes.data.notes.length === 1 ? 'note' : 'notes'}`,
    triggered > 0 && `${triggered} ${triggered === 1 ? 'alert' : 'alerts'} triggered`,
    due > 0 && `${due} ${due === 1 ? 'review' : 'reviews'} due`,
  ].filter(Boolean).join(' · ')
  const onTabKeyDown = (event: KeyboardEvent<HTMLDivElement>) => {
    const index = TABS.findIndex(item => item.key === tab)
    const delta = event.key === 'ArrowRight' || event.key === ']' ? 1 : event.key === 'ArrowLeft' || event.key === '[' ? -1 : 0
    if (!delta) return
    event.preventDefault()
    event.stopPropagation()
    const next = TABS[(index + delta + TABS.length) % TABS.length]
    selectTab(next.key)
    tabRefs.current[TABS.indexOf(next)]?.focus()
  }
  const current = TABS.find(item => item.key === tab) ?? TABS[0]
  const active = !showShortcuts

  return <div className="stack dense-page rs-page">
    <PageHeader
      eyebrow="Research"
      title="Research"
      description={book.data ? `Your private research: ${description}.` : 'Your private notes, tags, theses, alerts, and pricing tools.'}
      actions={<button type="button" className="icon-button" onClick={() => setShowShortcuts(true)} title="Keyboard shortcuts (?)" aria-label="Keyboard shortcuts"><Keyboard aria-hidden="true" /></button>}
    />
    <div className="rs-tabs" role="tablist" aria-label="Research" onKeyDown={onTabKeyDown}>
      {TABS.map((item, index) => <button
        key={item.key}
        ref={element => { tabRefs.current[index] = element }}
        type="button"
        role="tab"
        id={`rs-tab-${item.key}`}
        aria-controls={`rs-panel-${item.key}`}
        aria-selected={tab === item.key}
        tabIndex={tab === item.key ? 0 : -1}
        className={tab === item.key ? 'rs-tab active' : 'rs-tab'}
        onClick={() => selectTab(item.key)}
      ><kbd aria-hidden="true">{index + 1}</kbd>{item.label}{counts[item.key] && <small className={item.key === 'alerts' && triggered ? 'is-alert' : undefined}>{counts[item.key]}</small>}</button>)}
    </div>
    <div role="tabpanel" id={`rs-panel-${tab}`} aria-labelledby={`rs-tab-${tab}`} className="rs-panel">
      {tab === 'companies' && <BookView
        book={book.data}
        loading={book.isLoading}
        error={book.error}
        retry={() => { void book.refetch() }}
        selectedCode={company}
        onSelect={code => update({ company: code })}
        tag={tag}
        status={status}
        onFilter={next => update({ ...(next.tag !== undefined ? { tag: next.tag } : {}), ...(next.status !== undefined ? { status: next.status } : {}) })}
        active={active}
        today={today}
      />}
      {tab === 'notes' && <NotesView companies={companies} companyCode={company} onCompany={code => update({ company: code })} active={active} today={today} />}
      {tab === 'alerts' && <AlertsView book={book.data} active={active} today={today} onOpenCompany={code => navigate(`/research?company=${encodeURIComponent(code)}`)} />}
      {tab === 'options' && <OptionsView companyCode={company} onCompany={code => update({ company: code })} active={active} />}
      {tab === 'bonds' && <BondsView companyCode={company} onCompany={code => update({ company: code })} active={active} />}
    </div>
    {showShortcuts && <ShortcutsDialog groups={[ANYWHERE, current.shortcuts]} onClose={closeShortcuts} />}
  </div>
}
