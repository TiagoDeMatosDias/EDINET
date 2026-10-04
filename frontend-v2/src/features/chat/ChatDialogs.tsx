import { useQuery } from '@tanstack/react-query'
import { Check, Hash, Lock, MessageSquare, Search, Users, X } from 'lucide-react'
import { useDeferredValue, useEffect, useRef, useState, type KeyboardEvent, type ReactNode } from 'react'

import { chatApi } from './chatApi'
import { channelLabel, colorFor, personName } from './chatModel'
import type { Channel, Person } from './chatTypes'

/** A modal that owns the keyboard while open: Esc or the backdrop closes it, and focus returns afterwards. */
export function ChatDialog({ title, onClose, children, wide = false }: { title: string; onClose: () => void; children: ReactNode; wide?: boolean }) {
  useEffect(() => {
    const previous = document.activeElement as HTMLElement | null
    const onKey = (event: globalThis.KeyboardEvent) => {
      if (event.key === 'Escape') { event.preventDefault(); event.stopPropagation(); onClose() }
    }
    window.addEventListener('keydown', onKey, { capture: true })
    return () => { window.removeEventListener('keydown', onKey, { capture: true }); previous?.focus?.() }
  }, [onClose])
  return <div className="shortcuts-backdrop" onMouseDown={event => { if (event.target === event.currentTarget) onClose() }}>
    <div className={wide ? 'chat-dialog chat-dialog--wide' : 'chat-dialog'} role="dialog" aria-modal="true" aria-label={title}>
      <header><h2>{title}</h2><button type="button" className="icon-button" aria-label="Close" onClick={onClose}><X /></button></header>
      {children}
    </div>
  </div>
}

function usePeople(query: string) {
  const deferred = useDeferredValue(query.trim())
  return useQuery({ queryKey: ['chat', 'people', deferred], queryFn: () => chatApi.people(deferred), staleTime: 20_000 })
}

/**
 * Pick people: one for a direct message, several for a group or an invitation.
 * ↑ ↓ move, Space or Enter picks, and in a multi-pick Ctrl+Enter finishes.
 */
export function PeopleDialog({ mode, onClose, onDone, exclude = [], busy, error }: {
  mode: 'dm' | 'group' | 'invite'
  onClose: () => void
  onDone: (people: Person[], title?: string) => void
  exclude?: string[]
  busy?: boolean
  error?: string | null
}) {
  const [query, setQuery] = useState('')
  const [title, setTitle] = useState('')
  const [picked, setPicked] = useState<Person[]>([])
  const [active, setActive] = useState(0)
  const search = useRef<HTMLInputElement>(null)
  const titleInput = useRef<HTMLInputElement>(null)
  const people = usePeople(query)
  useEffect(() => { (mode === 'group' ? titleInput : search).current?.focus() }, [mode])
  const results = (people.data?.people ?? []).filter(person => !person.is_me && !exclude.includes(person.user_id))
  const index = Math.min(active, Math.max(0, results.length - 1))
  const multi = mode !== 'dm'
  const ready = multi ? picked.length > 0 && (mode !== 'group' || title.trim().length > 0) : false
  const toggle = (person: Person) => {
    if (!person.accepts_messages || person.blocked) return
    if (!multi) { onDone([person]); return }
    setPicked(current => current.some(item => item.user_id === person.user_id) ? current.filter(item => item.user_id !== person.user_id) : [...current, person])
  }
  const onKeyDown = (event: KeyboardEvent<HTMLInputElement>) => {
    if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
      event.preventDefault()
      if (results.length) setActive((index + (event.key === 'ArrowDown' ? 1 : -1) + results.length) % results.length)
    } else if (event.key === 'Enter' && (event.ctrlKey || event.metaKey) && ready) {
      event.preventDefault()
      onDone(picked, title.trim())
    } else if (event.key === 'Enter' || (event.key === ' ' && multi && !query.endsWith(' ') && results[index])) {
      if (!results[index]) return
      event.preventDefault()
      toggle(results[index])
    }
  }
  const heading = mode === 'dm' ? 'New direct message' : mode === 'group' ? 'New group' : 'Invite people'
  return <ChatDialog title={heading} onClose={onClose}>
    <div className="chat-dialog__body">
      {mode === 'group' && <label className="field"><span>Group name</span><input ref={titleInput} className="input" value={title} maxLength={80} onChange={event => setTitle(event.target.value)} onKeyDown={event => { if (event.key === 'Enter') { event.preventDefault(); search.current?.focus() } }} /></label>}
      <label className="chat-search"><Search aria-hidden="true" /><input ref={search} className="input" placeholder="Find people by name" aria-label="Find people" value={query} onChange={event => { setQuery(event.target.value); setActive(0) }} onKeyDown={onKeyDown} /></label>
      {multi && picked.length > 0 && <div className="chat-picked">{picked.map(person => <button key={person.user_id} type="button" className="chat-pill" onClick={() => toggle(person)}>{personName(person)}<X aria-hidden="true" /></button>)}</div>}
      <ul className="chat-options" role="listbox" aria-label="People" aria-multiselectable={multi}>
        {people.isLoading && <li className="chat-options__status">Searching…</li>}
        {!people.isLoading && !results.length && <li className="chat-options__status">No one matches.</li>}
        {results.map((person, position) => {
          const chosen = picked.some(item => item.user_id === person.user_id)
          const unavailable = !person.accepts_messages || person.blocked
          return <li key={person.user_id} role="option" aria-selected={position === index} aria-disabled={unavailable} className={[position === index && 'is-active', unavailable && 'is-disabled'].filter(Boolean).join(' ')} onMouseDown={event => { event.preventDefault(); toggle(person) }} onMouseEnter={() => setActive(position)}>
            <span className="chat-dot" style={{ background: colorFor(person.user_id) }} />
            <strong>{personName(person)}</strong><small>@{person.username}</small>
            {unavailable && <small className="muted">{person.blocked ? 'blocked' : 'not accepting messages'}</small>}
            {chosen && <Check aria-label="Picked" />}
          </li>
        })}
      </ul>
      {error && <p className="form-error" role="alert">{error}</p>}
      <p className="chat-dialog__hint"><Lock aria-hidden="true" />{mode === 'dm' ? 'Enter opens an end-to-end encrypted conversation.' : <>Space picks · <kbd>Ctrl</kbd>+<kbd>Enter</kbd> {mode === 'group' ? 'creates the group' : 'invites'}. Members can read the group’s history.</>}</p>
      {multi && <div className="button-row"><button type="button" className="button button--primary button--small" disabled={!ready || busy} onClick={() => onDone(picked, title.trim())}><Users aria-hidden="true" />{busy ? 'Working…' : mode === 'group' ? `Create group (${picked.length})` : `Invite ${picked.length}`}</button></div>}
    </div>
  </ChatDialog>
}

export interface SwitchItem { key: string; kind: 'channel' | 'company' | 'conversation' | 'person' | 'feed'; label: string; detail?: string; color: string; unread?: number; go: () => void }

/** "Jump to…": channels, companies, conversations, and people, by name. */
export function QuickSwitcher({ local, onClose, onChannel, onPerson }: {
  local: SwitchItem[]
  onClose: () => void
  onChannel: (channel: Channel) => void
  onPerson: (person: Person) => void
}) {
  const [query, setQuery] = useState('')
  const [active, setActive] = useState(0)
  const deferred = useDeferredValue(query.trim())
  const remote = useQuery({ queryKey: ['chat', 'switch', deferred], queryFn: () => chatApi.searchChannels(deferred), enabled: deferred.length >= 2, staleTime: 30_000 })
  const people = useQuery({ queryKey: ['chat', 'people', deferred], queryFn: () => chatApi.people(deferred), enabled: deferred.length >= 2, staleTime: 20_000 })
  const words = query.toLowerCase().split(/\s+/).filter(Boolean)
  const matches = (text: string) => words.every(word => text.toLowerCase().includes(word))
  const localMatches = local.filter(item => matches(`${item.label} ${item.detail ?? ''}`))
  const seen = new Set(localMatches.map(item => item.key))
  const remoteItems: SwitchItem[] = deferred.length >= 2 ? [
    ...(remote.data?.channels ?? []).filter(channel => !seen.has(channel.channel_id)).map(channel => ({
      key: channel.channel_id, kind: channel.kind === 'company' ? 'company' as const : 'channel' as const, label: channelLabel(channel), detail: channel.kind === 'company' ? [channel.company_code, channel.industry].filter(Boolean).join(' · ') : channel.description ?? undefined,
      color: colorFor(channel.channel_id), go: () => onChannel(channel),
    })),
    ...(people.data?.people ?? []).filter(person => !person.is_me && !person.blocked).map(person => ({
      key: `person:${person.user_id}`, kind: 'person' as const, label: `@${person.username}`, detail: person.display_name ? `${person.display_name} · message privately` : 'Message privately', color: colorFor(person.user_id), go: () => onPerson(person),
    })),
  ] : []
  const items = [...localMatches, ...remoteItems].slice(0, 40)
  const index = Math.min(active, Math.max(0, items.length - 1))
  const onKeyDown = (event: KeyboardEvent<HTMLInputElement>) => {
    if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
      event.preventDefault()
      if (items.length) setActive((index + (event.key === 'ArrowDown' ? 1 : -1) + items.length) % items.length)
    } else if (event.key === 'Enter' && items[index]) {
      event.preventDefault()
      items[index].go()
      onClose()
    }
  }
  const icon = (kind: SwitchItem['kind']) => kind === 'person' || kind === 'conversation' ? <MessageSquare aria-hidden="true" /> : <Hash aria-hidden="true" />
  return <ChatDialog title="Jump to" onClose={onClose}>
    <div className="chat-dialog__body">
      <label className="chat-search"><Search aria-hidden="true" /><input autoFocus className="input" placeholder="Channel, company, conversation, or person" aria-label="Jump to" value={query} onChange={event => { setQuery(event.target.value); setActive(0) }} onKeyDown={onKeyDown} /></label>
      <ul className="chat-options" role="listbox" aria-label="Destinations">
        {!items.length && <li className="chat-options__status">{deferred.length >= 2 && (remote.isLoading || people.isLoading) ? 'Searching…' : 'Nothing matches. Type a company name to open its channel.'}</li>}
        {items.map((item, position) => <li key={item.key} role="option" aria-selected={position === index} className={position === index ? 'is-active' : undefined} onMouseDown={event => { event.preventDefault(); item.go(); onClose() }} onMouseEnter={() => setActive(position)}>
          <span className="chat-dot" style={{ background: item.color }} />{icon(item.kind)}<strong>{item.label}</strong>{item.detail && <small>{item.detail}</small>}{item.unread ? <span className="chat-badge">{item.unread}</span> : null}
        </li>)}
      </ul>
      <p className="chat-dialog__hint">↑ ↓ choose · <kbd>Enter</kbd> open · <kbd>Esc</kbd> close</p>
    </div>
  </ChatDialog>
}
