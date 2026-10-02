import { FilePlus2, Search, Sparkles, Trash2 } from 'lucide-react'
import { useEffect, useMemo, useRef, useState, type KeyboardEvent } from 'react'

import type { StarterScreen } from './screenDefaults'
import type { SavedScreenSummary } from './types'

type Entry =
  | { kind: 'new'; key: string }
  | { kind: 'saved'; key: string; screen: SavedScreenSummary }
  | { kind: 'starter'; key: string; starter: StarterScreen }

const shortDate = new Intl.DateTimeFormat('en-GB', { day: 'numeric', month: 'short', year: 'numeric' })

function updated(value?: string | null) {
  const time = value ? Date.parse(value) : Number.NaN
  return Number.isFinite(time) ? shortDate.format(time) : ''
}

/**
 * Open a saved screen, a starter, or a blank screen: type to filter, ↑/↓ to
 * move, Enter to open. Saved screens show their shape and last change.
 */
export function ScreensMenu({ saved, starters, current, canSave, onOpenSaved, onOpenStarter, onNew, onDelete, onClose }: {
  saved: SavedScreenSummary[]; starters: StarterScreen[]; current: string | null; canSave: boolean
  onOpenSaved: (name: string) => void; onOpenStarter: (starter: StarterScreen) => void; onNew: () => void; onDelete: (name: string) => void; onClose: () => void
}) {
  const [query, setQuery] = useState('')
  const [active, setActive] = useState(0)
  const panel = useRef<HTMLDivElement>(null)
  useEffect(() => {
    const close = (event: MouseEvent) => {
      const target = event.target as HTMLElement
      if (!panel.current?.contains(target) && !target.closest('[data-screens-trigger]')) onClose()
    }
    document.addEventListener('mousedown', close)
    return () => document.removeEventListener('mousedown', close)
  }, [onClose])
  const needle = query.trim().toLowerCase()
  const entries = useMemo<Entry[]>(() => [
    ...(needle ? [] : [{ kind: 'new' as const, key: 'new' }]),
    ...saved.filter(screen => !needle || screen.name.toLowerCase().includes(needle)).map(screen => ({ kind: 'saved' as const, key: `saved:${screen.screen_id}`, screen })),
    ...starters.filter(starter => !needle || `${starter.name} ${starter.description}`.toLowerCase().includes(needle)).map(starter => ({ kind: 'starter' as const, key: `starter:${starter.id}`, starter })),
  ], [needle, saved, starters])
  const activeIndex = Math.min(active, Math.max(0, entries.length - 1))
  const choose = (entry: Entry | undefined) => {
    if (!entry) return
    if (entry.kind === 'new') onNew()
    if (entry.kind === 'saved') onOpenSaved(entry.screen.name)
    if (entry.kind === 'starter') onOpenStarter(entry.starter)
    onClose()
  }
  const onKeyDown = (event: KeyboardEvent<HTMLInputElement>) => {
    const keys: Record<string, () => void> = {
      ArrowDown: () => setActive((activeIndex + 1) % Math.max(1, entries.length)),
      ArrowUp: () => setActive((activeIndex - 1 + entries.length) % Math.max(1, entries.length)),
      Enter: () => choose(entries[activeIndex]),
      Escape: () => onClose(),
    }
    const action = keys[event.key]
    if (!action) return
    event.preventDefault()
    event.stopPropagation()
    action()
  }
  const row = (entry: Entry, index: number) => {
    const props = { id: `screen-entry-${index}`, role: 'option' as const, 'aria-selected': index === activeIndex, className: index === activeIndex ? 'screens-entry active' : 'screens-entry', onMouseEnter: () => setActive(index), onMouseDown: (event: React.MouseEvent) => event.preventDefault(), onClick: () => choose(entry) }
    if (entry.kind === 'new') return <li key={entry.key} {...props}><FilePlus2 aria-hidden="true" /><span><strong>New blank screen</strong><small>Start from no rules and the default columns</small></span></li>
    if (entry.kind === 'saved') return <li key={entry.key} {...props}>
      <span><strong>{entry.screen.name}{entry.screen.name === current ? ' · open' : ''}</strong><small>{entry.screen.rule_count} {entry.screen.rule_count === 1 ? 'rule' : 'rules'}{entry.screen.criteria_match === 'any' ? ' (any)' : ''} · {entry.screen.column_count} columns{entry.screen.screening_date ? ` · as of ${entry.screen.screening_date}` : ''} · {updated(entry.screen.updated_at)}</small></span>
      <button type="button" className="icon-button" aria-label={`Delete saved screen ${entry.screen.name}`} title="Delete this saved screen" onClick={event => { event.stopPropagation(); onDelete(entry.screen.name) }}><Trash2 /></button>
    </li>
    return <li key={entry.key} {...props}><Sparkles aria-hidden="true" /><span><strong>{entry.starter.name}</strong><small>{entry.starter.description}</small></span></li>
  }
  const savedEntries = entries.filter(entry => entry.kind === 'saved')
  const starterEntries = entries.filter(entry => entry.kind === 'starter')
  return <div ref={panel} className="screens-menu" role="dialog" aria-label="Open a screen">
    <label className="metric-picker__search">
      <Search aria-hidden="true" />
      <input autoFocus role="combobox" aria-expanded="true" aria-controls="screens-menu-list" aria-activedescendant={entries.length ? `screen-entry-${activeIndex}` : undefined} aria-label="Find a screen" placeholder="Find a saved or starter screen" value={query} onChange={event => { setQuery(event.target.value); setActive(0) }} onKeyDown={onKeyDown} />
    </label>
    <ul id="screens-menu-list" role="listbox" aria-label="Screens" className="screens-menu__list">
      {entries.filter(entry => entry.kind === 'new').map(entry => row(entry, entries.indexOf(entry)))}
      <li role="presentation" className="screens-menu__heading">Your screens {canSave ? '' : '(sign in to save screens)'}</li>
      {savedEntries.map(entry => row(entry, entries.indexOf(entry)))}
      {!savedEntries.length && <li role="presentation" className="screens-menu__none">{needle ? 'No saved screen matches.' : 'Nothing saved yet. Name a screen and press Ctrl+S.'}</li>}
      <li role="presentation" className="screens-menu__heading">Starter screens</li>
      {starterEntries.map(entry => row(entry, entries.indexOf(entry)))}
    </ul>
  </div>
}
