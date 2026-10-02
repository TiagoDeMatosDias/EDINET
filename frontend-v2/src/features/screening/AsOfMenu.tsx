import { CalendarDays, Check, History, Search } from 'lucide-react'
import { useEffect, useRef, useState, type KeyboardEvent } from 'react'

import { formatAsOf, parseAsOf, presetDates, readRecentDates, rememberDate, todayIso, type DateChoice } from './asOfDates'

type Entry = DateChoice & { key: string; kind: 'typed' | 'recent' | 'preset' }

/**
 * Chooses the date the screen runs at. Type a date or a shorthand (2023-06-30,
 * 2023-06, 2023, 18m, 5y) or pick a recent or preset date with ↑/↓; Enter
 * applies it and the screen runs. Esc closes.
 */
export function AsOfMenu({ value, onChoose, onClose }: { value: string; onChoose: (value: string) => void; onClose: () => void }) {
  const [query, setQuery] = useState('')
  const [active, setActive] = useState(0)
  const [presets] = useState(() => presetDates())
  const [recent] = useState(() => readRecentDates())
  const panel = useRef<HTMLDivElement>(null)
  useEffect(() => {
    const close = (event: MouseEvent) => {
      const target = event.target as HTMLElement
      if (!panel.current?.contains(target) && !target.closest('[data-asof-trigger]')) onClose()
    }
    document.addEventListener('mousedown', close)
    return () => document.removeEventListener('mousedown', close)
  }, [onClose])

  const parsed = parseAsOf(query)
  const needle = query.trim().toLowerCase()
  const entries: Entry[] = [
    ...(parsed && 'value' in parsed ? [{ key: 'typed', kind: 'typed' as const, value: parsed.value, label: parsed.value ? `Use ${formatAsOf(parsed.value)}` : 'Use the latest data', detail: parsed.value || undefined }] : []),
    ...(needle ? [] : recent.filter(date => !presets.some(preset => preset.value === date)).map(date => ({ key: `recent:${date}`, kind: 'recent' as const, value: date, label: formatAsOf(date), detail: 'Used recently' }))),
    ...presets.filter(preset => !needle || `${preset.label} ${preset.detail ?? ''}`.toLowerCase().includes(needle)).map(preset => ({ ...preset, key: `preset:${preset.label}`, kind: 'preset' as const })),
  ]
  const activeIndex = Math.min(active, Math.max(0, entries.length - 1))
  const choose = (entry: DateChoice | undefined) => {
    if (!entry) return
    rememberDate(entry.value)
    onChoose(entry.value)
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

  return <div ref={panel} className="screens-menu asof-menu" role="dialog" aria-label="Choose the as-of date">
    <label className="metric-picker__search">
      <Search aria-hidden="true" />
      <input autoFocus role="combobox" aria-expanded="true" aria-controls="asof-menu-list" aria-activedescendant={entries.length ? `asof-entry-${activeIndex}` : undefined} aria-label="As-of date" placeholder="2023-06-30, 2023, 18m, 5y…" value={query} onChange={event => { setQuery(event.target.value); setActive(0) }} onKeyDown={onKeyDown} />
    </label>
    {parsed && 'error' in parsed && <p className="asof-menu__error" role="status">{parsed.error}</p>}
    <ul id="asof-menu-list" role="listbox" aria-label="As-of dates" className="screens-menu__list">
      {entries.map((entry, index) => <li
        key={entry.key}
        id={`asof-entry-${index}`}
        role="option"
        aria-selected={index === activeIndex}
        className={index === activeIndex ? 'screens-entry active' : 'screens-entry'}
        onMouseEnter={() => setActive(index)}
        onMouseDown={event => event.preventDefault()}
        onClick={() => choose(entry)}
      >
        {entry.kind === 'recent' ? <History aria-hidden="true" /> : entry.value === value ? <Check aria-hidden="true" /> : <CalendarDays aria-hidden="true" />}
        <span><strong>{entry.label}{entry.value === value ? ' · current' : ''}</strong>{entry.detail && <small>{entry.detail}</small>}</span>
      </li>)}
    </ul>
    <footer className="asof-menu__foot">
      <span>Rules see only filings published by this date, and prices up to it.</span>
      <label>Calendar <input type="date" max={todayIso()} value={value} onChange={event => { if (event.target.value) choose({ value: event.target.value, label: '' }) }} /></label>
    </footer>
  </div>
}
