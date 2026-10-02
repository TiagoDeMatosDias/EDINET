import { ChevronDown, Search } from 'lucide-react'
import { useEffect, useId, useMemo, useRef, useState, type KeyboardEvent } from 'react'

import { searchMetrics, type MetricOption } from './metricCatalog'
import { useAnchoredPopover } from './useAnchoredPopover'

/**
 * A metric chooser that searches every table at once. Closed, it is a button
 * showing the chosen metric; typing on it (or Enter, Space, ↓) opens a search
 * list driven by ↑/↓, Enter, and Escape.
 */
export function MetricPicker({ options, value, onSelect, label, placeholder = 'Choose a metric', compact = false, autoOpen = false }: {
  options: MetricOption[]
  value?: MetricOption
  onSelect: (option: MetricOption) => void
  label: string
  placeholder?: string
  compact?: boolean
  autoOpen?: boolean
}) {
  const [open, setOpen] = useState(autoOpen)
  const [query, setQuery] = useState('')
  const [active, setActive] = useState(0)
  const wrapper = useRef<HTMLDivElement>(null)
  const button = useRef<HTMLButtonElement>(null)
  const list = useRef<HTMLUListElement>(null)
  const popover = useRef<HTMLDivElement>(null)
  const listId = useId()
  const results = useMemo(() => open ? searchMetrics(options, query) : [], [open, options, query])
  const activeIndex = Math.min(active, Math.max(0, results.length - 1))

  useEffect(() => {
    if (!open) return
    const onPointerDown = (event: MouseEvent) => {
      if (!wrapper.current?.contains(event.target as Node)) setOpen(false)
    }
    document.addEventListener('mousedown', onPointerDown)
    return () => document.removeEventListener('mousedown', onPointerDown)
  }, [open])
  useAnchoredPopover(button, popover, open)
  useEffect(() => {
    list.current?.querySelector<HTMLElement>('[aria-selected="true"]')?.scrollIntoView?.({ block: 'nearest' })
  }, [activeIndex, open])

  const openWith = (text = '') => { setQuery(text); setActive(0); setOpen(true) }
  const close = (refocus = true) => { setOpen(false); if (refocus) button.current?.focus() }
  const choose = (option: MetricOption | undefined) => {
    if (!option) return
    onSelect(option)
    close()
  }
  const onButtonKeyDown = (event: KeyboardEvent<HTMLButtonElement>) => {
    if (event.ctrlKey || event.metaKey || event.altKey) return
    if (event.key === 'ArrowDown' || event.key === 'Enter' || event.key === ' ') { event.preventDefault(); openWith() }
    else if (event.key.length === 1 && /\S/.test(event.key)) { event.preventDefault(); openWith(event.key) }
  }
  const onInputKeyDown = (event: KeyboardEvent<HTMLInputElement>) => {
    const keys: Record<string, () => void> = {
      ArrowDown: () => setActive((activeIndex + 1) % Math.max(1, results.length)),
      ArrowUp: () => setActive((activeIndex - 1 + results.length) % Math.max(1, results.length)),
      PageDown: () => setActive(Math.min(results.length - 1, activeIndex + 8)),
      PageUp: () => setActive(Math.max(0, activeIndex - 8)),
      Enter: () => choose(results[activeIndex]),
      Escape: () => close(),
    }
    if (event.key === 'Tab') { close(false); return }
    const action = keys[event.key]
    if (!action) return
    event.preventDefault()
    event.stopPropagation()
    action()
  }

  return <div ref={wrapper} className={compact ? 'metric-picker metric-picker--compact' : 'metric-picker'}>
    <button
      ref={button}
      type="button"
      className={value ? 'metric-picker__button' : 'metric-picker__button is-empty'}
      aria-haspopup="listbox"
      aria-expanded={open}
      aria-label={`${label}: ${value ? `${value.label} (${value.tableLabel})` : 'not chosen'}`}
      title={value ? [value.label, value.tableLabel, value.description].filter(Boolean).join(' · ') : 'Type to search metrics'}
      onClick={() => open ? close(false) : openWith()}
      onKeyDown={onButtonKeyDown}
    >
      <span className="metric-picker__value">{value?.label ?? placeholder}</span>
      {value && !compact && <small>{value.tableLabel}{value.format === 'percent' ? ' · %' : ''}</small>}
      <ChevronDown aria-hidden="true" />
    </button>
    {open && <div ref={popover} className="metric-picker__popover">
      <label className="metric-picker__search">
        <Search aria-hidden="true" />
        <input
          autoFocus
          role="combobox"
          aria-label={`Search ${label.toLowerCase()}`}
          aria-expanded="true"
          aria-controls={listId}
          aria-activedescendant={results.length ? `${listId}-${activeIndex}` : undefined}
          placeholder={`Search ${options.length.toLocaleString()} metrics: roe, net sales, p/e…`}
          value={query}
          onChange={event => { setQuery(event.target.value); setActive(0) }}
          onKeyDown={onInputKeyDown}
        />
      </label>
      {!query.trim() && <p className="metric-picker__hint">Popular metrics. Type to search every table.</p>}
      <ul ref={list} id={listId} role="listbox" aria-label={label} className="metric-picker__list">
        {results.map((option, index) => <li
          key={option.key}
          id={`${listId}-${index}`}
          role="option"
          aria-selected={index === activeIndex}
          className={[index === activeIndex ? 'active' : '', option.key === value?.key ? 'chosen' : ''].filter(Boolean).join(' ') || undefined}
          onMouseDown={event => event.preventDefault()}
          onMouseEnter={() => setActive(index)}
          onClick={() => choose(option)}
          title={option.description}
        >
          <span>{option.label}</span>
          <small>{option.tableLabel}{option.format === 'percent' ? ' · %' : ''}</small>
        </li>)}
        {!results.length && <li className="metric-picker__none" role="presentation">No metric matches “{query}”.</li>}
      </ul>
    </div>}
  </div>
}
