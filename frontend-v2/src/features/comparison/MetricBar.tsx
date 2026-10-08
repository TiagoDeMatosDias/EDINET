import { Check, Search, X } from 'lucide-react'
import { useEffect, useId, useMemo, useRef, useState, type KeyboardEvent, type Ref } from 'react'

import { groupMetrics, metricDefinition, type MetricDefinition } from '../../metrics'
import { buildMetricOptions, isTextMetric, searchMetrics, type MetricOption } from '../screening/metricCatalog'
import { orderMetrics } from './comparisonModel'
import { HotkeyKbd } from '../../hotkeys/HotkeyKbd'
import { comparisonScope } from './comparisonHotkeys'

// Tags and split events are not numbers a comparison can line up.
const HIDDEN_TABLES = ['Company_Tags', 'Stock_Splits']

function MetricSearch({ options, standardOptions, selected, inputRef, onChoose }: {
  options: MetricOption[]
  standardOptions: MetricOption[]
  selected: Set<string>
  inputRef: Ref<HTMLInputElement>
  onChoose: (metric: string) => void
}) {
  const [query, setQuery] = useState('')
  const [open, setOpen] = useState(false)
  const [active, setActive] = useState(0)
  const listId = useId()
  const list = useRef<HTMLUListElement>(null)
  const results = useMemo(() => !open ? [] : query.trim() ? searchMetrics([...standardOptions, ...options], query, 40) : standardOptions, [open, options, query, standardOptions])
  const activeIndex = Math.min(active, Math.max(0, results.length - 1))
  useEffect(() => { list.current?.querySelector<HTMLElement>('[aria-selected="true"]')?.scrollIntoView?.({ block: 'nearest' }) }, [activeIndex, open])
  const choose = (option?: MetricOption) => {
    if (!option) return
    onChoose(option.key)
    setQuery('')
    setActive(0)
  }
  const onKeyDown = (event: KeyboardEvent<HTMLInputElement>) => {
    const keys: Record<string, () => void> = {
      ArrowDown: () => { setOpen(true); setActive((activeIndex + 1) % Math.max(1, results.length)) },
      ArrowUp: () => setActive((activeIndex - 1 + results.length) % Math.max(1, results.length)),
      PageDown: () => setActive(Math.min(results.length - 1, activeIndex + 8)),
      PageUp: () => setActive(Math.max(0, activeIndex - 8)),
      Enter: () => choose(results[activeIndex]),
      Escape: () => { if (open && query) { setQuery(''); return } setOpen(false); event.currentTarget.blur() },
    }
    const action = keys[event.key]
    if (!action) return
    event.preventDefault()
    action()
  }
  return <div className="cmp-metric-search" onBlur={event => { if (!event.currentTarget.contains(event.relatedTarget as Node | null)) setOpen(false) }}>
    <Search aria-hidden="true" />
    <input
      ref={inputRef}
      className="input"
      role="combobox"
      aria-label="Add a metric"
      aria-expanded={open}
      aria-controls={listId}
      aria-activedescendant={open && results.length ? `${listId}-${activeIndex}` : undefined}
      autoComplete="off"
      placeholder={`Add a metric: search ${(options.length + standardOptions.length).toLocaleString()}…`}
      value={query}
      onFocus={() => setOpen(true)}
      onChange={event => { setQuery(event.target.value); setActive(0); setOpen(true) }}
      onKeyDown={onKeyDown}
    />
    <span aria-hidden="true"><HotkeyKbd hotkey={comparisonScope.byId['add-metric']} /></span>
    {open && <ul ref={list} id={listId} role="listbox" aria-label="Metrics" className="cmp-metric-search__list">
      {results.map((option, index) => <li
        key={option.key}
        id={`${listId}-${index}`}
        role="option"
        aria-selected={index === activeIndex}
        className={index === activeIndex ? 'active' : undefined}
        title={option.description}
        onMouseDown={event => event.preventDefault()}
        onMouseEnter={() => setActive(index)}
        onClick={() => choose(option)}
      >
        <span>{selected.has(option.key) && <Check aria-label="shown" />}{option.label}</span>
        <small>{option.tableLabel}</small>
      </li>)}
      {!results.length && <li className="cmp-metric-search__none" role="presentation">No metric matches “{query}”.</li>}
    </ul>}
  </div>
}

/**
 * Every standard metric as a toggle in its group, the added statement columns
 * after them, and a search over every numeric column (M).
 */
export function MetricBar({ standard, definitions, selected, catalog, searchRef, onChange, onFocusMetric }: {
  standard: string[]
  definitions: Record<string, MetricDefinition>
  selected: string[]
  catalog: Record<string, string[]>
  searchRef: Ref<HTMLInputElement>
  onChange: (metrics: string[]) => void
  onFocusMetric: (metric: string) => void
}) {
  const chosen = useMemo(() => new Set(selected), [selected])
  const standardOptions = useMemo<MetricOption[]>(() => standard.map(key => {
    const definition = metricDefinition(key, definitions)
    return { key, label: definition.label, tableLabel: definition.group, description: definition.description, order: -2, search: `${definition.label} ${key} ${definition.group} ${definition.description ?? ''}`.toLowerCase() }
  }), [definitions, standard])
  const columnOptions = useMemo(() => buildMetricOptions(catalog, {}, { includePresets: false, hideTables: HIDDEN_TABLES }).filter(option => !isTextMetric(option.table, option.column)), [catalog])
  const custom = selected.filter(metric => !standard.includes(metric))
  const set = (metrics: string[]) => onChange(orderMetrics(metrics, standard))
  const toggle = (metric: string) => set(chosen.has(metric) ? selected.filter(item => item !== metric) : [...selected, metric])
  const toggleGroup = (metrics: string[]) => {
    const allOn = metrics.every(metric => chosen.has(metric))
    set(allOn ? selected.filter(metric => !metrics.includes(metric)) : [...selected, ...metrics])
  }
  const isDefault = custom.length === 0 && standard.every(metric => chosen.has(metric))
  const shownStandard = standard.filter(metric => chosen.has(metric)).length

  return <section className="panel cmp-panel cmp-metrics" aria-labelledby="cmp-metrics-title">
    <header className="cmp-panel__header">
      <h2 id="cmp-metrics-title">Metrics <span className="cmp-count">{selected.length}</span></h2>
      <MetricSearch options={columnOptions} standardOptions={standardOptions} selected={chosen} inputRef={searchRef} onChoose={metric => { if (!chosen.has(metric)) set([...selected, metric]); onFocusMetric(metric) }} />
      <span className="cmp-panel__meta">{shownStandard} of {standard.length} standard{custom.length ? ` · ${custom.length} added` : ''}</span>
      <button type="button" className="button button--ghost button--small" disabled={isDefault} onClick={() => onChange(standard)}>Reset</button>
      <button type="button" className="button button--ghost button--small" disabled={!selected.length} onClick={() => onChange([])}>None</button>
    </header>
    <div className="cmp-metrics__groups">
      {groupMetrics(standard, definitions).map(({ group, metrics }) => <div className="cmp-metrics__group" role="group" aria-label={group} key={group}>
        <button type="button" className="cmp-metrics__group-name" onClick={() => toggleGroup(metrics)} title={`Show or hide every ${group.toLowerCase()} metric`}>{group}</button>
        {metrics.map(metric => {
          const definition = metricDefinition(metric, definitions)
          return <button key={metric} type="button" className="cmp-pill" aria-pressed={chosen.has(metric)} onClick={() => toggle(metric)} title={definition.description}>{definition.label}</button>
        })}
      </div>)}
      {custom.length > 0 && <div className="cmp-metrics__group" role="group" aria-label="Added columns">
        <span className="cmp-metrics__group-name">Added</span>
        {custom.map(metric => {
          const definition = metricDefinition(metric, definitions)
          return <span key={metric} className="cmp-pill cmp-pill--added" title={`${definition.group} · ${metric.split('.').slice(1).join('.')}`}>
            <button type="button" onClick={() => onFocusMetric(metric)}>{definition.label} <small>{definition.group}</small></button>
            <button type="button" className="cmp-pill__remove" onClick={() => toggle(metric)} aria-label={`Remove metric ${definition.label}`}><X /></button>
          </span>
        })}
      </div>}
    </div>
  </section>
}
