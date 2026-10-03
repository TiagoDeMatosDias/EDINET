import { Check, Search, X } from 'lucide-react'
import { useEffect, useRef, useState, type KeyboardEvent } from 'react'

import { compactMoney } from './portfolioFormat'
import type { IncomeCompany } from './portfolioTypes'

/**
 * Chooses which paying companies the Income tab shows. Type to filter, ↑/↓ to
 * move, Enter to add or remove the highlighted company, Backspace in an empty
 * field to remove the last one, Esc to close. No selection means every payer.
 */
export function IncomeCompanyPicker({ companies, selected, currency, onChange }: {
  companies: IncomeCompany[]
  selected: string[]
  currency: string
  onChange: (symbols: string[]) => void
}) {
  const [query, setQuery] = useState('')
  const [open, setOpen] = useState(false)
  const [active, setActive] = useState(0)
  const root = useRef<HTMLDivElement>(null)
  const list = useRef<HTMLUListElement>(null)
  useEffect(() => {
    if (!open) return
    const close = (event: MouseEvent) => { if (!root.current?.contains(event.target as Node)) setOpen(false) }
    document.addEventListener('mousedown', close)
    return () => document.removeEventListener('mousedown', close)
  }, [open])
  const needle = query.trim().toLowerCase()
  const matches = companies.filter(company => !needle || `${company.symbol} ${company.name}`.toLowerCase().includes(needle))
  const activeIndex = Math.min(active, Math.max(0, matches.length - 1))
  useEffect(() => { list.current?.querySelector('[aria-selected="true"]')?.scrollIntoView?.({ block: 'nearest' }) }, [activeIndex, open])
  const chosen = new Set(selected)
  const toggle = (symbol: string) => onChange(chosen.has(symbol) ? selected.filter(item => item !== symbol) : [...selected, symbol])
  const byNet = [...companies].sort((left, right) => right.net - left.net)

  const onKeyDown = (event: KeyboardEvent<HTMLInputElement>) => {
    const keys: Record<string, () => void> = {
      ArrowDown: () => { setOpen(true); setActive((activeIndex + 1) % Math.max(1, matches.length)) },
      ArrowUp: () => { setOpen(true); setActive((activeIndex - 1 + matches.length) % Math.max(1, matches.length)) },
      Enter: () => { const company = matches[activeIndex]; if (company) { toggle(company.symbol); setQuery('') } },
      Escape: () => { if (open) setOpen(false); else if (query) setQuery(''); else event.currentTarget.blur() },
      Backspace: () => { if (!query && selected.length) onChange(selected.slice(0, -1)) },
    }
    const action = keys[event.key]
    if (!action || (event.key === 'Backspace' && query)) return
    event.preventDefault()
    event.stopPropagation()
    action()
  }

  return <div className="pf-picker" ref={root}>
    <div className="pf-picker__field" onClick={() => root.current?.querySelector('input')?.focus()}>
      <Search aria-hidden="true" />
      {selected.map(symbol => <span key={symbol} className="pf-chip">{symbol}<button type="button" aria-label={`Remove ${symbol}`} onClick={event => { event.stopPropagation(); toggle(symbol) }}><X /></button></span>)}
      <input
        data-income-find
        role="combobox"
        aria-expanded={open}
        aria-controls="income-company-list"
        aria-activedescendant={open && matches.length ? `income-company-${activeIndex}` : undefined}
        aria-label="Filter companies"
        placeholder={selected.length ? 'Add a company…' : 'All payers — type to choose companies'}
        value={query}
        onFocus={() => setOpen(true)}
        onChange={event => { setQuery(event.target.value); setActive(0); setOpen(true) }}
        onKeyDown={onKeyDown}
      />
      <kbd aria-hidden="true">F</kbd>
    </div>
    <div className="pf-picker__quick" role="group" aria-label="Quick selections">
      <button type="button" className="period-tab" onClick={() => onChange(byNet.filter(company => company.is_held).map(company => company.symbol))}>Held now</button>
      <button type="button" className="period-tab" onClick={() => onChange(byNet.slice(0, 5).map(company => company.symbol))}>Top 5</button>
      <button type="button" className={`period-tab${selected.length ? '' : ' active'}`} onClick={() => onChange([])} title="Show every payer (X)">All payers</button>
    </div>
    {open && <ul id="income-company-list" ref={list} role="listbox" aria-label="Paying companies" aria-multiselectable="true" className="pf-picker__list">
      {matches.length ? matches.map((company, index) => <li
        key={company.symbol}
        id={`income-company-${index}`}
        role="option"
        aria-selected={index === activeIndex}
        aria-checked={chosen.has(company.symbol)}
        className={index === activeIndex ? 'is-active' : undefined}
        onMouseEnter={() => setActive(index)}
        onMouseDown={event => event.preventDefault()}
        onClick={() => toggle(company.symbol)}
      >
        <span className="pf-picker__check" aria-hidden="true">{chosen.has(company.symbol) && <Check />}</span>
        <span className="pf-picker__name"><strong>{company.symbol}</strong><small>{company.name}{company.is_held ? '' : ' · sold'}</small></span>
        <span className="pf-picker__value">{compactMoney(company.net, currency)}</span>
      </li>) : <li className="pf-picker__empty">No paying company matches “{query}”.</li>}
    </ul>}
  </div>
}
