import { Search } from 'lucide-react'
import { useRef, useState } from 'react'
import { useNavigate } from 'react-router-dom'

import { searchCompanies, useCompanySearch } from './CompanyPicker'
import type { SecuritySearchResult } from '../api/types'
import { useHotkeys } from '../hooks/useHotkeys'

function companyMeta(company: SecuritySearchResult) {
  return [company.ticker, company.company_code, company.industry, company.market]
    .filter(Boolean)
    .join(' · ')
}

function companyPath(company: SecuritySearchResult) {
  if (company.company_code) return `/analyze/${encodeURIComponent(company.company_code)}`
  return `/analyze?ticker=${encodeURIComponent(company.ticker)}`
}

export function GlobalCompanySearch() {
  const navigate = useNavigate()
  const input = useRef<HTMLInputElement>(null)
  const [query, setQuery] = useState('')
  const [open, setOpen] = useState(false)
  const [active, setActive] = useState(0)
  const search = useCompanySearch(query)
  // "/" jumps to company search from anywhere in the workspace.
  useHotkeys({ '/': () => { input.current?.focus(); input.current?.select() } })
  const choose = (company: SecuritySearchResult) => {
    setQuery(company.ticker || company.company_name)
    setOpen(false)
    input.current?.blur()
    navigate(companyPath(company))
  }
  const results = search.data?.results ?? []
  const activeIndex = Math.min(active, Math.max(0, results.length - 1))
  const submit = async (event: React.FormEvent) => {
    event.preventDefault()
    const value = query.trim()
    if (!value) return
    const current = search.data?.results ?? []
    const found = current.length ? current : (await searchCompanies(value)).results
    const chosen = found[current.length ? activeIndex : 0]
    if (chosen) choose(chosen)
  }
  const onKeyDown = (event: React.KeyboardEvent<HTMLInputElement>) => {
    if (event.key === 'Escape') {
      if (open && query) setOpen(false)
      else input.current?.blur()
      return
    }
    if (event.key !== 'ArrowDown' && event.key !== 'ArrowUp') return
    event.preventDefault()
    setOpen(true)
    if (!results.length) return
    const step = event.key === 'ArrowDown' ? 1 : -1
    setActive((activeIndex + step + results.length) % results.length)
  }
  const showResults = open && query.trim().length >= 2
  return (
    <div
      className="global-search-wrap"
      onFocus={() => setOpen(true)}
      onBlur={event => {
        if (!event.currentTarget.contains(event.relatedTarget as Node | null)) setOpen(false)
      }}
    >
      <form className="global-search" role="search" onSubmit={event => void submit(event)}>
        <Search aria-hidden="true" />
        <input
          ref={input}
          role="combobox"
          aria-label="Search companies"
          aria-autocomplete="list"
          value={query}
          onChange={event => { setQuery(event.target.value); setOpen(true); setActive(0) }}
          onKeyDown={onKeyDown}
          placeholder="Search companies by name, ticker, or EDINET code"
          autoComplete="off"
          aria-expanded={showResults && results.length > 0}
          aria-controls="global-company-results"
          aria-activedescendant={showResults && results.length ? `global-company-result-${activeIndex}` : undefined}
        />
        <kbd title="Press / to search from anywhere">/</kbd>
      </form>
      {showResults && (
        <div id="global-company-results" className="global-search-results" role="listbox">
          {search.isLoading && <span className="global-search-status">Searching…</span>}
          {!search.isLoading && results.length === 0 && <span className="global-search-status">No companies found</span>}
          {results.map((company, index) => (
            <button
              type="button"
              role="option"
              id={`global-company-result-${index}`}
              aria-selected={index === activeIndex}
              className={index === activeIndex ? 'active' : undefined}
              key={`${company.company_code ?? 'ticker'}-${company.ticker}-${company.company_name}`}
              tabIndex={-1}
              onMouseDown={event => event.preventDefault()}
              onMouseEnter={() => setActive(index)}
              onClick={() => choose(company)}
            >
              <strong>{company.company_name}</strong>
              <small>{companyMeta(company)}</small>
            </button>
          ))}
          {results.length > 0 && <span className="global-search-hint"><kbd>↑</kbd><kbd>↓</kbd> choose · <kbd>Enter</kbd> open · <kbd>Esc</kbd> close</span>}
        </div>
      )}
    </div>
  )
}
