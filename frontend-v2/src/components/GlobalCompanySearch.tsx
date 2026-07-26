import { Search } from 'lucide-react'
import { useState } from 'react'
import { useNavigate } from 'react-router-dom'

import { searchCompanies, useCompanySearch } from './CompanyPicker'
import type { SecuritySearchResult } from '../api/types'

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
  const [query, setQuery] = useState('')
  const [open, setOpen] = useState(false)
  const search = useCompanySearch(query)
  const choose = (company: SecuritySearchResult) => {
    setQuery(company.ticker || company.company_name)
    setOpen(false)
    navigate(companyPath(company))
  }
  const submit = async (event: React.FormEvent) => {
    event.preventDefault()
    const value = query.trim()
    if (!value) return
    const current = search.data?.results ?? []
    const results = current.length ? current : (await searchCompanies(value)).results
    if (results[0]) choose(results[0])
  }
  const results = search.data?.results ?? []
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
          aria-label="Search companies"
          value={query}
          onChange={event => { setQuery(event.target.value); setOpen(true) }}
          placeholder="Search companies"
          autoComplete="off"
          aria-expanded={open && results.length > 0}
          aria-controls="global-company-results"
        />
        <kbd>Enter</kbd>
      </form>
      {open && query.trim().length >= 2 && (
        <div id="global-company-results" className="global-search-results" role="listbox">
          {search.isLoading && <span className="global-search-status">Searching…</span>}
          {!search.isLoading && results.length === 0 && <span className="global-search-status">No companies found</span>}
          {results.map(company => (
            <button
              type="button"
              role="option"
              key={`${company.company_code ?? 'ticker'}-${company.ticker}-${company.company_name}`}
              onMouseDown={event => event.preventDefault()}
              onClick={() => choose(company)}
            >
              <strong>{company.company_name}</strong>
              <small>{companyMeta(company)}</small>
            </button>
          ))}
        </div>
      )}
    </div>
  )
}
