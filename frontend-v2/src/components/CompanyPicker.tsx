import { useQuery } from '@tanstack/react-query'
import { Search, X } from 'lucide-react'
import { useDeferredValue, useState } from 'react'

import { apiRequest, queryString } from '../api/client'
import type { SecuritySearchResult } from '../api/types'

export interface CompanySearchResponse {
  results: SecuritySearchResult[]
}

export function searchCompanies(query: string, limit = 8) {
  const value = query.trim()
  return apiRequest<CompanySearchResponse>(`/api/security/search${queryString({ q: value, limit })}`)
}

export function useCompanySearch(query: string, limit = 8) {
  const deferred = useDeferredValue(query.trim())
  return useQuery({
    queryKey: ['company-search', deferred, limit],
    enabled: deferred.length >= 2,
    queryFn: () => searchCompanies(deferred, limit),
  })
}

function companyMeta(company: SecuritySearchResult) {
  return [company.ticker, company.company_code, company.industry, company.market]
    .filter(Boolean)
    .join(' · ')
}

interface CompanyPickerProps {
  selected: SecuritySearchResult | null
  onSelect: (company: SecuritySearchResult | null) => void
  label?: string
  placeholder?: string
  clearOnSelect?: boolean
  requireCompanyCode?: boolean
  disabled?: boolean
}

export function CompanyPicker({
  selected,
  onSelect,
  label = 'Company',
  placeholder = 'Search by name, ticker, code, industry…',
  clearOnSelect = false,
  requireCompanyCode = true,
  disabled = false,
}: CompanyPickerProps) {
  const [typedQuery, setTypedQuery] = useState('')
  const [open, setOpen] = useState(false)
  const search = useCompanySearch(typedQuery)
  const selectedText = selected?.company_name || selected?.ticker || selected?.company_code || ''
  const query = selected && !clearOnSelect ? selectedText : typedQuery

  const choose = (company: SecuritySearchResult) => {
    if (requireCompanyCode && !company.company_code) return
    onSelect(company)
    setOpen(false)
    if (clearOnSelect) setTypedQuery('')
    else setTypedQuery(company.company_name || company.ticker || company.company_code || '')
  }

  const clear = () => {
    onSelect(null)
    setTypedQuery('')
    setOpen(false)
  }

  const results = search.data?.results ?? []
  return (
    <div
      className="company-picker"
      onFocus={() => setOpen(true)}
      onBlur={event => {
        if (!event.currentTarget.contains(event.relatedTarget as Node | null)) setOpen(false)
      }}
    >
      <span className="field-label">{label}</span>
      <div className="company-picker-input">
        <Search aria-hidden="true" />
        <input
          className="input"
          aria-label={label}
          value={query}
          disabled={disabled}
          placeholder={placeholder}
          autoComplete="off"
          onChange={event => {
            setTypedQuery(event.target.value)
            if (selected) onSelect(null)
            setOpen(true)
          }}
        />
        {selected && <button type="button" className="icon-button" aria-label={`Clear ${label}`} onClick={clear}><X /></button>}
      </div>
      {open && query.trim().length >= 2 && (
        <div className="company-picker-results" role="listbox">
          {search.isLoading && <span className="company-picker-status">Searching…</span>}
          {!search.isLoading && results.length === 0 && <span className="company-picker-status">No companies found</span>}
          {results.map(company => (
            <button
              type="button"
              role="option"
              key={`${company.company_code ?? 'ticker'}-${company.ticker}-${company.company_name}`}
              disabled={requireCompanyCode && !company.company_code}
              onMouseDown={event => event.preventDefault()}
              onClick={() => choose(company)}
            >
              <strong>{company.company_name || company.ticker || company.company_code}</strong>
              <small>{companyMeta(company)}</small>
            </button>
          ))}
        </div>
      )}
    </div>
  )
}
