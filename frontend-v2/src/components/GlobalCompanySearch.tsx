import { useQuery } from '@tanstack/react-query'
import { Search } from 'lucide-react'
import { useDeferredValue, useState } from 'react'
import { useNavigate } from 'react-router-dom'

import { apiRequest, queryString } from '../api/client'
import type { SecuritySearchResult } from '../api/types'

function companyMeta(company: SecuritySearchResult) {
  return [company.ticker, company.industry].filter(Boolean).join(' · ')
}

export function GlobalCompanySearch() {
  const navigate = useNavigate()
  const [query, setQuery] = useState('')
  const [open, setOpen] = useState(false)
  const deferred = useDeferredValue(query.trim())
  const search = useQuery({ queryKey: ['global-company-search', deferred], enabled: deferred.length >= 2, queryFn: () => apiRequest<{ results: SecuritySearchResult[] }>(`/api/security/search${queryString({ q: deferred, limit: 8 })}`) })
  const choose = (company: SecuritySearchResult) => { setQuery(company.ticker || company.company_name); setOpen(false); navigate(`/analyze/${encodeURIComponent(company.company_code)}`) }
  const submit = async (event: React.FormEvent) => {
    event.preventDefault()
    const value = query.trim()
    if (!value) return
    const current = search.data?.results ?? []
    const results = current.length ? current : (await apiRequest<{ results: SecuritySearchResult[] }>(`/api/security/search${queryString({ q: value, limit: 8 })}`)).results
    if (results[0]) choose(results[0])
  }
  const results = search.data?.results ?? []
  return <div className="global-search-wrap" onFocus={() => setOpen(true)} onBlur={event => { if (!event.currentTarget.contains(event.relatedTarget as Node | null)) setOpen(false) }}><form className="global-search" role="search" onSubmit={event => void submit(event)}><Search aria-hidden="true" /><input aria-label="Search companies" value={query} onChange={event => { setQuery(event.target.value); setOpen(true) }} placeholder="Search companies" autoComplete="off" aria-expanded={open && results.length > 0} aria-controls="global-company-results" /><kbd>Enter</kbd></form>{open && deferred.length >= 2 && <div id="global-company-results" className="global-search-results" role="listbox">{search.isLoading ? <span className="global-search-status">Searching…</span> : results.length ? results.map(company => <button type="button" role="option" key={company.company_code} onMouseDown={event => event.preventDefault()} onClick={() => choose(company)}><strong>{company.company_name}</strong><small>{companyMeta(company)}</small></button>) : <span className="global-search-status">No companies found</span>}</div>}</div>
}
