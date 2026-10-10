import { useQuery } from '@tanstack/react-query'
import { Search, Tag, X } from 'lucide-react'
import { useDeferredValue, useState, type Ref } from 'react'

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

/** A tag listed beside the companies: choosing it stands for its members. */
export interface PickerTag {
  name: string
  /** Shown under the name, such as how many companies the tag holds. */
  detail?: string
  disabled?: boolean
}

/**
 * The tags whose name contains the typed text (every tag while nothing is
 * typed): those starting with it first, and those that cannot be chosen last.
 */
function matchTags(tags: PickerTag[], query: string) {
  const text = query.trim().toLowerCase()
  const rank = (tag: PickerTag) => (tag.disabled ? 2 : 0) + (tag.name.toLowerCase().startsWith(text) ? 0 : 1)
  return tags.filter(tag => tag.name.toLowerCase().includes(text)).sort((a, b) => rank(a) - rank(b))
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
  /** Lets a page focus the finder from a keyboard shortcut. */
  inputRef?: Ref<HTMLInputElement>
  /** Tags to find by name as well; they are listed before the companies, and as soon as the finder has focus. */
  tags?: PickerTag[]
  onSelectTag?: (name: string) => void
}

const NO_TAGS: PickerTag[] = []

export function CompanyPicker({
  selected,
  onSelect,
  label = 'Company',
  placeholder = 'Search by name, ticker, code, industry…',
  clearOnSelect = false,
  requireCompanyCode = true,
  disabled = false,
  inputRef,
  tags = NO_TAGS,
  onSelectTag,
}: CompanyPickerProps) {
  const [typedQuery, setTypedQuery] = useState('')
  const [open, setOpen] = useState(false)
  const [active, setActive] = useState(0)
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

  const chooseTag = (tag: PickerTag) => {
    if (tag.disabled) return
    onSelectTag?.(tag.name)
    setOpen(false)
    setTypedQuery('')
  }

  const clear = () => {
    onSelect(null)
    setTypedQuery('')
    setOpen(false)
  }

  const searching = query.trim().length >= 2
  const results = searching ? search.data?.results ?? [] : []
  const tagMatches = matchTags(tags, query)
  const showing = open && (searching || tagMatches.length > 0)
  // One cursor runs through the tags, then the companies.
  const choosableTags = tagMatches.filter(tag => !tag.disabled)
  const choosable = results.filter(company => !requireCompanyCode || company.company_code)
  const count = choosableTags.length + choosable.length
  const activeIndex = Math.min(active, Math.max(0, count - 1))
  const activeTag = choosableTags[activeIndex]
  const activeCompany = choosable[activeIndex - choosableTags.length]
  const onKeyDown = (event: React.KeyboardEvent<HTMLInputElement>) => {
    if (event.key === 'Escape') {
      if (showing) setOpen(false)
      else event.currentTarget.blur()
      return
    }
    if (event.key === 'Enter') {
      if (showing && (activeTag || activeCompany)) {
        event.preventDefault()
        if (activeTag) chooseTag(activeTag)
        else choose(activeCompany)
      }
      return
    }
    if (event.key !== 'ArrowDown' && event.key !== 'ArrowUp') return
    event.preventDefault()
    setOpen(true)
    if (count) setActive((activeIndex + (event.key === 'ArrowDown' ? 1 : -1) + count) % count)
  }
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
          ref={inputRef}
          className="input"
          role="combobox"
          aria-label={label}
          aria-expanded={showing}
          aria-autocomplete="list"
          value={query}
          disabled={disabled}
          placeholder={placeholder}
          autoComplete="off"
          onChange={event => {
            setTypedQuery(event.target.value)
            if (selected) onSelect(null)
            setOpen(true)
            setActive(0)
          }}
          onClick={() => setOpen(true)}
          onKeyDown={onKeyDown}
        />
        {selected && <button type="button" className="icon-button" aria-label={`Clear ${label}`} onClick={clear}><X /></button>}
      </div>
      {showing && (
        <div className="company-picker-results" role="listbox">
          {tagMatches.map(tag => (
            <button
              type="button"
              role="option"
              key={`tag-${tag.name}`}
              aria-selected={activeTag === tag}
              className={activeTag === tag ? 'company-picker-tag active' : 'company-picker-tag'}
              tabIndex={-1}
              onMouseEnter={() => { const index = choosableTags.indexOf(tag); if (index !== -1) setActive(index) }}
              disabled={tag.disabled}
              onMouseDown={event => event.preventDefault()}
              onClick={() => chooseTag(tag)}
            >
              <strong><Tag aria-hidden="true" />{tag.name}</strong>
              {tag.detail && <small>{tag.detail}</small>}
            </button>
          ))}
          {searching && search.isLoading && <span className="company-picker-status">Searching…</span>}
          {searching && !search.isLoading && results.length === 0 && tagMatches.length === 0 && <span className="company-picker-status">No companies found</span>}
          {results.map(company => (
            <button
              type="button"
              role="option"
              key={`${company.company_code ?? 'ticker'}-${company.ticker}-${company.company_name}`}
              aria-selected={activeCompany === company}
              className={activeCompany === company ? 'active' : undefined}
              tabIndex={-1}
              onMouseEnter={() => { const index = choosable.indexOf(company); if (index !== -1) setActive(choosableTags.length + index) }}
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
