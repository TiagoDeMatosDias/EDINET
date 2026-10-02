import { useInfiniteQuery, useQuery } from '@tanstack/react-query'
import { ArrowRight, Building2, Download, Keyboard, X } from 'lucide-react'
import { useCallback, useEffect, useRef, useState } from 'react'
import { Link, useNavigate, useSearchParams } from 'react-router-dom'

import { apiRequest, queryString } from '../../api/client'
import type { SecuritySearchResult } from '../../api/types'
import { CompanyPicker } from '../../components/CompanyPicker'
import { EmptyState, ErrorState, LoadingState } from '../../components/Feedback'
import { ShortcutsDialog, type ShortcutGroup } from '../../components/ShortcutsDialog'
import { Tip } from '../../components/Tooltip'
import { useHotkeys } from '../../hooks/useHotkeys'
import { exportCompanyFilings } from './exportFilings'
import { companyName, formatDay, FORM_CATEGORIES, formCategory, type FormCategoryKey } from './filingFormat'
import { FilingsTable, type FilingRow } from './FilingsTable'
import './filings.css'

interface FilingCoverage {
  summary: {
    unique_filings: number
    unique_companies: number
    filings_with_issues: number
    first_submitted?: string | null
    last_submitted?: string | null
  }
  forms: Array<{ form_code: string | null; filings: number; companies: number }>
}

interface RecentWorkItem { work_id: string; kind: string; title: string; subtitle?: string | null; href: string; occurred_at: string }

type CategoryKey = FormCategoryKey | 'all'
const CATEGORY_KEYS: CategoryKey[] = ['all', ...FORM_CATEGORIES.map(category => category.key)]
const PAGE_SIZE = 50

const SHORTCUTS: ShortcutGroup[] = [
  { title: 'Anywhere', shortcuts: [
    { keys: ['/'], label: 'Search companies (opens Analysis)' },
    { keys: ['?'], label: 'Show or hide this list' },
  ] },
  { title: 'Filing Explorer', shortcuts: [
    { keys: ['F'], label: "Find a company's filings" },
    { keys: ['[', ']'], label: 'Previous or next report type' },
    { keys: ['J'], label: 'Jump into the filing list' },
    { keys: ['↑', '↓'], label: 'Move between filings (also J, K)' },
    { keys: ['Enter'], label: 'Open the focused filing' },
    { keys: ['X'], label: 'Clear the company' },
    { keys: ['A'], label: "Open the company's analysis" },
  ] },
]

function categoryCodes(key: CategoryKey) {
  return key === 'all' ? [] : [...(FORM_CATEGORIES.find(category => category.key === key)?.codes ?? [])]
}

function CategoryTabs({ active, counts, onChange }: { active: CategoryKey; counts: Record<string, number>; onChange: (key: CategoryKey) => void }) {
  const labels: Record<CategoryKey, string> = { all: 'All reports', ...Object.fromEntries(FORM_CATEGORIES.map(category => [category.key, category.label])) } as Record<CategoryKey, string>
  return <div className="filing-types" role="tablist" aria-label="Report type">
    {CATEGORY_KEYS.map(key => {
      const count = counts[key] ?? 0
      return <button key={key} type="button" role="tab" aria-selected={active === key} className={active === key ? 'filing-type active' : 'filing-type'} disabled={key !== 'all' && !count} onClick={() => onChange(key)}>
        {labels[key]}<small>{count.toLocaleString()}</small>
      </button>
    })}
    <span className="filing-types__keys" aria-hidden="true"><kbd>[</kbd><kbd>]</kbd></span>
  </div>
}

function RecentlyOpened() {
  const recent = useQuery({ queryKey: ['recent-work'], queryFn: () => apiRequest<{ items: RecentWorkItem[] }>('/api/research/recent-work?limit=50'), retry: false })
  const items = (recent.data?.items ?? []).filter(item => item.kind === 'filing').slice(0, 8)
  if (!items.length) return null
  return <nav className="filings-recent" aria-labelledby="recently-opened-title">
    <h3 id="recently-opened-title">Recently opened</h3>
    <ul>{items.map(item => <li key={item.work_id}><Link to={item.href} title={item.subtitle ?? undefined}><strong>{item.title}</strong><small>{item.subtitle?.split(' · ')[0]}</small></Link></li>)}</ul>
  </nav>
}

export default function FilingsPage() {
  const [searchParams, setSearchParams] = useSearchParams()
  const navigate = useNavigate()
  const companyCode = (searchParams.get('company') ?? '').trim()
  const requestedType = searchParams.get('type') as CategoryKey | null
  const category: CategoryKey = requestedType && CATEGORY_KEYS.includes(requestedType) ? requestedType : 'all'
  const finder = useRef<HTMLInputElement>(null)
  const [picked, setPicked] = useState<SecuritySearchResult | null>(null)
  const [exporting, setExporting] = useState(false)
  const [exportError, setExportError] = useState('')
  const [showShortcuts, setShowShortcuts] = useState(false)
  const closeShortcuts = useCallback(() => setShowShortcuts(false), [])

  // Older links carried the document as ?doc=; send them to the viewer.
  const legacyDoc = searchParams.get('doc')
  useEffect(() => {
    if (!legacyDoc) return
    const next = new URLSearchParams(searchParams)
    next.delete('doc')
    navigate(`/filings/${encodeURIComponent(legacyDoc)}${next.toString() ? `?${next.toString()}` : ''}`, { replace: true })
  }, [legacyDoc, navigate, searchParams])

  const coverage = useQuery({ queryKey: ['filing-coverage'], queryFn: () => apiRequest<FilingCoverage>('/api/filings/coverage') })
  const companyFilings = useQuery({
    queryKey: ['filings', 'company', companyCode],
    enabled: Boolean(companyCode),
    queryFn: () => apiRequest<{ filings: FilingRow[] }>(`/api/filings${queryString({ company_code: companyCode, limit: 500 })}`),
  })
  const latest = useInfiniteQuery({
    queryKey: ['filings', 'latest', category],
    enabled: !companyCode,
    initialPageParam: 0,
    queryFn: ({ pageParam }) => apiRequest<{ filings: FilingRow[] }>(`/api/filings${queryString({ limit: PAGE_SIZE, offset: pageParam, form: categoryCodes(category).join(',') || undefined })}`),
    getNextPageParam: (last, pages) => last.filings.length === PAGE_SIZE ? pages.length * PAGE_SIZE : undefined,
  })

  const update = (changes: Record<string, string | null>) => {
    const next = new URLSearchParams(searchParams)
    for (const [key, value] of Object.entries(changes)) {
      if (value) next.set(key, value)
      else next.delete(key)
    }
    setSearchParams(next)
  }
  const chooseCompany = (company: SecuritySearchResult | null) => {
    setPicked(company)
    setExportError('')
    update({ company: company?.company_code ?? null })
  }
  const setCategory = (key: CategoryKey) => update({ type: key === 'all' ? null : key })

  const companyRows = companyFilings.data?.filings ?? []
  const counts: Record<string, number> = {}
  if (companyCode) {
    counts.all = companyRows.length
    for (const row of companyRows) counts[formCategory(row.form_code)] = (counts[formCategory(row.form_code)] ?? 0) + 1
  } else {
    counts.all = coverage.data?.summary.unique_filings ?? 0
    for (const form of coverage.data?.forms ?? []) counts[formCategory(form.form_code)] = (counts[formCategory(form.form_code)] ?? 0) + form.filings
  }
  const enabledCategories = CATEGORY_KEYS.filter(key => key === 'all' || counts[key])
  const cycleCategory = (step: number) => {
    const index = enabledCategories.indexOf(category)
    setCategory(enabledCategories[(index + step + enabledCategories.length) % enabledCategories.length])
  }
  const visibleCompanyRows = category === 'all' ? companyRows : companyRows.filter(row => formCategory(row.form_code) === category)
  const latestRows = latest.data?.pages.flatMap(page => page.filings) ?? []
  const firstRow = companyRows[0]
  // Back and forward change the URL without a pick; only trust a pick for the company in the URL.
  const pick = picked?.company_code === companyCode ? picked : null
  const companyTitle = pick?.company_name || (firstRow ? companyName(firstRow) : companyCode)
  const ticker = pick?.ticker || firstRow?.ticker || ''
  const selected: SecuritySearchResult | null = companyCode ? { company_code: companyCode, ticker, company_name: companyTitle } : null
  const summary = coverage.data?.summary

  useHotkeys({
    f: () => finder.current?.focus(),
    '[': () => cycleCategory(-1),
    ']': () => cycleCategory(1),
    j: () => document.querySelector<HTMLTableRowElement>('.filings-table tbody tr[tabindex="0"]')?.focus(),
    x: () => { if (companyCode) chooseCompany(null) },
    a: () => { if (companyCode) navigate(`/analyze/${encodeURIComponent(companyCode)}`) },
    '?': () => setShowShortcuts(true),
  }, !showShortcuts)

  const exportAll = async () => {
    setExporting(true)
    setExportError('')
    try {
      await exportCompanyFilings(companyCode)
    } catch (error) {
      setExportError(error instanceof Error ? error.message : 'Export failed')
    } finally {
      setExporting(false)
    }
  }

  return <div className="filings-explorer">
    <header className="filings-explorer__header">
      <div>
        <span className="eyebrow">EDINET archive</span>
        <h1>Filings</h1>
        {summary && <p className="filings-explorer__stats">
          <span><strong>{summary.unique_filings.toLocaleString()}</strong> retained reports</span>
          <span><strong>{summary.unique_companies.toLocaleString()}</strong> filers</span>
          {summary.first_submitted && summary.last_submitted && <span>submitted {formatDay(summary.first_submitted)} – {formatDay(summary.last_submitted)}</span>}
          <Tip content="Reports where parsing recorded a data-quality issue; the filing viewer lists them under Details."><span><strong>{summary.filings_with_issues.toLocaleString()}</strong> with quality notes</span></Tip>
        </p>}
      </div>
      <div className="filings-explorer__finder">
        <CompanyPicker selected={selected} onSelect={chooseCompany} label="Find a company's filings" placeholder="Company name, ticker, or EDINET code" inputRef={finder} />
        <kbd aria-hidden="true">F</kbd>
        <button type="button" className="icon-button" onClick={() => setShowShortcuts(true)} title="Keyboard shortcuts (?)" aria-label="Keyboard shortcuts"><Keyboard /></button>
      </div>
    </header>

    {companyCode && <section className="filings-company" aria-label="Selected company">
      <div className="filings-company__id">
        <Building2 aria-hidden="true" />
        <div>
          <h2>{companyTitle}</h2>
          <p>{[ticker, companyCode, firstRow?.submitter_name && firstRow.submitter_name !== companyTitle ? firstRow.submitter_name : null].filter(Boolean).join(' · ')}</p>
        </div>
      </div>
      <div className="filings-company__actions">
        <Link className="button button--secondary button--small" to={`/analyze/${encodeURIComponent(companyCode)}`} title="Open the company analysis (A)">Analysis<ArrowRight aria-hidden="true" /></Link>
        {companyRows.length > 0 && <button type="button" className="button button--ghost button--small" disabled={exporting} onClick={() => void exportAll()} title="Download a ZIP of every retained filing archive with a manifest"><Download aria-hidden="true" />{exporting ? 'Preparing…' : 'Export all'}</button>}
        <button type="button" className="button button--ghost button--small" onClick={() => chooseCompany(null)} title="Back to the latest filings from every company (X)"><X aria-hidden="true" />Clear</button>
      </div>
      {exportError && <p className="form-error" role="alert">{exportError}</p>}
    </section>}

    {!companyCode && <RecentlyOpened />}

    <div className="filings-layout">
      <section className="filings-list panel" aria-label={companyCode ? `Filings by ${companyTitle}` : 'Latest filings'}>
        <CategoryTabs active={category} counts={counts} onChange={setCategory} />
        {companyCode ? <>
          {companyFilings.isLoading && <LoadingState label="Loading filings" />}
          {companyFilings.isError && <ErrorState error={companyFilings.error} retry={() => void companyFilings.refetch()} />}
          {companyFilings.data && !companyRows.length && <EmptyState title="No retained reports for this company" description="The filing pipeline has not acquired any XBRL packages for it yet." />}
          {visibleCompanyRows.length > 0 && <FilingsTable filings={visibleCompanyRows} from="filings" label={`Filings by ${companyTitle}`} />}
          {companyRows.length > 0 && <p className="filings-list__foot"><span>{visibleCompanyRows.length} of {companyRows.length} reports · newest first · <kbd>J</kbd> jumps into the list, <kbd>Enter</kbd> opens a filing</span></p>}
        </> : <>
          <header className="filings-list__header">
            <h2>Latest filings</h2>
            <span className="muted">Newest submissions from every filer</span>
          </header>
          {latest.isLoading && <LoadingState label="Loading the latest filings" />}
          {latest.isError && <ErrorState error={latest.error} retry={() => void latest.refetch()} />}
          {latestRows.length > 0 && <FilingsTable filings={latestRows} showCompany from="filings" label="Latest filings" />}
          {latest.data && !latestRows.length && <EmptyState title="No filings of this type" description="Choose another report type above." />}
          <div className="filings-list__foot">
            <span>{latestRows.length.toLocaleString()} shown · <kbd>J</kbd> jumps into the list, <kbd>Enter</kbd> opens a filing</span>
            {latest.hasNextPage && <button type="button" className="button button--ghost button--small" disabled={latest.isFetchingNextPage} onClick={() => void latest.fetchNextPage()}>{latest.isFetchingNextPage ? 'Loading…' : `Show ${PAGE_SIZE} more`}</button>}
          </div>
        </>}
      </section>
    </div>
    {showShortcuts && <ShortcutsDialog groups={SHORTCUTS} onClose={closeShortcuts} />}
  </div>
}
