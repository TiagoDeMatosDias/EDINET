import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { MemoryRouter, Route, Routes } from 'react-router-dom'

import { setAccessToken } from '../../api/client'
import FilingViewerPage from './FilingViewerPage'

function jsonResponse(value: unknown, status = 200) {
  return Promise.resolve(new Response(JSON.stringify(value), {
    status,
    headers: { 'Content-Type': 'application/json' },
  }))
}

function renderViewer() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return render(
    <QueryClientProvider client={client}>
      <MemoryRouter initialEntries={['/filings/S100TEST']}>
        <Routes>
          <Route path="/filings/:docId" element={<FilingViewerPage />} />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  )
}

const authorizations: Array<string | null> = []

function stubViewerBackend(translationStatus = 200) {
  const requests: string[] = []
  authorizations.length = 0
  vi.stubGlobal('fetch', vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
    const path = String(input)
    requests.push(path)
    authorizations.push(new Headers(init?.headers).get('Authorization'))
    if (path === '/api/filings/S100TEST') {
      return jsonResponse({
        filing: { doc_id: 'S100TEST', edinet_code: 'E00001', submitter_name: 'テスト株式会社', period_start: '2025-04-01', period_end: '2026-03-31', form_code: '030000', status: 'parsed', archive_sha256: 'abc' },
        artifacts: [],
      })
    }
    if (path === '/api/filings/S100TEST/statement-tables') {
      const current = { key: 'D:2025-04-01:2026-03-31', start: '2025-04-01', end: '2026-03-31', label: '2026-03-31', detail: '12 months' }
      const prior = { key: 'D:2024-04-01:2025-03-31', start: '2024-04-01', end: '2025-03-31', label: '2025-03-31', detail: '12 months' }
      return jsonResponse({
        source: 'linkbase',
        fact_count: 5,
        statements: [
          {
            id: 'rol_ConsolidatedStatementOfIncome',
            name: 'Consolidated statement of income',
            member_axes: [],
            periods: [{ ...current, filled: 2 }, { ...prior, filled: 2 }, { key: 'I:2024-03-31', start: null, end: '2024-03-31', label: '2024-03-31', detail: 'As of', filled: 0 }],
            rows: [
              { kind: 'heading', label: 'Revenue', concept: 'RevenueAbstract', depth: 0 },
              { kind: 'item', label: 'Net sales', concept: 'NetSales', depth: 1, unit: 'JPY', values: { [current.key]: 1_200_000, [prior.key]: 1_100_000 } },
              { kind: 'total', label: 'Profit', concept: 'ProfitLoss', depth: 0, unit: 'JPY', values: { [current.key]: 90_000, [prior.key]: 80_000 } },
            ],
          },
          {
            id: 'rol_NotesSegmentInformation-01',
            name: 'Notes segment information (1)',
            member_axes: ['Operating segments axis'],
            periods: [{ ...current, filled: 1 }],
            rows: [{ kind: 'item', label: 'Net sales', concept: 'NetSales', depth: 0, member: 'Automotive', unit: 'JPY', values: { [current.key]: 700_000 } }],
          },
        ],
      })
    }
    if (path === '/api/filings/S100TEST/artifact') {
      return Promise.resolve(new Response('zip', { status: 200, headers: { 'Content-Type': 'application/zip', 'Content-Disposition': 'attachment; filename=S100TEST.zip' } }))
    }
    if (path.startsWith('/api/filings/S100TEST/sections-translated')) {
      if (translationStatus !== 200) {
        return jsonResponse({ detail: 'Translation could not be completed: residual Japanese remains' }, translationStatus)
      }
      return jsonResponse({
        sections: [{ section_id: 'section-1', title: '事業', title_en: 'Business', text: '日本語の本文', text_en: 'Complete English body', ordinal: 1 }],
        count: 1,
      })
    }
    if (path.startsWith('/api/filings/S100TEST/sections')) {
      return jsonResponse({ sections: [{ section_id: 'section-1', title: '事業', text: '日本語の本文', ordinal: 1 }], count: 1 })
    }
    if (path === '/api/filings/S100TEST/quality') return jsonResponse({ issues: [] })
    if (path === '/api/filings/S100TEST/htm-files') {
      return jsonResponse({ files: [
        { artifact_id: 'cover', member_path: 'XBRL/PublicDoc/0000000_header.htm', filename: '0000000_header', size_bytes: 2048, label: 'Cover page', heading: '表紙', code: '0000000', group: 'cover' },
        { artifact_id: 'overview', member_path: 'XBRL/PublicDoc/0101010_honbun.htm', filename: '0101010_honbun', size_bytes: 4096, label: 'Company overview', heading: '企業の概況', code: '0101010', group: 'business' },
      ] })
    }
    if (path === '/api/filings/S100TEST/html/overview') return jsonResponse({ html: '<html><head></head><body><p>本文</p></body></html>' })
    if (path === '/api/filings/S100TEST/html/overview?translate=true') return jsonResponse({ html: '<html><head></head><body><p>本文</p></body></html>', html_en: '<html><head></head><body><p>Body</p></body></html>' })
    if (path === '/api/filings?company_code=E00001&limit=500') {
      const row = (doc_id: string, period_end: string) => ({ doc_id, edinet_code: 'E00001', company_name: 'TEST COMPANY', ticker: '99990', period_end, form_code: '030000', status: 'parsed' })
      return jsonResponse({ filings: [row('S100NEWER', '2027-03-31'), row('S100TEST', '2026-03-31'), row('S100OLDER', '2025-03-31')] })
    }
    return jsonResponse({})
  }))
  return requests
}

describe('filing document translation', () => {
  afterEach(() => {
    cleanup()
    vi.unstubAllGlobals()
    window.localStorage.clear()
  })

  it('loads one complete document translation and displays it beside the original', async () => {
    const requests = stubViewerBackend()
    renderViewer()

    fireEvent.click(await screen.findByRole('tab', { name: /Sections/ }))

    expect(await screen.findByText('Complete English body')).toBeInTheDocument()
    expect(screen.getByText('日本語の本文')).toBeInTheDocument()
    expect(requests.filter(path => path.includes('/sections-translated')).length).toBe(1)
    expect(requests.some(path => path.includes('/translate-body'))).toBe(false)
  })

  it('keeps the Japanese document visible and reports an incomplete translation', async () => {
    const requests = stubViewerBackend(503)
    renderViewer()

    fireEvent.click(await screen.findByRole('tab', { name: /Sections/ }))

    expect(await screen.findByText('日本語の本文')).toBeInTheDocument()
    expect(await screen.findByRole('alert')).toHaveTextContent('residual Japanese remains')
    await waitFor(() => expect(requests.filter(path => path.includes('/sections-translated')).length).toBe(1))
    expect(requests.some(path => path.includes('/translate-body'))).toBe(false)
  })
})

describe('filing statements and downloads', () => {
  afterEach(() => {
    cleanup()
    vi.unstubAllGlobals()
    setAccessToken(null)
  })

  it('shows each statement from the filing with its own periods, nesting, and members', async () => {
    stubViewerBackend()
    renderViewer()

    fireEvent.click(await screen.findByRole('tab', { name: /Statements/ }))

    const income = await screen.findByRole('region', { name: 'Consolidated statement of income' })
    const headers = Array.from(income.querySelectorAll('thead th')).map(cell => cell.textContent)
    expect(headers).toEqual(['JPY thousands', '2026-03-3112 months', '2025-03-3112 months', 'Change'])
    expect(Array.from(income.querySelectorAll('tbody td')).map(cell => cell.textContent)).toEqual(['1,200', '1,100', '+9.1%', '90', '80', '+12.5%'])
    fireEvent.click(screen.getByRole('button', { name: 'Show 1 sparse period' }))
    expect(Array.from(income.querySelectorAll('thead th')).length).toBe(5)

    fireEvent.click(screen.getByRole('button', { name: /Segment information \(1\)/ }))
    const segments = await screen.findByRole('region', { name: 'Notes segment information (1)' })
    expect(Array.from(segments.querySelectorAll('thead th')).map(cell => cell.textContent)[1]).toBe('Operating segments axis')
    expect(screen.getByText('Automotive')).toBeInTheDocument()
  })

  it('filters line items across statements and keeps their headings', async () => {
    stubViewerBackend()
    renderViewer()

    fireEvent.click(await screen.findByRole('tab', { name: /Statements/ }))
    await screen.findByRole('region', { name: 'Consolidated statement of income' })
    fireEvent.change(screen.getByRole('textbox', { name: 'Filter line items' }), { target: { value: 'profit' } })

    const income = screen.getByRole('region', { name: 'Consolidated statement of income' })
    expect(Array.from(income.querySelectorAll('tbody tr')).map(row => row.querySelector('th')?.textContent)).toEqual(['Profit'])
    expect(screen.queryByRole('button', { name: /Segment information/ })).not.toBeInTheDocument()
  })

  it('downloads the filing archive with the bearer token instead of an anchor navigation', async () => {
    const requests = stubViewerBackend()
    setAccessToken('token-123')
    const createObjectURL = vi.fn(() => 'blob:zip')
    vi.stubGlobal('URL', Object.assign(URL, { createObjectURL, revokeObjectURL: vi.fn() }))
    const click = vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(() => undefined)
    renderViewer()

    fireEvent.click(await screen.findByRole('button', { name: 'ZIP' }))

    await waitFor(() => expect(click).toHaveBeenCalled())
    const index = requests.indexOf('/api/filings/S100TEST/artifact')
    expect(authorizations[index]).toBe('Bearer token-123')
    expect(screen.queryByRole('link', { name: 'ZIP' })).not.toBeInTheDocument()
    click.mockRestore()
  })
})

describe('filing report documents and navigation', () => {
  afterEach(() => {
    cleanup()
    vi.unstubAllGlobals()
    window.localStorage.clear()
  })

  it('opens the first business document by name and translates only when asked', async () => {
    const requests = stubViewerBackend()
    renderViewer()

    expect(await screen.findByRole('heading', { name: 'Company overview' })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /Cover page/ })).toBeInTheDocument()
    expect(await screen.findByTitle('Company overview (Japanese original)')).toBeInTheDocument()
    expect(requests.some(path => path.includes('translate=true'))).toBe(false)

    fireEvent.click(screen.getByRole('button', { name: 'Side by side' }))

    expect(await screen.findByTitle('Company overview (English translation)')).toBeInTheDocument()
    expect(requests.filter(path => path.includes('translate=true')).length).toBe(1)
  })

  it('names the company in English and links the older and newer reports', async () => {
    stubViewerBackend()
    renderViewer()

    expect(await screen.findByRole('heading', { level: 1, name: 'TEST COMPANY' })).toBeInTheDocument()
    expect(screen.getByText('テスト株式会社')).toBeInTheDocument()
    expect(screen.getAllByText('Annual securities report').length).toBeGreaterThan(0)
    expect(screen.getByRole('link', { name: /FY 2025-03/ })).toHaveAttribute('href', '/filings/S100OLDER?company=E00001')
    expect(screen.getByRole('link', { name: /FY 2027-03/ })).toHaveAttribute('href', '/filings/S100NEWER?company=E00001')
  })
})
