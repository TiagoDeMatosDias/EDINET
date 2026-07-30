import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { MemoryRouter, Route, Routes } from 'react-router-dom'

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

function stubViewerBackend(translationStatus = 200) {
  const requests: string[] = []
  vi.stubGlobal('fetch', vi.fn((input: RequestInfo | URL) => {
    const path = String(input)
    requests.push(path)
    if (path === '/api/filings/S100TEST') {
      return jsonResponse({
        filing: { doc_id: 'S100TEST', submitter_name: 'Test Company', status: 'parsed' },
        artifacts: [],
      })
    }
    if (path.startsWith('/api/filings/S100TEST/facts-translated')) return jsonResponse({ facts: [], count: 0 })
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
    if (path === '/api/filings/S100TEST/htm-files') return jsonResponse({ files: [] })
    return jsonResponse({})
  }))
  return requests
}

describe('filing document translation', () => {
  afterEach(() => {
    cleanup()
    vi.unstubAllGlobals()
  })

  it('loads one complete document translation and displays it beside the original', async () => {
    const requests = stubViewerBackend()
    renderViewer()

    fireEvent.click(await screen.findByRole('button', { name: 'Sections' }))

    expect(await screen.findByText('Complete English body')).toBeInTheDocument()
    expect(screen.getByText('日本語の本文')).toBeInTheDocument()
    expect(requests.filter(path => path.includes('/sections-translated')).length).toBe(1)
    expect(requests.some(path => path.includes('/translate-body'))).toBe(false)
  })

  it('keeps the Japanese document visible and reports an incomplete translation', async () => {
    const requests = stubViewerBackend(503)
    renderViewer()

    fireEvent.click(await screen.findByRole('button', { name: 'Sections' }))

    expect(await screen.findByText('日本語の本文')).toBeInTheDocument()
    expect(await screen.findByRole('alert')).toHaveTextContent('residual Japanese remains')
    await waitFor(() => expect(requests.filter(path => path.includes('/sections-translated')).length).toBe(1))
    expect(requests.some(path => path.includes('/translate-body'))).toBe(false)
  })
})
