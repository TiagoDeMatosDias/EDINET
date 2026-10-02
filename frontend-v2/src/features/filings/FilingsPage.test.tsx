import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { apiRequest } from '../../api/client'
import FilingsPage from './FilingsPage'

vi.mock('../../api/client', () => ({
  apiRequest: vi.fn(),
  queryString: (params: Record<string, unknown>) => {
    const entries = Object.entries(params).filter(([, value]) => value !== undefined && value !== '')
    return entries.length ? `?${new URLSearchParams(entries.map(([key, value]) => [key, String(value)]))}` : ''
  },
}))

const LATEST = {
  filings: [{
    doc_id: 'S100NEW', edinet_code: 'E02144', company_name: 'TOYOTA MOTOR CORPORATION', ticker: '72030',
    submitter_name: 'トヨタ自動車株式会社', period_start: '2025-04-01', period_end: '2026-03-31',
    submitted_at: '2026-06-10 15:33', form_code: '030000', archive_size: 2_400_000, status: 'parsed',
  }],
}

function renderPage() {
  vi.mocked(apiRequest).mockImplementation(async (path: string) => {
    if (path === '/api/filings/coverage') {
      return {
        summary: { unique_filings: 12, unique_companies: 4, filings_with_issues: 2, first_submitted: '2016-08-04 10:33', last_submitted: '2026-07-27 16:30' },
        forms: [{ form_code: '030000', filings: 9, companies: 4 }, { form_code: '07A000', filings: 3, companies: 1 }],
      }
    }
    if (path.startsWith('/api/filings?')) return LATEST
    if (path.startsWith('/api/research/recent-work')) return { items: [] }
    throw new Error(`Unexpected request ${path}`)
  })
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return render(
    <QueryClientProvider client={client}>
      <MemoryRouter initialEntries={['/filings']}>
        <FilingsPage />
      </MemoryRouter>
    </QueryClientProvider>,
  )
}

describe('Filing Explorer landing page', () => {
  afterEach(() => {
    cleanup()
    vi.mocked(apiRequest).mockReset()
  })

  it('summarises the archive and lists the latest filings by English company name', async () => {
    renderPage()

    expect(await screen.findByText('TOYOTA MOTOR CORPORATION')).toBeInTheDocument()
    expect(screen.getByText('retained reports')).toBeInTheDocument()
    expect(screen.getByRole('tab', { name: /All reports\s*12/ })).toHaveAttribute('aria-selected', 'true')
    expect(screen.getByRole('tab', { name: /Annual reports\s*9/ })).toBeEnabled()
    expect(screen.getByRole('tab', { name: /Funds and trusts\s*3/ })).toBeEnabled()
    expect(screen.getByRole('tab', { name: /Amendments\s*0/ })).toBeDisabled()
    expect(screen.getByText('Annual securities report')).toBeInTheDocument()
    expect(vi.mocked(apiRequest)).toHaveBeenCalledWith('/api/filings?limit=50&offset=0')
  })

  it('asks the server for the chosen report family', async () => {
    renderPage()

    fireEvent.click(await screen.findByRole('tab', { name: /Funds and trusts\s*3/ }))

    await waitFor(() => expect(vi.mocked(apiRequest)).toHaveBeenCalledWith(expect.stringContaining('form=06G000%2C06I000%2C07A000%2C07B000%2C09A000%2C09E000')))
  })
})
