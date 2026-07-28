import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { render, screen, waitFor } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { describe, expect, it, vi } from 'vitest'

import { apiRequest } from '../../api/client'
import FilingsPage from './FilingsPage'

vi.mock('../../api/client', () => ({
  apiRequest: vi.fn(),
  queryString: vi.fn(() => ''),
}))

function renderPage() {
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
  it('shows statistics without loading a filing list before a company is selected', async () => {
    vi.mocked(apiRequest).mockResolvedValue({
      summary: {
        unique_filings: 12,
        unique_companies: 4,
        unique_archives: 9,
        parsed_filings: 11,
        error_filings: 1,
        filings_with_issues: 2,
      },
    })

    renderPage()

    await waitFor(() => expect(screen.getByText('Unique filings')).toBeInTheDocument())
    expect(screen.getByText('Companies with filings')).toBeInTheDocument()
    expect(screen.getByText('Unique archive packages')).toBeInTheDocument()
    expect(screen.queryByText('Show all')).not.toBeInTheDocument()
    expect(vi.mocked(apiRequest)).toHaveBeenCalledWith('/api/filings/coverage')
    expect(vi.mocked(apiRequest)).not.toHaveBeenCalledWith('/api/filings?limit=100')
  })
})
