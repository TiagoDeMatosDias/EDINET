import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { cleanup, render, screen } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { MemoryRouter } from 'react-router-dom'

import { App } from './App'

function jsonResponse(value: unknown) {
  return Promise.resolve(new Response(JSON.stringify(value), {
    status: 200,
    headers: { 'Content-Type': 'application/json' },
  }))
}

describe('workspace shell', () => {
  beforeEach(() => {
    vi.stubGlobal('fetch', vi.fn((input: RequestInfo | URL) => {
      const path = String(input)
      if (path === '/health') return jsonResponse({ status: 'healthy', timestamp: '2026-07-19T12:00:00Z', jobs_active: 0 })
      if (path.startsWith('/api/jobs')) return jsonResponse([])
      if (path === '/api/steps') return jsonResponse({ steps: [] })
      if (path === '/api/portfolio/activity-summary') return jsonResponse({ by_activity: {} })
      return jsonResponse({})
    }))
  })

  afterEach(() => {
    cleanup()
    vi.unstubAllGlobals()
  })

  it('renders the overview and primary research journeys', async () => {
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    render(
      <QueryClientProvider client={client}>
        <MemoryRouter initialEntries={['/']}>
          <App />
        </MemoryRouter>
      </QueryClientProvider>,
    )

    expect(await screen.findByRole('heading', { name: 'Overview' })).toBeInTheDocument()
    expect(screen.getAllByRole('link', { name: 'Screen' })[0]).toHaveAttribute('href', '/screen')
    expect(screen.getAllByRole('link', { name: 'Analyze' })[0]).toHaveAttribute('href', '/analyze')
    expect(screen.getByText('Data service ready')).toBeInTheDocument()
  })
})

