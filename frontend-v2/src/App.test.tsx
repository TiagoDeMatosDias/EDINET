import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { cleanup, render, screen } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { MemoryRouter } from 'react-router-dom'

import { App } from './App'

function jsonResponse(value: unknown, status = 200) {
  return Promise.resolve(new Response(JSON.stringify(value), {
    status,
    headers: { 'Content-Type': 'application/json' },
  }))
}

function stubBackend(authMode: 'accounts' | 'disabled' = 'accounts') {
  vi.stubGlobal('fetch', vi.fn((input: RequestInfo | URL) => {
    const path = String(input)
    if (path === '/api/auth/status') {
      return jsonResponse({
        mode: authMode,
        registration_open: authMode === 'accounts',
        bootstrap_required: false,
        password_min_length: 15,
      })
    }
    if (path === '/api/auth/refresh') return jsonResponse({ detail: 'No session' }, 401)
    if (path === '/health') return jsonResponse({ status: 'healthy', timestamp: '2026-07-19T12:00:00Z', jobs_active: 0 })
    if (path.startsWith('/api/jobs')) return jsonResponse([])
    if (path === '/api/steps') return jsonResponse({ steps: [] })
    if (path === '/api/portfolio/activity-summary') return jsonResponse({ by_activity: {} })
    return jsonResponse({})
  }))
}

function renderApp(path: string) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return render(
    <QueryClientProvider client={client}>
      <MemoryRouter initialEntries={[path]}>
        <App />
      </MemoryRouter>
    </QueryClientProvider>,
  )
}

describe('public pages and workspace shell', () => {
  beforeEach(() => {
    stubBackend()
  })

  afterEach(() => {
    cleanup()
    vi.unstubAllGlobals()
  })

  it('keeps the homepage public and links to authentication and pricing', async () => {
    renderApp('/')

    expect(await screen.findByRole('heading', { name: 'Research companies with the evidence still attached.' })).toBeInTheDocument()
    expect(screen.getAllByRole('link', { name: /create (your )?account/i }).some(link => link.getAttribute('href') === '/register')).toBe(true)
    expect(screen.getAllByRole('link', { name: 'Sign in' }).some(link => link.getAttribute('href') === '/login')).toBe(true)
    expect(screen.getAllByRole('link', { name: /pricing/i }).some(link => link.getAttribute('href') === '/pricing')).toBe(true)
  })

  it('renders the single pricing tier and both billing options', async () => {
    renderApp('/pricing')

    expect(await screen.findByRole('heading', { name: 'One plan. The entire research workspace.' })).toBeInTheDocument()
    expect(screen.getByText('€10')).toBeInTheDocument()
    expect(screen.getByText('€100 per year')).toBeInTheDocument()
    expect(screen.getByRole('link', { name: /create your account/i })).toHaveAttribute('href', '/register')
  })

  it('opens the registration route in registration mode', async () => {
    renderApp('/register')

    expect(await screen.findByRole('heading', { name: 'Create your account' })).toBeInTheDocument()
    expect(screen.getByRole('link', { name: 'Already have an account? Sign in' })).toHaveAttribute('href', '/login')
  })

  it('keeps workspace routes protected when accounts are enabled', async () => {
    renderApp('/overview')

    expect(await screen.findByRole('heading', { name: 'Sign in' })).toBeInTheDocument()
    expect(screen.queryByRole('heading', { name: 'Overview' })).not.toBeInTheDocument()
  })

  it('renders the overview and primary research journeys in the workspace', async () => {
    stubBackend('disabled')
    renderApp('/overview')

    expect(await screen.findByRole('heading', { name: 'Overview' })).toBeInTheDocument()
    expect(screen.getAllByRole('link', { name: 'Screen' })[0]).toHaveAttribute('href', '/screen')
    expect(screen.getAllByRole('link', { name: 'Analyze' })[0]).toHaveAttribute('href', '/analyze')
    expect(screen.getByText('Data service ready')).toBeInTheDocument()
  })
})
