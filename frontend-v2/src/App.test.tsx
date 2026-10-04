import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { MemoryRouter } from 'react-router-dom'

import { App } from './App'

function jsonResponse(value: unknown, status = 200) {
  return Promise.resolve(new Response(JSON.stringify(value), {
    status,
    headers: { 'Content-Type': 'application/json' },
  }))
}

const MEMBER = { user_id: 'u-1', username: 'alice', email: null, role: 'member', status: 'active' }

function stubBackend(
  authMode: 'accounts' | 'disabled' = 'accounts',
  { signedIn = false, tokenLifetimeMs = 900_000, refreshesBeforeExpiry = Infinity, statusFailures = 0 } = {},
) {
  let session = signedIn
  let refreshes = 0
  let statusCalls = 0
  const token = () => ({
    access_token: 'token-1',
    expires_at: new Date(Date.now() + tokenLifetimeMs).toISOString(),
    expires_in: tokenLifetimeMs / 1000,
    user: MEMBER,
  })
  const refreshCalls = () => refreshes
  vi.stubGlobal('fetch', vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
    const path = String(input)
    if (path === '/api/auth/login' && init?.method === 'POST') {
      session = true
      return jsonResponse(token())
    }
    if (path === '/api/auth/refresh') {
      refreshes += 1
      if (refreshes > refreshesBeforeExpiry) session = false
      return session ? jsonResponse(token()) : jsonResponse({ detail: 'No session' }, 401)
    }
    if (path === '/api/auth/logout') {
      session = false
      return Promise.resolve(new Response(null, { status: 204 }))
    }
    if (path === '/api/auth/me') return session ? jsonResponse(MEMBER) : jsonResponse({ detail: 'Authentication required' }, 401)
    if (path === '/api/auth/status') {
      statusCalls += 1
      if (statusCalls <= statusFailures) return jsonResponse({ detail: 'Service unavailable' }, 503)
      return jsonResponse({
        mode: authMode,
        registration_open: authMode === 'accounts',
        bootstrap_required: false,
        password_min_length: 15,
      })
    }
    if (path === '/health') return jsonResponse({ status: 'healthy', version: '1.0.0', timestamp: '2026-07-19T12:00:00Z' })
    if (path === '/api/overview') return jsonResponse({ today: '2026-07-19', portfolio: null, research: { followed: 2, alerts: 1, notes: 3, reviews_due: [], recent_notes: [], theses: {} }, data: { latest_price_date: '2026-07-18', priced_securities: 3800, filings: null } })
    if (path.startsWith('/api/jobs')) return jsonResponse([])
    if (path === '/api/steps') return jsonResponse({ steps: [] })
    if (path === '/api/portfolio/activity-summary') return jsonResponse({ by_activity: {} })
    return jsonResponse({})
  }))
  return { refreshCalls }
}

function renderApp(path: string, client = new QueryClient({ defaultOptions: { queries: { retry: false } } })) {
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

  it('shows each page shortcut in the sidebar and highlights them after G', async () => {
    stubBackend('disabled')
    renderApp('/overview')
    await screen.findByRole('heading', { name: 'Overview' })
    const sidebar = screen.getByRole('navigation', { name: 'Primary navigation' })

    const screenLink = within(sidebar).getByRole('link', { name: 'Screen' })
    expect(screenLink).toHaveAttribute('title', 'Screen (G then S)')
    expect(screenLink.querySelector('kbd')).toHaveTextContent('S')
    expect(sidebar).toHaveTextContent('Press G then a letter')

    fireEvent.keyDown(document.body, { key: 'g' })
    expect(sidebar).toHaveClass('primary-nav--keys')
    fireEvent.keyDown(document.body, { key: 'Escape' })
    expect(sidebar).not.toHaveClass('primary-nav--keys')
  })
})

describe('account sessions', () => {
  afterEach(() => {
    cleanup()
    vi.unstubAllGlobals()
  })

  it('establishes the workspace session from the dedicated sign-in page', async () => {
    stubBackend()
    renderApp('/login')

    fireEvent.change(await screen.findByLabelText('Username or email'), { target: { value: 'alice' } })
    fireEvent.change(screen.getByLabelText('Password'), { target: { value: 'correct horse battery staple' } })
    fireEvent.click(screen.getByRole('button', { name: 'Sign in' }))

    expect(await screen.findByRole('heading', { name: 'Overview' })).toBeInTheDocument()
    expect(screen.queryByRole('heading', { name: 'Sign in' })).not.toBeInTheDocument()
  })

  it('drops cached private data when a new account signs in', async () => {
    stubBackend()
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    client.setQueryData(['research-tags'], { tags: ['previous-account-tag'] })
    renderApp('/login', client)

    fireEvent.change(await screen.findByLabelText('Username or email'), { target: { value: 'alice' } })
    fireEvent.change(screen.getByLabelText('Password'), { target: { value: 'correct horse battery staple' } })
    fireEvent.click(screen.getByRole('button', { name: 'Sign in' }))

    await screen.findByRole('heading', { name: 'Overview' })
    expect(client.getQueryData(['research-tags'])).toBeUndefined()
  })

  it('drops cached private data on sign out', async () => {
    stubBackend('accounts', { signedIn: true })
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    renderApp('/overview', client)

    await screen.findByRole('heading', { name: 'Overview' })
    client.setQueryData(['research-tags'], { tags: ['private-tag'] })
    fireEvent.click(screen.getByRole('button', { name: /alice/ }))
    fireEvent.click(screen.getByRole('button', { name: 'Sign out' }))

    expect(await screen.findByRole('heading', { name: 'Sign in' })).toBeInTheDocument()
    await waitFor(() => expect(client.getQueryData(['research-tags'])).toBeUndefined())
  })

  it('names the registration password field without its hint text', async () => {
    stubBackend()
    renderApp('/register')

    const password = await screen.findByLabelText('Password')
    expect(password).toHaveAttribute('type', 'password')
    expect(password).toHaveAccessibleName('Password')
    expect(password).toHaveAccessibleDescription(/Minimum 15 characters/)
  })

  it('reports an unreachable server instead of assuming authentication is disabled', async () => {
    stubBackend('accounts', { statusFailures: 1 })
    renderApp('/overview')

    expect(await screen.findByRole('heading', { name: 'Service unavailable' })).toBeInTheDocument()
    expect(screen.queryByText('Authentication is disabled.')).not.toBeInTheDocument()
    expect(screen.queryByRole('heading', { name: 'Overview' })).not.toBeInTheDocument()

    fireEvent.click(screen.getByRole('button', { name: 'Try again' }))
    expect(await screen.findByRole('heading', { name: 'Sign in' })).toBeInTheDocument()
  })

  it('keeps public pages available when the server is unreachable', async () => {
    stubBackend('accounts', { statusFailures: 1 })
    renderApp('/')

    expect(await screen.findByRole('heading', { name: 'Research companies with the evidence still attached.' })).toBeInTheDocument()
  })

  it('renews the access token before the expiry the server set', async () => {
    const backend = stubBackend('accounts', { signedIn: true, tokenLifetimeMs: 500 })
    renderApp('/overview')

    await screen.findByRole('heading', { name: 'Overview' })
    await waitFor(() => expect(backend.refreshCalls()).toBeGreaterThan(1), { timeout: 2000 })
    expect(screen.getByRole('heading', { name: 'Overview' })).toBeInTheDocument()
  })

  it('signs out when renewal finds the session has ended', async () => {
    stubBackend('accounts', { signedIn: true, tokenLifetimeMs: 500, refreshesBeforeExpiry: 1 })
    renderApp('/overview')

    await screen.findByRole('heading', { name: 'Overview' })
    expect(await screen.findByRole('heading', { name: 'Sign in' }, { timeout: 2000 })).toBeInTheDocument()
  })
})
