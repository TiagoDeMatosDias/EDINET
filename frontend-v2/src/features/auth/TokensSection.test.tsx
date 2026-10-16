import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { cleanup, fireEvent, render, screen, within } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { TokensSection } from './AccountPage'

const TOKENS = [
  { token_id: 't1', name: 'Nightly screen', token_prefix: 'ed_pat_abcd', scopes_json: '["read"]', created_at: '2026-10-01T00:00:00Z', expires_at: null, last_used_at: null, revoked_at: null },
  { token_id: 't2', name: 'Trading bot', token_prefix: 'ed_pat_efgh', scopes_json: '["*"]', created_at: '2026-10-02T00:00:00Z', expires_at: '2027-01-01T00:00:00Z', last_used_at: null, revoked_at: null },
  { token_id: 't3', name: 'Old', token_prefix: 'ed_pat_ijkl', scopes_json: '["*"]', created_at: '2026-01-01T00:00:00Z', expires_at: null, last_used_at: null, revoked_at: '2026-02-01T00:00:00Z' },
]
const calls: Array<{ method: string; path: string; body?: Record<string, unknown> }> = []

beforeEach(() => {
  calls.length = 0
  vi.stubGlobal('fetch', vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
    const path = String(input)
    const method = init?.method ?? 'GET'
    calls.push({ method, path, body: init?.body ? JSON.parse(String(init.body)) as Record<string, unknown> : undefined })
    const body = method === 'POST' ? { token: 'ed_pat_new_secret', warning: 'Copy this token now; it is not shown again.' } : TOKENS
    return Promise.resolve(new Response(JSON.stringify(body), { status: method === 'POST' ? 201 : 200, headers: { 'Content-Type': 'application/json' } }))
  }))
  Object.assign(navigator, { clipboard: { writeText: vi.fn(() => Promise.resolve()) } })
})
afterEach(() => { cleanup(); vi.unstubAllGlobals() })

function renderTokens() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return render(<QueryClientProvider client={client}><MemoryRouter><TokensSection /></MemoryRouter></QueryClientProvider>)
}

describe('TokensSection', () => {
  it('lists active tokens with their access', async () => {
    renderTokens()
    const nightly = (await screen.findByText('Nightly screen')).closest('tr')!
    expect(within(nightly).getByText('Read-only')).toBeTruthy()
    expect(within(screen.getByText('Trading bot').closest('tr')!).getByText('Full')).toBeTruthy()
    expect(screen.queryByText('Old')).toBeNull()
    expect(screen.getByRole('link', { name: 'Read the API docs' }).getAttribute('href')).toBe('/api-docs/start')
  })

  it('creates a read-only token that expires, then shows it once with a ready example', async () => {
    renderTokens()
    await screen.findByText('Nightly screen')
    fireEvent.change(screen.getByLabelText('New token name'), { target: { value: '  Reports  ' } })
    fireEvent.change(screen.getByLabelText('Token access'), { target: { value: 'read' } })
    fireEvent.change(screen.getByLabelText('Token expiry'), { target: { value: '30' } })
    const before = Date.now()
    fireEvent.click(screen.getByRole('button', { name: 'Create' }))
    expect(await screen.findByText('Copy this token now; it is not shown again.')).toBeTruthy()
    const created = calls.find(call => call.method === 'POST')!
    expect(created.path).toBe('/api/auth/tokens')
    expect(created.body?.name).toBe('Reports')
    expect(created.body?.scopes).toEqual(['read'])
    const expires = Date.parse(String(created.body?.expires_at))
    expect(expires - before).toBeGreaterThan(29.9 * 86_400_000)
    expect(expires - before).toBeLessThan(30.1 * 86_400_000)
    expect(screen.getByText('ed_pat_new_secret')).toBeTruthy()
    expect(screen.getByText(/export SHADE_TOKEN=ed_pat_new_secret/).textContent).toContain('curl -s "$SHADE_URL/api/auth/me" -H "Authorization: Bearer $SHADE_TOKEN"')
    fireEvent.click(screen.getByRole('button', { name: 'Copy token' }))
    expect(navigator.clipboard.writeText).toHaveBeenCalledWith('ed_pat_new_secret')
    expect(await screen.findByRole('button', { name: 'Copied' })).toBeTruthy()
  })

  it('creates a full-access token that never expires by choice', async () => {
    renderTokens()
    await screen.findByText('Nightly screen')
    fireEvent.change(screen.getByLabelText('New token name'), { target: { value: 'Bot' } })
    fireEvent.change(screen.getByLabelText('Token expiry'), { target: { value: '0' } })
    fireEvent.click(screen.getByRole('button', { name: 'Create' }))
    await screen.findByText('ed_pat_new_secret')
    const created = calls.find(call => call.method === 'POST')!
    expect(created.body).toEqual({ name: 'Bot', scopes: ['*'], expires_at: null })
  })
})
