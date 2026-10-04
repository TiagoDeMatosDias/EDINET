import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import AdminPage from './AdminPage'
import { AuthContext, type AuthContextValue } from './authContext'

const USERS = [
  { user_id: 'u1', username: 'admin', email: null, role: 'admin', status: 'active', token_version: 1, created_at: '2026-01-01T00:00:00Z', updated_at: '', last_login_at: '2026-10-03T00:00:00Z' },
  { user_id: 'u2', username: 'bob', email: 'bob@example.com', role: 'member', status: 'active', token_version: 1, created_at: '2026-02-01T00:00:00Z', updated_at: '', last_login_at: null },
]
const calls: Array<{ method: string; path: string; body?: unknown }> = []

beforeEach(() => {
  calls.length = 0
  vi.stubGlobal('fetch', vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
    const path = String(input)
    const method = init?.method ?? 'GET'
    calls.push({ method, path, body: init?.body ? JSON.parse(String(init.body)) : undefined })
    const body = path === '/api/admin/auth/users' ? USERS
      : path.startsWith('/api/admin/auth/audit') ? [{ event_id: 'e1', user_id: 'u2', event_type: 'login_failed', occurred_at: new Date().toISOString(), remote_addr: '127.0.0.1', detail: null }]
        : path === '/api/admin/auth/settings' ? { registration_mode: 'open', default_role: 'member', password_min_length: 5, access_token_seconds: 900, refresh_idle_seconds: 1209600, refresh_absolute_seconds: null, updated_at: null }
          : path.startsWith('/api/admin/auth/credential-resets') ? { reset_token: 'reset-123' }
            : path === '/api/admin/auth/invitations' ? { invitation_token: 'invite-456' }
              : path === '/api/admin/pipeline-schedules' ? []
                : {}
    return Promise.resolve(new Response(JSON.stringify(body), { status: 200, headers: { 'Content-Type': 'application/json' } }))
  }))
})
afterEach(() => { cleanup(); vi.unstubAllGlobals() })

function renderAdmin() {
  const auth = { user: { user_id: 'u1', username: 'admin', role: 'admin', status: 'active' }, status: { mode: 'accounts' }, loading: false } as unknown as AuthContextValue
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return render(<QueryClientProvider client={client}><AuthContext.Provider value={auth}><MemoryRouter><AdminPage /></MemoryRouter></AuthContext.Provider></QueryClientProvider>)
}

describe('AdminPage', () => {
  it('walks users from the keyboard and issues a one-time reset link', async () => {
    renderAdmin()
    const table = (await screen.findAllByRole('table'))[0]
    await within(table).findByText('bob')
    expect(screen.getByText('Failed logins, 24 h').nextSibling).toHaveTextContent('1')
    fireEvent.keyDown(window, { key: '1' })
    const rows = within(table).getAllByRole('row').slice(1)
    expect(rows[0]).toHaveFocus()
    fireEvent.keyDown(rows[0], { key: 'j' })
    expect(rows[1]).toHaveFocus()
    fireEvent.keyDown(rows[1], { key: 'p' })
    expect(await screen.findByText(/\/login\?reset=reset-123$/)).toBeInTheDocument()
    expect(calls.some(call => call.method === 'POST' && call.path === '/api/admin/auth/credential-resets?target_user_id=u2')).toBe(true)
    fireEvent.keyDown(rows[1], { key: 'x' })
    expect(within(rows[1]).getByRole('button', { name: 'Disable: sure?' })).toBeInTheDocument()
    expect(calls.some(call => call.path.endsWith('/disable'))).toBe(false)
  })

  it('creates an invitation link and saves invitation-only registration', async () => {
    renderAdmin()
    await screen.findByText('Open: anyone with the address')
    fireEvent.click(screen.getByRole('button', { name: /Create invitation/ }))
    expect(await screen.findByText(/\/register\?invite=invite-456$/)).toBeInTheDocument()
    fireEvent.change(screen.getByRole('combobox', { name: 'Registration' }), { target: { value: 'invite' } })
    fireEvent.click(screen.getByRole('button', { name: 'Save access settings' }))
    await waitFor(() => expect(calls.find(call => call.method === 'PATCH' && call.path === '/api/admin/auth/settings')?.body).toMatchObject({ registration_mode: 'invite', password_min_length: 5 }))
  })
})
