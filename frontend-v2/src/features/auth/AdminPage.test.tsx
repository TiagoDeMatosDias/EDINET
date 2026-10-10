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
const SERVER_SETTINGS = [
  { key: 'edinet.api_key', label: 'EDINET API key', description: 'Subscription key for the EDINET API.', kind: 'secret', choices: [], minimum: null, restart_required: false, value: null, is_set: false, updated_at: null },
  { key: 'auth.mode', label: 'Sign-in', description: 'accounts or disabled.', kind: 'choice', choices: ['accounts', 'disabled'], minimum: null, restart_required: true, value: 'accounts', is_set: false, updated_at: null },
  { key: 'server.trusted_hosts', label: 'Trusted host names', description: 'Host names for remote access.', kind: 'list', choices: [], minimum: null, restart_required: true, value: [], is_set: false, updated_at: null },
  { key: 'tunnel.enabled', label: 'Cloudflare tunnel', description: 'on publishes the workstation.', kind: 'choice', choices: ['off', 'on'], minimum: null, restart_required: false, value: 'off', is_set: false, updated_at: null },
  { key: 'tunnel.token', label: 'Cloudflare tunnel token', description: 'Token of a tunnel created in Cloudflare Zero Trust.', kind: 'secret', choices: [], minimum: null, restart_required: false, value: null, is_set: false, updated_at: null },
]
const TUNNEL_OFF = { enabled: false, kind: 'quick', state: 'off', url: null, message: null, origin: 'https://127.0.0.1:8000', blocked_reason: null }
const calls: Array<{ method: string; path: string; body?: unknown }> = []
let serverRunning = true
let tunnel: Record<string, unknown> = TUNNEL_OFF

beforeEach(() => {
  calls.length = 0
  serverRunning = true
  tunnel = TUNNEL_OFF
  vi.stubGlobal('fetch', vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
    const path = String(input)
    const method = init?.method ?? 'GET'
    calls.push({ method, path, body: init?.body ? JSON.parse(String(init.body)) : undefined })
    if (!serverRunning) return Promise.reject(new TypeError('Failed to fetch'))
    if (path === '/api/admin/server/shutdown') serverRunning = false
    // The server follows the setting at once; the first status after turning it on is still connecting.
    if (path === '/api/admin/settings/tunnel.enabled') tunnel = JSON.parse(String(init?.body)).value === 'on' ? { ...TUNNEL_OFF, enabled: true, state: 'starting' } : TUNNEL_OFF
    const tunnelNow = tunnel
    if (path === '/api/admin/server/tunnel' && tunnel.state === 'starting') tunnel = { ...tunnel, state: 'running', url: 'https://calm-test-words.trycloudflare.com' }
    const body = path === '/api/admin/auth/users' ? USERS
      : path.startsWith('/api/admin/auth/audit') ? [{ event_id: 'e1', user_id: 'u2', event_type: 'login_failed', occurred_at: new Date().toISOString(), remote_addr: '127.0.0.1', detail: null }]
        : path === '/api/admin/auth/settings' ? { registration_mode: 'open', default_role: 'member', password_min_length: 5, access_token_seconds: 900, refresh_idle_seconds: 1209600, refresh_absolute_seconds: null, updated_at: null }
          : path.startsWith('/api/admin/auth/credential-resets') ? { reset_token: 'reset-123' }
            : path === '/api/admin/auth/invitations' ? { invitation_token: 'invite-456' }
              : path === '/api/admin/pipeline-schedules' ? []
                : path === '/api/admin/settings' ? { settings: SERVER_SETTINGS }
                  : path === '/api/admin/server/tunnel' ? tunnelNow
                  : path === '/api/system/status' ? { version: '1', timestamp: '', jobs: { queue_depth: 0, active: 1, counts_by_status: { running: 1 } } }
                  : path.startsWith('/api/admin/settings/') ? { ...SERVER_SETTINGS.find(item => path.endsWith(item.key)), is_set: method === 'PUT', updated_at: '2026-10-10T00:00:00Z' }
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

  it('saves server settings without ever showing the API key', async () => {
    renderAdmin()
    const key = await screen.findByLabelText('EDINET API key')
    expect(key).toHaveAttribute('type', 'password')
    expect(key).toHaveAttribute('placeholder', 'Not set')
    fireEvent.change(key, { target: { value: 'provider-secret' } })
    fireEvent.click(within(key.closest('form') as HTMLElement).getByRole('button', { name: 'Save' }))
    await waitFor(() => expect(calls.find(call => call.method === 'PUT' && call.path === '/api/admin/settings/edinet.api_key')?.body).toEqual({ value: 'provider-secret' }))

    const hosts = screen.getByLabelText('Trusted host names *')
    fireEvent.change(hosts, { target: { value: 'research.example, shade.example' } })
    fireEvent.click(within(hosts.closest('form') as HTMLElement).getByRole('button', { name: 'Save' }))
    await waitFor(() => expect(calls.find(call => call.method === 'PUT' && call.path === '/api/admin/settings/server.trusted_hosts')?.body).toEqual({ value: ['research.example', 'shade.example'] }))
    expect(await screen.findByText('Restart the server to apply the change.')).toBeInTheDocument()
  })

  it('opens a Cloudflare tunnel from Remote access and shows its address', async () => {
    renderAdmin()
    const turnOn = await screen.findByRole('button', { name: 'Turn on' })
    const section = turnOn.closest('section') as HTMLElement
    expect(within(section).getByRole('heading', { name: 'Remote access' })).toBeInTheDocument()
    expect(within(section).getByText(/Registration is open/)).toBeInTheDocument()
    // The tunnel's settings are edited here, not in the Server settings list.
    expect(await within(section).findByLabelText('Cloudflare tunnel token')).toHaveAttribute('type', 'password')
    expect(screen.getAllByLabelText('Cloudflare tunnel token')).toHaveLength(1)
    expect(screen.queryByLabelText('Cloudflare tunnel')).not.toBeInTheDocument()

    fireEvent.click(turnOn)
    await waitFor(() => expect(calls.find(call => call.method === 'PUT' && call.path === '/api/admin/settings/tunnel.enabled')?.body).toEqual({ value: 'on' }))
    expect(await within(section).findByText('Connecting to Cloudflare…')).toBeInTheDocument()
    const address = await within(section).findByRole('link', { name: 'https://calm-test-words.trycloudflare.com' }, { timeout: 4000 })
    expect(address).toHaveAttribute('href', 'https://calm-test-words.trycloudflare.com')
    expect(within(section).getByText(/A temporary address/)).toBeInTheDocument()

    fireEvent.click(within(section).getByRole('button', { name: 'Turn off' }))
    await waitFor(() => expect(calls.filter(call => call.path === '/api/admin/settings/tunnel.enabled').at(-1)?.body).toEqual({ value: 'off' }))
    expect(await within(section).findByRole('button', { name: 'Turn on' })).toBeInTheDocument()
    expect(within(section).queryByRole('link')).not.toBeInTheDocument()
  })

  it('makes invitation and reset links with the tunnel address while a tunnel is open', async () => {
    tunnel = { ...TUNNEL_OFF, enabled: true, state: 'running', url: 'https://calm-test-words.trycloudflare.com' }
    renderAdmin()
    await screen.findByRole('link', { name: 'https://calm-test-words.trycloudflare.com' })
    fireEvent.click(screen.getByRole('button', { name: /Create invitation/ }))
    expect(await screen.findByText('https://calm-test-words.trycloudflare.com/register?invite=invite-456')).toBeInTheDocument()

    // The address is asked for with each link: this tunnel has started again since the page loaded.
    tunnel = { ...tunnel, url: 'https://new-test-words.trycloudflare.com' }
    fireEvent.click((await screen.findAllByRole('button', { name: /Reset/ }))[0])
    expect(await screen.findByText('https://new-test-words.trycloudflare.com/login?reset=reset-123')).toBeInTheDocument()

    // Closed again: links fall back to the address the administrator is using.
    tunnel = TUNNEL_OFF
    fireEvent.click(screen.getByRole('button', { name: /Create invitation/ }))
    expect(await screen.findByText(`${window.location.origin}/register?invite=invite-456`)).toBeInTheDocument()
  })

  it('does not offer a tunnel while sign-in is disabled', async () => {
    tunnel = { ...TUNNEL_OFF, blocked_reason: 'Sign-in is disabled (the auth.mode setting), so the tunnel stays closed.' }
    renderAdmin()
    expect(await screen.findByText(/Sign-in is disabled/)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Turn on' })).toBeDisabled()
  })

  it('shuts the server down only after confirming, then says how to start it again', async () => {
    renderAdmin()
    const open = await screen.findByRole('button', { name: 'Shut down server' })
    open.focus()
    fireEvent.click(open)
    const dialog = screen.getByRole('alertdialog', { name: 'Shut down the server?' })
    expect(within(dialog).getByRole('button', { name: 'Cancel' })).toHaveFocus()
    fireEvent.keyDown(dialog, { key: '1' })
    expect(within(dialog).getByRole('button', { name: 'Cancel' })).toHaveFocus()
    expect(await within(dialog).findByText(/A pipeline job is active/)).toBeInTheDocument()
    fireEvent.keyDown(dialog, { key: 'Escape' })
    expect(screen.queryByRole('alertdialog')).not.toBeInTheDocument()
    expect(open).toHaveFocus()
    expect(calls.some(call => call.path === '/api/admin/server/shutdown')).toBe(false)

    fireEvent.click(open)
    fireEvent.click(screen.getByRole('button', { name: 'Shut down' }))
    const stopped = await screen.findByRole('alertdialog', { name: 'The server has shut down' })
    expect(calls.find(call => call.path === '/api/admin/server/shutdown')).toMatchObject({ method: 'POST', body: { confirm: true } })
    fireEvent.keyDown(stopped, { key: 'Escape' })
    expect(stopped).toBeInTheDocument()
    fireEvent.click(within(stopped).getByRole('button', { name: 'Reload page' }))
    expect(await within(stopped).findByText('The server is not running yet.')).toBeInTheDocument()
  })
})
