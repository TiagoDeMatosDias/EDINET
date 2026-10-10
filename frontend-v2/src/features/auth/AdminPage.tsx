import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Copy, Link2, RotateCcw, UserPlus } from 'lucide-react'
import { useEffect, useMemo, useRef, useState, type FormEvent, type KeyboardEvent, type ReactNode } from 'react'
import { Link } from 'react-router-dom'

import { apiRequest } from '../../api/client'
import { LoadingState } from '../../components/Feedback'
import { HotkeyHelpButton } from '../../hotkeys/HotkeyHelpButton'
import { HotkeyKbd } from '../../hotkeys/HotkeyKbd'
import { useHotkeyScope } from '../../hotkeys/useHotkeyScope'
import { useAuth } from './authContext'
import { ADMIN_SECTIONS, adminScope } from './adminHotkeys'
import { ShutdownServer } from './ShutdownServer'
import './admin.css'

interface AdminUser {
  user_id: string
  username: string
  email: string | null
  role: string
  status: string
  token_version: number
  created_at: string
  updated_at: string
  last_login_at: string | null
}

interface AuditEvent {
  event_id: string
  user_id: string | null
  event_type: string
  occurred_at: string
  remote_addr: string | null
  detail: string | null
}

interface AuthSettings {
  registration_mode: 'open' | 'closed' | 'invite'
  default_role: 'admin' | 'operator' | 'member'
  password_min_length: number
  access_token_seconds: number
  refresh_idle_seconds: number
  refresh_absolute_seconds: number | null
  updated_at?: string | null
}

/** One operator setting stored in app.db; secret values are never sent. */
interface ServerSetting {
  key: string
  label: string
  description: string
  kind: 'secret' | 'choice' | 'integer' | 'list' | 'path' | 'text'
  choices: string[]
  minimum: number | null
  restart_required: boolean
  value: string | number | string[] | null
  is_set: boolean
  updated_at: string | null
}

/** What the server reports about its Cloudflare tunnel. */
interface TunnelStatus {
  enabled: boolean
  kind: 'quick' | 'named'
  state: 'off' | 'blocked' | 'downloading' | 'starting' | 'running' | 'failed'
  url: string | null
  message: string | null
  origin: string
  blocked_reason: string | null
}

const TUNNEL_KEY = ['admin-tunnel']
const fetchTunnel = () => apiRequest<TunnelStatus>('/api/admin/server/tunnel')

type SavedSetup = { name: string; steps: Array<{ name: string; overwrite?: boolean }>; config: Record<string, unknown> }
type PipelineSchedule = { schedule_id: string; name: string; frequency: 'daily' | 'weekly' | 'monthly'; enabled: boolean; steps: Array<{ name: string; overwrite?: boolean }>; config: Record<string, unknown>; last_run_at: string | null }
type PipelineSchedulerStatus = { checked_at: string; next_check_at: string; active_pipeline: boolean; triggered_job_ids: string[] }

const SETUPS_KEY = 'shade.pipeline.setups'
const EVENT_LABELS: Record<string, string> = {
  account_created: 'Account created', login_succeeded: 'Login', login_failed: 'Failed login', logout: 'Logout',
  refresh_succeeded: 'Token refresh', refresh_reuse_detected: 'Refresh reuse', password_changed: 'Password changed',
  password_change_failed: 'Failed password change', profile_updated: 'Profile updated', session_revoked: 'Session revoked',
  all_sessions_revoked: 'All sessions revoked', api_token_created: 'API token created', user_disabled: 'User disabled', role_changed: 'Role changed',
  server_shutdown: 'Server shut down', tunnel_restarted: 'Tunnel restarted', setting_updated: 'Setting changed', setting_reset: 'Setting reset',
}
const ALARMING = new Set(['login_failed', 'refresh_reuse_detected', 'password_change_failed', 'user_disabled'])

const SECTIONS = ADMIN_SECTIONS

const stamp = new Intl.DateTimeFormat('en-GB', { day: 'numeric', month: 'short', year: '2-digit', hour: '2-digit', minute: '2-digit' })
const when = (value: string | null | undefined) => (value ? stamp.format(new Date(value)) : '—')

function readSavedSetups(): SavedSetup[] {
  try {
    const parsed: unknown = JSON.parse(localStorage.getItem(SETUPS_KEY) ?? '[]')
    if (!Array.isArray(parsed)) return []
    return parsed.flatMap(value => {
      if (!value || typeof value !== 'object' || typeof value.name !== 'string' || !Array.isArray(value.steps)) return []
      const steps = value.steps.filter((step: unknown) => step && typeof step === 'object' && typeof (step as { name?: unknown }).name === 'string')
      const config = value.config && typeof value.config === 'object' && !Array.isArray(value.config) ? value.config : {}
      return [{ name: value.name, steps, config }]
    })
  } catch {
    return []
  }
}

function pipelineConfig(setup: SavedSetup): Record<string, unknown> {
  const nested: Record<string, unknown> = {}
  for (const [key, value] of Object.entries(setup.config)) {
    const dot = key.indexOf('.')
    if (dot < 0) { nested[key] = value; continue }
    const stepKey = key.slice(0, dot)
    const current = nested[stepKey]
    nested[stepKey] = { ...(current && typeof current === 'object' && !Array.isArray(current) ? current : {}), [key.slice(dot + 1)]: value }
  }
  return nested
}

function parseFrequency(value: string): PipelineSchedule['frequency'] {
  return value === 'weekly' || value === 'monthly' ? value : 'daily'
}

function Section({ index, title, meta, actions, children, className = '' }: { index: number; title: string; meta?: ReactNode; actions?: ReactNode; children: ReactNode; className?: string }) {
  return <section className={`console-section ${className}`} id={`console-section-${index}`} aria-labelledby={`console-section-${index}-title`}>
    <header><span aria-hidden="true"><HotkeyKbd hotkey={adminScope.byId[`section-${index}`]} /></span><h2 id={`console-section-${index}-title`} tabIndex={-1}>{title}</h2>{meta && <span className="console-section__meta">{meta}</span>}{actions && <div className="console-section__actions">{actions}</div>}</header>
    {children}
  </section>
}

function focusSection(index: number) {
  const section = document.getElementById(`console-section-${index}`)
  section?.scrollIntoView?.({ block: 'nearest', behavior: 'smooth' })
  ;(section?.querySelector<HTMLElement>('[data-row][tabindex="0"]') ?? section?.querySelector<HTMLElement>('input, select, button, h2'))?.focus({ preventScroll: true })
}

/** A one-time link with a copy button. */
function OneTimeLink({ label, url, onDismiss }: { label: string; url: string; onDismiss: () => void }) {
  const [copied, setCopied] = useState(false)
  const copy = async () => {
    try { await navigator.clipboard.writeText(url); setCopied(true) } catch { setCopied(false) }
  }
  return <div className="console-link" role="status">
    <strong>{label}</strong>
    <code>{url}</code>
    <button type="button" className="button button--secondary button--small" onClick={() => void copy()}><Copy aria-hidden="true" />{copied ? 'Copied' : 'Copy'}</button>
    <button type="button" className="text-button" onClick={onDismiss}>Dismiss</button>
    <small>Shown once. It works a single time; share it privately.</small>
  </div>
}

export default function AdminPage() {
  const auth = useAuth()
  const client = useQueryClient()
  const [filter, setFilter] = useState('')
  const [cursor, setCursor] = useState(0)
  const [armed, setArmed] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [link, setLink] = useState<{ label: string; url: string } | null>(null)
  const [inviteRole, setInviteRole] = useState('member')
  const [inviteEmail, setInviteEmail] = useState('')
  // "Last 24 hours" and "last 7 days" are measured from when the page opened.
  const [now] = useState(() => Date.now())
  const filterInput = useRef<HTMLInputElement>(null)
  const usersBody = useRef<HTMLTableSectionElement>(null)
  const inviteButton = useRef<HTMLButtonElement>(null)

  const users = useQuery({ queryKey: ['admin-users'], queryFn: () => apiRequest<AdminUser[]>('/api/admin/auth/users') })
  const audit = useQuery({ queryKey: ['admin-audit'], queryFn: () => apiRequest<AuditEvent[]>('/api/admin/auth/audit?limit=300'), refetchInterval: 60_000 })
  const settings = useQuery({ queryKey: ['admin-auth-settings'], queryFn: () => apiRequest<AuthSettings>('/api/admin/auth/settings') })
  const usersById = useMemo(() => new Map((users.data ?? []).map(user => [user.user_id, user])), [users.data])
  const shown = useMemo(() => (users.data ?? []).filter(user => `${user.username} ${user.email ?? ''} ${user.role} ${user.status}`.toLowerCase().includes(filter.trim().toLowerCase())), [users.data, filter])
  const index = Math.min(cursor, Math.max(0, shown.length - 1))
  const current = shown[index] as AdminUser | undefined

  const fail = (err: unknown) => setError(err instanceof Error ? err.message : String(err))
  const updateRole = useMutation({
    mutationFn: ({ userId, role }: { userId: string; role: string }) => apiRequest(`/api/admin/auth/users/${encodeURIComponent(userId)}/role`, { method: 'PATCH', body: JSON.stringify({ role }) }),
    onSuccess: () => { setError(null); void client.invalidateQueries({ queryKey: ['admin-users'] }) },
    onError: fail,
  })
  const disableUser = useMutation({
    mutationFn: (userId: string) => apiRequest(`/api/admin/auth/users/${encodeURIComponent(userId)}/disable`, { method: 'PATCH' }),
    onSuccess: () => { setError(null); void client.invalidateQueries({ queryKey: ['admin-users'] }) },
    onError: fail,
  })
  // One-time links are opened by other people, so they carry the public
  // address while a tunnel is open. It is asked for each link, because a
  // temporary address changes whenever the tunnel starts again.
  const linkOrigin = () => client.fetchQuery({ queryKey: TUNNEL_KEY, queryFn: fetchTunnel })
    .then(tunnel => (tunnel.state === 'running' && tunnel.url ? tunnel.url : window.location.origin), () => window.location.origin)
  const resetLink = useMutation({
    mutationFn: async (user: AdminUser) => {
      const result = await apiRequest<{ reset_token: string }>(`/api/admin/auth/credential-resets?target_user_id=${encodeURIComponent(user.user_id)}`, { method: 'POST' })
      return { user, token: result.reset_token, origin: await linkOrigin() }
    },
    onSuccess: ({ user, token, origin }) => setLink({ label: `Password-reset link for ${user.username}`, url: `${origin}/login?reset=${encodeURIComponent(token)}` }),
    onError: fail,
  })
  const invite = useMutation({
    mutationFn: async () => {
      const result = await apiRequest<{ invitation_token: string }>('/api/admin/auth/invitations', { method: 'POST', body: JSON.stringify({ role: inviteRole, email: inviteEmail.trim() || undefined }) })
      return { token: result.invitation_token, origin: await linkOrigin() }
    },
    onSuccess: ({ token, origin }) => { setInviteEmail(''); setLink({ label: `Invitation link (${inviteRole})`, url: `${origin}/register?invite=${encodeURIComponent(token)}` }) },
    onError: fail,
  })

  useEffect(() => {
    if (!armed) return
    const timer = setTimeout(() => setArmed(null), 4000)
    return () => clearTimeout(timer)
  }, [armed])

  const focusRow = (next: number) => {
    const bounded = Math.max(0, Math.min(shown.length - 1, next))
    setCursor(bounded)
    usersBody.current?.querySelectorAll<HTMLElement>('[data-row]')[bounded]?.focus()
  }
  const disable = (user: AdminUser) => {
    if (user.user_id === auth.user?.user_id) { setError('You cannot disable your own account here.'); return }
    if (armed === `disable:${user.user_id}`) { setArmed(null); disableUser.mutate(user.user_id) } else setArmed(`disable:${user.user_id}`)
  }
  const onUsersKey = (event: KeyboardEvent<HTMLTableSectionElement>) => {
    if ((event.target as HTMLElement).tagName === 'SELECT') return
    const key = event.key
    if (key === 'ArrowDown' || key === 'j') { event.preventDefault(); focusRow(index + 1) }
    else if (key === 'ArrowUp' || key === 'k') { event.preventDefault(); focusRow(index - 1) }
    else if (key === 'Enter' && current) { event.preventDefault(); usersBody.current?.querySelectorAll<HTMLSelectElement>('select')[index]?.focus() }
    else if (key === 'x' && current?.status === 'active') { event.preventDefault(); event.stopPropagation(); disable(current) }
    else if (key === 'p' && current) { event.preventDefault(); event.stopPropagation(); resetLink.mutate(current) }
  }

  useHotkeyScope(adminScope, {
    ...Object.fromEntries(SECTIONS.map((_, position) => [`section-${position + 1}`, () => focusSection(position + 1)])),
    filter: () => filterInput.current?.focus(),
    invite: () => inviteButton.current?.focus(),
  })

  const allUsers = users.data ?? []
  const day = 24 * 3600 * 1000
  const failures = (audit.data ?? []).filter(event => event.event_type === 'login_failed' && now - new Date(event.occurred_at).getTime() < day).length

  return <div className="console-page">
    <header className="console-head">
      <div><span className="eyebrow">Administration</span><h1>Accounts and access</h1></div>
      <nav className="console-jump" aria-label="Sections">{SECTIONS.map((section, position) => <button key={section} type="button" onClick={() => focusSection(position + 1)}><HotkeyKbd hotkey={adminScope.byId[`section-${position + 1}`]} />{section}</button>)}</nav>
      <HotkeyHelpButton />
    </header>
    <div className="console-kpis">
      <div><span>Accounts</span><strong>{allUsers.length}</strong><small>{allUsers.filter(user => user.status === 'active').length} active · {allUsers.filter(user => user.status !== 'active').length} disabled</small></div>
      <div><span>Administrators</span><strong>{allUsers.filter(user => user.role === 'admin').length}</strong><small>{allUsers.filter(user => user.role === 'operator').length} operators</small></div>
      <div><span>Signed in, 7 days</span><strong>{allUsers.filter(user => user.last_login_at && now - new Date(user.last_login_at).getTime() < 7 * day).length}</strong><small>of {allUsers.length}</small></div>
      <div className={failures ? 'is-alarm' : undefined}><span>Failed logins, 24 h</span><strong>{failures}</strong><small>from the audit log</small></div>
      <div><span>Registration</span><strong>{settings.data?.registration_mode ?? '—'}</strong><small>new accounts are {settings.data?.default_role ?? '—'}s</small></div>
      <div><span>Password minimum</span><strong>{settings.data?.password_min_length ?? '—'}</strong><small>characters</small></div>
    </div>
    {error && <p className="form-error" role="alert">{error} <button type="button" className="text-button" onClick={() => setError(null)}>Dismiss</button></p>}
    {link && <OneTimeLink label={link.label} url={link.url} onDismiss={() => setLink(null)} />}

    <div className="console-grid">
      <div className="console-column">
        <Section index={1} title="Users" meta={`${shown.length} of ${allUsers.length}`} actions={<input ref={filterInput} className="input" placeholder="Filter (F)" aria-label="Filter users" value={filter} onChange={event => { setFilter(event.target.value); setCursor(0) }} onKeyDown={event => { if (event.key === 'ArrowDown' || event.key === 'Enter') { event.preventDefault(); focusRow(0) } if (event.key === 'Escape') { setFilter(''); event.currentTarget.blur() } }} />}>
          {users.isLoading ? <LoadingState label="Loading users" /> : <div className="console-scroll"><table className="console-table">
            <thead><tr><th>User</th><th>Email</th><th>Role</th><th>Status</th><th>Created</th><th>Last login</th><th><span className="sr-only">Actions</span></th></tr></thead>
            <tbody ref={usersBody} onKeyDown={onUsersKey}>
              {shown.map((user, position) => <tr key={user.user_id} data-row tabIndex={position === index ? 0 : -1} className={position === index ? 'is-cursor' : undefined} onFocus={() => setCursor(position)} onClick={() => setCursor(position)}>
                <td><Link to={`/people/${encodeURIComponent(user.username)}`}>{user.username}</Link>{user.user_id === auth.user?.user_id && <small> (you)</small>}</td>
                <td className="console-muted">{user.email || '—'}</td>
                <td><select className="select" aria-label={`Role of ${user.username}`} value={user.role} disabled={user.status !== 'active' || updateRole.isPending} onChange={event => updateRole.mutate({ userId: user.user_id, role: event.target.value })}><option value="admin">Admin</option><option value="operator">Operator</option><option value="member">Member</option></select></td>
                <td><span className={user.status === 'active' ? 'console-pill' : 'console-pill console-pill--off'}>{user.status}</span></td>
                <td className="console-mono">{when(user.created_at)}</td>
                <td className="console-mono">{when(user.last_login_at)}</td>
                <td className="console-actions">
                  <button type="button" className="text-button" onClick={() => resetLink.mutate(user)} title="Make a one-time password-reset link (P)"><RotateCcw aria-hidden="true" />Reset</button>
                  {user.status === 'active' && user.user_id !== auth.user?.user_id && <button type="button" className={armed === `disable:${user.user_id}` ? 'text-button is-danger' : 'text-button'} onClick={() => disable(user)} title="Disable the account (X twice)">{armed === `disable:${user.user_id}` ? 'Disable: sure?' : 'Disable'}</button>}
                </td>
              </tr>)}
            </tbody>
          </table></div>}
        </Section>

        <Section index={2} title="Invite and reset" meta="one-time links">
          <form className="console-inline" onSubmit={(event: FormEvent) => { event.preventDefault(); invite.mutate() }}>
            <label className="console-field"><span>Role</span><select className="select" value={inviteRole} onChange={event => setInviteRole(event.target.value)}><option value="member">Member</option><option value="operator">Operator</option><option value="admin">Admin</option></select></label>
            <label className="console-field console-field--grow"><span>Email (optional, to restrict it)</span><input className="input" type="email" value={inviteEmail} onChange={event => setInviteEmail(event.target.value)} /></label>
            <button ref={inviteButton} type="submit" className="button button--primary button--small" disabled={invite.isPending}><UserPlus aria-hidden="true" />Create invitation <HotkeyKbd hotkey={adminScope.byId.invite} /></button>
          </form>
          <p className="console-muted"><Link2 aria-hidden="true" /> Invitations open registration for one person even when it is closed or invitation-only. For a forgotten password, use <em>Reset</em> on the user’s row (<kbd>P</kbd>): the link sets a new password once. Links carry the tunnel’s address while one is open under Remote access, and this page’s address otherwise.</p>
        </Section>
        <Section index={6} title="Server settings" meta="stored in app.db" actions={<ShutdownServer />}><ServerSettingsSection /></Section>
      </div>

      <div className="console-column">
        <Section index={3} title="Access" meta={settings.data?.updated_at ? `changed ${when(settings.data.updated_at)}` : 'deployment defaults'}>
          {settings.data ? <AccessForm key={settings.data.updated_at ?? 'defaults'} settings={settings.data} /> : <LoadingState label="Loading settings" />}
        </Section>
        <Section index={7} title="Remote access" meta="Cloudflare tunnel"><RemoteAccessSection registrationOpen={settings.data?.registration_mode === 'open'} /></Section>
        <Section index={4} title="Pipeline schedules"><SchedulesSection /></Section>
        <Section index={5} title="Audit log" meta={audit.data ? `${audit.data.length} latest events` : undefined}>
          <AuditTable events={audit.data ?? []} users={usersById} loading={audit.isLoading} />
        </Section>
      </div>
    </div>
  </div>
}

function AccessForm({ settings }: { settings: AuthSettings }) {
  const client = useQueryClient()
  const [draft, setDraft] = useState(settings)
  const [message, setMessage] = useState<string | null>(null)
  const save = useMutation({
    mutationFn: () => apiRequest<AuthSettings>('/api/admin/auth/settings', { method: 'PATCH', body: JSON.stringify({
      registration_mode: draft.registration_mode, default_role: draft.default_role, password_min_length: draft.password_min_length,
      access_token_seconds: draft.access_token_seconds, refresh_idle_seconds: draft.refresh_idle_seconds,
      refresh_absolute_seconds: draft.refresh_absolute_seconds ?? undefined,
    }) }),
    onSuccess: () => { setMessage('Saved.'); void client.invalidateQueries({ queryKey: ['admin-auth-settings'] }) },
    onError: (err: Error) => setMessage(err.message),
  })
  const set = (patch: Partial<AuthSettings>) => { setMessage(null); setDraft(current => ({ ...current, ...patch })) }
  const valid = draft.password_min_length >= 5 && draft.password_min_length <= 128
  return <form className="console-form" onSubmit={event => { event.preventDefault(); if (valid) save.mutate() }}>
    <label className="console-field"><span>Registration</span><select className="select" value={draft.registration_mode} onChange={event => set({ registration_mode: event.target.value as AuthSettings['registration_mode'] })}><option value="open">Open: anyone with the address</option><option value="invite">Invitation only</option><option value="closed">Closed</option></select></label>
    <label className="console-field"><span>New accounts are</span><select className="select" value={draft.default_role} onChange={event => set({ default_role: event.target.value as AuthSettings['default_role'] })}><option value="member">Members</option><option value="operator">Operators</option></select></label>
    <label className="console-field"><span>Password minimum</span><input className="input" type="number" min={5} max={128} value={draft.password_min_length} onChange={event => set({ password_min_length: Number(event.target.value) })} /></label>
    <label className="console-field"><span>Access token (minutes)</span><input className="input" type="number" min={1} max={1440} value={Math.round(draft.access_token_seconds / 60)} onChange={event => set({ access_token_seconds: Number(event.target.value) * 60 })} /></label>
    <label className="console-field"><span>Signed out after idle (days)</span><input className="input" type="number" min={1} max={365} value={Math.round(draft.refresh_idle_seconds / 86400)} onChange={event => set({ refresh_idle_seconds: Number(event.target.value) * 86400 })} /></label>
    <label className="console-field"><span>Session limit (days)</span><input className="input" type="number" min={1} max={365} placeholder="none" value={draft.refresh_absolute_seconds ? Math.round(draft.refresh_absolute_seconds / 86400) : ''} onChange={event => set({ refresh_absolute_seconds: event.target.value ? Number(event.target.value) * 86400 : null })} /></label>
    <div className="console-form__actions">
      <button type="submit" className="button button--primary button--small" disabled={!valid || save.isPending}>{save.isPending ? 'Saving…' : 'Save access settings'}</button>
      {draft.registration_mode === 'open' && <small className="console-warn">Open registration lets anyone who reaches this address create an account. Before sharing a public link, consider invitation only.</small>}
      {message && <small role="status">{message}</small>}
    </div>
  </form>
}

function settingText(setting: ServerSetting): string {
  if (setting.kind === 'secret') return ''
  if (Array.isArray(setting.value)) return setting.value.join(', ')
  return setting.value === null ? '' : String(setting.value)
}

function settingValue(setting: ServerSetting, text: string): unknown {
  if (setting.kind === 'integer') return Number(text)
  if (setting.kind === 'list') return text.split(',').map(item => item.trim()).filter(Boolean)
  return text.trim()
}

function useServerSettings() {
  return useQuery({ queryKey: ['admin-server-settings'], queryFn: () => apiRequest<{ settings: ServerSetting[] }>('/api/admin/settings') })
}

/** The tunnel's settings are edited under Remote access, next to its status. */
const isTunnelSetting = (setting: ServerSetting) => setting.key.startsWith('tunnel.')

function ServerSettingsSection() {
  const settings = useServerSettings()
  const [restart, setRestart] = useState(false)
  if (!settings.data) return <LoadingState label="Loading settings" />
  return <div className="console-settings">
    <p className="console-muted">Everything the server needs is configured here, or with <code>main.py config</code>. Settings marked * apply after a restart; how the server listens (host, port, remote access) is chosen when it is started.</p>
    {restart && <p className="console-warn" role="status">Restart the server to apply the change.</p>}
    {settings.data.settings.filter(setting => !isTunnelSetting(setting)).map(setting => <SettingRow key={`${setting.key}:${setting.updated_at ?? 'default'}`} setting={setting} onSaved={saved => { if (saved.restart_required) setRestart(true) }} />)}
  </div>
}

const TUNNEL_STATE_LABELS: Record<TunnelStatus['state'], string> = { off: 'Off', blocked: 'Blocked', downloading: 'Starting', starting: 'Starting', running: 'Public', failed: 'Failed' }

/**
 * Publishing the workstation through a Cloudflare tunnel. Turning it on or
 * off saves the `tunnel.enabled` setting, which the server follows at once.
 */
function RemoteAccessSection({ registrationOpen }: { registrationOpen: boolean }) {
  const client = useQueryClient()
  const tunnel = useQuery({
    queryKey: TUNNEL_KEY,
    queryFn: fetchTunnel,
    // Follow the tunnel closely while it is coming up or retrying.
    refetchInterval: query => (['off', 'running'].includes(query.state.data?.state ?? 'off') ? 30_000 : 1500),
  })
  const settings = useServerSettings()
  const [copied, setCopied] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const refresh = () => { setError(null); setCopied(false); void client.invalidateQueries({ queryKey: TUNNEL_KEY }) }
  const failed = (err: Error) => setError(err.message)
  const turn = useMutation({
    mutationFn: (on: boolean) => apiRequest<ServerSetting>('/api/admin/settings/tunnel.enabled', { method: 'PUT', body: JSON.stringify({ value: on ? 'on' : 'off' }) }),
    onSuccess: () => { refresh(); void client.invalidateQueries({ queryKey: ['admin-server-settings'] }) },
    onError: failed,
  })
  const restart = useMutation({ mutationFn: () => apiRequest<TunnelStatus>('/api/admin/server/tunnel/restart', { method: 'POST' }), onSuccess: refresh, onError: failed })
  if (!tunnel.data) return tunnel.isError ? <p className="form-error" role="alert">{tunnel.error.message}</p> : <LoadingState label="Loading tunnel" />
  const { enabled, kind, state, url, message, origin, blocked_reason: blocked } = tunnel.data
  const token = settings.data?.settings.find(setting => setting.key === 'tunnel.token')
  const busy = turn.isPending || restart.isPending
  const copy = async () => {
    try { await navigator.clipboard.writeText(url ?? ''); setCopied(true) } catch { setCopied(false) }
  }
  return <div className="console-tunnel">
    <p className="console-muted">Publishes this workstation on the internet through Cloudflare, without opening a port on this machine. Visitors sign in as usual.</p>
    <div className="console-tunnel__status" role="status">
      <span className={state === 'off' ? 'console-muted' : state === 'blocked' || state === 'failed' ? 'console-pill console-pill--off' : 'console-pill'}>{TUNNEL_STATE_LABELS[state]}</span>
      {state === 'running' && url && <><a href={url} target="_blank" rel="noreferrer">{url}</a><button type="button" className="text-button" onClick={() => void copy()}><Copy aria-hidden="true" />{copied ? 'Copied' : 'Copy'}</button></>}
      {state === 'running' && !url && <span>Connected. Its address is the public hostname set for this tunnel in Cloudflare.</span>}
      {state === 'starting' && <span>Connecting to Cloudflare…</span>}
      {state === 'downloading' && <span>Downloading cloudflared (about 40 MB, once)…</span>}
      {state === 'failed' && <span>{message} It is tried again automatically.</span>}
      {(state === 'blocked' || (state === 'off' && blocked)) && <span>{message ?? blocked}</span>}
      <span className="console-tunnel__actions">
        {state === 'failed' && <button type="button" className="text-button" disabled={busy} onClick={() => restart.mutate()}>Try again</button>}
        {state === 'running' && kind === 'quick' && <button type="button" className="text-button" disabled={busy} onClick={() => restart.mutate()} title="Close this address and open a new one">New address</button>}
        <button type="button" className="button button--secondary button--small" disabled={busy || (!enabled && Boolean(blocked))} onClick={() => turn.mutate(!enabled)}>{enabled ? 'Turn off' : 'Turn on'}</button>
      </span>
    </div>
    {state === 'running' && kind === 'quick' && <small className="console-muted">A temporary address: it changes whenever the tunnel or the server starts again, and a new one can take a minute to start working. Add a token below for an address that stays.</small>}
    {registrationOpen && <small className="console-warn">Registration is open: anyone with the address can create an account. Under Access, set Registration to Invitation only.</small>}
    {error && <small className="form-error" role="alert">{error}</small>}
    {token && <SettingRow key={`${token.key}:${token.updated_at ?? 'default'}`} setting={token} onSaved={refresh} />}
    <small className="console-muted">For a token, create a tunnel in Cloudflare Zero Trust and give it a public hostname with the service <code>{origin}</code> and <em>No TLS Verify</em> turned on.</small>
  </div>
}

function SettingRow({ setting, onSaved }: { setting: ServerSetting; onSaved: (saved: ServerSetting) => void }) {
  const client = useQueryClient()
  const [draft, setDraft] = useState(() => settingText(setting))
  const [message, setMessage] = useState<string | null>(null)
  const path = `/api/admin/settings/${encodeURIComponent(setting.key)}`
  const done = (saved: ServerSetting, text: string) => { setMessage(text); onSaved(saved); void client.invalidateQueries({ queryKey: ['admin-server-settings'] }) }
  const save = useMutation({
    mutationFn: () => apiRequest<ServerSetting>(path, { method: 'PUT', body: JSON.stringify({ value: settingValue(setting, draft) }) }),
    onSuccess: saved => done(saved, 'Saved.'),
    onError: (err: Error) => setMessage(err.message),
  })
  const reset = useMutation({
    mutationFn: () => apiRequest<ServerSetting>(path, { method: 'DELETE' }),
    onSuccess: saved => done(saved, setting.kind === 'secret' ? 'Cleared.' : 'Back to the default.'),
    onError: (err: Error) => setMessage(err.message),
  })
  const label = `${setting.label}${setting.restart_required ? ' *' : ''}`
  const input = setting.kind === 'choice'
    ? <select className="select" value={draft} onChange={event => setDraft(event.target.value)}>{setting.choices.map(choice => <option key={choice}>{choice}</option>)}</select>
    : <input
      className="input"
      type={setting.kind === 'secret' ? 'password' : setting.kind === 'integer' ? 'number' : 'text'}
      autoComplete="off"
      min={setting.minimum ?? undefined}
      placeholder={setting.kind === 'secret' ? (setting.is_set ? 'Set; type a new value to replace it' : 'Not set') : setting.kind === 'path' ? 'In the data folder' : undefined}
      value={draft}
      onChange={event => { setDraft(event.target.value); setMessage(null) }}
    />
  const unchanged = setting.kind !== 'secret' && draft === settingText(setting)
  return <form className="console-setting" onSubmit={event => { event.preventDefault(); save.mutate() }}>
    <label className="console-field console-field--grow"><span>{label}</span>{input}</label>
    <div className="console-setting__actions">
      <button type="submit" className="button button--secondary button--small" disabled={save.isPending || unchanged || (setting.kind === 'secret' && !draft)}>Save</button>
      {setting.is_set && <button type="button" className="text-button" disabled={reset.isPending} onClick={() => reset.mutate()}>{setting.kind === 'secret' ? 'Clear' : 'Default'}</button>}
    </div>
    <small className="console-muted">{setting.description}<span className="sr-only"> ({setting.key})</span></small>
    {message && <small role="status">{message}</small>}
  </form>
}

function AuditTable({ events, users, loading }: { events: AuditEvent[]; users: Map<string, AdminUser>; loading: boolean }) {
  const [query, setQuery] = useState('')
  const [onlyAlarming, setOnlyAlarming] = useState(false)
  const shown = events.filter(event => (!onlyAlarming || ALARMING.has(event.event_type))
    && `${EVENT_LABELS[event.event_type] ?? event.event_type} ${users.get(event.user_id ?? '')?.username ?? event.user_id ?? ''} ${event.detail ?? ''} ${event.remote_addr ?? ''}`.toLowerCase().includes(query.trim().toLowerCase()))
  if (loading) return <LoadingState label="Loading audit log" />
  return <>
    <div className="console-inline">
      <input className="input" placeholder="Filter events" aria-label="Filter audit events" value={query} onChange={event => setQuery(event.target.value)} />
      <label className="console-check"><input type="checkbox" checked={onlyAlarming} onChange={event => setOnlyAlarming(event.target.checked)} />Failures and security events only</label>
    </div>
    <div className="console-scroll console-scroll--audit"><table className="console-table">
      <thead><tr><th>Time</th><th>Event</th><th>User</th><th>From</th><th>Detail</th></tr></thead>
      <tbody>{shown.map(event => <tr key={event.event_id} className={ALARMING.has(event.event_type) ? 'is-alarm' : undefined}>
        <td className="console-mono">{when(event.occurred_at)}</td>
        <td>{EVENT_LABELS[event.event_type] ?? event.event_type}</td>
        <td>{users.get(event.user_id ?? '')?.username ?? (event.user_id ? event.user_id.slice(0, 8) : '—')}</td>
        <td className="console-mono">{event.remote_addr ?? '—'}</td>
        <td className="console-muted" title={event.detail ?? ''}>{event.detail || '—'}</td>
      </tr>)}</tbody>
    </table></div>
  </>
}

function SchedulesSection() {
  const client = useQueryClient()
  const setups = readSavedSetups()
  const [setupName, setSetupName] = useState(setups[0]?.name ?? '')
  const [scheduleName, setScheduleName] = useState(setups[0]?.name ?? '')
  const [frequency, setFrequency] = useState<PipelineSchedule['frequency']>('daily')
  const [message, setMessage] = useState<string | null>(null)
  const schedules = useQuery({ queryKey: ['admin-pipeline-schedules'], queryFn: () => apiRequest<PipelineSchedule[]>('/api/admin/pipeline-schedules'), refetchInterval: 60_000 })
  const status = useQuery({ queryKey: ['admin-pipeline-scheduler-status'], queryFn: () => apiRequest<PipelineSchedulerStatus>('/api/admin/pipeline-schedules/status'), refetchInterval: 30_000 })
  const done = (text: string) => { setMessage(text); void client.invalidateQueries({ queryKey: ['admin-pipeline-schedules'] }) }
  const failed = (err: Error) => setMessage(err.message)
  const checkNow = useMutation({
    mutationFn: () => apiRequest<PipelineSchedulerStatus>('/api/admin/pipeline-schedules/check', { method: 'POST' }),
    onSuccess: result => { client.setQueryData(['admin-pipeline-scheduler-status'], result); done(result.triggered_job_ids.length ? `Started ${result.triggered_job_ids.length} scheduled run.` : 'Checked: nothing was due.') },
    onError: failed,
  })
  const create = useMutation({
    mutationFn: () => {
      const setup = setups.find(item => item.name === setupName)
      if (!setup) throw new Error('Save a pipeline sequence on the Data pipeline page first.')
      return apiRequest<PipelineSchedule>('/api/admin/pipeline-schedules', { method: 'POST', body: JSON.stringify({ name: scheduleName.trim() || setup.name, frequency, enabled: true, steps: setup.steps.map(step => ({ name: step.name, overwrite: Boolean(step.overwrite) })), config: pipelineConfig(setup) }) })
    },
    onSuccess: () => done('Schedule saved.'),
    onError: failed,
  })
  const update = useMutation({
    mutationFn: ({ id, changes }: { id: string; changes: Partial<Pick<PipelineSchedule, 'enabled' | 'frequency'>> }) => apiRequest(`/api/admin/pipeline-schedules/${encodeURIComponent(id)}`, { method: 'PATCH', body: JSON.stringify(changes) }),
    onSuccess: () => done('Schedule updated.'),
    onError: failed,
  })
  const remove = useMutation({ mutationFn: (id: string) => apiRequest(`/api/admin/pipeline-schedules/${encodeURIComponent(id)}`, { method: 'DELETE' }), onSuccess: () => done('Schedule deleted.'), onError: failed })
  const reset = useMutation({ mutationFn: (id: string) => apiRequest(`/api/admin/pipeline-schedules/${encodeURIComponent(id)}/reset-last-run`, { method: 'POST' }), onSuccess: () => done('Due on the next check.'), onError: failed })
  return <div className="console-form console-form--schedules">
    <p className="console-muted">Checked every five minutes; never while another run is active. Next check {status.data?.next_check_at ? when(status.data.next_check_at) : '…'}{status.data?.active_pipeline ? ' · a run is active, so checks wait' : ''}. <button type="button" className="text-button" disabled={checkNow.isPending} onClick={() => checkNow.mutate()}>Check now</button></p>
    {setups.length ? <div className="console-inline">
      <label className="console-field"><span>Saved sequence</span><select className="select" value={setupName} onChange={event => setSetupName(event.target.value)}>{setups.map(setup => <option key={setup.name}>{setup.name}</option>)}</select></label>
      <label className="console-field console-field--grow"><span>Name</span><input className="input" value={scheduleName} onChange={event => setScheduleName(event.target.value)} /></label>
      <label className="console-field"><span>Every</span><select className="select" value={frequency} onChange={event => setFrequency(parseFrequency(event.target.value))}><option value="daily">Day</option><option value="weekly">Week</option><option value="monthly">Month</option></select></label>
      <button type="button" className="button button--secondary button--small" disabled={create.isPending} onClick={() => create.mutate()}>Add</button>
    </div> : <p className="console-muted">Save a sequence on the Data pipeline page to schedule it.</p>}
    {schedules.data?.length ? <table className="console-table">
      <thead><tr><th>Schedule</th><th>Every</th><th>On</th><th>Last run</th><th /></tr></thead>
      <tbody>{schedules.data.map(item => <tr key={item.schedule_id}>
        <td title={item.steps.map(step => step.name).join(' → ')}>{item.name}</td>
        <td><select className="select" aria-label={`Frequency of ${item.name}`} value={item.frequency} onChange={event => update.mutate({ id: item.schedule_id, changes: { frequency: parseFrequency(event.target.value) } })}><option value="daily">Day</option><option value="weekly">Week</option><option value="monthly">Month</option></select></td>
        <td><input type="checkbox" aria-label={`Enable ${item.name}`} checked={item.enabled} onChange={event => update.mutate({ id: item.schedule_id, changes: { enabled: event.target.checked } })} /></td>
        <td className="console-mono">{when(item.last_run_at)}</td>
        <td className="console-actions"><button type="button" className="text-button" onClick={() => reset.mutate(item.schedule_id)}>Run next check</button><button type="button" className="text-button" onClick={() => remove.mutate(item.schedule_id)}>Delete</button></td>
      </tr>)}</tbody>
    </table> : !schedules.isLoading && <p className="console-muted">No schedules.</p>}
    {message && <small role="status">{message}</small>}
  </div>
}
