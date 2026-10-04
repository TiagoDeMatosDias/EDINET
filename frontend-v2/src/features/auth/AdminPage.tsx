import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Copy, Keyboard, Link2, RotateCcw, UserPlus } from 'lucide-react'
import { useCallback, useEffect, useMemo, useRef, useState, type FormEvent, type KeyboardEvent, type ReactNode } from 'react'
import { Link } from 'react-router-dom'

import { apiRequest } from '../../api/client'
import { LoadingState } from '../../components/Feedback'
import { ShortcutsDialog, type ShortcutGroup } from '../../components/ShortcutsDialog'
import { useHotkeys } from '../../hooks/useHotkeys'
import { useAuth } from './authContext'
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

type SavedSetup = { name: string; steps: Array<{ name: string; overwrite?: boolean }>; config: Record<string, unknown> }
type PipelineSchedule = { schedule_id: string; name: string; frequency: 'daily' | 'weekly' | 'monthly'; enabled: boolean; steps: Array<{ name: string; overwrite?: boolean }>; config: Record<string, unknown>; last_run_at: string | null }
type PipelineSchedulerStatus = { checked_at: string; next_check_at: string; active_pipeline: boolean; triggered_job_ids: string[] }

const SETUPS_KEY = 'shade.pipeline.setups'
const EVENT_LABELS: Record<string, string> = {
  account_created: 'Account created', login_succeeded: 'Login', login_failed: 'Failed login', logout: 'Logout',
  refresh_succeeded: 'Token refresh', refresh_reuse_detected: 'Refresh reuse', password_changed: 'Password changed',
  password_change_failed: 'Failed password change', profile_updated: 'Profile updated', session_revoked: 'Session revoked',
  all_sessions_revoked: 'All sessions revoked', api_token_created: 'API token created', user_disabled: 'User disabled', role_changed: 'Role changed',
}
const ALARMING = new Set(['login_failed', 'refresh_reuse_detected', 'password_change_failed', 'user_disabled'])

const SECTIONS = ['Users', 'Invite and reset', 'Access', 'Pipeline schedules', 'Audit log'] as const
const SHORTCUTS: ShortcutGroup[] = [
  { title: 'Administration', shortcuts: [
    { keys: ['1', '2', '3', '4', '5'], label: SECTIONS.join(', ') },
    { keys: ['F'], label: 'Filter users' },
    { keys: ['J', 'K'], label: 'Users or audit log: next or previous row (↓ ↑)' },
    { keys: ['Enter'], label: 'Users: change the role' },
    { keys: ['P'], label: 'Users: make a password-reset link' },
    { keys: ['X'], label: 'Users: disable the account (press twice)' },
    { keys: ['I'], label: 'Create an invitation link' },
    { keys: ['?'], label: 'Show or hide this list' },
  ] },
]

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
    <header><kbd aria-hidden="true">{index}</kbd><h2 id={`console-section-${index}-title`} tabIndex={-1}>{title}</h2>{meta && <span className="console-section__meta">{meta}</span>}{actions && <div className="console-section__actions">{actions}</div>}</header>
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
  const [help, setHelp] = useState(false)
  const closeHelp = useCallback(() => setHelp(false), [])
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
  const resetLink = useMutation({
    mutationFn: (user: AdminUser) => apiRequest<{ reset_token: string }>(`/api/admin/auth/credential-resets?target_user_id=${encodeURIComponent(user.user_id)}`, { method: 'POST' }).then(result => ({ user, token: result.reset_token })),
    onSuccess: ({ user, token }) => setLink({ label: `Password-reset link for ${user.username}`, url: `${window.location.origin}/login?reset=${encodeURIComponent(token)}` }),
    onError: fail,
  })
  const invite = useMutation({
    mutationFn: () => apiRequest<{ invitation_token: string }>('/api/admin/auth/invitations', { method: 'POST', body: JSON.stringify({ role: inviteRole, email: inviteEmail.trim() || undefined }) }),
    onSuccess: result => { setInviteEmail(''); setLink({ label: `Invitation link (${inviteRole})`, url: `${window.location.origin}/register?invite=${encodeURIComponent(result.invitation_token)}` }) },
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

  useHotkeys({
    ...Object.fromEntries(SECTIONS.map((_, position) => [String(position + 1), () => focusSection(position + 1)])),
    f: () => filterInput.current?.focus(),
    i: () => inviteButton.current?.focus(),
    '?': () => setHelp(true),
  }, !help)

  const allUsers = users.data ?? []
  const day = 24 * 3600 * 1000
  const failures = (audit.data ?? []).filter(event => event.event_type === 'login_failed' && now - new Date(event.occurred_at).getTime() < day).length

  return <div className="console-page">
    <header className="console-head">
      <div><span className="eyebrow">Administration</span><h1>Accounts and access</h1></div>
      <nav className="console-jump" aria-label="Sections">{SECTIONS.map((section, position) => <button key={section} type="button" onClick={() => focusSection(position + 1)}><kbd>{position + 1}</kbd>{section}</button>)}</nav>
      <button type="button" className="icon-button" onClick={() => setHelp(true)} title="Keyboard shortcuts (?)" aria-label="Keyboard shortcuts"><Keyboard /></button>
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
            <button ref={inviteButton} type="submit" className="button button--primary button--small" disabled={invite.isPending}><UserPlus aria-hidden="true" />Create invitation <kbd>I</kbd></button>
          </form>
          <p className="console-muted"><Link2 aria-hidden="true" /> Invitations open registration for one person even when it is closed or invitation-only. For a forgotten password, use <em>Reset</em> on the user’s row (<kbd>P</kbd>): the link sets a new password once. Links use this page’s address, so make them from the address you share (your tunnel URL).</p>
        </Section>
      </div>

      <div className="console-column">
        <Section index={3} title="Access" meta={settings.data?.updated_at ? `changed ${when(settings.data.updated_at)}` : 'deployment defaults'}>
          {settings.data ? <AccessForm key={settings.data.updated_at ?? 'defaults'} settings={settings.data} /> : <LoadingState label="Loading settings" />}
        </Section>
        <Section index={4} title="Pipeline schedules"><SchedulesSection /></Section>
        <Section index={5} title="Audit log" meta={audit.data ? `${audit.data.length} latest events` : undefined}>
          <AuditTable events={audit.data ?? []} users={usersById} loading={audit.isLoading} />
        </Section>
      </div>
    </div>
    {help && <ShortcutsDialog groups={SHORTCUTS} onClose={closeHelp} />}
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
