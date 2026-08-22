import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'

import { apiRequest } from '../../api/client'
import { LoadingState } from '../../components/Feedback'
import { PageHeader } from '../../components/Page'

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
  registration_mode?: string
  default_role?: string
  password_min_length: number
}

type SavedSetup = {
  name: string
  steps: Array<{ name: string; overwrite?: boolean }>
  config: Record<string, unknown>
}

type PipelineSchedule = {
  schedule_id: string
  name: string
  frequency: 'daily' | 'weekly' | 'monthly'
  enabled: boolean
  steps: Array<{ name: string; overwrite?: boolean }>
  config: Record<string, unknown>
  last_run_at: string | null
}

type PipelineSchedulerStatus = {
  checked_at: string
  next_check_at: string
  active_pipeline: boolean
  triggered_job_ids: string[]
}

const SETUPS_KEY = 'shade.pipeline.setups'

function isSavedStep(value: unknown): value is { name: string; overwrite?: boolean } {
  if (!value || typeof value !== 'object' || Array.isArray(value) || !('name' in value)) return false
  return typeof value.name === 'string'
}

function readSavedSetups(): SavedSetup[] {
  try {
    const parsed: unknown = JSON.parse(localStorage.getItem(SETUPS_KEY) ?? '[]')
    if (!Array.isArray(parsed)) return []
    const setups: SavedSetup[] = []
    for (const value of parsed) {
      if (!value || typeof value !== 'object' || Array.isArray(value) || !('name' in value) || !('steps' in value)) continue
      if (typeof value.name !== 'string' || !Array.isArray(value.steps)) continue
      const config = 'config' in value && value.config && typeof value.config === 'object' && !Array.isArray(value.config)
        ? value.config
        : {}
      setups.push({ name: value.name, steps: value.steps.filter(isSavedStep), config })
    }
    return setups
  } catch {
    return []
  }
}

function pipelineConfig(setup: SavedSetup): Record<string, unknown> {
  const nested: Record<string, unknown> = {}
  for (const [key, value] of Object.entries(setup.config)) {
    const dot = key.indexOf('.')
    if (dot < 0) {
      nested[key] = value
      continue
    }
    const stepKey = key.slice(0, dot)
    const fieldKey = key.slice(dot + 1)
    const current = nested[stepKey]
    const stepConfig = current && typeof current === 'object' && !Array.isArray(current) ? current : {}
    nested[stepKey] = { ...stepConfig, [fieldKey]: value }
  }
  return nested
}
function parseFrequency(value: string): PipelineSchedule['frequency'] {
  if (value === 'weekly' || value === 'monthly') return value
  return 'daily'
}
type AdminTab = 'users' | 'audit' | 'settings' | 'schedules'

export default function AdminPage() {
  const [tab, setTab] = useState<AdminTab>('users')

  return (
    <div className="stack dense-page">
      <PageHeader
        eyebrow="Administration"
        title="Account management"
        description="Manage users, roles, pipeline schedules, and the authentication audit log."
      />
      <div className="card">
        <div className="tabs-bar">
          <button className={tab === 'users' ? 'tab tab--active' : 'tab'} onClick={() => setTab('users')}>Users</button>
          <button className={tab === 'schedules' ? 'tab tab--active' : 'tab'} onClick={() => setTab('schedules')}>Pipeline schedules</button>
          <button className={tab === 'audit' ? 'tab tab--active' : 'tab'} onClick={() => setTab('audit')}>Audit log</button>
          <button className={tab === 'settings' ? 'tab tab--active' : 'tab'} onClick={() => setTab('settings')}>Settings</button>
        </div>
        <div className="card-body">
          {tab === 'users' && <UsersSection />}
          {tab === 'schedules' && <SchedulesSection />}
          {tab === 'audit' && <AuditSection />}
          {tab === 'settings' && <SettingsSection />}
        </div>
      </div>
    </div>
  )
}

function SchedulesSection() {
  const client = useQueryClient()
  const setups = readSavedSetups()
  const [setupName, setSetupName] = useState(setups[0]?.name ?? '')
  const [scheduleName, setScheduleName] = useState(setups[0]?.name ?? '')
  const [frequency, setFrequency] = useState<PipelineSchedule['frequency']>('daily')
  const [enabled, setEnabled] = useState(true)
  const [message, setMessage] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)
  const schedules = useQuery({
    queryKey: ['admin-pipeline-schedules'],
    queryFn: () => apiRequest<PipelineSchedule[]>('/api/admin/pipeline-schedules'),
    refetchInterval: 60_000,
  })
  const schedulerStatus = useQuery({
    queryKey: ['admin-pipeline-scheduler-status'],
    queryFn: () => apiRequest<PipelineSchedulerStatus>('/api/admin/pipeline-schedules/status'),
    refetchInterval: 30_000,
  })
  const checkNow = useMutation({
    mutationFn: () => apiRequest<PipelineSchedulerStatus>('/api/admin/pipeline-schedules/check', { method: 'POST' }),
    onSuccess: result => {
      client.setQueryData(['admin-pipeline-scheduler-status'], result)
      void client.invalidateQueries({ queryKey: ['admin-pipeline-schedules'] })
      setMessage(result.triggered_job_ids.length ? `Triggered ${result.triggered_job_ids.length} scheduled pipeline.` : 'Check complete. No schedule was due.')
      setError(null)
    },
    onError: (err: Error) => {
      setError(err.message)
      setMessage(null)
    },
  })
  const create = useMutation({
    mutationFn: () => {
      const setup = setups.find(item => item.name === setupName)
      if (!setup) throw new Error('Choose a saved pipeline sequence first.')
      return apiRequest<PipelineSchedule>('/api/admin/pipeline-schedules', {
        method: 'POST',
        body: JSON.stringify({
          name: scheduleName.trim() || setup.name,
          frequency,
          enabled,
          steps: setup.steps.map(step => ({ name: step.name, overwrite: Boolean(step.overwrite) })),
          config: pipelineConfig(setup),
        }),
      })
    },
    onSuccess: () => {
      setMessage('Pipeline schedule saved.')
      setError(null)
      void client.invalidateQueries({ queryKey: ['admin-pipeline-schedules'] })
    },
    onError: (err: Error) => {
      setError(err.message)
      setMessage(null)
    },
  })
  const update = useMutation({
    mutationFn: ({ scheduleId, changes }: { scheduleId: string; changes: Partial<Pick<PipelineSchedule, 'enabled' | 'frequency'>> }) =>
      apiRequest<PipelineSchedule>(`/api/admin/pipeline-schedules/${encodeURIComponent(scheduleId)}`, {
        method: 'PATCH',
        body: JSON.stringify(changes),
      }),
    onSuccess: () => void client.invalidateQueries({ queryKey: ['admin-pipeline-schedules'] }),
    onError: (err: Error) => setError(err.message),
  })
  const remove = useMutation({
    mutationFn: (scheduleId: string) => apiRequest<void>(`/api/admin/pipeline-schedules/${encodeURIComponent(scheduleId)}`, { method: 'DELETE' }),
    onSuccess: () => void client.invalidateQueries({ queryKey: ['admin-pipeline-schedules'] }),
    onError: (err: Error) => setError(err.message),
  })
  const resetLastRun = useMutation({
    mutationFn: (scheduleId: string) => apiRequest<PipelineSchedule>(`/api/admin/pipeline-schedules/${encodeURIComponent(scheduleId)}/reset-last-run`, { method: 'POST' }),
    onSuccess: () => {
      void client.invalidateQueries({ queryKey: ['admin-pipeline-schedules'] })
      setMessage('Last run time reset. The schedule is due on the next check.')
      setError(null)
    },
    onError: (err: Error) => setError(err.message),
  })

  return (
    <div className="stack">
      <div>
        <h3>Automatic pipeline runs</h3>
        <p className="text-muted">Select a saved sequence and interval. Enabled schedules are checked every five minutes. A check never starts a pipeline while another pipeline is pending or running.</p>
        <div className="button-row">
          <span className="text-muted">Next automatic check: {schedulerStatus.data?.next_check_at ? new Date(schedulerStatus.data.next_check_at).toLocaleString() : 'Loading…'}</span>
          <button className="button button--secondary" onClick={() => checkNow.mutate()} disabled={checkNow.isPending}>{checkNow.isPending ? 'Checking…' : 'Check now'}</button>
        </div>
        {schedulerStatus.data && <small className="text-muted">{schedulerStatus.data.active_pipeline ? 'A pipeline is currently active; automatic execution is paused.' : 'Pipeline queue is idle.'}</small>}
      </div>
      {setups.length === 0 && <p className="callout callout--warning">Save a pipeline sequence on the Data pipeline page before creating a schedule.</p>}
      <div className="field-row">
        <label className="field-label">Saved sequence<select className="select" value={setupName} onChange={event => { setSetupName(event.target.value); if (!scheduleName) setScheduleName(event.target.value) }} disabled={!setups.length}>{setups.map(setup => <option key={setup.name} value={setup.name}>{setup.name}</option>)}</select></label>
        <label className="field-label">Schedule name<input className="input" value={scheduleName} onChange={event => setScheduleName(event.target.value)} placeholder="Daily data refresh" /></label>
        <label className="field-label">Run interval<select className="select" value={frequency} onChange={event => setFrequency(parseFrequency(event.target.value))}><option value="daily">Daily</option><option value="weekly">Weekly</option><option value="monthly">Monthly</option></select></label>
      </div>
      <label className="check"><input type="checkbox" checked={enabled} onChange={event => setEnabled(event.target.checked)} />Enable automatic run</label>
      {message && <p className="form-success">{message}</p>}
      {error && <p className="form-error" role="alert">{error}</p>}
      <button className="button button--primary" disabled={create.isPending || !setups.length} onClick={() => create.mutate()}>Save schedule</button>
      <div className="stack">
        <h3>Configured schedules</h3>
        {schedules.isLoading && <LoadingState label="Loading pipeline schedules" />}
        {!schedules.isLoading && !schedules.data?.length && <p className="text-muted">No automatic pipeline schedules configured.</p>}
        {schedules.data?.map(schedule => (
          <div className="pipeline-schedule-row" key={schedule.schedule_id}>
            <div><strong>{schedule.name}</strong><small>{schedule.frequency} · {schedule.last_run_at ? `Last run ${new Date(schedule.last_run_at).toLocaleString()}` : 'Not run yet'}</small></div>
            <label className="check"><input type="checkbox" checked={schedule.enabled} onChange={event => update.mutate({ scheduleId: schedule.schedule_id, changes: { enabled: event.target.checked } })} />Enabled</label>
            <select className="select compact" value={schedule.frequency} onChange={event => update.mutate({ scheduleId: schedule.schedule_id, changes: { frequency: parseFrequency(event.target.value) } })}><option value="daily">Daily</option><option value="weekly">Weekly</option><option value="monthly">Monthly</option></select>
            <button className="button button--ghost" onClick={() => remove.mutate(schedule.schedule_id)} disabled={remove.isPending}>Delete</button>
            <button className="button button--ghost" onClick={() => resetLastRun.mutate(schedule.schedule_id)} disabled={resetLastRun.isPending}>Reset last run</button>
          </div>
        ))}
      </div>
    </div>
  )
}

function SettingsSection() {
  const client = useQueryClient()
  const [draftMinimum, setDraftMinimum] = useState<number | null>(null)
  const [message, setMessage] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)
  const settings = useQuery({
    queryKey: ['admin-auth-settings'],
    queryFn: () => apiRequest<AuthSettings>('/api/admin/auth/settings'),
  })
  const minimum = draftMinimum ?? settings.data?.password_min_length ?? 15
  const update = useMutation({
    mutationFn: () => apiRequest<AuthSettings>('/api/admin/auth/settings', {
      method: 'PATCH',
      body: JSON.stringify({ password_min_length: minimum }),
    }),
    onSuccess: result => { setDraftMinimum(result.password_min_length); setMessage('Password policy updated.'); setError(null); void client.invalidateQueries({ queryKey: ['admin-auth-settings'] }) },
    onError: (err: Error) => { setError(err.message); setMessage(null) },
  })
  return <div className="stack"><h3>Security settings</h3><p className="text-muted">Choose the minimum password length for new accounts, invitations, resets, and password changes. The secure lower bound is 15 characters.</p>{settings.isLoading && <LoadingState label="Loading settings" />}{message && <p className="form-success">{message}</p>}{error && <p className="form-error">{error}</p>}<label className="field-label">Minimum password length<input className="input" type="number" min={15} max={128} value={minimum} onChange={event => setDraftMinimum(Number(event.target.value))} /></label><button className="button button--primary" disabled={update.isPending || minimum < 15 || minimum > 128} onClick={() => update.mutate()}>Save password policy</button></div>
}

function UsersSection() {
  const client = useQueryClient()
  const [error, setError] = useState<string | null>(null)

  const users = useQuery({
    queryKey: ['admin-users'],
    queryFn: () => apiRequest<AdminUser[]>('/api/admin/auth/users'),
  })

  const updateRole = useMutation({
    mutationFn: ({ userId, role }: { userId: string; role: string }) =>
      apiRequest(`/api/admin/auth/users/${userId}/role`, {
        method: 'PATCH',
        body: JSON.stringify({ role }),
      }),
    onSuccess: () => void client.invalidateQueries({ queryKey: ['admin-users'] }),
    onError: (err: Error) => setError(err.message),
  })

  const disableUser = useMutation({
    mutationFn: (userId: string) =>
      apiRequest(`/api/admin/auth/users/${userId}/disable`, { method: 'PATCH' }),
    onSuccess: () => void client.invalidateQueries({ queryKey: ['admin-users'] }),
    onError: (err: Error) => setError(err.message),
  })

  return (
    <div className="stack">
      {error && <p className="form-error">{error}</p>}
      {users.isLoading && <LoadingState label="Loading users" />}
      <div className="table-scroll">
        <table className="data-grid">
          <thead>
            <tr>
              <th>Username</th>
              <th>Email</th>
              <th>Role</th>
              <th>Status</th>
              <th>Last login</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>
            {users.data?.map(u => (
              <tr key={u.user_id}>
                <td><strong>{u.username}</strong></td>
                <td>{u.email || '—'}</td>
                <td>
                  <select
                    className="input compact"
                    value={u.role}
                    disabled={u.status !== 'active'}
                    onChange={e => updateRole.mutate({ userId: u.user_id, role: e.target.value })}
                  >
                    <option value="admin">Admin</option>
                    <option value="operator">Operator</option>
                    <option value="member">Member</option>
                  </select>
                </td>
                <td>
                  <span className={u.status === 'active' ? 'status-pill status-pill--ok' : 'status-pill status-pill--warn'}>
                    {u.status}
                  </span>
                </td>
                <td><small>{u.last_login_at ?? 'Never'}</small></td>
                <td>
                  {u.status === 'active' && (
                    <button
                      className="text-button text-button--danger"
                      onClick={() => disableUser.mutate(u.user_id)}
                      disabled={disableUser.isPending}
                    >
                      Disable
                    </button>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

function AuditSection() {
  const events = useQuery({
    queryKey: ['admin-audit'],
    queryFn: () => apiRequest<AuditEvent[]>('/api/admin/auth/audit?limit=200'),
    refetchInterval: 60_000,
  })

  const EVENT_LABELS: Record<string, string> = {
    account_created: 'Account created',
    login_succeeded: 'Login',
    login_failed: 'Failed login',
    logout: 'Logout',
    refresh_succeeded: 'Token refresh',
    refresh_reuse_detected: '⚠️ Refresh reuse',
    password_changed: 'Password changed',
    password_change_failed: 'Failed password change',
    profile_updated: 'Profile updated',
    session_revoked: 'Session revoked',
    all_sessions_revoked: 'All sessions revoked',
    api_token_created: 'API token created',
    user_disabled: 'User disabled',
    role_changed: 'Role changed',
  }

  return (
    <div className="stack">
      <p className="text-muted">Authentication audit events are retained in the local database.</p>
      {events.isLoading && <LoadingState label="Loading audit log" />}
      <div className="table-scroll">
        <table className="data-grid">
          <thead>
            <tr>
              <th>Time</th>
              <th>Event</th>
              <th>User</th>
              <th>Detail</th>
            </tr>
          </thead>
          <tbody>
            {events.data?.map(event => (
              <tr key={event.event_id}>
                <td><small>{event.occurred_at}</small></td>
                <td>{EVENT_LABELS[event.event_type] ?? event.event_type}</td>
                <td><small>{event.user_id ?? '—'}</small></td>
                <td><small>{event.detail || '—'}</small></td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
