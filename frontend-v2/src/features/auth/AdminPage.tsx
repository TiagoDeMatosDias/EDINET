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

type AdminTab = 'users' | 'audit'

export default function AdminPage() {
  const [tab, setTab] = useState<AdminTab>('users')

  return (
    <div className="stack dense-page">
      <PageHeader
        eyebrow="Administration"
        title="Account management"
        description="Manage users, roles, and review the authentication audit log."
      />
      <div className="card">
        <div className="tabs-bar">
          <button className={tab === 'users' ? 'tab tab--active' : 'tab'} onClick={() => setTab('users')}>Users</button>
          <button className={tab === 'audit' ? 'tab tab--active' : 'tab'} onClick={() => setTab('audit')}>Audit log</button>
        </div>
        <div className="card-body">
          {tab === 'users' && <UsersSection />}
          {tab === 'audit' && <AuditSection />}
        </div>
      </div>
    </div>
  )
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
