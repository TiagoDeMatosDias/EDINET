import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'

import { apiRequest } from '../../api/client'
import { useAuth } from './AuthProvider'
import { LoadingState } from '../../components/Feedback'
import { PageHeader } from '../../components/Page'

interface ApiToken {
  token_id: string
  name: string
  token_prefix: string
  scopes_json: string
  created_at: string
  expires_at: string | null
  last_used_at: string | null
  revoked_at: string | null
}

interface Session {
  session_id: string
  token_type: string
  created_at: string
  expires_at: string | null
  revoked_at: string | null
  user_agent: string | null
}

type AccountTab = 'profile' | 'password' | 'tokens' | 'sessions'

const TABS: { key: AccountTab; label: string }[] = [
  { key: 'profile', label: 'Profile' },
  { key: 'password', label: 'Password' },
  { key: 'tokens', label: 'API tokens' },
  { key: 'sessions', label: 'Sessions' },
]

export default function AccountPage() {
  const { user } = useAuth()
  const client = useQueryClient()
  const [tab, setTab] = useState<AccountTab>('profile')

  return (
    <div className="stack dense-page">
      <PageHeader
        eyebrow="Account"
        title={user?.username ?? 'Settings'}
        description="Manage your profile, credentials, and active sessions."
      />
      <div className="card">
        <div className="tabs-bar">
          {TABS.map(t => (
            <button
              key={t.key}
              className={tab === t.key ? 'tab tab--active' : 'tab'}
              onClick={() => setTab(t.key)}
            >
              {t.label}
            </button>
          ))}
        </div>
        <div className="card-body">
          {tab === 'profile' && <ProfileSection />}
          {tab === 'password' && <PasswordSection />}
          {tab === 'tokens' && <TokensSection />}
          {tab === 'sessions' && <SessionsSection />}
        </div>
      </div>
    </div>
  )
}

function ProfileSection() {
  const { user } = useAuth()
  const client = useQueryClient()
  const [username, setUsername] = useState(user?.username ?? '')
  const [email, setEmail] = useState(user?.email ?? '')
  const [message, setMessage] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)

  const update = useMutation({
    mutationFn: () =>
      apiRequest('/api/auth/me', {
        method: 'PATCH',
        body: JSON.stringify({ username: username || undefined, email: email || undefined }),
      }),
    onSuccess: () => {
      setMessage('Profile updated.')
      setError(null)
      void client.invalidateQueries({ queryKey: ['auth-me'] })
    },
    onError: (err: Error) => {
      setError(err.message)
      setMessage(null)
    },
  })

  return (
    <div className="stack">
      <h3>Profile details</h3>
      <label className="field-label">
        Username
        <input className="input" value={username} onChange={e => setUsername(e.target.value)} />
      </label>
      <label className="field-label">
        Email
        <input className="input" type="email" value={email} onChange={e => setEmail(e.target.value)} />
      </label>
      {message && <p className="form-success">{message}</p>}
      {error && <p className="form-error">{error}</p>}
      <button
        className="button button--primary"
        disabled={update.isPending || !username.trim()}
        onClick={() => update.mutate()}
      >
        Save
      </button>
    </div>
  )
}

function PasswordSection() {
  const [currentPassword, setCurrentPassword] = useState('')
  const [newPassword, setNewPassword] = useState('')
  const [message, setMessage] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)

  const change = useMutation({
    mutationFn: () =>
      apiRequest('/api/auth/change-password', {
        method: 'POST',
        body: JSON.stringify({ current_password: currentPassword, new_password: newPassword }),
      }),
    onSuccess: () => {
      setMessage('Password changed. Your other sessions have been revoked.')
      setError(null)
      setCurrentPassword('')
      setNewPassword('')
    },
    onError: (err: Error) => {
      setError(err.message)
      setMessage(null)
    },
  })

  return (
    <div className="stack">
      <h3>Change your password</h3>
      <p className="text-muted">Changing your password revokes all other active sessions.</p>
      <label className="field-label">
        Current password
        <input className="input" type="password" value={currentPassword} onChange={e => setCurrentPassword(e.target.value)} />
      </label>
      <label className="field-label">
        New password (minimum 15 characters)
        <input className="input" type="password" value={newPassword} onChange={e => setNewPassword(e.target.value)} />
      </label>
      {message && <div className="callout callout--success">{message}</div>}
      {error && <div className="callout callout--warning">{error}</div>}
      <button
        className="button button--primary"
        disabled={change.isPending || !currentPassword || newPassword.length < 15}
        onClick={() => change.mutate()}
      >
        {change.isPending ? 'Changing password…' : 'Change password'}
      </button>
    </div>
  )
}

function TokensSection() {
  const client = useQueryClient()
  const [name, setName] = useState('')
  const [created, setCreated] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)

  const tokens = useQuery({
    queryKey: ['auth-tokens'],
    queryFn: () => apiRequest<ApiToken[]>('/api/auth/tokens'),
  })

  const create = useMutation({
    mutationFn: () =>
      apiRequest<{ token: string }>('/api/auth/tokens', {
        method: 'POST',
        body: JSON.stringify({ name, scopes: ['*'] }),
      }),
    onSuccess: data => {
      setCreated(data.token)
      setName('')
      void client.invalidateQueries({ queryKey: ['auth-tokens'] })
    },
    onError: (err: Error) => setError(err.message),
  })

  const revoke = useMutation({
    mutationFn: (tokenId: string) => apiRequest(`/api/auth/tokens/${tokenId}`, { method: 'DELETE' }),
    onSuccess: () => void client.invalidateQueries({ queryKey: ['auth-tokens'] }),
  })

  return (
    <div className="stack">
      <h3>Personal API tokens</h3>
      <p className="text-muted">Tokens are shown only once after creation. Store them securely.</p>
      {created && (
        <div className="callout callout--warning">
          <strong>Copy this token now:</strong>
          <pre className="token-display">{created}</pre>
          <button className="text-button" onClick={() => setCreated(null)}>Dismiss</button>
        </div>
      )}
      <div className="button-row">
        <input className="input" value={name} onChange={e => setName(e.target.value)} placeholder="Token name" />
        <button
          className="button button--primary"
          disabled={create.isPending || !name.trim()}
          onClick={() => create.mutate()}
        >
          Create
        </button>
      </div>
      {error && <p className="form-error">{error}</p>}
      {tokens.isLoading && <LoadingState label="Loading tokens" />}
      {tokens.data?.map(token => (
        <div className="research-row" key={token.token_id}>
          <div>
            <strong>{token.name}</strong>
            <small>{token.token_prefix}… · {token.revoked_at ? 'Revoked' : 'Active'}</small>
          </div>
          {!token.revoked_at && (
            <button className="text-button" onClick={() => revoke.mutate(token.token_id)}>Revoke</button>
          )}
        </div>
      ))}
    </div>
  )
}

function SessionsSection() {
  const client = useQueryClient()

  const sessions = useQuery({
    queryKey: ['auth-sessions'],
    queryFn: () => apiRequest<Session[]>('/api/auth/sessions'),
    refetchInterval: 30_000,
  })

  const revoke = useMutation({
    mutationFn: (sessionId: string) => apiRequest(`/api/auth/sessions/${sessionId}`, { method: 'DELETE' }),
    onSuccess: () => void client.invalidateQueries({ queryKey: ['auth-sessions'] }),
  })

  return (
    <div className="stack">
      <h3>Active sessions</h3>
      <p className="text-muted">Revoke sessions you no longer recognize.</p>
      {sessions.isLoading && <LoadingState label="Loading sessions" />}
      {sessions.data?.map(session => (
        <div className="research-row" key={session.session_id}>
          <div>
            <strong>{session.token_type === 'access' ? 'Access token' : 'Refresh token'}</strong>
            <small>
              Created {session.created_at}
              {session.user_agent ? ` · ${session.user_agent}` : ''}
            </small>
          </div>
          <div>
            {session.revoked_at ? (
              <span className="status-pill">Revoked</span>
            ) : (
              <button
                className="text-button"
                onClick={() => revoke.mutate(session.session_id)}
                disabled={revoke.isPending}
              >
                Revoke
              </button>
            )}
          </div>
        </div>
      ))}
    </div>
  )
}
