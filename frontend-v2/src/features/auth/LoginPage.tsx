import { useState } from 'react'
import { Link, Navigate, useNavigate } from 'react-router-dom'

import { apiRequest, setAccessToken } from '../../api/client'
import { useAuth, type AuthUser } from './AuthProvider'

type AuthMode = 'login' | 'register'

export default function LoginPage({ initialMode = 'login' }: { initialMode?: AuthMode }) {
  const auth = useAuth()
  const navigate = useNavigate()

  // If already logged in, redirect to the signed-in workspace.
  if (auth.user) return <Navigate to="/overview" replace />
  // If auth is disabled, no login needed
  if (auth.status?.mode === 'disabled') return <Navigate to="/overview" replace />

  const bootstrapRequired = auth.status?.bootstrap_required ?? false
  const registrationOpen = auth.status?.registration_open ?? false
  const registrationAvailable = bootstrapRequired || registrationOpen
  const resolvedMode: AuthMode = bootstrapRequired
    ? 'register'
    : initialMode === 'register' && registrationAvailable
      ? 'register'
      : 'login'

  return (
    <LoginForm
      initialMode={resolvedMode}
      registrationUnavailable={initialMode === 'register' && !registrationAvailable}
      bootstrapRequired={bootstrapRequired}
      registrationOpen={registrationOpen}
      passwordMinimum={auth.status?.password_min_length ?? 15}
      onSuccess={() => navigate('/overview')}
    />
  )
}

function LoginForm({
  initialMode,
  registrationUnavailable,
  bootstrapRequired,
  registrationOpen,
  passwordMinimum,
  onSuccess,
}: {
  initialMode: AuthMode
  registrationUnavailable: boolean
  bootstrapRequired: boolean
  registrationOpen: boolean
  passwordMinimum: number
  onSuccess: () => void
}) {
  const mode = initialMode
  const [loginField, setLoginField] = useState('')
  const [username, setUsername] = useState('')
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [confirmPassword, setConfirmPassword] = useState('')
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)
  const [success, setSuccess] = useState<string | null>(null)

  const canSubmit = busy
    ? false
    : mode === 'register'
      ? username.trim().length >= 3 && password.length >= passwordMinimum && password === confirmPassword
      : loginField.trim().length > 0 && password.length > 0

  const submit = async () => {
    setError(null)
    setSuccess(null)
    setBusy(true)
    try {
      if (mode === 'register') {
        const result = await apiRequest<{ user: AuthUser; bootstrap_admin?: boolean }>('/api/auth/register', {
          method: 'POST',
          body: JSON.stringify({ username: username.trim(), email: email.trim() || undefined, password }),
        })
        if (result.bootstrap_admin) {
          setSuccess('Administrator account created. Signing in…')
        }
      }
      const loginResult = await apiRequest<{ access_token: string; user: AuthUser }>('/api/auth/login', {
        method: 'POST',
        body: JSON.stringify({ login: mode === 'register' ? username.trim() : loginField.trim(), password }),
      })
      setAccessToken(loginResult.access_token)
      onSuccess()
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Authentication failed')
    } finally {
      setBusy(false)
    }
  }

  return (
    <main className="auth-page">
      <div className="auth-card card">
        <span className="eyebrow">Shade research workspace</span>
        <h1>{mode === 'register' ? 'Create your account' : 'Sign in'}</h1>

        {registrationUnavailable && (
          <div className="callout callout--warning">
            Registration is currently closed. Sign in with an existing account or contact the administrator.
          </div>
        )}
        {bootstrapRequired && mode === 'register' && (
          <div className="callout callout--info">
            <strong>First-time setup.</strong> The first account becomes the local administrator.
          </div>
        )}
        {!bootstrapRequired && (
          <p>Sign in to access research tools and your private data.</p>
        )}
        {success && <div className="callout callout--success">{success}</div>}

        <form className="auth-form stack" onSubmit={e => { e.preventDefault(); void submit() }}>
          {mode === 'register' ? (
            <>
              <label className="field-label">
                Username
                <input className="input" autoComplete="username" value={username}
                  onChange={e => setUsername(e.target.value)} minLength={3} maxLength={64} required />
              </label>
              <label className="field-label">
                Email <small>(optional)</small>
                <input className="input" type="email" autoComplete="email" value={email}
                  onChange={e => setEmail(e.target.value)} />
              </label>
            </>
          ) : (
            <label className="field-label">
              Username or email
              <input className="input" autoComplete="username" value={loginField}
                onChange={e => setLoginField(e.target.value)} required />
            </label>
          )}

          <label className="field-label">
            Password
            <input className="input" type="password"
              autoComplete={mode === 'register' ? 'new-password' : 'current-password'}
              value={password} onChange={e => setPassword(e.target.value)}
              minLength={mode === 'register' ? passwordMinimum : 1} maxLength={128} required />
            {mode === 'register' && <small>Minimum {passwordMinimum} characters. Use a passphrase or password manager.</small>}
          </label>

          {mode === 'register' && (
            <label className="field-label">
              Confirm password
              <input className="input" type="password" autoComplete="new-password" value={confirmPassword}
                onChange={e => setConfirmPassword(e.target.value)} minLength={passwordMinimum} maxLength={128} required />
            </label>
          )}

          {error && <p className="form-error">{error}</p>}

          <button className="button button--primary button--full" type="submit" disabled={!canSubmit}>
            {busy ? 'Please wait…' : mode === 'register' ? 'Create account' : 'Sign in'}
          </button>
        </form>

        <div className="auth-footer">
          {mode === 'register' && !bootstrapRequired ? (
            <Link className="text-button" to="/login">
              Already have an account? Sign in
            </Link>
          ) : registrationOpen ? (
            <Link className="text-button" to="/register">
              Create an account
            </Link>
          ) : null}
        </div>
        <Link className="auth-home-link" to="/">Back to the homepage</Link>
      </div>
    </main>
  )
}
