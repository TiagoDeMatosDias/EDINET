import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState, type ReactNode } from 'react'
import { useNavigate } from 'react-router-dom'

import { apiRequest, getAccessToken, setAccessToken } from '../../api/client'

export interface AuthUser {
  user_id: string
  username: string
  email?: string | null
  role: string
  status: string
}

export interface AuthStatus {
  mode: 'disabled' | 'accounts'
  registration_open: boolean
  bootstrap_required: boolean
}

export interface AuthContextValue {
  user: AuthUser | null
  status: AuthStatus | null
  loading: boolean
  login: (login: string, password: string) => Promise<void>
  register: (username: string, password: string, email?: string) => Promise<AuthUser>
  logout: () => Promise<void>
}

const AuthContext = createContext<AuthContextValue | null>(null)

export function AuthProvider({ children }: { children: ReactNode }) {
  const [status, setStatus] = useState<AuthStatus | null>(null)
  const [user, setUser] = useState<AuthUser | null>(null)
  const [loading, setLoading] = useState(true)
  const refreshTimer = useRef<ReturnType<typeof setTimeout>>()

  const refreshLoop = useCallback(async () => {
    try {
      const current = getAccessToken()
      if (!current) {
        const refreshed = await apiRequest<{ access_token: string; user: AuthUser }>('/api/auth/refresh', { method: 'POST' })
        setAccessToken(refreshed.access_token)
        setUser(refreshed.user)
      } else {
        const me = await apiRequest<AuthUser>('/api/auth/me')
        setUser(me)
      }
      // Refresh every 10 minutes
      refreshTimer.current = setTimeout(() => { void refreshLoop() }, 600_000)
    } catch {
      setAccessToken(null)
      setUser(null)
      // Retry in 30 seconds
      refreshTimer.current = setTimeout(() => { void refreshLoop() }, 30_000)
    }
  }, [])

  useEffect(() => {
    let cancelled = false
    void apiRequest<AuthStatus>('/api/auth/status')
      .then(value => {
        if (cancelled) return
        setStatus(value)
        if (value.mode !== 'accounts') {
          setLoading(false)
          return
        }
        // Try to restore session via refresh cookie
        return apiRequest<{ access_token: string; user: AuthUser }>('/api/auth/refresh', { method: 'POST' })
          .then(result => {
            if (cancelled) return
            setAccessToken(result.access_token)
            setUser(result.user)
            refreshTimer.current = setTimeout(() => { void refreshLoop() }, 600_000)
          })
          .catch(() => {
            if (!cancelled) setUser(null)
          })
          .finally(() => { if (!cancelled) setLoading(false) })
      })
      .catch(() => {
        if (!cancelled) {
          setStatus({ mode: 'disabled', registration_open: false, bootstrap_required: false })
          setLoading(false)
        }
      })
    return () => {
      cancelled = true
      if (refreshTimer.current) clearTimeout(refreshTimer.current)
    }
  }, [refreshLoop])

  const value = useMemo<AuthContextValue>(() => ({
    user,
    status,
    loading,
    login: async (loginParam, password) => {
      const result = await apiRequest<{ access_token: string; user: AuthUser }>('/api/auth/login', {
        method: 'POST',
        body: JSON.stringify({ login: loginParam, password }),
      })
      setAccessToken(result.access_token)
      setUser(result.user)
    },
    register: async (username, password, email) => {
      await apiRequest('/api/auth/register', {
        method: 'POST',
        body: JSON.stringify({ username, password, email: email || undefined }),
      })
      const result = await apiRequest<{ access_token: string; user: AuthUser }>('/api/auth/login', {
        method: 'POST',
        body: JSON.stringify({ login: username, password }),
      })
      setAccessToken(result.access_token)
      setUser(result.user)
      return result.user
    },
    logout: async () => {
      await apiRequest('/api/auth/logout', { method: 'POST' }).catch(() => undefined)
      setAccessToken(null)
      setUser(null)
    },
  }), [loading, status, user])

  if (loading) {
    return (
      <div className="app-loading">
        <div className="splash">
          <h1>Shade</h1>
          <p>Research workspace</p>
          <div className="loading-spinner" />
          <small>Checking account session…</small>
        </div>
      </div>
    )
  }

  return (
    <AuthContext.Provider value={value}>
      {status?.mode === 'accounts' && !user ? (
        <AuthGate
          status={status}
          onLogin={(loggedInUser) => setUser(loggedInUser)}
        />
      ) : (
        <>
          {status?.mode === 'disabled' && <AuthDisabledBanner />}
          {children}
        </>
      )}
    </AuthContext.Provider>
  )
}

function AuthDisabledBanner() {
  const [dismissed, setDismissed] = useState(false)
  if (dismissed) return null
  return (
    <div className="auth-banner auth-banner--warn">
      <span>
        <strong>Authentication is disabled.</strong>
        {' '}All API access is unrestricted on this loopback session.
        Set <code>EDINET_AUTH_MODE=accounts</code> to require accounts.
      </span>
      <button className="text-button" onClick={() => setDismissed(true)}>Dismiss</button>
    </div>
  )
}

function AuthGate({ status, onLogin }: { status: AuthStatus; onLogin: (user: AuthUser) => void }) {
  const navigate = useNavigate()
  const [mode, setMode] = useState<'login' | 'register'>(status.bootstrap_required ? 'register' : 'login')
  const [loginField, setLoginField] = useState('')
  const [username, setUsername] = useState('')
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [confirmPassword, setConfirmPassword] = useState('')
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)
  const [successMessage, setSuccessMessage] = useState<string | null>(null)

  const canSubmit = busy
    ? false
    : mode === 'register'
      ? username.trim().length >= 3 && password.length >= 15 && password === confirmPassword
      : loginField.trim().length > 0 && password.length > 0

  const submit = async () => {
    setError(null)
    setSuccessMessage(null)
    setBusy(true)
    try {
      if (mode === 'register') {
        const result = await apiRequest<{ user: AuthUser; bootstrap_admin?: boolean }>('/api/auth/register', {
          method: 'POST',
          body: JSON.stringify({ username: username.trim(), email: email.trim() || undefined, password }),
        })
        if (result.bootstrap_admin) {
          setSuccessMessage('Administrator account created. Signing in…')
        }
        // Auto-login after register
        const loginResult = await apiRequest<{ access_token: string; user: AuthUser }>('/api/auth/login', {
          method: 'POST',
          body: JSON.stringify({ login: username.trim(), password }),
        })
        setAccessToken(loginResult.access_token)
        onLogin(loginResult.user)
        navigate('/')
      } else {
        const loginResult = await apiRequest<{ access_token: string; user: AuthUser }>('/api/auth/login', {
          method: 'POST',
          body: JSON.stringify({ login: loginField.trim(), password }),
        })
        setAccessToken(loginResult.access_token)
        onLogin(loginResult.user)
        navigate('/')
      }
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
        <p>
          {status.bootstrap_required && mode === 'register'
            ? 'The first account becomes the local administrator.'
            : mode === 'register'
              ? 'Create an account to access research tools.'
              : 'Sign in to your account to access research tools.'}
        </p>

        {successMessage && <div className="callout callout--success">{successMessage}</div>}

        <form
          className="auth-form stack"
          onSubmit={event => {
            event.preventDefault()
            void submit()
          }}
        >
          {mode === 'register' ? (
            <>
              <label className="field-label">
                Username
                <input
                  className="input"
                  name="username"
                  autoComplete="username"
                  value={username}
                  onChange={e => setUsername(e.target.value)}
                  minLength={3}
                  maxLength={64}
                  required
                />
              </label>
              <label className="field-label">
                Email <small>(optional)</small>
                <input
                  className="input"
                  type="email"
                  name="email"
                  autoComplete="email"
                  value={email}
                  onChange={e => setEmail(e.target.value)}
                />
              </label>
            </>
          ) : (
            <label className="field-label">
              Username or email
              <input
                className="input"
                name="username"
                autoComplete="username"
                value={loginField}
                onChange={e => setLoginField(e.target.value)}
                required
              />
            </label>
          )}

          <label className="field-label">
            Password
            <input
              className="input"
              type="password"
              name="password"
              autoComplete={mode === 'register' ? 'new-password' : 'current-password'}
              value={password}
              onChange={e => setPassword(e.target.value)}
              minLength={mode === 'register' ? 15 : 1}
              maxLength={128}
              required
            />
            {mode === 'register' && (
              <small>Minimum 15 characters. Use a passphrase or password manager.</small>
            )}
          </label>

          {mode === 'register' && (
            <label className="field-label">
              Confirm password
              <input
                className="input"
                type="password"
                name="confirm-password"
                autoComplete="new-password"
                value={confirmPassword}
                onChange={e => setConfirmPassword(e.target.value)}
                minLength={15}
                maxLength={128}
                required
              />
            </label>
          )}

          {error && <p className="form-error">{error}</p>}

          <button
            className="button button--primary button--full"
            type="submit"
            disabled={!canSubmit}
          >
            {busy ? 'Please wait…' : mode === 'register' ? 'Create account' : 'Sign in'}
          </button>
        </form>

        <div className="auth-footer">
          {mode === 'register' ? (
            <button className="text-button" onClick={() => { setMode('login'); setError(null) }}>
              Already have an account? Sign in
            </button>
          ) : (
            <>
              {status.registration_open && (
                <button className="text-button" onClick={() => { setMode('register'); setError(null) }}>
                  Create an account
                </button>
              )}
            </>
          )}
        </div>
      </div>
    </main>
  )
}

export function useAuth() {
  const value = useContext(AuthContext)
  if (!value) throw new Error('useAuth must be used inside AuthProvider')
  return value
}
