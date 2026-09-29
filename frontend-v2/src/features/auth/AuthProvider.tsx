import { useQueryClient } from '@tanstack/react-query'
import { useCallback, useEffect, useMemo, useState, type ReactNode } from 'react'
import { useLocation } from 'react-router-dom'

import { ApiError, apiRequest, setAccessToken } from '../../api/client'
import { BrandLockup } from '../../components/Brand'
import { AuthForm, type AuthMode } from './AuthForm'
import { AuthContext, DEFAULT_PASSWORD_MIN_LENGTH, type AuthContextValue, type AuthStatus, type AuthUser } from './authContext'

export type { AuthContextValue, AuthStatus, AuthUser } from './authContext'

const PUBLIC_PATHS = new Set(['/', '/pricing', '/login', '/register'])
// Renew once this share of the access token's server-set lifetime has passed.
const RENEW_AT_LIFETIME_SHARE = 0.8
// Back-off when renewal fails for a reason other than an ended session.
const RENEW_RETRY_MS = 30_000

type TokenResult = { access_token: string; expires_in?: number; user: AuthUser }
type Renewal = { dueAt: number }

/**
 * When to renew, on this client's clock. ``expires_in`` is measured by the
 * server, so a skewed client clock cannot make renewal fire too early or late.
 */
function renewalFor(result: TokenResult): Renewal | null {
  const seconds = result.expires_in
  return typeof seconds === 'number' && seconds > 0
    ? { dueAt: Date.now() + seconds * 1000 * RENEW_AT_LIFETIME_SHARE }
    : null
}

export function AuthProvider({ children }: { children: ReactNode }) {
  const location = useLocation()
  const queryClient = useQueryClient()
  const [status, setStatus] = useState<AuthStatus | null>(null)
  const [user, setUser] = useState<AuthUser | null>(null)
  const [renewal, setRenewal] = useState<Renewal | null>(null)
  const [loading, setLoading] = useState(true)

  // Every identity change drops the query cache so one account's private data
  // is never served from memory to the next account in the same tab.
  const startSession = useCallback((result: TokenResult) => {
    queryClient.clear()
    setAccessToken(result.access_token)
    setUser(result.user)
    setRenewal(renewalFor(result))
  }, [queryClient])

  const endSession = useCallback(() => {
    queryClient.clear()
    setAccessToken(null)
    setUser(null)
    setRenewal(null)
  }, [queryClient])

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
        return apiRequest<TokenResult>('/api/auth/refresh', { method: 'POST' })
          .then(result => { if (!cancelled) startSession(result) })
          .catch(() => { if (!cancelled) setUser(null) })
          .finally(() => { if (!cancelled) setLoading(false) })
      })
      .catch(() => {
        if (!cancelled) {
          setStatus({ mode: 'disabled', registration_open: false, bootstrap_required: false, password_min_length: DEFAULT_PASSWORD_MIN_LENGTH })
          setLoading(false)
        }
      })
    return () => { cancelled = true }
  }, [startSession])

  // Renew the access token before the expiry the server set, which also
  // confirms the session and refreshes the account's role. A 401 means the
  // session is gone, so sign out and stop; other failures retry. Nothing runs
  // while signed out.
  const userId = user?.user_id
  useEffect(() => {
    if (!userId || !renewal) return
    let cancelled = false
    let timer: ReturnType<typeof setTimeout>
    const renew = async () => {
      try {
        const result = await apiRequest<TokenResult>('/api/auth/refresh', { method: 'POST' })
        if (cancelled) return
        setAccessToken(result.access_token)
        setUser(result.user)
        setRenewal(renewalFor(result))
      } catch (err) {
        if (cancelled) return
        if (err instanceof ApiError && err.status === 401) {
          endSession()
          return
        }
        timer = setTimeout(() => { void renew() }, RENEW_RETRY_MS)
      }
    }
    timer = setTimeout(() => { void renew() }, Math.max(0, renewal.dueAt - Date.now()))
    return () => {
      cancelled = true
      clearTimeout(timer)
    }
  }, [userId, renewal, endSession])

  const value = useMemo<AuthContextValue>(() => ({
    user,
    status,
    loading,
    login: async (loginParam, password) => {
      const result = await apiRequest<TokenResult>('/api/auth/login', {
        method: 'POST',
        body: JSON.stringify({ login: loginParam, password }),
      })
      startSession(result)
    },
    register: async (username, password, email) => {
      await apiRequest('/api/auth/register', {
        method: 'POST',
        body: JSON.stringify({ username, password, email: email || undefined }),
      })
      const result = await apiRequest<TokenResult>('/api/auth/login', {
        method: 'POST',
        body: JSON.stringify({ login: username, password }),
      })
      startSession(result)
      return result.user
    },
    logout: async () => {
      await apiRequest('/api/auth/logout', { method: 'POST' }).catch(() => undefined)
      endSession()
    },
  }), [endSession, loading, startSession, status, user])

  const normalizedPath = location.pathname.replace(/\/+$/, '') || '/'
  const isPublicPath = PUBLIC_PATHS.has(normalizedPath)

  if (loading) {
    return (
      <div className="app-loading">
        <div className="splash">
          <BrandLockup className="splash-brand" showTagline />
          <div className="loading-spinner" />
          <small>Checking account session…</small>
        </div>
      </div>
    )
  }

  return (
    <AuthContext.Provider value={value}>
      {status?.mode === 'accounts' && !user && !isPublicPath ? (
        <AuthGate status={status} />
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

/**
 * Shown in place of a protected route while signed out. Signing in here keeps
 * the requested route, so deep links survive authentication.
 */
function AuthGate({ status }: { status: AuthStatus }) {
  const [mode, setMode] = useState<AuthMode>(status.bootstrap_required ? 'register' : 'login')

  return (
    <main className="auth-page">
      <div className="auth-card card">
        <BrandLockup className="auth-brand" showTagline />
        <h1>{mode === 'register' ? 'Create your account' : 'Sign in'}</h1>
        <p>
          {status.bootstrap_required && mode === 'register'
            ? 'The first account becomes the local administrator.'
            : mode === 'register'
              ? 'Create an account to access research tools.'
              : 'Sign in to your account to access research tools.'}
        </p>

        <AuthForm key={mode} mode={mode} passwordMinimum={status.password_min_length} />

        <div className="auth-footer">
          {mode === 'register' ? (
            !status.bootstrap_required && (
              <button className="text-button" onClick={() => setMode('login')}>
                Already have an account? Sign in
              </button>
            )
          ) : status.registration_open ? (
            <button className="text-button" onClick={() => setMode('register')}>
              Create an account
            </button>
          ) : null}
        </div>
      </div>
    </main>
  )
}
