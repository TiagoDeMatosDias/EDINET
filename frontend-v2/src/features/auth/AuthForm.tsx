import { useId, useState } from 'react'

import { useAuth } from './authContext'

export type AuthMode = 'login' | 'register'

/**
 * Sign-in / registration form shared by the dedicated auth pages and the
 * in-app AuthGate. Submits through the AuthProvider context so the provider's
 * user state (and query-cache isolation) is always updated.
 */
export function AuthForm({
  mode,
  passwordMinimum,
  onSuccess,
}: {
  mode: AuthMode
  passwordMinimum: number
  onSuccess?: () => void
}) {
  const auth = useAuth()
  const passwordId = useId()
  const passwordHintId = useId()
  const [loginField, setLoginField] = useState('')
  const [username, setUsername] = useState('')
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [confirmPassword, setConfirmPassword] = useState('')
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)

  const canSubmit = busy
    ? false
    : mode === 'register'
      ? username.trim().length >= 3 && password.length >= passwordMinimum && password === confirmPassword
      : loginField.trim().length > 0 && password.length > 0

  const submit = async () => {
    setError(null)
    setBusy(true)
    try {
      if (mode === 'register') {
        await auth.register(username.trim(), password, email.trim() || undefined)
      } else {
        await auth.login(loginField.trim(), password)
      }
      onSuccess?.()
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Authentication failed')
    } finally {
      setBusy(false)
    }
  }

  return (
    <form className="auth-form stack" onSubmit={event => { event.preventDefault(); void submit() }}>
      {mode === 'register' ? (
        <>
          <label className="field-label">
            Username
            <input className="input" name="username" autoComplete="username" value={username}
              onChange={e => setUsername(e.target.value)} minLength={3} maxLength={64} required />
          </label>
          <label className="field-label">
            Email <small>(optional)</small>
            <input className="input" type="email" name="email" autoComplete="email" value={email}
              onChange={e => setEmail(e.target.value)} />
          </label>
        </>
      ) : (
        <label className="field-label">
          Username or email
          <input className="input" name="username" autoComplete="username" value={loginField}
            onChange={e => setLoginField(e.target.value)} required />
        </label>
      )}

      <div className="field-label">
        <label htmlFor={passwordId}>Password</label>
        <input id={passwordId} className="input" type="password" name="password"
          autoComplete={mode === 'register' ? 'new-password' : 'current-password'}
          aria-describedby={mode === 'register' ? passwordHintId : undefined}
          value={password} onChange={e => setPassword(e.target.value)}
          minLength={mode === 'register' ? passwordMinimum : 1} maxLength={128} required />
        {mode === 'register' && (
          <small id={passwordHintId}>Minimum {passwordMinimum} characters. Use a passphrase or password manager.</small>
        )}
      </div>

      {mode === 'register' && (
        <label className="field-label">
          Confirm password
          <input className="input" type="password" name="confirm-password" autoComplete="new-password"
            value={confirmPassword} onChange={e => setConfirmPassword(e.target.value)}
            minLength={passwordMinimum} maxLength={128} required />
        </label>
      )}

      {error && <p className="form-error" role="alert">{error}</p>}

      <button className="button button--primary button--full" type="submit" disabled={!canSubmit}>
        {busy ? 'Please wait…' : mode === 'register' ? 'Create account' : 'Sign in'}
      </button>
    </form>
  )
}
