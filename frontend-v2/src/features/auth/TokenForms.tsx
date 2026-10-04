import { useState, type FormEvent } from 'react'

import { apiRequest } from '../../api/client'
import { useAuth } from './authContext'

/** Create an account from an administrator's invitation link, then sign in. */
export function InvitationForm({ token, passwordMinimum, onSuccess }: { token: string; passwordMinimum: number; onSuccess: () => void }) {
  const auth = useAuth()
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [confirm, setConfirm] = useState('')
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)
  const valid = username.trim().length >= 3 && password.length >= passwordMinimum && password === confirm
  const submit = async (event: FormEvent) => {
    event.preventDefault()
    if (!valid || busy) return
    setBusy(true)
    setError(null)
    try {
      await apiRequest('/api/auth/accept-invitation', { method: 'POST', body: JSON.stringify({ invitation_token: token, username: username.trim(), password }) })
      await auth.login(username.trim(), password)
      onSuccess()
    } catch (err) {
      setError(err instanceof Error ? err.message : 'The invitation could not be accepted')
    } finally {
      setBusy(false)
    }
  }
  return <form className="auth-form stack" onSubmit={event => void submit(event)} aria-label="Accept invitation">
    <label className="field-label">Username<input className="input" autoComplete="username" value={username} minLength={3} maxLength={64} onChange={event => setUsername(event.target.value)} required /></label>
    <label className="field-label">Password<input className="input" type="password" autoComplete="new-password" value={password} minLength={passwordMinimum} maxLength={128} onChange={event => setPassword(event.target.value)} required /><small>Minimum {passwordMinimum} characters.</small></label>
    <label className="field-label">Confirm password<input className="input" type="password" autoComplete="new-password" value={confirm} onChange={event => setConfirm(event.target.value)} required /></label>
    {error && <p className="form-error" role="alert">{error}</p>}
    <button className="button button--primary button--full" type="submit" disabled={!valid || busy}>{busy ? 'Please wait…' : 'Create account'}</button>
  </form>
}

/** Choose a new password from an administrator's reset link. */
export function ResetForm({ token, passwordMinimum, onDone }: { token: string; passwordMinimum: number; onDone: () => void }) {
  const [password, setPassword] = useState('')
  const [confirm, setConfirm] = useState('')
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)
  const valid = password.length >= passwordMinimum && password === confirm
  const submit = async (event: FormEvent) => {
    event.preventDefault()
    if (!valid || busy) return
    setBusy(true)
    setError(null)
    try {
      await apiRequest('/api/auth/reset-password', { method: 'POST', body: JSON.stringify({ reset_token: token, new_password: password }) })
      onDone()
    } catch (err) {
      setError(err instanceof Error ? err.message : 'The password could not be reset')
    } finally {
      setBusy(false)
    }
  }
  return <form className="auth-form stack" onSubmit={event => void submit(event)} aria-label="Reset password">
    <label className="field-label">New password<input className="input" type="password" autoComplete="new-password" value={password} minLength={passwordMinimum} maxLength={128} onChange={event => setPassword(event.target.value)} required /><small>Minimum {passwordMinimum} characters.</small></label>
    <label className="field-label">Confirm password<input className="input" type="password" autoComplete="new-password" value={confirm} onChange={event => setConfirm(event.target.value)} required /></label>
    {error && <p className="form-error" role="alert">{error}</p>}
    <button className="button button--primary button--full" type="submit" disabled={!valid || busy}>{busy ? 'Please wait…' : 'Set new password'}</button>
  </form>
}
