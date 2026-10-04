import { useState } from 'react'
import { Link, Navigate, useNavigate, useSearchParams } from 'react-router-dom'

import { BrandLockup } from '../../components/Brand'
import { AuthForm, type AuthMode } from './AuthForm'
import { DEFAULT_PASSWORD_MIN_LENGTH, useAuth } from './authContext'
import { InvitationForm, ResetForm } from './TokenForms'

export default function LoginPage({ initialMode = 'login' }: { initialMode?: AuthMode }) {
  const auth = useAuth()
  const navigate = useNavigate()
  const [params] = useSearchParams()
  const [resetDone, setResetDone] = useState(false)
  const invite = params.get('invite')
  const reset = params.get('reset')
  const minimum = auth.status?.password_min_length ?? DEFAULT_PASSWORD_MIN_LENGTH

  // If already logged in, redirect to the signed-in workspace.
  if (auth.user) return <Navigate to="/overview" replace />
  // If auth is disabled, no login needed
  if (auth.status?.mode === 'disabled') return <Navigate to="/overview" replace />
  if (invite || (reset && !resetDone)) {
    return <main className="auth-page">
      <div className="auth-card card">
        <BrandLockup className="auth-brand" showTagline />
        <h1>{invite ? 'You are invited' : 'Choose a new password'}</h1>
        <p>{invite ? 'Pick a username and password to create your account.' : 'An administrator issued this reset link. It works once.'}</p>
        {invite
          ? <InvitationForm token={invite} passwordMinimum={minimum} onSuccess={() => navigate('/overview')} />
          : <ResetForm token={reset!} passwordMinimum={minimum} onDone={() => setResetDone(true)} />}
        <Link className="auth-home-link" to="/login">Sign in instead</Link>
      </div>
    </main>
  }

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
      passwordMinimum={auth.status?.password_min_length ?? DEFAULT_PASSWORD_MIN_LENGTH}
      onSuccess={() => navigate('/overview')}
      notice={resetDone ? 'Password changed. Sign in with the new one.' : undefined}
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
  notice,
}: {
  initialMode: AuthMode
  registrationUnavailable: boolean
  bootstrapRequired: boolean
  registrationOpen: boolean
  passwordMinimum: number
  onSuccess: () => void
  notice?: string
}) {
  const mode = initialMode

  return (
    <main className="auth-page">
      <div className="auth-card card">
        <BrandLockup className="auth-brand" showTagline />
        <h1>{mode === 'register' ? 'Create your account' : 'Sign in'}</h1>

        {notice && <div className="callout callout--success" role="status">{notice}</div>}
        {registrationUnavailable && (
          <div className="callout callout--warning">
            Registration is by invitation only. Ask an administrator for an invitation link, or sign in with an existing account.
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

        <AuthForm mode={mode} passwordMinimum={passwordMinimum} onSuccess={onSuccess} />

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
