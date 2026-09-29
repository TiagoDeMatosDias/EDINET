import { Link, Navigate, useNavigate } from 'react-router-dom'

import { BrandLockup } from '../../components/Brand'
import { AuthForm, type AuthMode } from './AuthForm'
import { DEFAULT_PASSWORD_MIN_LENGTH, useAuth } from './authContext'

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
      passwordMinimum={auth.status?.password_min_length ?? DEFAULT_PASSWORD_MIN_LENGTH}
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

  return (
    <main className="auth-page">
      <div className="auth-card card">
        <BrandLockup className="auth-brand" showTagline />
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
