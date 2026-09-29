import { createContext, useContext } from 'react'

export interface AuthUser {
  user_id: string
  username: string
  email?: string | null
  role: string
  status: string
}

/** Used only until /api/auth/status reports the server's configured minimum. */
export const DEFAULT_PASSWORD_MIN_LENGTH = 15

export interface AuthStatus {
  mode: 'disabled' | 'accounts'
  registration_open: boolean
  bootstrap_required: boolean
  password_min_length: number
}

export interface AuthContextValue {
  user: AuthUser | null
  status: AuthStatus | null
  loading: boolean
  login: (login: string, password: string) => Promise<void>
  register: (username: string, password: string, email?: string) => Promise<AuthUser>
  logout: () => Promise<void>
}

export const AuthContext = createContext<AuthContextValue | null>(null)

export function useAuth() {
  const value = useContext(AuthContext)
  if (!value) throw new Error('useAuth must be used inside AuthProvider')
  return value
}
