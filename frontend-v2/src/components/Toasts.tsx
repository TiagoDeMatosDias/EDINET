import { X } from 'lucide-react'
import { createContext, useCallback, useContext, useEffect, useMemo, useState, type KeyboardEvent, type ReactNode } from 'react'
import { Link } from 'react-router-dom'

import './jobs.css'

export const TOAST_LIFETIME_MS = 8_000

export interface ToastInput {
  message: string
  tone?: 'info' | 'success' | 'danger'
  /** Client-side route for an "Open" link. */
  link?: string | null
}

interface Toast extends ToastInput { id: number }

interface ToastApi {
  push: (toast: ToastInput) => number
  dismiss: (id: number) => void
}

const noop: ToastApi = { push: () => 0, dismiss: () => undefined }
const ToastContext = createContext<ToastApi>(noop)

/** Push toasts from anywhere under ``ToastProvider``; outside it the calls do nothing. */
export function useToasts(): ToastApi {
  return useContext(ToastContext)
}

function ToastItem({ toast, onDismiss }: { toast: Toast; onDismiss: (id: number) => void }) {
  useEffect(() => {
    const timer = setTimeout(() => onDismiss(toast.id), TOAST_LIFETIME_MS)
    return () => clearTimeout(timer)
  }, [toast.id, onDismiss])
  const onKeyDown = (event: KeyboardEvent) => { if (event.key === 'Escape') { event.stopPropagation(); onDismiss(toast.id) } }
  return <div className={`toast toast--${toast.tone ?? 'info'}`} role="status" onKeyDown={onKeyDown}>
    <span className="toast__message">{toast.message}</span>
    {toast.link && <Link className="toast__link" to={toast.link} onClick={() => onDismiss(toast.id)}>Open</Link>}
    <button type="button" className="toast__close" aria-label="Dismiss notification" onClick={() => onDismiss(toast.id)}><X aria-hidden="true" size={14} /></button>
  </div>
}

export function ToastProvider({ children }: { children: ReactNode }) {
  const [toasts, setToasts] = useState<Toast[]>([])
  const dismiss = useCallback((id: number) => setToasts(current => current.filter(toast => toast.id !== id)), [])
  const push = useCallback((toast: ToastInput) => {
    const id = nextId++
    setToasts(current => [...current.slice(-4), { ...toast, id }])
    return id
  }, [])
  const api = useMemo(() => ({ push, dismiss }), [push, dismiss])
  return <ToastContext.Provider value={api}>
    {children}
    {toasts.length > 0 && <div className="toast-stack">{toasts.map(toast => <ToastItem key={toast.id} toast={toast} onDismiss={dismiss} />)}</div>}
  </ToastContext.Provider>
}

let nextId = 1
