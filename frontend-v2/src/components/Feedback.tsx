import { AlertCircle, Inbox, LoaderCircle } from 'lucide-react'
import type { ReactNode } from 'react'

export function LoadingState({ label = 'Loading' }: { label?: string }) {
  return <div className="state-panel" role="status"><LoaderCircle className="spin" aria-hidden="true" /><span>{label}</span></div>
}

export function EmptyState({ title, description, action }: { title: string; description: string; action?: ReactNode }) {
  return <div className="state-panel"><Inbox aria-hidden="true" /><strong>{title}</strong><span className="muted">{description}</span>{action}</div>
}

export function ErrorState({ error, retry }: { error: unknown; retry?: () => void }) {
  const rawMessage = error instanceof Error ? error.message : 'Something went wrong.'
  const message = rawMessage.length > 320 ? `${rawMessage.slice(0, 320)}…` : rawMessage
  return <div className="state-panel state-panel--error" role="alert"><AlertCircle aria-hidden="true" /><strong>Could not load this view</strong><span>{message}</span>{retry && <button className="button button--secondary" onClick={retry}>Try again</button>}</div>
}
