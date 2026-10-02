import { useState } from 'react'

function read<T>(key: string, fallback: T, allowed?: readonly T[]): T {
  try {
    const stored = window.localStorage.getItem(key)
    if (stored === null) return fallback
    const value = JSON.parse(stored) as T
    return allowed && !allowed.includes(value) ? fallback : value
  } catch {
    return fallback
  }
}

/** A per-browser display preference (a chart range, a table view); falls back silently when storage is unavailable. */
export function usePersistentState<T>(key: string, fallback: T, allowed?: readonly T[]) {
  const [value, setValue] = useState<T>(() => read(key, fallback, allowed))
  const update = (next: T) => {
    setValue(next)
    try { window.localStorage.setItem(key, JSON.stringify(next)) } catch { /* preference only */ }
  }
  return [value, update] as const
}
