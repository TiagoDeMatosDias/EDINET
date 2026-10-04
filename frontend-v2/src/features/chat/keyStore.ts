import { useEffect, useState, useSyncExternalStore } from 'react'

import type { Identity } from './crypto'

/**
 * The unlocked identity for this tab, optionally remembered on this device.
 *
 * The private key is a non-extractable CryptoKey: IndexedDB can hold it, but
 * no script can read its bytes back out. Signing out forgets it.
 */

const DB_NAME = 'shade-chat'
const STORE = 'identities'

let current: Identity | null = null
const listeners = new Set<() => void>()

function emit() {
  for (const listener of listeners) listener()
}

function openDatabase(): Promise<IDBDatabase> | null {
  const factory = (globalThis as { indexedDB?: IDBFactory }).indexedDB
  if (!factory) return null
  return new Promise((resolve, reject) => {
    const request = factory.open(DB_NAME, 1)
    request.onupgradeneeded = () => { request.result.createObjectStore(STORE, { keyPath: 'userId' }) }
    request.onsuccess = () => resolve(request.result)
    request.onerror = () => reject(request.error)
  })
}

async function withStore<T>(mode: IDBTransactionMode, run: (store: IDBObjectStore) => IDBRequest<T>): Promise<T | undefined> {
  const opening = openDatabase()
  if (!opening) return undefined
  try {
    const database = await opening
    return await new Promise<T>((resolve, reject) => {
      const transaction = database.transaction(STORE, mode)
      const request = run(transaction.objectStore(STORE))
      transaction.oncomplete = () => { database.close(); resolve(request.result) }
      transaction.onerror = () => { database.close(); reject(transaction.error) }
    })
  } catch {
    // Private windows and blocked storage: the identity lives in memory only.
    return undefined
  }
}

export function currentIdentity() {
  return current
}

export function setIdentity(identity: Identity | null) {
  current = identity
  emit()
}

export function useIdentity(userId: string | undefined) {
  const identity = useSyncExternalStore(
    listener => { listeners.add(listener); return () => listeners.delete(listener) },
    () => current,
    () => current,
  )
  return identity && identity.userId === userId ? identity : null
}

/** Keep the identity for this tab, and on this device too when ``remember`` is set. */
export async function rememberIdentity(identity: Identity, remember: boolean) {
  setIdentity(identity)
  if (remember) await withStore('readwrite', store => store.put(identity))
  else await withStore('readwrite', store => store.delete(identity.userId))
}

/** The identity remembered on this device for the user, if it still matches their active key. */
export async function restoreIdentity(userId: string, activeKeyId: string | null) {
  if (current?.userId === userId && current.keyId === activeKeyId) return current
  const stored = await withStore<Identity | undefined>('readonly', store => store.get(userId))
  if (!stored || stored.keyId !== activeKeyId) {
    if (stored) await withStore('readwrite', store => store.delete(userId))
    return null
  }
  setIdentity(stored)
  return stored
}

export async function isRemembered(userId: string) {
  return Boolean(await withStore<Identity | undefined>('readonly', store => store.get(userId)))
}

/** Lock: forget the unlocked key in this tab and on this device. */
export async function forgetIdentity(userId?: string) {
  if (!userId || current?.userId === userId) setIdentity(null)
  await withStore('readwrite', store => (userId ? store.delete(userId) : store.clear()))
}

/** The unlocked identity for this user, restored from this device when it was remembered. */
export function useChatIdentity(userId: string | undefined, activeKeyId: string | null | undefined) {
  const identity = useIdentity(userId)
  const [checked, setChecked] = useState<string | null>(null)
  const want = userId && activeKeyId !== undefined ? `${userId}:${activeKeyId}` : null
  useEffect(() => {
    if (!userId || activeKeyId === undefined || checked === want) return
    let cancelled = false
    void restoreIdentity(userId, activeKeyId).finally(() => { if (!cancelled) setChecked(want) })
    return () => { cancelled = true }
  }, [userId, activeKeyId, checked, want])
  // A key replaced on another device locks this one.
  const unlocked = identity && identity.keyId === activeKeyId ? identity : null
  return { identity: unlocked, restoring: Boolean(want) && checked !== want }
}
