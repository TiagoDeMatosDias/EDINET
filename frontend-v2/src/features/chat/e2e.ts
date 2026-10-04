import { useEffect, useRef, useState, useSyncExternalStore } from 'react'

import { chatApi } from './chatApi'
import type { ChatMessage, Conversation, ConversationKeys, MessageBody, PublicKey } from './chatTypes'
import {
  decryptPayload,
  encryptPayload,
  fingerprint,
  importConversationKey,
  messageAad,
  newConversationKey,
  unwrapConversationKey,
  wrapConversationKey,
  type Identity,
} from './crypto'

/**
 * Conversation keys this tab has unwrapped, per conversation and version.
 * They live in memory only and are dropped when the identity changes.
 */

interface VersionKey { raw: Uint8Array; key: CryptoKey }

const rings = new Map<string, Map<number, VersionKey>>()
let ringOwner: string | null = null
let revision = 0
const listeners = new Set<() => void>()

function bump() {
  revision += 1
  for (const listener of listeners) listener()
}

function ring(identity: Identity, conversationId: string) {
  if (ringOwner !== identity.keyId) {
    rings.clear()
    ringOwner = identity.keyId
  }
  let found = rings.get(conversationId)
  if (!found) {
    found = new Map()
    rings.set(conversationId, found)
  }
  return found
}

async function remember(identity: Identity, conversationId: string, version: number, raw: Uint8Array) {
  ring(identity, conversationId).set(version, { raw, key: await importConversationKey(raw) })
  bump()
}

/** Re-renders when a conversation key is added, so locked messages get another try. */
export function useKeyringRevision() {
  return useSyncExternalStore(listener => { listeners.add(listener); return () => listeners.delete(listener) }, () => revision, () => revision)
}

export function hasKey(identity: Identity, conversationId: string, version: number) {
  return ring(identity, conversationId).has(version)
}

export class MissingKeyError extends Error {
  constructor() {
    super('This conversation’s key has not been shared with you yet. It will be once another member opens it.')
    this.name = 'MissingKeyError'
  }
}

/** Public keys whose fingerprint matches their bytes; anything else is ignored. */
async function verified(keys: PublicKey[]) {
  const result = new Map<string, PublicKey>()
  for (const key of keys) if (await fingerprint(key.public_key) === key.fingerprint) result.set(key.key_id, key)
  return result
}

/** Unwrap every version of the conversation key shared with this identity. */
export async function loadConversationKeys(conversationId: string, identity: Identity): Promise<ConversationKeys> {
  const response = await chatApi.conversationKeys(conversationId)
  const wrappers = await verified(response.wrapper_keys)
  const held = ring(identity, conversationId)
  for (const wrap of response.wraps) {
    if (wrap.recipient_key_id !== identity.keyId || held.has(wrap.key_version)) continue
    const wrapper = wrappers.get(wrap.wrapper_key_id)
    if (!wrapper) continue
    try {
      await remember(identity, conversationId, wrap.key_version, await unwrapConversationKey(wrap, identity, wrapper.public_key, conversationId))
    } catch {
      // A wrap that does not open (tampered, or for an earlier key) is skipped; others may still work.
    }
  }
  return response
}

type Recipient = { user_id: string; key_id?: string | null }

async function wrapsFor(identity: Identity, conversationId: string, versions: Array<[number, Uint8Array]>, recipients: Recipient[]) {
  const keyIds = [...new Set(recipients.map(item => item.key_id).filter((id): id is string => Boolean(id)))]
  const keys = keyIds.length ? await verified((await chatApi.publicKeys({ keyIds })).keys) : new Map<string, PublicKey>()
  keys.set(identity.keyId, { key_id: identity.keyId, user_id: identity.userId, public_key: identity.publicKey, fingerprint: '', active: true })
  const wraps = []
  for (const recipient of recipients) {
    const key = recipient.key_id ? keys.get(recipient.key_id) : undefined
    if (!key || key.user_id !== recipient.user_id) continue
    for (const [version, raw] of versions) {
      const wrapped = await wrapConversationKey(raw, identity, { keyId: key.key_id, publicKey: key.public_key }, conversationId, version)
      wraps.push({ user_id: recipient.user_id, recipient_key_id: key.key_id, key_version: version, ...wrapped })
    }
  }
  return wraps
}

/**
 * Give every current member a copy of each key version this tab holds that they lack:
 * someone who joined, or who reset their key on a new device. Returns how many were shared.
 */
export async function shareMissingKeys(conversation: Conversation, identity: Identity, keys: ConversationKeys) {
  if (conversation.my_status !== 'active') return 0
  const held = [...ring(identity, conversation.conversation_id).entries()]
  if (!held.length) return 0
  const pending: Array<{ member: Recipient; versions: Array<[number, Uint8Array]> }> = []
  for (const member of conversation.members) {
    if (!member.key_id) continue
    const covered = keys.coverage[member.user_id]?.[member.key_id] ?? []
    const versions = held.filter(([version]) => !covered.includes(version)).map(([version, value]) => [version, value.raw] as [number, Uint8Array])
    if (versions.length) pending.push({ member, versions })
  }
  const wraps = []
  for (const item of pending) wraps.push(...await wrapsFor(identity, conversation.conversation_id, item.versions, [item.member]))
  if (wraps.length) await chatApi.shareKeys(conversation.conversation_id, { wrapper_key_id: identity.keyId, wraps })
  return wraps.length
}

/** Start a conversation: a fresh key, wrapped for you and every member who has published a key. */
export async function createConversation(identity: Identity, input: { kind: 'dm' | 'group'; memberIds: string[]; title?: string }) {
  const conversationId = crypto.randomUUID()
  const { keys } = await chatApi.publicKeys({ userIds: input.memberIds })
  const raw = newConversationKey()
  const recipients: Recipient[] = [{ user_id: identity.userId, key_id: identity.keyId }, ...keys.filter(key => key.active).map(key => ({ user_id: key.user_id, key_id: key.key_id }))]
  const wraps = await wrapsFor(identity, conversationId, [[1, raw]], recipients)
  const created = await chatApi.createConversation({
    conversation_id: conversationId, kind: input.kind, member_ids: input.memberIds, title: input.title,
    wrapper_key_id: identity.keyId, wraps,
  })
  await remember(identity, conversationId, 1, raw)
  return created
}

/** Replace the conversation key (after someone leaves or is removed) for the members still in it. */
export async function rotateConversationKey(conversation: Conversation, identity: Identity) {
  const version = conversation.key_version + 1
  const raw = newConversationKey()
  const members = conversation.members.map(member => member.user_id === identity.userId ? { ...member, key_id: identity.keyId } : member)
  const wraps = await wrapsFor(identity, conversation.conversation_id, [[version, raw]], members)
  await chatApi.rotate(conversation.conversation_id, { key_version: version, wrapper_key_id: identity.keyId, wraps })
  await remember(identity, conversation.conversation_id, version, raw)
  return version
}

/** Invite people to a group, sharing every key version you hold so they can read its history. */
export async function inviteToConversation(conversation: Conversation, identity: Identity, userIds: string[]) {
  const { keys } = await chatApi.publicKeys({ userIds })
  const held = [...ring(identity, conversation.conversation_id).entries()].map(([version, value]) => [version, value.raw] as [number, Uint8Array])
  const recipients = keys.filter(key => key.active).map(key => ({ user_id: key.user_id, key_id: key.key_id }))
  const wraps = held.length ? await wrapsFor(identity, conversation.conversation_id, held, recipients) : []
  return chatApi.invite(conversation.conversation_id, { user_ids: userIds, wrapper_key_id: identity.keyId, wraps })
}

export async function encryptMessage(conversation: Conversation, identity: Identity, body: MessageBody, messageId: string) {
  const version = conversation.key_version
  const key = ring(identity, conversation.conversation_id).get(version)
  if (!key) throw new MissingKeyError()
  const sealed = await encryptPayload(key.key, body, messageAad(conversation.conversation_id, version, identity.userId, messageId))
  return { ...sealed, key_version: version }
}

export type Decrypted = { state: 'ok'; body: MessageBody } | { state: 'missing' } | { state: 'error' }

function isBody(value: unknown): value is MessageBody {
  return typeof value === 'object' && value !== null && typeof (value as MessageBody).text === 'string'
    && typeof ((value as MessageBody).refs ?? {}) === 'object'
}

export async function decryptMessage(message: ChatMessage, identity: Identity): Promise<Decrypted> {
  const sealed = message.encrypted
  if (!sealed || !message.conversation_id) return { state: 'error' }
  const key = ring(identity, message.conversation_id).get(sealed.key_version)
  if (!key) return { state: 'missing' }
  try {
    const body = await decryptPayload<unknown>(key.key, sealed.ciphertext, sealed.nonce, messageAad(message.conversation_id, sealed.key_version, message.sender.user_id, message.message_id))
    return isBody(body) ? { state: 'ok', body: { text: body.text, refs: body.refs ?? {} } } : { state: 'error' }
  } catch {
    return { state: 'error' }
  }
}

// Each result remembers the keyring revision it was tried at; a failure is retried only once keys change.
const decrypted = new Map<string, { result: Decrypted; revision: number }>()

/** Decrypted bodies for the encrypted messages given, filled in as keys arrive. */
export function useDecryptedBodies(messages: ChatMessage[], identity: Identity | null) {
  const keysRevision = useKeyringRevision()
  const [, setDone] = useState(0)
  useEffect(() => {
    if (!identity) return
    let cancelled = false
    const pending = messages.filter(message => {
      if (!message.encrypted) return false
      const seen = decrypted.get(`${identity.keyId}:${message.message_id}`)
      return !seen || (seen.result.state !== 'ok' && seen.revision !== keysRevision)
    })
    if (!pending.length) return
    void (async () => {
      for (const message of pending) decrypted.set(`${identity.keyId}:${message.message_id}`, { result: await decryptMessage(message, identity), revision: keysRevision })
      if (!cancelled) setDone(value => value + 1)
    })()
    return () => { cancelled = true }
  }, [messages, identity, keysRevision])
  return (message: ChatMessage): Decrypted | undefined => (identity ? decrypted.get(`${identity.keyId}:${message.message_id}`)?.result : undefined)
}

function signature(identity: Identity, conversation: Conversation) {
  return `${identity.keyId}|${conversation.key_version}|${conversation.my_status}|${conversation.my_wraps}|${conversation.members.map(member => `${member.user_id}:${member.key_id ?? ''}:${member.status}`).join(',')}`
}

/**
 * Unwrap the keys of every conversation listed, and share them with members who
 * lack a copy. Runs again for a conversation when its members, their keys, or its
 * key version change.
 */
export function useConversationKeys(identity: Identity | null, conversations: Conversation[]) {
  const done = useRef(new Map<string, string>())
  const working = useRef(new Set<string>())
  const [error, setError] = useState<string | null>(null)
  useEffect(() => {
    if (!identity) return
    let cancelled = false
    void (async () => {
      for (const conversation of conversations) {
        const id = conversation.conversation_id
        const current = signature(identity, conversation)
        if (done.current.get(id) === current || working.current.has(id)) continue
        working.current.add(id)
        try {
          // Finished even if this effect is superseded meanwhile: a half-shared key helps no one.
          await shareMissingKeys(conversation, identity, await loadConversationKeys(id, identity))
          done.current.set(id, current)
        } catch (err) {
          if (!cancelled) setError(err instanceof Error ? err.message : 'Conversation keys could not be loaded')
        } finally {
          working.current.delete(id)
        }
      }
    })()
    return () => { cancelled = true }
  }, [identity, conversations])
  return error
}

/** Forget every unwrapped conversation key (on lock or sign-out). */
export function clearConversationKeys() {
  rings.clear()
  decrypted.clear()
  ringOwner = null
  bump()
}
