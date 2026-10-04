import { describe, expect, it } from 'vitest'

import {
  decryptPayload,
  encryptPayload,
  fingerprint,
  formatFingerprint,
  fromBase64,
  generateIdentityKeys,
  importConversationKey,
  importPrivateKey,
  messageAad,
  newConversationKey,
  toBase64,
  unwrapConversationKey,
  unwrapPrivateKey,
  wrapConversationKey,
  wrapPrivateKey,
  WrongPassphraseError,
  type Identity,
} from './crypto'

// Fewer PBKDF2 rounds than production keep the suite fast; the server accepts 100,000 and up.
const ROUNDS = 100_000

async function person(userId: string, keyId: string): Promise<Identity> {
  const keys = await generateIdentityKeys()
  return { userId, keyId, publicKey: keys.publicKey, privateKey: await importPrivateKey(keys.pkcs8) }
}

describe('chat encryption', () => {
  it('round-trips base64 of every byte value', () => {
    const all = Uint8Array.from({ length: 256 }, (_, index) => index)
    expect(fromBase64(toBase64(all))).toEqual(all)
  })

  it('unlocks a private key only with its passphrase', async () => {
    const keys = await generateIdentityKeys()
    expect(keys.fingerprint).toBe(await fingerprint(keys.publicKey))
    expect(formatFingerprint(keys.fingerprint)).toMatch(/^([0-9A-F]{4} ){9}[0-9A-F]{4}$/)
    const wrapped = await wrapPrivateKey(keys.pkcs8, 'a long chat passphrase', ROUNDS)
    expect(wrapped.wrap_iterations).toBe(ROUNDS)
    const unlocked = await unwrapPrivateKey(wrapped, 'a long chat passphrase')
    expect(unlocked.pkcs8).toEqual(keys.pkcs8)
    await expect(unwrapPrivateKey(wrapped, 'a wrong passphrase')).rejects.toBeInstanceOf(WrongPassphraseError)
  })

  it('shares a conversation key that only the intended member can unwrap, and only in its place', async () => {
    const alice = await person('alice', '11111111-1111-1111-1111-111111111111')
    const bob = await person('bob', '22222222-2222-2222-2222-222222222222')
    const carol = await person('carol', '33333333-3333-3333-3333-333333333333')
    const raw = newConversationKey()
    const wrap = await wrapConversationKey(raw, alice, { keyId: bob.keyId, publicKey: bob.publicKey }, 'conv-1', 1)
    const stored = { ...wrap, key_version: 1, wrapper_key_id: alice.keyId, recipient_key_id: bob.keyId }

    expect(await unwrapConversationKey(stored, bob, alice.publicKey, 'conv-1')).toEqual(raw)
    await expect(unwrapConversationKey(stored, carol, alice.publicKey, 'conv-1')).rejects.toThrow()
    await expect(unwrapConversationKey(stored, bob, alice.publicKey, 'conv-2')).rejects.toThrow()
    await expect(unwrapConversationKey({ ...stored, key_version: 2 }, bob, alice.publicKey, 'conv-1')).rejects.toThrow()
    // A self-wrap works the same way.
    const own = await wrapConversationKey(raw, alice, { keyId: alice.keyId, publicKey: alice.publicKey }, 'conv-1', 1)
    expect(await unwrapConversationKey({ ...own, key_version: 1, wrapper_key_id: alice.keyId, recipient_key_id: alice.keyId }, alice, alice.publicKey, 'conv-1')).toEqual(raw)
  })

  it('detects a message moved to another sender, conversation, or id', async () => {
    const key = await importConversationKey(newConversationKey())
    const aad = messageAad('conv-1', 1, 'alice', 'm1')
    const sealed = await encryptPayload(key, { text: 'こんにちは $7203', refs: {} }, aad)
    expect(await decryptPayload(key, sealed.ciphertext, sealed.nonce, aad)).toEqual({ text: 'こんにちは $7203', refs: {} })
    for (const forged of [messageAad('conv-1', 1, 'bob', 'm1'), messageAad('conv-2', 1, 'alice', 'm1'), messageAad('conv-1', 1, 'alice', 'm2')]) {
      await expect(decryptPayload(key, sealed.ciphertext, sealed.nonce, forged)).rejects.toThrow()
    }
  })
})
