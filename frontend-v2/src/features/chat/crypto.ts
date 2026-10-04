/**
 * End-to-end encryption for direct and group conversations, using WebCrypto only.
 *
 * - Each person has an ECDH P-256 identity key pair. The private key leaves the
 *   browser only wrapped with AES-GCM under a key derived (PBKDF2-SHA-256) from
 *   their chat passphrase, which the server never sees.
 * - Each conversation has a random AES-256 key per version. It is wrapped for
 *   every member under HKDF(ECDH(wrapper, member)), bound to the conversation,
 *   version, and both key ids, so a wrap cannot be replayed elsewhere.
 * - Messages are AES-GCM encrypted with the conversation key; the associated
 *   data binds the conversation, key version, sender, and message id, so the
 *   server cannot move a message or change who sent it without detection.
 */

const encoder = new TextEncoder()
const decoder = new TextDecoder()
const PROTOCOL = 'shade-chat-v1'
export const PBKDF2_ITERATIONS = 600_000

function subtle(): SubtleCrypto {
  const value = globalThis.crypto?.subtle
  if (!value) throw new Error('This browser cannot encrypt messages here; open the workspace over HTTPS.')
  return value
}

function bytes(length: number) {
  return globalThis.crypto.getRandomValues(new Uint8Array(length))
}

// WebCrypto wants ArrayBuffer-backed views; copying also detaches callers' buffers from ours.
function buffer(data: Uint8Array): ArrayBuffer {
  return data.slice().buffer as ArrayBuffer
}

export function toBase64(data: ArrayBuffer | Uint8Array) {
  const view = data instanceof Uint8Array ? data : new Uint8Array(data)
  let text = ''
  for (let index = 0; index < view.length; index += 0x8000) text += String.fromCharCode(...view.subarray(index, index + 0x8000))
  return btoa(text)
}

export function fromBase64(text: string) {
  const raw = atob(text)
  const view = new Uint8Array(raw.length)
  for (let index = 0; index < raw.length; index += 1) view[index] = raw.charCodeAt(index)
  return view
}

function hex(data: ArrayBuffer) {
  return [...new Uint8Array(data)].map(value => value.toString(16).padStart(2, '0')).join('')
}

export class WrongPassphraseError extends Error {
  constructor() {
    super('That passphrase does not unlock your encryption key.')
    this.name = 'WrongPassphraseError'
  }
}

/** An unlocked identity: the private key never leaves this object in the clear. */
export interface Identity {
  userId: string
  keyId: string
  privateKey: CryptoKey
  publicKey: string
}

export interface WrappedIdentity {
  wrapped_private_key: string
  wrap_salt: string
  wrap_iv: string
  wrap_iterations: number
}

export async function fingerprint(publicKey: string) {
  return hex(await subtle().digest('SHA-256', buffer(fromBase64(publicKey))))
}

/** The first 20 bytes of a fingerprint, in groups of four, for comparing out loud or side by side. */
export function formatFingerprint(value: string | null | undefined) {
  if (!value) return '—'
  return value.slice(0, 40).toUpperCase().match(/.{1,4}/g)!.join(' ')
}

export async function generateIdentityKeys() {
  const pair = await subtle().generateKey({ name: 'ECDH', namedCurve: 'P-256' }, true, ['deriveBits']) as CryptoKeyPair
  const publicKey = toBase64(await subtle().exportKey('spki', pair.publicKey))
  const pkcs8 = new Uint8Array(await subtle().exportKey('pkcs8', pair.privateKey))
  return { publicKey, pkcs8, fingerprint: await fingerprint(publicKey) }
}

async function passphraseKey(passphrase: string, salt: Uint8Array, iterations: number) {
  const material = await subtle().importKey('raw', buffer(encoder.encode(passphrase.normalize('NFKC'))), 'PBKDF2', false, ['deriveKey'])
  return subtle().deriveKey(
    { name: 'PBKDF2', hash: 'SHA-256', salt: buffer(salt), iterations },
    material,
    { name: 'AES-GCM', length: 256 },
    false,
    ['encrypt', 'decrypt'],
  )
}

const IDENTITY_AAD = buffer(encoder.encode(`${PROTOCOL}|identity`))

export async function wrapPrivateKey(pkcs8: Uint8Array, passphrase: string, iterations = PBKDF2_ITERATIONS): Promise<WrappedIdentity> {
  const salt = bytes(16)
  const iv = bytes(12)
  const key = await passphraseKey(passphrase, salt, iterations)
  const wrapped = await subtle().encrypt({ name: 'AES-GCM', iv: buffer(iv), additionalData: IDENTITY_AAD }, key, buffer(pkcs8))
  return { wrapped_private_key: toBase64(wrapped), wrap_salt: toBase64(salt), wrap_iv: toBase64(iv), wrap_iterations: iterations }
}

/** Unlock a stored private key. Returns the PKCS#8 bytes too, for re-wrapping under a new passphrase. */
export async function unwrapPrivateKey(wrapped: WrappedIdentity, passphrase: string) {
  const key = await passphraseKey(passphrase, fromBase64(wrapped.wrap_salt), wrapped.wrap_iterations)
  let pkcs8: Uint8Array
  try {
    pkcs8 = new Uint8Array(await subtle().decrypt({ name: 'AES-GCM', iv: buffer(fromBase64(wrapped.wrap_iv)), additionalData: IDENTITY_AAD }, key, buffer(fromBase64(wrapped.wrapped_private_key))))
  } catch {
    throw new WrongPassphraseError()
  }
  const privateKey = await subtle().importKey('pkcs8', buffer(pkcs8), { name: 'ECDH', namedCurve: 'P-256' }, false, ['deriveBits'])
  return { privateKey, pkcs8 }
}

export async function importPrivateKey(pkcs8: Uint8Array) {
  return subtle().importKey('pkcs8', buffer(pkcs8), { name: 'ECDH', namedCurve: 'P-256' }, false, ['deriveBits'])
}

async function importPublicKey(publicKey: string) {
  return subtle().importKey('spki', buffer(fromBase64(publicKey)), { name: 'ECDH', namedCurve: 'P-256' }, false, [])
}

export function wrapInfo(conversationId: string, version: number, wrapperKeyId: string, recipientKeyId: string) {
  return `${PROTOCOL}|wrap|conv:${conversationId}|v:${version}|from:${wrapperKeyId}|to:${recipientKeyId}`
}

/** The key-encryption key both sides of a wrap can derive: HKDF over the ECDH secret, bound to ``info``. */
async function sharedKey(privateKey: CryptoKey, peerPublicKey: string, info: string) {
  const secret = await subtle().deriveBits({ name: 'ECDH', public: await importPublicKey(peerPublicKey) }, privateKey, 256)
  const material = await subtle().importKey('raw', secret, 'HKDF', false, ['deriveKey'])
  return subtle().deriveKey(
    { name: 'HKDF', hash: 'SHA-256', salt: buffer(encoder.encode(PROTOCOL)), info: buffer(encoder.encode(info)) },
    material,
    { name: 'AES-GCM', length: 256 },
    false,
    ['encrypt', 'decrypt'],
  )
}

export function newConversationKey() {
  return bytes(32)
}

export async function wrapConversationKey(raw: Uint8Array, identity: Identity, recipient: { keyId: string; publicKey: string }, conversationId: string, version: number) {
  const info = wrapInfo(conversationId, version, identity.keyId, recipient.keyId)
  const key = await sharedKey(identity.privateKey, recipient.publicKey, info)
  const iv = bytes(12)
  const wrapped = await subtle().encrypt({ name: 'AES-GCM', iv: buffer(iv), additionalData: buffer(encoder.encode(info)) }, key, buffer(raw))
  return { wrapped_key: toBase64(wrapped), iv: toBase64(iv) }
}

export async function unwrapConversationKey(
  wrap: { wrapped_key: string; iv: string; key_version: number; wrapper_key_id: string; recipient_key_id: string },
  identity: Identity,
  wrapperPublicKey: string,
  conversationId: string,
) {
  const info = wrapInfo(conversationId, wrap.key_version, wrap.wrapper_key_id, wrap.recipient_key_id)
  const key = await sharedKey(identity.privateKey, wrapperPublicKey, info)
  return new Uint8Array(await subtle().decrypt({ name: 'AES-GCM', iv: buffer(fromBase64(wrap.iv)), additionalData: buffer(encoder.encode(info)) }, key, buffer(fromBase64(wrap.wrapped_key))))
}

export async function importConversationKey(raw: Uint8Array) {
  return subtle().importKey('raw', buffer(raw), { name: 'AES-GCM' }, false, ['encrypt', 'decrypt'])
}

export function messageAad(conversationId: string, version: number, senderId: string, messageId: string) {
  return `${PROTOCOL}|message|conv:${conversationId}|v:${version}|sender:${senderId}|id:${messageId}`
}

export async function encryptPayload(key: CryptoKey, payload: unknown, aad: string) {
  const iv = bytes(12)
  const data = await subtle().encrypt({ name: 'AES-GCM', iv: buffer(iv), additionalData: buffer(encoder.encode(aad)) }, key, buffer(encoder.encode(JSON.stringify(payload))))
  return { ciphertext: toBase64(data), nonce: toBase64(iv) }
}

export async function decryptPayload<T>(key: CryptoKey, ciphertext: string, nonce: string, aad: string): Promise<T> {
  const data = await subtle().decrypt({ name: 'AES-GCM', iv: buffer(fromBase64(nonce)), additionalData: buffer(encoder.encode(aad)) }, key, buffer(fromBase64(ciphertext)))
  return JSON.parse(decoder.decode(data)) as T
}
