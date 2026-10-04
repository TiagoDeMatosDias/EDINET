import { useQueryClient } from '@tanstack/react-query'
import { KeyRound, Lock, ShieldCheck } from 'lucide-react'
import { useEffect, useRef, useState, type FormEvent } from 'react'

import { chatApi, invalidateChat } from './chatApi'
import { formatFingerprint, generateIdentityKeys, importPrivateKey, unwrapPrivateKey, wrapPrivateKey, WrongPassphraseError, type Identity } from './crypto'
import { clearConversationKeys } from './e2e'
import { rememberIdentity } from './keyStore'

export const MIN_PASSPHRASE = 10

async function createIdentity(userId: string, passphrase: string): Promise<Identity> {
  const keys = await generateIdentityKeys()
  const wrapped = await wrapPrivateKey(keys.pkcs8, passphrase)
  const { key } = await chatApi.publishKey({ public_key: keys.publicKey, fingerprint: keys.fingerprint, ...wrapped })
  return { userId, keyId: key.key_id, publicKey: keys.publicKey, privateKey: await importPrivateKey(keys.pkcs8) }
}

async function unlockIdentity(userId: string, passphrase: string): Promise<Identity> {
  const { key } = await chatApi.ownKey()
  if (!key) throw new Error('You have no encryption key yet.')
  const { privateKey } = await unwrapPrivateKey(key, passphrase)
  return { userId, keyId: key.key_id, publicKey: key.public_key, privateKey }
}

/**
 * Create or unlock the key that reads direct and group messages. The
 * passphrase stays in this browser; the server stores the key only wrapped by it.
 */
export function KeyPanel({ userId, serverKey, compact = false }: {
  userId: string
  serverKey: { key_id: string; fingerprint: string } | null
  compact?: boolean
}) {
  const client = useQueryClient()
  const [mode, setMode] = useState<'unlock' | 'create'>(serverKey ? 'unlock' : 'create')
  const [passphrase, setPassphrase] = useState('')
  const [confirm, setConfirm] = useState('')
  const [remember, setRemember] = useState(true)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [resetArmed, setResetArmed] = useState(false)
  const input = useRef<HTMLInputElement>(null)
  useEffect(() => { input.current?.focus() }, [mode])

  const creating = mode === 'create'
  const tooShort = creating && passphrase.length < MIN_PASSPHRASE
  const mismatch = creating && confirm !== passphrase
  const submit = async (event: FormEvent) => {
    event.preventDefault()
    if (busy || !passphrase || tooShort || mismatch) return
    setBusy(true)
    setError(null)
    try {
      const identity = creating ? await createIdentity(userId, passphrase) : await unlockIdentity(userId, passphrase)
      clearConversationKeys()
      await rememberIdentity(identity, remember)
      setPassphrase('')
      setConfirm('')
      invalidateChat(client)
    } catch (err) {
      setError(err instanceof WrongPassphraseError ? err.message : err instanceof Error ? err.message : 'That did not work')
    } finally {
      setBusy(false)
    }
  }

  return <form className={compact ? 'chat-keys chat-keys--compact' : 'chat-keys'} onSubmit={event => void submit(event)} aria-label={creating ? 'Set up encryption' : 'Unlock encrypted messages'}>
    <header>{creating ? <ShieldCheck aria-hidden="true" /> : <Lock aria-hidden="true" />}<h3>{creating ? (serverKey ? 'Create a new encryption key' : 'Set up end-to-end encryption') : 'Unlock encrypted messages'}</h3></header>
    {!compact && <p>{creating
      ? 'Direct and group messages are encrypted in your browser. Choose a chat passphrase: it never leaves this device, and the server keeps your key only locked by it. Use a different one from your password.'
      : 'Enter your chat passphrase to read and send direct and group messages on this device.'}</p>}
    {serverKey && !creating && <p className="chat-keys__fingerprint">Your key <code>{formatFingerprint(serverKey.fingerprint)}</code></p>}
    <label className="field"><span>Chat passphrase</span><input ref={input} className="input" type="password" autoComplete={creating ? 'new-password' : 'current-password'} value={passphrase} onChange={event => setPassphrase(event.target.value)} /></label>
    {creating && <label className="field"><span>Repeat it</span><input className="input" type="password" autoComplete="new-password" value={confirm} onChange={event => setConfirm(event.target.value)} /></label>}
    <label className="check"><input type="checkbox" checked={remember} onChange={event => setRemember(event.target.checked)} />Stay unlocked on this device until I sign out</label>
    {creating && passphrase && tooShort && <small className="muted">At least {MIN_PASSPHRASE} characters.</small>}
    {creating && confirm && mismatch && <small className="form-error">The passphrases differ.</small>}
    {error && <small className="form-error" role="alert">{error}</small>}
    <div className="button-row">
      <button type="submit" className="button button--primary button--small" disabled={busy || !passphrase || tooShort || mismatch}><KeyRound aria-hidden="true" />{busy ? (creating ? 'Creating…' : 'Unlocking…') : creating ? 'Create my key' : 'Unlock'}</button>
      {serverKey && (creating
        ? <button type="button" className="text-button" onClick={() => setMode('unlock')}>Back to unlocking</button>
        : <button type="button" className={resetArmed ? 'text-button is-danger' : 'text-button'} onClick={() => { if (resetArmed) { setMode('create'); setResetArmed(false) } else setResetArmed(true) }}>
          {resetArmed ? 'Messages stay locked until a member re-shares them: continue?' : 'Forgot the passphrase? Create a new key'}
        </button>)}
    </div>
  </form>
}
