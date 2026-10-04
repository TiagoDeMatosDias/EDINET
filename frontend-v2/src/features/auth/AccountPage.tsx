import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Ban, ExternalLink, Keyboard, KeyRound, Lock, X } from 'lucide-react'
import { useCallback, useEffect, useRef, useState, type FormEvent, type KeyboardEvent, type ReactNode } from 'react'
import { Link, useNavigate } from 'react-router-dom'

import { apiRequest } from '../../api/client'
import type { SecuritySearchResult } from '../../api/types'
import { CompanyPicker } from '../../components/CompanyPicker'
import { LoadingState } from '../../components/Feedback'
import { ShortcutsDialog, type ShortcutGroup } from '../../components/ShortcutsDialog'
import { useHotkeys } from '../../hooks/useHotkeys'
import { chatApi, chatKeys, invalidateChat, useChatSummary } from '../chat/chatApi'
import { channelLabel, colorFor, personName } from '../chat/chatModel'
import type { ChatProfile, Person } from '../chat/chatTypes'
import { formatFingerprint, unwrapPrivateKey, wrapPrivateKey, WrongPassphraseError } from '../chat/crypto'
import { KeyPanel, MIN_PASSPHRASE } from '../chat/KeyPanel'
import { forgetIdentity, useChatIdentity } from '../chat/keyStore'
import { useAuth } from './authContext'
import './account.css'

interface ApiToken {
  token_id: string
  name: string
  token_prefix: string
  scopes_json: string
  created_at: string
  expires_at: string | null
  last_used_at: string | null
  revoked_at: string | null
}

interface Session {
  session_id: string
  token_type: string
  created_at: string
  expires_at: string | null
  revoked_at: string | null
  user_agent: string | null
}

const SECTIONS = [
  { id: 'profile', label: 'Public profile' },
  { id: 'account', label: 'Sign-in details' },
  { id: 'password', label: 'Password' },
  { id: 'encryption', label: 'Chat encryption' },
  { id: 'blocks', label: 'Blocked' },
  { id: 'tokens', label: 'API tokens' },
  { id: 'sessions', label: 'Sessions' },
] as const

const SHORTCUTS: ShortcutGroup[] = [
  { title: 'Account', shortcuts: [
    { keys: ['1', '2', '3', '4', '5', '6', '7'], label: SECTIONS.map(section => section.label).join(', ') },
    { keys: ['V'], label: 'View your public profile' },
    { keys: ['Enter'], label: 'In a field: save that section' },
    { keys: ['J', 'K'], label: 'In a list: next or previous entry (↓ ↑)' },
    { keys: ['U'], label: 'In the blocked list: unblock the entry' },
    { keys: ['?'], label: 'Show or hide this list' },
  ] },
]

const dateTime = new Intl.DateTimeFormat('en-GB', { dateStyle: 'medium', timeStyle: 'short' })
const when = (value: string | null | undefined) => (value ? dateTime.format(new Date(value)) : '—')

function Section({ id, index, title, description, actions, children }: { id: string; index: number; title: string; description?: string; actions?: ReactNode; children: ReactNode }) {
  return <section id={`account-${id}`} className="account-section" aria-labelledby={`account-${id}-title`}>
    <header><kbd aria-hidden="true">{index + 1}</kbd><h2 id={`account-${id}-title`}>{title}</h2>{description && <p>{description}</p>}{actions}</header>
    {children}
  </section>
}

function focusSection(id: string) {
  const section = document.getElementById(`account-${id}`)
  section?.scrollIntoView?.({ block: 'start', behavior: 'smooth' })
  section?.querySelector<HTMLElement>('input, textarea, select, button')?.focus({ preventScroll: true })
}

export default function AccountPage() {
  const { user, status } = useAuth()
  const navigate = useNavigate()
  const [help, setHelp] = useState(false)
  const closeHelp = useCallback(() => setHelp(false), [])
  const accounts = status?.mode === 'accounts'
  useEffect(() => {
    const hash = window.location.hash.replace('#', '')
    if (SECTIONS.some(section => section.id === hash)) setTimeout(() => focusSection(hash), 50)
  }, [])
  useHotkeys({
    ...Object.fromEntries(SECTIONS.map((section, index) => [String(index + 1), () => focusSection(section.id)])),
    v: () => { if (user) navigate(`/people/${encodeURIComponent(user.username)}`) },
    '?': () => setHelp(true),
  }, !help)

  return <div className="account-page dense-page">
    <header className="account-head">
      <div><span className="eyebrow">Account</span><h1>{user?.username ?? 'Local workspace'}</h1><p>{user ? `${user.role} · ${user.email ?? 'no email'}` : 'Authentication is disabled; account settings apply to the local user.'}</p></div>
      <nav className="account-jump" aria-label="Sections">{SECTIONS.map((section, index) => <button key={section.id} type="button" onClick={() => focusSection(section.id)}><kbd>{index + 1}</kbd>{section.label}</button>)}</nav>
      <div className="account-head__actions">
        {user && <Link className="button button--secondary button--small" to={`/people/${encodeURIComponent(user.username)}`}><ExternalLink aria-hidden="true" />Public profile <kbd>V</kbd></Link>}
        <button type="button" className="icon-button" onClick={() => setHelp(true)} title="Keyboard shortcuts (?)" aria-label="Keyboard shortcuts"><Keyboard /></button>
      </div>
    </header>
    <div className="account-grid">
      <div className="account-column">
        <Section id="profile" index={0} title="Public profile" description="What other members see on your profile and next to your messages.">
          <PublicProfileSection />
        </Section>
        {accounts && user && <>
          <Section id="account" index={1} title="Sign-in details"><SignInSection /></Section>
          <Section id="password" index={2} title="Password" description="Changing it signs out your other sessions."><PasswordSection /></Section>
        </>}
      </div>
      <div className="account-column">
        <Section id="encryption" index={3} title="Chat encryption" description="Direct and group messages are end-to-end encrypted with a key only your browsers can unlock."><EncryptionSection /></Section>
        <Section id="blocks" index={4} title="Blocked people and channels" description="You do not see their messages anywhere, and blocked people cannot message you."><BlocksSection /></Section>
        {accounts && user && <>
          <Section id="tokens" index={5} title="Personal API tokens" description="Shown once when created. Revoke any you no longer use."><TokensSection /></Section>
          <Section id="sessions" index={6} title="Sessions" description="Revoke sessions you do not recognise."><SessionsSection /></Section>
        </>}
      </div>
    </div>
    {help && <ShortcutsDialog groups={SHORTCUTS} onClose={closeHelp} />}
  </div>
}

function useMessage() {
  const [message, setMessage] = useState<{ kind: 'ok' | 'error'; text: string } | null>(null)
  const node = message ? <p className={message.kind === 'ok' ? 'form-success' : 'form-error'} role={message.kind === 'error' ? 'alert' : 'status'}>{message.text}</p> : null
  return { node, ok: (text: string) => setMessage({ kind: 'ok', text }), fail: (err: unknown) => setMessage({ kind: 'error', text: err instanceof Error ? err.message : String(err) }), clear: () => setMessage(null) }
}

function PublicProfileSection() {
  const client = useQueryClient()
  const profile = useQuery({ queryKey: chatKeys.myProfile, queryFn: () => apiRequest<ChatProfile & { public: { companies: Array<{ code: string; name: string }> } }>('/api/profiles/me') })
  const [draft, setDraft] = useState<ChatProfile | null>(null)
  const [names, setNames] = useState<Record<string, string>>({})
  const message = useMessage()
  const loaded = profile.data
  const value = draft ?? loaded ?? null
  const companyNames = { ...Object.fromEntries((loaded?.public.companies ?? []).map(item => [item.code, item.name])), ...names }
  const save = useMutation({
    mutationFn: (next: ChatProfile) => apiRequest('/api/profiles/me', { method: 'PATCH', body: JSON.stringify({
      display_name: next.display_name ?? '', bio: next.bio ?? '', location: next.location ?? '', website: next.website ?? '',
      interests: next.interests, companies: next.companies, allow_dms: next.allow_dms, discoverable: next.discoverable,
    }) }),
    onSuccess: () => { message.ok('Profile saved.'); setDraft(null); void client.invalidateQueries({ queryKey: chatKeys.myProfile }); invalidateChat(client) },
    onError: message.fail,
  })
  if (profile.isLoading || !value) return <LoadingState label="Loading profile" />
  const set = (patch: Partial<ChatProfile>) => { message.clear(); setDraft({ ...value, ...patch }) }
  const addCompany = (company: SecuritySearchResult | null) => {
    const code = company?.company_code
    if (!code || value.companies.includes(code) || value.companies.length >= 12) return
    setNames(current => ({ ...current, [code]: company.company_name || code }))
    set({ companies: [...value.companies, code] })
  }
  const submit = (event: FormEvent) => { event.preventDefault(); save.mutate(value) }
  return <form className="account-form" onSubmit={submit}>
    <div className="account-fields account-fields--2">
      <label className="field"><span>Display name</span><input className="input" maxLength={60} value={value.display_name ?? ''} placeholder={value.username} onChange={event => set({ display_name: event.target.value })} /></label>
      <label className="field"><span>Location</span><input className="input" maxLength={80} value={value.location ?? ''} onChange={event => set({ location: event.target.value })} /></label>
    </div>
    <label className="field"><span>Bio</span><textarea className="input" rows={3} maxLength={1000} value={value.bio ?? ''} placeholder="What you research, how you invest" onChange={event => set({ bio: event.target.value })} onKeyDown={event => { if (event.key === 'Enter' && (event.ctrlKey || event.metaKey)) submit(event) }} /></label>
    <div className="account-fields account-fields--2">
      <label className="field"><span>Website</span><input className="input" type="url" maxLength={200} value={value.website ?? ''} placeholder="https://" onChange={event => set({ website: event.target.value })} /></label>
      <label className="field"><span>Interests <small>(comma-separated, up to 12)</small></span><input className="input" value={value.interests.join(', ')} onChange={event => set({ interests: event.target.value.split(',').map(item => item.trimStart()).slice(0, 12) })} onBlur={() => set({ interests: value.interests.map(item => item.trim()).filter(Boolean) })} /></label>
    </div>
    <div className="field"><span>Companies you follow <small>(shown on your profile, up to 12)</small></span>
      <div className="account-chips">{value.companies.map(code => <span key={code} className="account-chip">{companyNames[code] ?? code}<button type="button" className="icon-button" aria-label={`Remove ${companyNames[code] ?? code}`} onClick={() => set({ companies: value.companies.filter(item => item !== code) })}><X /></button></span>)}</div>
      {value.companies.length < 12 && <CompanyPicker selected={null} onSelect={addCompany} clearOnSelect label="Add a company" />}
    </div>
    <div className="account-fields account-fields--2">
      <label className="check"><input type="checkbox" checked={value.allow_dms === 'everyone'} onChange={event => set({ allow_dms: event.target.checked ? 'everyone' : 'nobody' })} />Accept private messages from members</label>
      <label className="check"><input type="checkbox" checked={value.discoverable} onChange={event => set({ discoverable: event.target.checked })} />Appear in people search (others can still find your exact username)</label>
    </div>
    <div className="account-actions">
      <button type="submit" className="button button--primary button--small" disabled={save.isPending || !draft}>{save.isPending ? 'Saving…' : 'Save profile'}</button>
      {draft && <button type="button" className="text-button" onClick={() => setDraft(null)}>Discard changes</button>}
      {message.node}
    </div>
  </form>
}

function SignInSection() {
  const { user } = useAuth()
  const client = useQueryClient()
  const [username, setUsername] = useState(user?.username ?? '')
  const [email, setEmail] = useState(user?.email ?? '')
  const message = useMessage()
  const update = useMutation({
    mutationFn: () => apiRequest('/api/auth/me', { method: 'PATCH', body: JSON.stringify({ username: username || undefined, email: email || undefined }) }),
    onSuccess: () => { message.ok('Saved. A new username applies from your next sign-in.'); void client.invalidateQueries({ queryKey: ['auth-me'] }) },
    onError: message.fail,
  })
  return <form className="account-form" onSubmit={event => { event.preventDefault(); if (username.trim()) update.mutate() }}>
    <div className="account-fields account-fields--2">
      <label className="field"><span>Username</span><input className="input" autoComplete="username" value={username} onChange={event => setUsername(event.target.value)} /></label>
      <label className="field"><span>Email</span><input className="input" type="email" autoComplete="email" value={email} onChange={event => setEmail(event.target.value)} /></label>
    </div>
    <div className="account-actions"><button type="submit" className="button button--primary button--small" disabled={update.isPending || !username.trim()}>Save</button>{message.node}</div>
  </form>
}

function PasswordSection() {
  const { status } = useAuth()
  const minimum = status?.password_min_length ?? 5
  const [current, setCurrent] = useState('')
  const [next, setNext] = useState('')
  const message = useMessage()
  const change = useMutation({
    mutationFn: () => apiRequest('/api/auth/change-password', { method: 'POST', body: JSON.stringify({ current_password: current, new_password: next }) }),
    onSuccess: () => { message.ok('Password changed; your other sessions were signed out.'); setCurrent(''); setNext('') },
    onError: message.fail,
  })
  return <form className="account-form" onSubmit={event => { event.preventDefault(); if (current && next.length >= minimum) change.mutate() }}>
    <div className="account-fields account-fields--2">
      <label className="field"><span>Current password</span><input className="input" type="password" autoComplete="current-password" value={current} onChange={event => setCurrent(event.target.value)} /></label>
      <label className="field"><span>New password <small>(at least {minimum})</small></span><input className="input" type="password" autoComplete="new-password" minLength={minimum} value={next} onChange={event => setNext(event.target.value)} /></label>
    </div>
    <div className="account-actions"><button type="submit" className="button button--primary button--small" disabled={change.isPending || !current || next.length < minimum}>{change.isPending ? 'Changing…' : 'Change password'}</button>{message.node}</div>
  </form>
}

function EncryptionSection() {
  const summary = useChatSummary()
  const me = summary.data?.me
  const serverKey = summary.data?.key ?? null
  const { identity, restoring } = useChatIdentity(me?.user_id, summary.data ? serverKey?.key_id ?? null : undefined)
  const [current, setCurrent] = useState('')
  const [next, setNext] = useState('')
  const [confirm, setConfirm] = useState('')
  const [busy, setBusy] = useState(false)
  const message = useMessage()
  if (summary.isLoading || restoring) return <LoadingState label="Checking your key" />
  if (!me) return <p className="muted">Chat is unavailable.</p>
  if (!identity || !serverKey) return <KeyPanel userId={me.user_id} serverKey={serverKey} compact />
  const valid = current && next.length >= MIN_PASSPHRASE && next === confirm
  const change = async (event: FormEvent) => {
    event.preventDefault()
    if (!valid || busy) return
    setBusy(true)
    message.clear()
    try {
      const { key } = await chatApi.ownKey()
      if (!key) throw new Error('You have no key to change.')
      const { pkcs8 } = await unwrapPrivateKey(key, current)
      await chatApi.rewrapKey(key.key_id, await wrapPrivateKey(pkcs8, next))
      setCurrent(''); setNext(''); setConfirm('')
      message.ok('Passphrase changed. Use the new one on your other devices.')
    } catch (err) {
      message.fail(err instanceof WrongPassphraseError ? new Error('The current passphrase is not right.') : err)
    } finally {
      setBusy(false)
    }
  }
  return <div className="account-form">
    <dl className="account-facts">
      <div><dt>Status</dt><dd><span className="account-ok"><KeyRound aria-hidden="true" />Unlocked on this device</span></dd></div>
      <div><dt>Fingerprint</dt><dd><code title="Others can compare this on your profile to be sure messages reach only you">{formatFingerprint(serverKey.fingerprint)}</code></dd></div>
      <div><dt>Created</dt><dd>{when(serverKey.created_at)}</dd></div>
    </dl>
    <form onSubmit={event => void change(event)} className="account-form">
      <div className="account-fields account-fields--3">
        <label className="field"><span>Current passphrase</span><input className="input" type="password" autoComplete="current-password" value={current} onChange={event => setCurrent(event.target.value)} /></label>
        <label className="field"><span>New passphrase</span><input className="input" type="password" autoComplete="new-password" value={next} onChange={event => setNext(event.target.value)} /></label>
        <label className="field"><span>Repeat it</span><input className="input" type="password" autoComplete="new-password" value={confirm} onChange={event => setConfirm(event.target.value)} /></label>
      </div>
      <div className="account-actions">
        <button type="submit" className="button button--primary button--small" disabled={!valid || busy}>{busy ? 'Changing…' : 'Change passphrase'}</button>
        <button type="button" className="button button--ghost button--small" onClick={() => void forgetIdentity(me.user_id)}><Lock aria-hidden="true" />Lock on this device</button>
        {next && next.length < MIN_PASSPHRASE && <small className="muted">At least {MIN_PASSPHRASE} characters.</small>}
        {confirm && next !== confirm && <small className="form-error">The new passphrases differ.</small>}
        {message.node}
      </div>
    </form>
  </div>
}

function BlocksSection() {
  const client = useQueryClient()
  const blocks = useQuery({ queryKey: chatKeys.blocks, queryFn: chatApi.blocks })
  const [query, setQuery] = useState('')
  const [cursor, setCursor] = useState(0)
  const message = useMessage()
  const list = useRef<HTMLUListElement>(null)
  const people = useQuery({ queryKey: chatKeys.people(query.trim()), queryFn: () => chatApi.people(query.trim()), enabled: query.trim().length >= 2 })
  const entries = [
    ...(blocks.data?.users ?? []).map(item => ({ key: `user:${item.user_id}`, kind: 'user' as const, id: item.user_id, label: personName(item), detail: `@${item.username}`, href: `/people/${encodeURIComponent(item.username)}`, color: colorFor(item.user_id), created: item.created_at })),
    ...(blocks.data?.channels ?? []).map(item => ({ key: `channel:${item.channel_id}`, kind: 'channel' as const, id: item.channel_id, label: channelLabel(item), detail: item.kind === 'company' ? 'company channel' : 'channel', href: `/chat?c=${encodeURIComponent(item.channel_id)}`, color: colorFor(item.channel_id), created: item.created_at })),
  ]
  const index = Math.min(cursor, Math.max(0, entries.length - 1))
  const change = async (kind: 'user' | 'channel', id: string, on: boolean, label: string) => {
    try {
      await chatApi.block(kind, id, on)
      message.ok(on ? `Blocked ${label}.` : `Unblocked ${label}.`)
      invalidateChat(client)
      setQuery('')
    } catch (err) {
      message.fail(err)
    }
  }
  const onKeyDown = (event: KeyboardEvent<HTMLUListElement>) => {
    const delta = event.key === 'ArrowDown' || event.key === 'j' ? 1 : event.key === 'ArrowUp' || event.key === 'k' ? -1 : 0
    if (delta) { event.preventDefault(); const next = Math.max(0, Math.min(entries.length - 1, index + delta)); setCursor(next); list.current?.querySelectorAll<HTMLElement>('li')[next]?.focus() }
    if (event.key === 'u' && entries[index]) { event.preventDefault(); void change(entries[index].kind, entries[index].id, false, entries[index].label) }
  }
  const candidates = (people.data?.people ?? []).filter((person: Person) => !person.is_me && !person.blocked).slice(0, 6)
  return <div className="account-form">
    {blocks.isLoading ? <LoadingState label="Loading blocks" /> : entries.length ? <ul ref={list} className="account-list" aria-label="Blocked" onKeyDown={onKeyDown}>
      {entries.map((entry, position) => <li key={entry.key} tabIndex={position === index ? 0 : -1} className={position === index ? 'is-cursor' : undefined} onFocus={() => setCursor(position)}>
        <span className="chat-dot" style={{ background: entry.color }} />
        <Link to={entry.href}>{entry.label}</Link><small>{entry.detail}</small><small className="account-list__date">{when(entry.created)}</small>
        <button type="button" className="text-button" onClick={() => void change(entry.kind, entry.id, false, entry.label)}>Unblock <kbd>U</kbd></button>
      </li>)}
    </ul> : <p className="muted">You have not blocked anyone. Block people from their messages or profile (B), and channels from the channel (B with nothing selected).</p>}
    <label className="field"><span>Block someone</span><input className="input" placeholder="Find a person by name" value={query} onChange={event => setQuery(event.target.value)} /></label>
    {candidates.length > 0 && <ul className="account-list">{candidates.map(person => <li key={person.user_id}>
      <span className="chat-dot" style={{ background: colorFor(person.user_id) }} />{personName(person)}<small>@{person.username}</small>
      <button type="button" className="text-button" onClick={() => void change('user', person.user_id, true, person.username)}><Ban aria-hidden="true" />Block</button>
    </li>)}</ul>}
    {message.node}
  </div>
}

function TokensSection() {
  const client = useQueryClient()
  const [name, setName] = useState('')
  const [created, setCreated] = useState<string | null>(null)
  const message = useMessage()
  const tokens = useQuery({ queryKey: ['auth-tokens'], queryFn: () => apiRequest<ApiToken[]>('/api/auth/tokens') })
  const create = useMutation({
    mutationFn: () => apiRequest<{ token: string }>('/api/auth/tokens', { method: 'POST', body: JSON.stringify({ name, scopes: ['*'] }) }),
    onSuccess: data => { setCreated(data.token); setName(''); void client.invalidateQueries({ queryKey: ['auth-tokens'] }) },
    onError: message.fail,
  })
  const revoke = useMutation({
    mutationFn: (tokenId: string) => apiRequest(`/api/auth/tokens/${tokenId}`, { method: 'DELETE' }),
    onSuccess: () => void client.invalidateQueries({ queryKey: ['auth-tokens'] }),
    onError: message.fail,
  })
  const active = (tokens.data ?? []).filter(token => !token.revoked_at)
  return <div className="account-form">
    {created && <div className="callout callout--warning"><strong>Copy this token now; it is not shown again.</strong><pre className="token-display">{created}</pre><button type="button" className="text-button" onClick={() => setCreated(null)}>Dismiss</button></div>}
    <form className="account-inline" onSubmit={event => { event.preventDefault(); if (name.trim()) create.mutate() }}>
      <input className="input" value={name} onChange={event => setName(event.target.value)} placeholder="New token name" aria-label="New token name" />
      <button type="submit" className="button button--primary button--small" disabled={create.isPending || !name.trim()}>Create</button>
    </form>
    {tokens.isLoading ? <LoadingState label="Loading tokens" /> : <table className="account-table">
      <thead><tr><th>Name</th><th>Prefix</th><th>Created</th><th>Last used</th><th>Expires</th><th /></tr></thead>
      <tbody>{active.length ? active.map(token => <tr key={token.token_id}>
        <td>{token.name}</td><td><code>{token.token_prefix}…</code></td><td>{when(token.created_at)}</td><td>{when(token.last_used_at)}</td><td>{token.expires_at ? when(token.expires_at) : 'never'}</td>
        <td><button type="button" className="text-button" onClick={() => revoke.mutate(token.token_id)}>Revoke</button></td>
      </tr>) : <tr><td colSpan={6} className="muted">No active tokens.</td></tr>}</tbody>
    </table>}
    {message.node}
  </div>
}

function SessionsSection() {
  const client = useQueryClient()
  const sessions = useQuery({ queryKey: ['auth-sessions'], queryFn: () => apiRequest<Session[]>('/api/auth/sessions'), refetchInterval: 30_000 })
  const revoke = useMutation({
    mutationFn: (sessionId: string) => apiRequest(`/api/auth/sessions/${sessionId}`, { method: 'DELETE' }),
    onSuccess: () => void client.invalidateQueries({ queryKey: ['auth-sessions'] }),
  })
  const active = (sessions.data ?? []).filter(session => !session.revoked_at && session.token_type === 'refresh')
  return sessions.isLoading ? <LoadingState label="Loading sessions" /> : <table className="account-table">
    <thead><tr><th>Signed in</th><th>Expires</th><th>Browser</th><th /></tr></thead>
    <tbody>{active.length ? active.map(session => <tr key={session.session_id}>
      <td>{when(session.created_at)}</td><td>{when(session.expires_at)}</td><td className="account-table__agent" title={session.user_agent ?? ''}>{session.user_agent ?? '—'}</td>
      <td><button type="button" className="text-button" disabled={revoke.isPending} onClick={() => revoke.mutate(session.session_id)}>Revoke</button></td>
    </tr>) : <tr><td colSpan={4} className="muted">No other sessions.</td></tr>}</tbody>
  </table>
}
