import { BarChart3, Bell, BellOff, Crown, Lock, LogOut, MessageSquare, Pencil, ShieldCheck, UserPlus, UserX, X } from 'lucide-react'
import { useState } from 'react'
import { Link } from 'react-router-dom'

import { channelLabel, colorFor, personName, referencedCompanies } from './chatModel'
import type { Channel, ChatMessage, Conversation, MessageBody } from './chatTypes'
import { formatFingerprint, type Identity } from './crypto'
import { HotkeyKbd } from '../../hotkeys/HotkeyKbd'
import { chatScope } from './chatHotkeys'

interface Tally { code: string; name: string; count: number }

function companiesIn(messages: ChatMessage[], bodyOf: (message: ChatMessage) => MessageBody | undefined): Tally[] {
  const counts = new Map<string, Tally>()
  for (const message of messages) {
    for (const ref of referencedCompanies(bodyOf(message))) {
      const current = counts.get(ref.code) ?? { code: ref.code, name: ref.name, count: 0 }
      current.count += 1
      counts.set(ref.code, current)
    }
  }
  return [...counts.values()].sort((a, b) => b.count - a.count).slice(0, 12)
}

function peopleIn(messages: ChatMessage[], meId: string) {
  const seen = new Map<string, ChatMessage['sender'] & { count: number }>()
  for (const message of messages) {
    if (message.kind !== 'message' || message.sender.user_id === meId) continue
    const current = seen.get(message.sender.user_id) ?? { ...message.sender, count: 0 }
    current.count += 1
    seen.set(message.sender.user_id, current)
  }
  return [...seen.values()].sort((a, b) => b.count - a.count).slice(0, 10)
}

function Mentioned({ tallies }: { tallies: Tally[] }) {
  if (!tallies.length) return null
  return <section className="chat-details__section" aria-label="Companies mentioned">
    <h4>Companies mentioned</h4>
    <ul className="chat-details__list">{tallies.map(item => <li key={item.code}>
      <Link to={`/analyze/${encodeURIComponent(item.code)}`} title="Open the analysis">{item.name}</Link>
      <Link className="chat-details__aside" to={`/chat?c=company:${encodeURIComponent(item.code)}`} title="Open the company's channel">#</Link>
      <span className="chat-details__count">{item.count}</span>
    </li>)}</ul>
  </section>
}

export function ChannelDetails({ channel, messages, bodyOf, meId, armedBlock, onSubscribe, onBlock }: {
  channel: Channel
  messages: ChatMessage[]
  bodyOf: (message: ChatMessage) => MessageBody | undefined
  meId: string
  armedBlock: boolean
  onSubscribe: (on: boolean) => void
  onBlock: () => void
}) {
  const people = peopleIn(messages, meId)
  return <aside className="chat-details" aria-label="Channel details">
    <header><span className="chat-dot" style={{ background: colorFor(channel.channel_id) }} /><h3>{channelLabel(channel)}</h3></header>
    {channel.description && <p>{channel.description}</p>}
    <dl className="chat-details__facts">
      <div><dt>Subscribers</dt><dd>{channel.subscribers ?? 0}</dd></div>
      <div><dt>Messages</dt><dd>{channel.message_count ?? messages.length}</dd></div>
      {channel.company_code && <div><dt>EDINET</dt><dd>{channel.company_code}</dd></div>}
    </dl>
    <div className="chat-details__actions">
      <button type="button" className={channel.subscribed ? 'button button--secondary button--small' : 'button button--primary button--small'} onClick={() => onSubscribe(!channel.subscribed)} title="Subscribe or unsubscribe (S)">
        {channel.subscribed ? <BellOff aria-hidden="true" /> : <Bell aria-hidden="true" />}{channel.subscribed ? 'Unsubscribe' : 'Subscribe'} <HotkeyKbd hotkey={chatScope.byId.subscribe} />
      </button>
      {channel.company_code && <Link className="button button--ghost button--small" to={`/analyze/${encodeURIComponent(channel.company_code)}`} title="Open the analysis"><BarChart3 aria-hidden="true" />Analysis <HotkeyKbd hotkey={chatScope.byId['open-company']} /></Link>}
      <button type="button" className={armedBlock ? 'button button--danger button--small' : 'button button--ghost button--small'} onClick={onBlock} title="Hide this channel everywhere (B twice with nothing selected)"><UserX aria-hidden="true" />{armedBlock ? 'Block: sure?' : 'Block channel'}</button>
    </div>
    <p className="chat-details__note"><ShieldCheck aria-hidden="true" />Public to signed-in members; stored encrypted on the server.</p>
    <Mentioned tallies={companiesIn(messages, bodyOf)} />
    {people.length > 0 && <section className="chat-details__section" aria-label="People here">
      <h4>Recently here</h4>
      <ul className="chat-details__list">{people.map(person => <li key={person.user_id}>
        <span className="chat-dot" style={{ background: colorFor(person.user_id) }} />
        <Link to={`/people/${encodeURIComponent(person.username)}`}>{personName(person)}</Link>
        <Link className="chat-details__aside" to={`/chat?dm=${encodeURIComponent(person.user_id)}`} title="Message privately"><MessageSquare aria-hidden="true" /></Link>
        <span className="chat-details__count">{person.count}</span>
      </li>)}</ul>
    </section>}
  </aside>
}

export function ConversationDetails({ conversation, identity, meId, messages, bodyOf, onInvite, onLeave, onRemove, onRename, onHide, onBlockPerson, armed }: {
  conversation: Conversation
  identity: Identity | null
  meId: string
  messages: ChatMessage[]
  bodyOf: (message: ChatMessage) => MessageBody | undefined
  onInvite: () => void
  onLeave: () => void
  onRemove: (userId: string) => void
  onRename: (title: string) => void
  onHide: () => void
  onBlockPerson: (userId: string) => void
  armed: string | null
}) {
  const [editing, setEditing] = useState(false)
  const [title, setTitle] = useState(conversation.title ?? '')
  const owner = conversation.my_role === 'owner'
  const group = conversation.kind === 'group'
  const withoutKeys = conversation.members.filter(member => !member.key_id)
  return <aside className="chat-details" aria-label="Conversation details">
    <header><Lock aria-hidden="true" />
      {editing ? <form onSubmit={event => { event.preventDefault(); if (title.trim()) { onRename(title.trim()); setEditing(false) } }}><input className="input" autoFocus value={title} maxLength={80} aria-label="Group name" onChange={event => setTitle(event.target.value)} onKeyDown={event => { if (event.key === 'Escape') { event.stopPropagation(); setEditing(false) } }} /></form>
        : <h3>{group ? conversation.title || 'Untitled group' : 'Direct message'}</h3>}
      {group && conversation.my_status === 'active' && !editing && <button type="button" className="icon-button" aria-label="Rename the group" onClick={() => setEditing(true)}><Pencil /></button>}
    </header>
    <p className="chat-details__note"><ShieldCheck aria-hidden="true" />End-to-end encrypted: only members’ browsers hold the key{conversation.key_version > 1 ? ` (version ${conversation.key_version})` : ''}. Compare fingerprints with the person to be sure no one is in between.</p>
    {identity && <p className="chat-details__fingerprint">You <code>{formatFingerprint(conversation.members.find(member => member.user_id === meId)?.fingerprint)}</code></p>}
    {withoutKeys.length > 0 && <p className="callout callout--warning">{withoutKeys.map(personName).join(', ')} {withoutKeys.length === 1 ? 'has' : 'have'} not set up encryption yet; they can read earlier messages once they do and you open this conversation.</p>}
    <section className="chat-details__section" aria-label="Members">
      <h4>{group ? `Members (${conversation.members.length})` : 'With'}</h4>
      <ul className="chat-details__list chat-details__members">{conversation.members.map(member => <li key={member.user_id}>
        <span className="chat-dot" style={{ background: colorFor(member.user_id) }} />
        <Link to={`/people/${encodeURIComponent(member.username)}`}>{personName(member)}{member.user_id === meId && ' (you)'}</Link>
        {member.role === 'owner' && group && <Crown className="chat-details__icon" aria-label="Owner" />}
        {member.status === 'invited' && <small className="muted">invited</small>}
        <code className="chat-details__fp" title="Key fingerprint">{member.fingerprint ? formatFingerprint(member.fingerprint).slice(0, 14) : 'no key'}</code>
        {owner && group && member.user_id !== meId && <button type="button" className={armed === `remove:${member.user_id}` ? 'icon-button is-armed' : 'icon-button'} aria-label={armed === `remove:${member.user_id}` ? `Confirm removing ${member.username}` : `Remove ${member.username}`} onClick={() => onRemove(member.user_id)}><X /></button>}
        {!group && member.user_id !== meId && <button type="button" className={armed === `block:${member.user_id}` ? 'icon-button is-armed' : 'icon-button'} aria-label={armed === `block:${member.user_id}` ? `Confirm blocking ${member.username}` : `Block ${member.username}`} onClick={() => onBlockPerson(member.user_id)}><UserX /></button>}
      </li>)}</ul>
    </section>
    <div className="chat-details__actions">
      {group && conversation.my_status === 'active' && <button type="button" className="button button--secondary button--small" onClick={onInvite}><UserPlus aria-hidden="true" />Invite</button>}
      {group && <button type="button" className={armed === 'leave' ? 'button button--danger button--small' : 'button button--ghost button--small'} onClick={onLeave}><LogOut aria-hidden="true" />{armed === 'leave' ? 'Leave: sure?' : 'Leave'}</button>}
      {!group && <button type="button" className="button button--ghost button--small" onClick={onHide} title="Hide until a new message arrives">Hide</button>}
    </div>
    <Mentioned tallies={companiesIn(messages, bodyOf)} />
  </aside>
}
