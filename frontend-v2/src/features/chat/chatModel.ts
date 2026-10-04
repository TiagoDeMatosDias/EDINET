import { SERIES_COLORS } from '../../brand'
import type { Channel, ChatMessage, ChatTarget, CompanyRef, Conversation, MessageBody } from './chatTypes'

/** Colour-coding for channels and people: the validated series colours, then four quieter brand tones. */
export const CHAT_COLORS = [...SERIES_COLORS, '#7D8BA6', '#CC8574', '#5F7466', '#7A3A2E'] as const

function hash(value: string) {
  let result = 2166136261
  for (let index = 0; index < value.length; index += 1) {
    result ^= value.charCodeAt(index)
    result = Math.imul(result, 16777619)
  }
  return result >>> 0
}

export function colorFor(id: string) {
  return CHAT_COLORS[hash(id) % CHAT_COLORS.length]
}

export function targetFromParam(value: string | null): ChatTarget {
  if (value?.startsWith('conv:')) return { type: 'conversation', id: value.slice(5) }
  if (value && /^(topic|company):[A-Za-z0-9_-]+$/.test(value)) return { type: 'channel', id: value }
  return { type: 'feed' }
}

export function targetParam(target: ChatTarget) {
  if (target.type === 'conversation') return `conv:${target.id}`
  if (target.type === 'channel') return target.id
  return ''
}

export function sameTarget(a: ChatTarget, b: ChatTarget) {
  return a.type === b.type && (a.type === 'feed' || (a as { id: string }).id === (b as { id: string }).id)
}

export function channelPrefix(channel: Pick<Channel, 'kind'> | undefined) {
  return channel?.kind === 'company' ? '$' : '#'
}

export function channelLabel(channel: Pick<Channel, 'kind' | 'name' | 'slug' | 'ticker'> | undefined, fallback = '') {
  if (!channel) return fallback
  if (channel.kind === 'topic') return `#${channel.slug ?? channel.name.toLowerCase()}`
  return channel.ticker ? `$${channel.ticker} ${channel.name}` : `$${channel.name}`
}

export function personName(person: { username: string; display_name?: string | null } | undefined) {
  if (!person) return 'unknown'
  return person.display_name?.trim() || person.username
}

export function conversationTitle(conversation: Conversation, meId: string) {
  if (conversation.kind === 'group') return conversation.title?.trim() || 'Untitled group'
  const other = conversation.members.find(member => member.user_id !== meId)
  return other ? personName(other) : 'Direct message'
}

export function otherMember(conversation: Conversation, meId: string) {
  return conversation.members.find(member => member.user_id !== meId)
}

// -- message text ---------------------------------------------------------------

export type Segment =
  | { type: 'text'; value: string }
  | { type: 'company'; token: string; ref?: CompanyRef }
  | { type: 'mention'; username: string }
  | { type: 'link'; url: string }
  | { type: 'channel'; slug: string }

// Same company token as the server: "$7203", "$130A", "$E02144".
const TOKENS = /(?<![\w$])\$(E\d{5}|\d{3}[0-9A-Z])(?!\w)|(?<![\w@])@([a-z0-9][a-z0-9_.-]{2,63})|(https?:\/\/[^\s<]+[^\s<.,;:!?)\]'"])|(?<![\w#])#([a-z]{2,20})\b/g

/** "72030", "7203.T" → "7203": the code people type after "$". */
export function tseCode(ticker: string | null | undefined) {
  const match = /^(\d{3}[0-9A-Z])(?:0|\.T|\.JP)?$/i.exec(String(ticker ?? '').trim())
  return match ? match[1].toUpperCase() : null
}

export function companyTokens(text: string) {
  return [...new Set([...text.matchAll(TOKENS)].map(match => match[1]).filter(Boolean))]
}

export function segments(text: string, refs: Record<string, CompanyRef> = {}, channelSlugs: ReadonlySet<string> = new Set()): Segment[] {
  const result: Segment[] = []
  let last = 0
  const push = (value: string) => {
    if (!value) return
    const previous = result[result.length - 1]
    if (previous?.type === 'text') previous.value += value
    else result.push({ type: 'text', value })
  }
  for (const match of text.matchAll(TOKENS)) {
    const index = match.index ?? 0
    push(text.slice(last, index))
    last = index + match[0].length
    if (match[1]) result.push({ type: 'company', token: match[1], ref: refs[match[1]] })
    else if (match[2]) result.push({ type: 'mention', username: match[2].replace(/[.-]+$/, '') })
    else if (match[3]) result.push({ type: 'link', url: match[3] })
    else if (match[4] && channelSlugs.has(match[4])) result.push({ type: 'channel', slug: match[4] })
    else push(match[0])
  }
  push(text.slice(last))
  return result
}

export function mentions(text: string, username: string) {
  return segments(text).some(segment => segment.type === 'mention' && segment.username === username)
}

/** The companies a message refers to, in order, with their codes. */
export function referencedCompanies(body: MessageBody | undefined) {
  if (!body) return []
  return companyTokens(body.text).map(token => body.refs[token]).filter((ref): ref is CompanyRef => Boolean(ref))
}

// -- filtering ----------------------------------------------------------------

export type KindFilter = 'all' | 'topics' | 'companies' | 'direct' | 'groups'
export const KIND_FILTERS: Array<{ key: KindFilter; label: string }> = [
  { key: 'all', label: 'All' },
  { key: 'topics', label: 'Topics' },
  { key: 'companies', label: 'Companies' },
  { key: 'direct', label: 'Direct' },
  { key: 'groups', label: 'Groups' },
]

export interface MessageFilter {
  text: string
  kind: KindFilter
  mentionsMe: boolean
  withCompanies: boolean
}

export const EMPTY_FILTER: MessageFilter = { text: '', kind: 'all', mentionsMe: false, withCompanies: false }

export function isFiltering(filter: MessageFilter) {
  return Boolean(filter.text.trim()) || filter.kind !== 'all' || filter.mentionsMe || filter.withCompanies
}

export function messageKind(message: ChatMessage, conversations: Map<string, Conversation>): KindFilter {
  if (message.channel_id) return message.channel_id.startsWith('company:') ? 'companies' : 'topics'
  return conversations.get(message.conversation_id ?? '')?.kind === 'group' ? 'groups' : 'direct'
}

/**
 * Whether a message passes the filter. Text matches the message, its sender, and where it was
 * posted; ``from:name`` and ``in:name`` narrow to a sender or a channel.
 */
export function matchesFilter(
  message: ChatMessage,
  body: MessageBody | undefined,
  filter: MessageFilter,
  context: { where: string; me: string; conversations: Map<string, Conversation> },
) {
  if (filter.kind !== 'all' && messageKind(message, context.conversations) !== filter.kind) return false
  if (filter.mentionsMe && !(body && mentions(body.text, context.me))) return false
  if (filter.withCompanies && !referencedCompanies(body).length) return false
  const words = filter.text.trim().toLowerCase().split(/\s+/).filter(Boolean)
  if (!words.length) return true
  const sender = `${message.sender.username} ${message.sender.display_name ?? ''}`.toLowerCase()
  const where = context.where.toLowerCase()
  const content = `${body?.text ?? ''} ${Object.values(body?.refs ?? {}).map(ref => ref.name).join(' ')}`.toLowerCase()
  return words.every(word => {
    if (word.startsWith('from:')) return sender.includes(word.slice(5).replace(/^@/, ''))
    if (word.startsWith('in:')) return where.includes(word.slice(3).replace(/^[#$]/, ''))
    return content.includes(word) || sender.includes(word) || where.includes(word)
  })
}

// -- time -----------------------------------------------------------------------

const timeFormat = new Intl.DateTimeFormat('en-GB', { hour: '2-digit', minute: '2-digit' })
const dayFormat = new Intl.DateTimeFormat('en-GB', { weekday: 'short', day: 'numeric', month: 'short' })
const fullFormat = new Intl.DateTimeFormat('en-GB', { dateStyle: 'medium', timeStyle: 'short' })

export function messageTime(value: string) {
  return timeFormat.format(new Date(value))
}

export function fullTime(value: string) {
  return fullFormat.format(new Date(value))
}

export function dayKey(value: string) {
  const date = new Date(value)
  return `${date.getFullYear()}-${date.getMonth()}-${date.getDate()}`
}

export function dayLabel(value: string, now = new Date()) {
  const date = new Date(value)
  const today = new Date(now.getFullYear(), now.getMonth(), now.getDate()).getTime()
  const day = new Date(date.getFullYear(), date.getMonth(), date.getDate()).getTime()
  if (day === today) return 'Today'
  if (day === today - 86_400_000) return 'Yesterday'
  return dayFormat.format(date)
}

export function relativeTime(value: string | null | undefined, now = Date.now()) {
  if (!value) return ''
  const minutes = Math.round((now - new Date(value).getTime()) / 60_000)
  if (minutes < 1) return 'now'
  if (minutes < 60) return `${minutes}m`
  const hours = Math.round(minutes / 60)
  if (hours < 24) return `${hours}h`
  const days = Math.round(hours / 24)
  return days < 30 ? `${days}d` : dayFormat.format(new Date(value))
}

/** A message continues the one before it when the same person posted in the same place within five minutes. */
export function continues(previous: ChatMessage | undefined, message: ChatMessage) {
  if (!previous || previous.kind !== 'message' || message.kind !== 'message') return false
  if (previous.sender.user_id !== message.sender.user_id) return false
  if ((previous.channel_id ?? previous.conversation_id) !== (message.channel_id ?? message.conversation_id)) return false
  return new Date(message.created_at).getTime() - new Date(previous.created_at).getTime() < 5 * 60_000
}

export function systemText(message: ChatMessage, names: (userId: string) => string) {
  const event = message.system
  if (!event) return ''
  const actor = names(message.sender.user_id)
  const who = (event.members ?? []).map(names).join(', ')
  switch (event.event) {
    case 'created': return event.kind === 'group' ? `${actor} created the group${event.title ? ` “${event.title}”` : ''}${who ? ` and invited ${who}` : ''}` : `${actor} started the conversation`
    case 'invited': return `${actor} invited ${who}`
    case 'joined': return `${actor} joined`
    case 'declined': return `${actor} declined the invitation`
    case 'left': return `${actor} left`
    case 'removed': return `${actor} removed ${who}`
    case 'renamed': return `${actor} renamed the group to “${event.title ?? ''}”`
    case 'key_added': return `${actor} set up encryption`
    case 'key_changed': return `${actor}’s encryption key changed. If you did not expect this, compare fingerprints on their profile.`
    case 'rotated': return `${actor} replaced the conversation key`
    default: return ''
  }
}

/**
 * Keep messages unique by id and ordered by ``seq``. A ``deleted`` event marks its message
 * deleted (and is kept out of the list); later copies of a message replace earlier ones.
 */
/** Events that update state but are not shown as lines in the conversation. */
export function isHiddenEvent(message: ChatMessage) {
  return message.system?.event === 'deleted' || message.system?.event === 'keys_shared'
}

export function mergeMessages(current: ChatMessage[], incoming: ChatMessage[]) {
  if (!incoming.length) return current
  const byId = new Map(current.map(message => [message.message_id, message]))
  const deleted = new Set<string>()
  for (const message of incoming) {
    if (message.system?.event === 'deleted') { if (message.system.message_id) deleted.add(message.system.message_id); continue }
    if (message.system?.event === 'keys_shared') continue
    byId.set(message.message_id, message)
  }
  for (const id of deleted) {
    const message = byId.get(id)
    if (message) byId.set(id, { ...message, deleted: true, body: undefined, encrypted: undefined })
  }
  return [...byId.values()].sort((a, b) => a.seq - b.seq)
}

// -- sidebar ----------------------------------------------------------------------

export interface SidebarEntry { target: ChatTarget; key: string; label: string; color: string; unread: number; muted?: boolean; icon: 'feed' | 'topic' | 'company' | 'dm' | 'group' | 'invite'; detail?: string }

/** Everything the sidebar lists, in the order [ and ] walk through it. */
export function sidebarEntries(channels: Channel[], conversations: Conversation[], meId: string, totalUnread: number): SidebarEntry[] {
  const visible = channels.filter(channel => !channel.blocked)
  const topics = visible.filter(channel => channel.kind === 'topic')
  const companies = visible.filter(channel => channel.kind === 'company').sort((a, b) => Number(b.subscribed) - Number(a.subscribed) || (b.last_message_at ?? '').localeCompare(a.last_message_at ?? ''))
  const shown = conversations.filter(conversation => !conversation.hidden || conversation.unread > 0)
  const channelEntry = (channel: Channel): SidebarEntry => ({
    target: { type: 'channel', id: channel.channel_id }, key: channel.channel_id, label: channel.kind === 'topic' ? channel.slug ?? channel.name : channel.name,
    color: colorFor(channel.channel_id), unread: channel.subscribed ? channel.unread ?? 0 : 0, muted: !channel.subscribed, icon: channel.kind,
    detail: channel.kind === 'company' ? channel.ticker ?? channel.company_code : undefined,
  })
  const conversationEntry = (conversation: Conversation): SidebarEntry => ({
    target: { type: 'conversation', id: conversation.conversation_id }, key: conversation.conversation_id, label: conversationTitle(conversation, meId),
    color: colorFor(conversation.conversation_id), unread: conversation.my_status === 'invited' ? 1 : conversation.unread,
    icon: conversation.my_status === 'invited' ? 'invite' : conversation.kind === 'dm' ? 'dm' : 'group',
  })
  return [
    { target: { type: 'feed' }, key: 'feed', label: 'All messages', color: 'var(--ink)', unread: totalUnread, icon: 'feed' },
    ...shown.filter(item => item.my_status === 'invited').map(conversationEntry),
    ...topics.map(channelEntry),
    ...companies.map(channelEntry),
    ...shown.filter(item => item.my_status === 'active' && item.kind === 'dm').map(conversationEntry),
    ...shown.filter(item => item.my_status === 'active' && item.kind === 'group').map(conversationEntry),
  ]
}
