import { useQuery, useQueryClient } from '@tanstack/react-query'
import { AtSign, DollarSign, Filter, Info, Keyboard, Lock, Search, Unlock, X } from 'lucide-react'
import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { Link, useNavigate, useSearchParams } from 'react-router-dom'

import { ApiError } from '../../api/client'
import { ErrorState, LoadingState } from '../../components/Feedback'
import { ShortcutsDialog } from '../../components/ShortcutsDialog'
import { useHotkeys } from '../../hooks/useHotkeys'
import { usePersistentState } from '../../hooks/usePersistentState'
import { chatApi, chatKeys, invalidateChat, useChatSummary, type MessagePage } from './chatApi'
import { ChannelDetails, ConversationDetails } from './ChatDetails'
import { PeopleDialog, QuickSwitcher, type SwitchItem } from './ChatDialogs'
import {
  channelLabel,
  colorFor,
  conversationTitle,
  EMPTY_FILTER,
  isFiltering,
  isHiddenEvent,
  KIND_FILTERS,
  matchesFilter,
  mergeMessages,
  otherMember,
  personName,
  referencedCompanies,
  sameTarget,
  sidebarEntries,
  targetFromParam,
  targetParam,
  type KindFilter,
  type MessageFilter,
} from './chatModel'
import { CHAT_SHORTCUTS } from './chatShortcuts'
import { ChatSidebar } from './ChatSidebar'
import type { Channel, ChatMessage, ChatTarget, MessageBody, Person } from './chatTypes'
import { Composer } from './Composer'
import { createConversation, encryptMessage, hasKey, inviteToConversation, loadConversationKeys, rotateConversationKey, useConversationKeys, useDecryptedBodies } from './e2e'
import { KeyPanel } from './KeyPanel'
import { forgetIdentity, useChatIdentity } from './keyStore'
import { MessageList, type MessageAction, type Where } from './MessageList'
import { applyIncoming, useChatLive } from './useChatLive'
import './chat.css'

type DialogKind = 'switch' | 'dm' | 'group' | 'invite' | 'help' | null

function listKey(target: ChatTarget) {
  return target.type === 'feed' ? chatKeys.feed : target.type === 'channel' ? chatKeys.channelMessages(target.id) : chatKeys.conversationMessages(target.id)
}

function fetchList(target: ChatTarget, before?: number) {
  return target.type === 'feed' ? chatApi.feed(before) : target.type === 'channel' ? chatApi.channelMessages(target.id, before) : chatApi.conversationMessages(target.id, before)
}

export default function ChatPage() {
  const [params, setParams] = useSearchParams()
  const navigate = useNavigate()
  const client = useQueryClient()
  const summary = useChatSummary()
  const me = summary.data?.me
  const meId = me?.user_id ?? ''
  const { identity, restoring } = useChatIdentity(me?.user_id, summary.data ? summary.data.key?.key_id ?? null : undefined)
  const target = targetFromParam(params.get('c'))
  const [dialog, setDialog] = useState<DialogKind>(null)
  const [dialogError, setDialogError] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)
  const [filter, setFilter] = useState<MessageFilter>(EMPTY_FILTER)
  const [selectedId, setSelectedId] = useState<string | null>(null)
  const [armed, setArmed] = useState<string | null>(null)
  const [replyTo, setReplyTo] = useState<ChatMessage | null>(null)
  const [showDetails, setShowDetails] = usePersistentState('chat.details', true)
  const [feedTarget, setFeedTarget] = usePersistentState('chat.feedTarget', 'topic:general')
  const [focusList, setFocusList] = useState(0)
  const [notice, setNotice] = useState<string | null>(null)
  const [pendingDm, setPendingDm] = useState<string | null>(null)
  const composer = useRef<HTMLTextAreaElement>(null)
  const filterInput = useRef<HTMLInputElement>(null)

  const channels = useMemo(() => summary.data?.channels ?? [], [summary.data])
  const conversations = useMemo(() => summary.data?.conversations ?? [], [summary.data])
  const channelMap = useMemo(() => new Map(channels.map(channel => [channel.channel_id, channel])), [channels])
  const conversationMap = useMemo(() => new Map(conversations.map(conversation => [conversation.conversation_id, conversation])), [conversations])
  const conversation = target.type === 'conversation' ? conversationMap.get(target.id) : undefined

  const list = useQuery({
    queryKey: listKey(target),
    queryFn: () => fetchList(target),
    enabled: Boolean(summary.data) && (target.type !== 'conversation' || Boolean(conversation)),
    staleTime: Infinity,
    retry: false,
  })
  const channel: Channel | undefined = target.type === 'channel' ? channelMap.get(target.id) ?? list.data?.channel : undefined
  const messages = useMemo(() => (list.data?.messages ?? []).filter(message => !isHiddenEvent(message)), [list.data])
  const [loadingMore, setLoadingMore] = useState(false)
  const loadOlder = async () => {
    const first = list.data?.messages[0]?.seq
    if (!first || loadingMore) return
    setLoadingMore(true)
    try {
      const older = await fetchList(target, first)
      client.setQueryData<MessagePage>(listKey(target), page => page ? { ...page, messages: mergeMessages(page.messages, older.messages), has_more: older.has_more } : page)
    } finally {
      setLoadingMore(false)
    }
  }

  useChatLive({ enabled: Boolean(summary.data), startCursor: summary.data?.cursor, watched: target.type === 'channel' ? [target.id] : [] })
  const keyError = useConversationKeys(identity, conversations)
  const decrypted = useDecryptedBodies(messages, identity)
  const bodyOf = useCallback((message: ChatMessage): MessageBody | undefined => {
    if (message.body) return message.body
    const result = decrypted(message)
    return result?.state === 'ok' ? result.body : undefined
  }, [decrypted])

  const names = useMemo(() => {
    const known = new Map<string, string>()
    for (const item of conversations) for (const member of item.members) known.set(member.user_id, personName(member))
    for (const message of messages) known.set(message.sender.user_id, personName(message.sender))
    return (userId: string) => known.get(userId) ?? 'someone'
  }, [conversations, messages])

  const whereOf = useCallback((message: ChatMessage): Where | undefined => {
    if (message.channel_id) {
      const info = channelMap.get(message.channel_id)
      return { label: info ? channelLabel(info) : message.channel_id.replace(/^topic:/, '#').replace(/^company:/, '$'), color: colorFor(message.channel_id), href: `/chat?c=${encodeURIComponent(message.channel_id)}` }
    }
    const item = conversationMap.get(message.conversation_id ?? '')
    return { label: item ? conversationTitle(item, meId) : 'conversation', color: colorFor(message.conversation_id ?? ''), href: `/chat?c=conv:${message.conversation_id}`, locked: true }
  }, [channelMap, conversationMap, meId])

  const visible = useMemo(() => messages.filter(message => matchesFilter(message, bodyOf(message), filter, {
    where: whereOf(message)?.label ?? '', me: me?.username ?? '', conversations: conversationMap,
  })), [messages, bodyOf, filter, whereOf, me, conversationMap])
  const found = selectedId ? visible.findIndex(message => message.message_id === selectedId) : -1
  const cursor = found >= 0 ? found : visible.length - 1
  const selected = visible[cursor] as ChatMessage | undefined
  const setCursor = (index: number) => { setSelectedId(visible[index]?.message_id ?? null); setArmed(null) }

  // Switching place clears what belonged to the old one.
  const placeKey = targetParam(target)
  const [shownPlace, setShownPlace] = useState(placeKey)
  if (shownPlace !== placeKey) {
    setShownPlace(placeKey)
    setSelectedId(null)
    setArmed(null)
    setReplyTo(null)
  }
  useEffect(() => {
    if (!armed) return
    const timer = setTimeout(() => setArmed(null), 4000)
    return () => clearTimeout(timer)
  }, [armed])
  useEffect(() => {
    if (!notice) return
    const timer = setTimeout(() => setNotice(null), 5000)
    return () => clearTimeout(timer)
  }, [notice])

  const go = useCallback((next: ChatTarget) => {
    const value = targetParam(next)
    setParams(value ? { c: value } : {}, { replace: false })
  }, [setParams])

  // -- reading ---------------------------------------------------------------
  const lastSeq = list.data?.messages[list.data.messages.length - 1]?.seq ?? 0
  useEffect(() => {
    if (!summary.data || !lastSeq || document.visibilityState === 'hidden') return
    const reads: Array<{ kind: 'channel' | 'conversation'; id: string; seq: number }> = []
    if (target.type === 'channel' && channel?.subscribed && lastSeq > (channel.last_read_seq ?? 0)) reads.push({ kind: 'channel', id: target.id, seq: lastSeq })
    if (target.type === 'conversation' && conversation && lastSeq > conversation.last_read_seq) reads.push({ kind: 'conversation', id: target.id, seq: lastSeq })
    if (target.type === 'feed') {
      for (const message of list.data?.messages ?? []) {
        const scope = message.channel_id ? channelMap.get(message.channel_id) : conversationMap.get(message.conversation_id ?? '')
        if (!scope || !(scope.unread ?? 0)) continue
        const kind = message.channel_id ? 'channel' : 'conversation'
        const id = message.channel_id ?? message.conversation_id!
        const existing = reads.find(item => item.id === id)
        if (existing) existing.seq = Math.max(existing.seq, message.seq)
        else reads.push({ kind, id, seq: message.seq })
      }
    }
    if (!reads.length) return
    const timer = setTimeout(() => {
      void Promise.all(reads.map(item => item.kind === 'channel' ? chatApi.readChannel(item.id, item.seq) : chatApi.readConversation(item.id, item.seq)))
        .then(() => { void client.invalidateQueries({ queryKey: chatKeys.summary }); void client.invalidateQueries({ queryKey: chatKeys.unread }) })
        .catch(() => undefined)
    }, 600)
    return () => clearTimeout(timer)
  }, [summary.data, lastSeq, target, channel, conversation, channelMap, conversationMap, list.data, client])

  // -- sending ---------------------------------------------------------------
  const sendTarget: ChatTarget = target.type === 'feed' ? targetFromParam(feedTarget) : target
  const sendConversation = sendTarget.type === 'conversation' ? conversationMap.get(sendTarget.id) : undefined
  const sendChannel = sendTarget.type === 'channel' ? (channelMap.get(sendTarget.id) ?? (target.type === 'channel' ? channel : undefined)) : undefined

  const send = async (body: MessageBody) => {
    const messageId = crypto.randomUUID()
    const reply = replyTo?.message_id ?? null
    let sent: ChatMessage
    if (sendTarget.type === 'channel') {
      sent = await chatApi.postChannel(sendTarget.id, { message_id: messageId, text: body.text, refs: body.refs, reply_to: reply })
    } else if (sendTarget.type === 'conversation' && sendConversation) {
      if (!identity) throw new Error('Unlock encrypted messages first.')
      let active = sendConversation
      if (active.rekey_needed) {
        const version = await rotateConversationKey(active, identity)
        active = { ...active, key_version: version, rekey_needed: false }
        void client.invalidateQueries({ queryKey: chatKeys.summary })
      }
      if (!hasKey(identity, active.conversation_id, active.key_version)) await loadConversationKeys(active.conversation_id, identity)
      const sealed = await encryptMessage(active, identity, body, messageId)
      sent = await chatApi.postConversation(active.conversation_id, { message_id: messageId, ...sealed, reply_to: reply })
    } else {
      throw new Error('Choose where to send the message.')
    }
    applyIncoming(client, [sent])
    setReplyTo(null)
    setSelectedId(null)
  }

  // -- conversations ---------------------------------------------------------
  const openDm = useCallback(async (userId: string) => {
    const existing = conversations.find(item => item.kind === 'dm' && item.members.some(member => member.user_id === userId))
    if (existing) { go({ type: 'conversation', id: existing.conversation_id }); return }
    if (!identity) { setPendingDm(userId); go({ type: 'feed' }); setNotice('Unlock encrypted messages to start a private conversation.'); return }
    try {
      const created = await createConversation(identity, { kind: 'dm', memberIds: [userId] })
      invalidateChat(client)
      go({ type: 'conversation', id: created.conversation_id })
    } catch (err) {
      const existingId = err instanceof ApiError ? (err.payload as { detail?: { conversation_id?: string } })?.detail?.conversation_id : undefined
      if (existingId) { invalidateChat(client); go({ type: 'conversation', id: existingId }); return }
      setNotice(err instanceof Error ? err.message : 'The conversation could not be started')
    }
  }, [conversations, identity, go, client, setPendingDm, setNotice])

  // Profiles link here with ?dm=<user id>; open it once the summary (and key) are ready.
  const dmParam = params.get('dm')
  const summaryReady = Boolean(summary.data)
  useEffect(() => {
    if (!dmParam || !summaryReady) return
    let cancelled = false
    void Promise.resolve().then(() => {
      if (cancelled) return
      setParams(current => { const next = new URLSearchParams(current); next.delete('dm'); return next }, { replace: true })
      return openDm(dmParam)
    })
    return () => { cancelled = true }
  }, [dmParam, summaryReady, setParams, openDm])
  useEffect(() => {
    if (!pendingDm || !identity) return
    let cancelled = false
    void Promise.resolve().then(() => {
      if (cancelled) return
      setPendingDm(null)
      return openDm(pendingDm)
    })
    return () => { cancelled = true }
  }, [pendingDm, identity, openDm])

  const finishPeople = async (people: Person[], title?: string) => {
    setDialogError(null)
    if (dialog === 'dm') { setDialog(null); void openDm(people[0].user_id); return }
    if (!identity) { setDialogError('Unlock encrypted messages first.'); return }
    setBusy(true)
    try {
      if (dialog === 'group') {
        const created = await createConversation(identity, { kind: 'group', memberIds: people.map(person => person.user_id), title })
        invalidateChat(client)
        go({ type: 'conversation', id: created.conversation_id })
      } else if (dialog === 'invite' && conversation) {
        await inviteToConversation(conversation, identity, people.map(person => person.user_id))
        invalidateChat(client)
      }
      setDialog(null)
    } catch (err) {
      setDialogError(err instanceof Error ? err.message : 'That did not work')
    } finally {
      setBusy(false)
    }
  }

  const run = async (action: () => Promise<unknown>, after?: () => void) => {
    try {
      await action()
      invalidateChat(client)
      after?.()
    } catch (err) {
      setNotice(err instanceof Error ? err.message : 'That did not work')
    }
  }

  const subscribe = (on: boolean) => {
    if (target.type !== 'channel') return
    void run(() => chatApi.subscribe(target.id, on), () => setNotice(on ? `Subscribed to ${channelLabel(channel)}` : `Unsubscribed from ${channelLabel(channel)}`))
  }
  const blockPerson = (userId: string, username: string) => run(async () => {
    await chatApi.block('user', userId, true)
    for (const key of [listKey(target), chatKeys.feed]) {
      client.setQueryData<MessagePage>(key, page => page ? { ...page, messages: page.messages.filter(message => message.sender.user_id !== userId) } : page)
    }
  }, () => setNotice(`Blocked ${username}. Unblock in Account.`))
  const blockChannel = () => {
    if (target.type !== 'channel') return
    void run(() => chatApi.block('channel', target.id, true), () => { setNotice(`Blocked ${channelLabel(channel)}. Unblock in Account.`); go({ type: 'feed' }) })
  }
  const removeMessage = (message: ChatMessage) => run(async () => {
    await chatApi.deleteMessage(message.message_id)
    applyIncoming(client, [{ ...message, deleted: true, body: undefined, encrypted: undefined }])
  })

  const act = (action: MessageAction, message: ChatMessage) => {
    if (action === 'reply') { setReplyTo(message); if (target.type === 'feed') setFeedTarget(message.channel_id ?? `conv:${message.conversation_id}`); composer.current?.focus() }
    if (action === 'dm') void openDm(message.sender.user_id)
    if (action === 'profile') navigate(`/people/${encodeURIComponent(message.sender.username)}`)
    if (action === 'delete') { if (armed === message.message_id) { setArmed(null); void removeMessage(message) } else setArmed(message.message_id) }
    if (action === 'block') {
      const key = `block:${message.sender.user_id}`
      if (armed === key) { setArmed(null); void blockPerson(message.sender.user_id, message.sender.username) } else setArmed(key)
    }
  }

  // -- navigation ------------------------------------------------------------
  const unreadTotal = channels.reduce((sum, item) => sum + (item.subscribed ? item.unread ?? 0 : 0), 0) + conversations.reduce((sum, item) => sum + item.unread, 0)
  const entries = useMemo(() => sidebarEntries(channels, conversations, meId, unreadTotal), [channels, conversations, meId, unreadTotal])
  const entryIndex = entries.findIndex(entry => sameTarget(entry.target, target))
  const stepEntry = (delta: number) => {
    if (!entries.length) return
    const next = entries[((entryIndex < 0 ? 0 : entryIndex) + delta + entries.length) % entries.length]
    go(next.target)
  }
  const nextUnread = () => {
    const start = entryIndex < 0 ? 0 : entryIndex
    for (let step = 1; step <= entries.length; step += 1) {
      const entry = entries[(start + step) % entries.length]
      if (entry.unread > 0 && entry.icon !== 'feed') { go(entry.target); return }
    }
    setNotice('Nothing unread.')
  }
  const openCompany = () => {
    const ref = referencedCompanies(selected ? bodyOf(selected) : undefined)[0]
    const code = ref?.code ?? channel?.company_code
    if (code) navigate(`/analyze/${encodeURIComponent(code)}`)
    else setNotice('The selected message mentions no company.')
  }
  const openPlace = () => {
    if (!selected) return
    if (target.type === 'feed') { go(selected.channel_id ? { type: 'channel', id: selected.channel_id } : { type: 'conversation', id: selected.conversation_id! }); return }
    const ref = referencedCompanies(bodyOf(selected))[0]
    if (ref) go({ type: 'channel', id: `company:${ref.code}` })
  }
  const toggleKind = (kind: KindFilter) => setFilter(current => ({ ...current, kind: current.kind === kind ? 'all' : kind }))

  useHotkeys({
    j: () => setCursor(Math.min(visible.length - 1, cursor + 1)),
    k: () => setCursor(Math.max(0, cursor - 1)),
    Enter: () => composer.current?.focus(),
    Escape: () => { setSelectedId(null); setArmed(null); setReplyTo(null) },
    '[': () => stepEntry(-1),
    ']': () => stepEntry(1),
    u: nextUnread,
    a: () => go({ type: 'feed' }),
    t: () => setDialog('switch'),
    f: () => filterInput.current?.focus(),
    ...Object.fromEntries(KIND_FILTERS.map((item, index) => [String(index + 1), () => setFilter(current => ({ ...current, kind: item.key }))])),
    m: () => setFilter(current => ({ ...current, mentionsMe: !current.mentionsMe })),
    $: () => setFilter(current => ({ ...current, withCompanies: !current.withCompanies })),
    s: () => { if (channel) subscribe(!channel.subscribed) },
    n: () => setDialog('dm'),
    N: () => setDialog('group'),
    r: () => { if (selected?.kind === 'message' && !selected.deleted) act('reply', selected) },
    o: openCompany,
    c: openPlace,
    p: () => { if (selected) act('profile', selected) },
    d: () => { if (selected && selected.sender.user_id !== meId) act('dm', selected) },
    x: () => { if (selected?.kind === 'message' && !selected.deleted && (selected.sender.user_id === meId || (me?.role === 'admin' && selected.channel_id))) act('delete', selected) },
    b: () => {
      if (selected?.kind === 'message' && selected.sender.user_id !== meId && selectedId) act('block', selected)
      else if (target.type === 'channel') { if (armed === 'block-channel') { setArmed(null); blockChannel() } else setArmed('block-channel') }
    },
    i: () => setShowDetails(!showDetails),
    l: () => setFocusList(value => value + 1),
    '?': () => setDialog('help'),
  }, dialog === null)

  if (summary.isLoading) return <LoadingState label="Loading chat" />
  if (summary.isError || !summary.data || !me) return <ErrorState error={summary.error} retry={() => summary.refetch()} />

  const locked = !identity
  const needsKey = target.type === 'conversation' && locked
  const invited = conversation?.my_status === 'invited'
  const title = target.type === 'feed' ? 'All messages' : target.type === 'channel' ? channelLabel(channel, target.id) : conversation ? conversationTitle(conversation, meId) : 'Conversation'
  const subtitle = target.type === 'feed'
    ? `${channels.filter(item => item.subscribed).length} subscribed channels and ${conversations.length} conversations, newest last`
    : target.type === 'channel' ? (channel?.description ?? '') + (channel && !channel.subscribed ? ' · not subscribed (S)' : '')
      : conversation ? (conversation.kind === 'dm' ? `@${otherMember(conversation, meId)?.username ?? ''} · ` : `${conversation.members.length} members · `) + 'end-to-end encrypted' : ''
  const sendOptions = [
    ...channels.filter(item => item.subscribed && !item.blocked).map(item => ({ value: item.channel_id, label: channelLabel(item) })),
    ...conversations.filter(item => item.my_status === 'active').map(item => ({ value: `conv:${item.conversation_id}`, label: `🔒 ${conversationTitle(item, meId)}` })),
  ]
  if (!sendOptions.some(option => option.value === feedTarget)) sendOptions.unshift({ value: feedTarget, label: channelLabel(channelMap.get(feedTarget), feedTarget) })
  const composerDisabled = (sendTarget.type === 'conversation' && (locked || sendConversation?.my_status !== 'active'))
    || (sendTarget.type === 'channel' && Boolean(sendChannel?.blocked))
    || (target.type === 'conversation' && !conversation)
  const disabledReason = sendTarget.type === 'conversation' && locked ? 'Unlock encrypted messages to write here'
    : sendConversation?.my_status === 'invited' ? 'Accept the invitation to reply'
      : sendChannel?.blocked ? 'You blocked this channel' : undefined
  const switchItems: SwitchItem[] = entries.map(entry => ({
    key: entry.key, kind: entry.icon === 'company' ? 'company' : entry.icon === 'topic' ? 'channel' : entry.icon === 'feed' ? 'feed' : 'conversation',
    label: entry.icon === 'topic' ? `#${entry.label}` : entry.label, detail: entry.detail, color: entry.color, unread: entry.unread, go: () => go(entry.target),
  }))
  const bodies = (message: ChatMessage) => bodyOf(message)

  return <div className="chat-page">
    <div className={showDetails ? 'chat-grid chat-grid--details' : 'chat-grid'}>
      <ChatSidebar entries={entries} current={target} onSelect={go} onNewDm={() => setDialog('dm')} onNewGroup={() => setDialog('group')} locked={locked} />
      <section className="chat-main" aria-label={title}>
        <header className="chat-head">
          <span className="chat-dot" style={{ background: target.type === 'feed' ? 'var(--ink)' : colorFor(target.id) }} aria-hidden="true" />
          <div className="chat-head__title"><h1>{target.type === 'conversation' && <Lock aria-hidden="true" />}{title}</h1><p>{subtitle}</p></div>
          <div className="chat-head__tools">
            {target.type === 'feed' && <div className="chat-kinds" role="group" aria-label="Show">{KIND_FILTERS.map((item, index) => <button key={item.key} type="button" aria-pressed={filter.kind === item.key} onClick={() => toggleKind(item.key)} title={`${item.label} (${index + 1})`}><kbd>{index + 1}</kbd>{item.label}</button>)}</div>}
            <label className="chat-filter"><Filter aria-hidden="true" /><input ref={filterInput} placeholder="Filter: words, from:name, in:channel" aria-label="Filter messages" value={filter.text} onChange={event => setFilter(current => ({ ...current, text: event.target.value }))} onKeyDown={event => { if (event.key === 'Escape') { event.preventDefault(); if (filter.text) setFilter(current => ({ ...current, text: '' })); else event.currentTarget.blur() } if (event.key === 'Enter') { event.preventDefault(); event.currentTarget.blur(); setFocusList(value => value + 1) } }} /><kbd>F</kbd></label>
            <button type="button" className={filter.mentionsMe ? 'chat-toggle is-on' : 'chat-toggle'} aria-pressed={filter.mentionsMe} onClick={() => setFilter(current => ({ ...current, mentionsMe: !current.mentionsMe }))} title="Only messages that mention you (M)"><AtSign aria-hidden="true" /></button>
            <button type="button" className={filter.withCompanies ? 'chat-toggle is-on' : 'chat-toggle'} aria-pressed={filter.withCompanies} onClick={() => setFilter(current => ({ ...current, withCompanies: !current.withCompanies }))} title="Only messages that reference a company ($)"><DollarSign aria-hidden="true" /></button>
            {isFiltering(filter) && <button type="button" className="chat-toggle" onClick={() => setFilter(EMPTY_FILTER)} title="Clear the filter"><X aria-hidden="true" /></button>}
            <button type="button" className="chat-toggle" onClick={() => setDialog('switch')} title="Jump to a channel, company, or person (T)"><Search aria-hidden="true" /></button>
            {identity
              ? <button type="button" className="chat-toggle" onClick={() => void forgetIdentity(meId)} title="Lock encrypted messages on this device"><Unlock aria-hidden="true" /></button>
              : <span className="chat-toggle chat-toggle--static" title="Encrypted messages are locked"><Lock aria-hidden="true" /></span>}
            <button type="button" className={showDetails ? 'chat-toggle is-on' : 'chat-toggle'} aria-pressed={showDetails} onClick={() => setShowDetails(!showDetails)} title="Details (I)"><Info aria-hidden="true" /></button>
            <button type="button" className="chat-toggle" onClick={() => setDialog('help')} title="Keyboard shortcuts (?)" aria-label="Keyboard shortcuts"><Keyboard aria-hidden="true" /></button>
          </div>
        </header>
        {notice && <div className="chat-notice" role="status">{notice}<button type="button" className="icon-button" aria-label="Dismiss" onClick={() => setNotice(null)}><X /></button></div>}
        {keyError && <div className="chat-notice chat-notice--error" role="alert">{keyError}</div>}
        {invited && conversation && <div className="chat-invite" role="region" aria-label="Invitation">
          <span>You were invited to <strong>{conversationTitle(conversation, meId)}</strong>. Accepting shows you to its members and lets you reply.</span>
          <button type="button" className="button button--primary button--small" onClick={() => void run(() => chatApi.respond(conversation.conversation_id, true))}>Accept</button>
          <button type="button" className="button button--ghost button--small" onClick={() => void run(() => chatApi.respond(conversation.conversation_id, false), () => go({ type: 'feed' }))}>Decline</button>
        </div>}
        {(needsKey || (pendingDm && locked)) && !restoring && <div className="chat-locked"><KeyPanel userId={meId} serverKey={summary.data.key} /></div>}
        {target.type === 'conversation' && !conversation && <ErrorState error={new Error('This conversation is not available. You may have left it, or been removed.')} />}
        {list.isError && <ErrorState error={list.error} retry={() => list.refetch()} />}
        {list.isLoading && target.type !== 'conversation' ? <LoadingState label="Loading messages" /> : !needsKey && <MessageList
          label={`Messages in ${title}`}
          messages={visible}
          cursor={cursor}
          onCursor={setCursor}
          me={{ user_id: meId, username: me.username, role: me.role }}
          bodyOf={message => message.body ?? decrypted(message)}
          whereOf={target.type === 'feed' ? whereOf : undefined}
          names={names}
          channelSlugs={new Set(channels.filter(item => item.kind === 'topic').map(item => item.slug ?? ''))}
          armed={armed}
          onAction={act}
          hasMore={list.data?.has_more}
          loadingMore={loadingMore}
          onLoadMore={() => void loadOlder()}
          focusRequest={focusList}
          empty={isFiltering(filter) ? 'No message matches the filter.' : target.type === 'feed' ? 'Subscribe to channels (S on a channel) or start a conversation (N) and their messages gather here.' : 'No messages yet. Say something.'}
        />}
        <Composer
          draftKey={target.type === 'feed' ? `feed:${feedTarget}` : placeKey}
          placeholder={target.type === 'feed' ? `Message ${sendOptions.find(option => option.value === feedTarget)?.label ?? feedTarget}` : `Message ${title}`}
          disabled={composerDisabled}
          disabledReason={disabledReason}
          encrypted={sendTarget.type === 'conversation'}
          replyTo={replyTo ? `Replying to ${personName(replyTo.sender)}: ${(bodyOf(replyTo)?.text ?? '').slice(0, 80)}` : null}
          onCancelReply={() => setReplyTo(null)}
          onSend={send}
          inputRef={composer}
          topics={channels.filter(item => item.kind === 'topic').map(item => ({ slug: item.slug ?? '', name: item.name }))}
          target={target.type === 'feed' ? <select className="select chat-composer__target" aria-label="Send to" value={feedTarget} onChange={event => setFeedTarget(event.target.value)}>{sendOptions.map(option => <option key={option.value} value={option.value}>{option.label}</option>)}</select> : undefined}
        />
      </section>
      {showDetails && (target.type === 'channel' && channel
        ? <ChannelDetails channel={channel} messages={messages} bodyOf={bodies} meId={meId} armedBlock={armed === 'block-channel'} onSubscribe={subscribe} onBlock={() => { if (armed === 'block-channel') { setArmed(null); blockChannel() } else setArmed('block-channel') }} />
        : target.type === 'conversation' && conversation
          ? <ConversationDetails
            key={conversation.conversation_id}
            conversation={conversation}
            identity={identity}
            meId={meId}
            messages={messages}
            bodyOf={bodies}
            armed={armed}
            onInvite={() => setDialog('invite')}
            onRename={title => void run(() => chatApi.updateConversation(conversation.conversation_id, { title }))}
            onHide={() => void run(() => chatApi.updateConversation(conversation.conversation_id, { hidden: true }), () => go({ type: 'feed' }))}
            onLeave={() => { if (armed === 'leave') { setArmed(null); void run(() => chatApi.removeMember(conversation.conversation_id, meId), () => go({ type: 'feed' })) } else setArmed('leave') }}
            onRemove={userId => {
              const key = `remove:${userId}`
              if (armed !== key) { setArmed(key); return }
              setArmed(null)
              void run(async () => {
                await chatApi.removeMember(conversation.conversation_id, userId)
                // Replace the key at once so the removed member cannot read what follows.
                if (identity) await rotateConversationKey({ ...conversation, members: conversation.members.filter(member => member.user_id !== userId) }, identity)
              })
            }}
            onBlockPerson={userId => {
              const key = `block:${userId}`
              if (armed !== key) { setArmed(key); return }
              setArmed(null)
              const member = conversation.members.find(item => item.user_id === userId)
              void blockPerson(userId, member?.username ?? 'them').then(() => go({ type: 'feed' }))
            }}
          />
          : <FeedDetails channels={channels} onOpen={go} messages={messages} bodyOf={bodies} />)}
    </div>
    {dialog === 'switch' && <QuickSwitcher local={switchItems} onClose={() => setDialog(null)} onChannel={item => go({ type: 'channel', id: item.channel_id })} onPerson={person => void openDm(person.user_id)} />}
    {(dialog === 'dm' || dialog === 'group' || dialog === 'invite') && <PeopleDialog
      mode={dialog}
      onClose={() => { setDialog(null); setDialogError(null) }}
      onDone={(people, groupTitle) => void finishPeople(people, groupTitle)}
      exclude={dialog === 'invite' && conversation ? conversation.members.map(member => member.user_id) : []}
      busy={busy}
      error={dialogError ?? (dialog !== 'dm' && locked ? 'Unlock encrypted messages first (open a conversation to unlock).' : null)}
    />}
    {dialog === 'help' && <ShortcutsDialog groups={CHAT_SHORTCUTS} onClose={() => setDialog(null)} />}
  </div>
}

function FeedDetails({ channels, onOpen, messages, bodyOf }: { channels: Channel[]; onOpen: (target: ChatTarget) => void; messages: ChatMessage[]; bodyOf: (message: ChatMessage) => MessageBody | undefined }) {
  const subscribed = channels.filter(item => item.subscribed && !item.blocked)
  const counts = new Map<string, { code: string; name: string; count: number }>()
  for (const message of messages) for (const ref of referencedCompanies(bodyOf(message))) counts.set(ref.code, { code: ref.code, name: ref.name, count: (counts.get(ref.code)?.count ?? 0) + 1 })
  const top = [...counts.values()].sort((a, b) => b.count - a.count).slice(0, 10)
  return <aside className="chat-details" aria-label="Overview">
    <header><h3>Following</h3></header>
    {subscribed.length ? <ul className="chat-details__list">{subscribed.map(item => <li key={item.channel_id}>
      <span className="chat-dot" style={{ background: colorFor(item.channel_id) }} />
      <button type="button" className="text-button" onClick={() => onOpen({ type: 'channel', id: item.channel_id })}>{channelLabel(item)}</button>
      {(item.unread ?? 0) > 0 && <span className="chat-badge">{item.unread}</span>}
    </li>)}</ul> : <p className="muted">Open a channel and press <kbd>S</kbd> to follow it here.</p>}
    {top.length > 0 && <section className="chat-details__section" aria-label="Companies mentioned">
      <h4>Companies mentioned</h4>
      <ul className="chat-details__list">{top.map(item => <li key={item.code}>
        <Link to={`/analyze/${encodeURIComponent(item.code)}`} title="Open the analysis">{item.name}</Link>
        <button type="button" className="text-button chat-details__aside" onClick={() => onOpen({ type: 'channel', id: `company:${item.code}` })} title="Open the company's channel">#</button>
        <span className="chat-details__count">{item.count}</span>
      </li>)}</ul>
    </section>}
    <section className="chat-details__section" aria-label="Filtering">
      <h4>Filtering</h4>
      <p className="muted"><kbd>1</kbd>–<kbd>5</kbd> by kind, <kbd>M</kbd> mentions of you, <kbd>$</kbd> with companies, <kbd>F</kbd> words; <code>from:alice</code> and <code>in:stocks</code> narrow further.</p>
    </section>
  </aside>
}
