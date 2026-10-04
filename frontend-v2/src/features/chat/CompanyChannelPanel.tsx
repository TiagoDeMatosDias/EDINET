import { useQuery, useQueryClient } from '@tanstack/react-query'
import { Bell, BellOff, MessagesSquare } from 'lucide-react'
import { useMemo, useState, type Ref } from 'react'
import { Link, useNavigate } from 'react-router-dom'

import { chatApi, chatKeys, invalidateChat, useChatSummary } from './chatApi'
import { channelLabel, colorFor } from './chatModel'
import type { ChatMessage, MessageBody } from './chatTypes'
import { Composer } from './Composer'
import { MessageList, type MessageAction } from './MessageList'
import { applyIncoming, useChatLive } from './useChatLive'
import './chat.css'

/**
 * A company's public channel, inline on its analysis page, with messages from
 * other channels that mention it. Live while open; posting goes to the channel.
 */
export function CompanyChannelPanel({ companyCode, composerRef }: { companyCode: string; composerRef?: Ref<HTMLTextAreaElement> }) {
  const client = useQueryClient()
  const navigate = useNavigate()
  const channelId = `company:${companyCode}`
  const summary = useChatSummary()
  const me = summary.data?.me
  const [tab, setTab] = useState<'channel' | 'mentions'>('channel')
  const [selectedId, setSelectedId] = useState<string | null>(null)
  const [armed, setArmed] = useState<string | null>(null)
  const [replyTo, setReplyTo] = useState<ChatMessage | null>(null)
  const [error, setError] = useState<string | null>(null)
  const page = useQuery({ queryKey: chatKeys.channelMessages(channelId), queryFn: () => chatApi.channelMessages(channelId), staleTime: Infinity, retry: false })
  const mentions = useQuery({ queryKey: chatKeys.mentions(companyCode), queryFn: () => chatApi.mentions(companyCode), enabled: tab === 'mentions', staleTime: Infinity, retry: false })
  useChatLive({ enabled: Boolean(summary.data), startCursor: summary.data?.cursor, watched: [channelId] })

  const channel = page.data?.channel
  const messages = useMemo(() => {
    const source = tab === 'channel' ? page.data?.messages ?? [] : (mentions.data?.messages ?? []).filter(message => message.channel_id !== channelId)
    return source.filter(message => message.kind === 'message')
  }, [tab, page.data, mentions.data, channelId])
  const found = selectedId ? messages.findIndex(message => message.message_id === selectedId) : -1
  const cursor = found >= 0 ? found : messages.length - 1
  const topics = (summary.data?.channels ?? []).filter(item => item.kind === 'topic').map(item => ({ slug: item.slug ?? '', name: item.name }))

  const send = async (body: MessageBody) => {
    const sent = await chatApi.postChannel(channelId, { message_id: crypto.randomUUID(), text: body.text, refs: body.refs, reply_to: replyTo?.message_id ?? null })
    applyIncoming(client, [sent])
    setReplyTo(null)
    setSelectedId(null)
  }
  const subscribe = async () => {
    if (!channel) return
    try {
      await chatApi.subscribe(channelId, !channel.subscribed)
      invalidateChat(client)
      void page.refetch()
    } catch (err) {
      setError(err instanceof Error ? err.message : 'That did not work')
    }
  }
  const act = (action: MessageAction, message: ChatMessage) => {
    if (action === 'reply') setReplyTo(message)
    if (action === 'dm') navigate(`/chat?dm=${encodeURIComponent(message.sender.user_id)}`)
    if (action === 'profile') navigate(`/people/${encodeURIComponent(message.sender.username)}`)
    if (action === 'delete' || action === 'block') {
      const key = action === 'delete' ? message.message_id : `block:${message.sender.user_id}`
      if (armed !== key) { setArmed(key); return }
      setArmed(null)
      const request = action === 'delete' ? chatApi.deleteMessage(message.message_id) : chatApi.block('user', message.sender.user_id, true)
      void request.then(() => { invalidateChat(client); void page.refetch() }).catch(err => setError(err instanceof Error ? err.message : 'That did not work'))
    }
  }

  return <section className="company-chat" aria-label="Company discussion">
    <header>
      <MessagesSquare aria-hidden="true" />
      <h3><span className="chat-dot" style={{ background: colorFor(channelId) }} /> {channel ? channelLabel(channel) : 'Discussion'}</h3>
      <small>{channel ? `${channel.subscribers ?? 0} subscribers · ${channel.message_count ?? 0} messages · public to members` : ''}</small>
      <div className="company-chat__tabs" role="group" aria-label="Show">
        <button type="button" aria-pressed={tab === 'channel'} onClick={() => setTab('channel')}>This channel</button>
        <button type="button" aria-pressed={tab === 'mentions'} onClick={() => setTab('mentions')}>Mentioned elsewhere</button>
      </div>
      {channel && <button type="button" className={channel.subscribed ? 'button button--ghost button--small' : 'button button--secondary button--small'} onClick={() => void subscribe()}>{channel.subscribed ? <BellOff aria-hidden="true" /> : <Bell aria-hidden="true" />}{channel.subscribed ? 'Unsubscribe' : 'Subscribe'}</button>}
      <Link className="button button--ghost button--small" to={`/chat?c=${encodeURIComponent(channelId)}`}>Open in Chat</Link>
    </header>
    {error && <p className="form-error" role="alert">{error}</p>}
    {page.isError ? <p className="panel__note">{page.error instanceof Error ? page.error.message : 'The discussion could not be loaded.'}</p>
      : <MessageList
        label="Company discussion"
        messages={messages}
        cursor={cursor}
        onCursor={index => { setSelectedId(messages[index]?.message_id ?? null); setArmed(null) }}
        me={{ user_id: me?.user_id ?? '', username: me?.username ?? '', role: me?.role }}
        bodyOf={message => message.body}
        whereOf={tab === 'mentions' ? message => ({ label: message.channel_id?.replace(/^topic:/, '#').replace(/^company:/, '$') ?? '', color: colorFor(message.channel_id ?? ''), href: `/chat?c=${encodeURIComponent(message.channel_id ?? '')}` }) : undefined}
        names={() => 'someone'}
        channelSlugs={new Set(topics.map(item => item.slug))}
        armed={armed}
        onAction={act}
        empty={page.isLoading ? 'Loading…' : tab === 'channel' ? 'No one has discussed this company yet. Start the conversation below.' : 'No other channel mentions this company yet.'}
      />}
    {tab === 'channel' && <Composer
      draftKey={channelId}
      placeholder={`Message ${channel ? channelLabel(channel) : 'this company’s channel'} ($ for another company)`}
      replyTo={replyTo ? `Replying to ${replyTo.sender.username}` : null}
      onCancelReply={() => setReplyTo(null)}
      onSend={send}
      inputRef={composerRef}
      topics={topics}
      disabled={Boolean(channel?.blocked)}
      disabledReason={channel?.blocked ? 'You blocked this channel' : undefined}
    />}
  </section>
}
