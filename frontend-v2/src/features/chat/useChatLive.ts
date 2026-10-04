import { useQueryClient, type QueryClient } from '@tanstack/react-query'
import { useEffect, useRef } from 'react'

import { chatApi, chatKeys, type MessagePage } from './chatApi'
import { mergeMessages } from './chatModel'
import type { ChatMessage, ChatSummary } from './chatTypes'

const RETRY_MS = 4000

function appendTo(client: QueryClient, key: readonly unknown[], messages: ChatMessage[]) {
  client.setQueryData<MessagePage>(key, page => page ? { ...page, messages: mergeMessages(page.messages, messages) } : page)
}

/** File new messages under every loaded list they belong to, and refresh counts when they change. */
export function applyIncoming(client: QueryClient, messages: ChatMessage[]) {
  if (!messages.length) return
  const byChannel = new Map<string, ChatMessage[]>()
  const byConversation = new Map<string, ChatMessage[]>()
  for (const message of messages) {
    if (message.channel_id) byChannel.set(message.channel_id, [...(byChannel.get(message.channel_id) ?? []), message])
    else if (message.conversation_id) byConversation.set(message.conversation_id, [...(byConversation.get(message.conversation_id) ?? []), message])
  }
  for (const [id, items] of byChannel) appendTo(client, chatKeys.channelMessages(id), items)
  for (const [id, items] of byConversation) appendTo(client, chatKeys.conversationMessages(id), items)
  // The combined view holds followed channels and conversations, not channels only being looked at.
  const subscribed = new Set((client.getQueryData<ChatSummary>(chatKeys.summary)?.channels ?? []).filter(item => item.subscribed).map(item => item.channel_id))
  appendTo(client, chatKeys.feed, messages.filter(message => message.conversation_id || subscribed.has(message.channel_id ?? '')))
  const byCompany = new Map<string, ChatMessage[]>()
  for (const message of messages) {
    for (const ref of Object.values(message.body?.refs ?? {})) byCompany.set(ref.code, [...(byCompany.get(ref.code) ?? []), message])
  }
  for (const [code, items] of byCompany) {
    client.setQueryData<{ messages: ChatMessage[] }>(chatKeys.mentions(code), page => page ? { messages: mergeMessages(page.messages, items) } : page)
  }
  void client.invalidateQueries({ queryKey: chatKeys.summary })
  void client.invalidateQueries({ queryKey: chatKeys.unread })
}

/**
 * Keep chat lists current with a long poll: each request waits on the server until
 * something new arrives. ``watched`` adds channels the user is looking at without
 * following them; changing it restarts the poll from the same position.
 */
export function useChatLive({ enabled, startCursor, watched = [], onMessages }: {
  enabled: boolean
  startCursor: number | undefined
  watched?: string[]
  onMessages?: (messages: ChatMessage[]) => void
}) {
  const client = useQueryClient()
  const cursor = useRef<number | undefined>(undefined)
  const callback = useRef(onMessages)
  useEffect(() => { callback.current = onMessages })
  const watchedKey = watched.join(',')

  useEffect(() => {
    if (cursor.current === undefined) cursor.current = startCursor
    if (!enabled || cursor.current === undefined) return
    const controller = new AbortController()
    let timer: ReturnType<typeof setTimeout> | undefined
    const channels = watchedKey ? watchedKey.split(',') : []
    const poll = async () => {
      while (!controller.signal.aborted) {
        try {
          const result = await chatApi.updates(cursor.current ?? 0, channels, controller.signal)
          cursor.current = result.cursor
          if (result.messages.length) {
            applyIncoming(client, result.messages)
            callback.current?.(result.messages)
          }
        } catch {
          if (controller.signal.aborted) return
          await new Promise(resolve => { timer = setTimeout(resolve, RETRY_MS) })
        }
      }
    }
    void poll()
    return () => { controller.abort(); if (timer) clearTimeout(timer) }
  }, [client, enabled, watchedKey, startCursor])
}
