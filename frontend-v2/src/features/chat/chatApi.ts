import { useQuery, type QueryClient } from '@tanstack/react-query'

import { apiPost, apiRequest, queryString } from '../../api/client'
import type { Blocks, Channel, ChatMessage, ChatSummary, Conversation, ConversationKeys, OwnKey, Person, PublicKey, PublicProfile } from './chatTypes'

export interface MessagePage { messages: ChatMessage[]; has_more: boolean; channel?: Channel }

export const chatKeys = {
  summary: ['chat', 'summary'] as const,
  unread: ['chat', 'unread'] as const,
  channel: (id: string) => ['chat', 'channel', id] as const,
  channelMessages: (id: string) => ['chat', 'messages', 'channel', id] as const,
  conversationMessages: (id: string) => ['chat', 'messages', 'conversation', id] as const,
  feed: ['chat', 'messages', 'feed'] as const,
  mentions: (code: string) => ['chat', 'mentions', code] as const,
  ownKey: ['chat', 'own-key'] as const,
  people: (query: string) => ['chat', 'people', query] as const,
  profile: (username: string) => ['chat', 'profile', username] as const,
  myProfile: ['chat', 'my-profile'] as const,
  blocks: ['chat', 'blocks'] as const,
}

const enc = encodeURIComponent

export function useChatSummary(enabled = true) {
  return useQuery({ queryKey: chatKeys.summary, queryFn: () => apiRequest<ChatSummary>('/api/chat/summary'), enabled, retry: false, staleTime: 15_000 })
}

export function useChatUnread(enabled = true) {
  return useQuery({
    queryKey: chatKeys.unread,
    queryFn: () => apiRequest<{ total: number; channels: number; conversations: number; invitations: number }>('/api/chat/unread'),
    enabled,
    retry: false,
    refetchInterval: 45_000,
    staleTime: 20_000,
  })
}

export const chatApi = {
  channel: (id: string) => apiRequest<Channel>(`/api/chat/channels/${enc(id)}`),
  channelMessages: (id: string, before?: number) => apiRequest<MessagePage>(`/api/chat/channels/${enc(id)}/messages${queryString({ before, limit: 80 })}`),
  conversationMessages: (id: string, before?: number) => apiRequest<MessagePage>(`/api/chat/conversations/${enc(id)}/messages${queryString({ before, limit: 80 })}`),
  feed: (before?: number) => apiRequest<MessagePage>(`/api/chat/feed${queryString({ before, limit: 120 })}`),
  mentions: (code: string) => apiRequest<{ messages: ChatMessage[] }>(`/api/chat/companies/${enc(code)}/mentions?limit=30`),
  postChannel: (id: string, body: { message_id: string; text: string; refs: Record<string, unknown>; reply_to?: string | null }) =>
    apiPost<ChatMessage>(`/api/chat/channels/${enc(id)}/messages`, body),
  postConversation: (id: string, body: { message_id: string; ciphertext: string; nonce: string; key_version: number; reply_to?: string | null }) =>
    apiPost<ChatMessage>(`/api/chat/conversations/${enc(id)}/messages`, body),
  subscribe: (id: string, on: boolean) => apiRequest(`/api/chat/channels/${enc(id)}/subscription`, { method: on ? 'PUT' : 'DELETE' }),
  readChannel: (id: string, seq: number) => apiPost(`/api/chat/channels/${enc(id)}/read`, { seq }),
  readConversation: (id: string, seq: number) => apiPost(`/api/chat/conversations/${enc(id)}/read`, { seq }),
  searchChannels: (q: string) => apiRequest<{ channels: Channel[] }>(`/api/chat/channels${queryString({ q, limit: 12 })}`),
  resolve: (tokens: string[]) => apiRequest<{ refs: Record<string, { code: string; name: string; ticker?: string | null }> }>(`/api/chat/resolve${queryString({ tokens: tokens.join(',') })}`),
  deleteMessage: (id: string) => apiRequest(`/api/chat/messages/${enc(id)}`, { method: 'DELETE' }),
  updates: (since: number, channels: string[], signal: AbortSignal) =>
    apiRequest<{ cursor: number; messages: ChatMessage[] }>(`/api/chat/updates${queryString({ since, wait: 25, channels: channels.join(',') })}`, { signal }),
  ownKey: () => apiRequest<{ key: OwnKey | null }>('/api/chat/keys/me'),
  publishKey: (body: Omit<OwnKey, 'key_id' | 'user_id' | 'created_at'>) => apiPost<{ key: OwnKey }>('/api/chat/keys', body),
  rewrapKey: (keyId: string, body: Pick<OwnKey, 'wrapped_private_key' | 'wrap_salt' | 'wrap_iv' | 'wrap_iterations'>) =>
    apiRequest(`/api/chat/keys/${enc(keyId)}/wrap`, { method: 'PUT', body: JSON.stringify(body) }),
  publicKeys: (params: { userIds?: string[]; keyIds?: string[] }) =>
    apiRequest<{ keys: PublicKey[] }>(`/api/chat/keys${queryString({ user_ids: params.userIds?.join(','), key_ids: params.keyIds?.join(',') })}`),
  conversationKeys: (id: string) => apiRequest<ConversationKeys>(`/api/chat/conversations/${enc(id)}/keys`),
  shareKeys: (id: string, body: { wrapper_key_id: string; wraps: unknown[] }) => apiPost(`/api/chat/conversations/${enc(id)}/keys`, body),
  rotate: (id: string, body: { key_version: number; wrapper_key_id: string; wraps: unknown[] }) => apiPost(`/api/chat/conversations/${enc(id)}/rotate`, body),
  createConversation: (body: { conversation_id: string; kind: 'dm' | 'group'; member_ids: string[]; title?: string; wrapper_key_id: string; wraps: unknown[] }) =>
    apiPost<Conversation>('/api/chat/conversations', body),
  updateConversation: (id: string, body: { title?: string; hidden?: boolean }) =>
    apiRequest<Conversation>(`/api/chat/conversations/${enc(id)}`, { method: 'PATCH', body: JSON.stringify(body) }),
  invite: (id: string, body: { user_ids: string[]; wrapper_key_id?: string; wraps: unknown[] }) => apiPost(`/api/chat/conversations/${enc(id)}/members`, body),
  removeMember: (id: string, userId: string) => apiRequest(`/api/chat/conversations/${enc(id)}/members/${enc(userId)}`, { method: 'DELETE' }),
  respond: (id: string, accept: boolean) => apiPost(`/api/chat/conversations/${enc(id)}/invitation`, { accept }),
  block: (kind: 'user' | 'channel', id: string, on: boolean) => apiRequest(`/api/chat/blocks/${kind}/${enc(id)}`, { method: on ? 'PUT' : 'DELETE' }),
  blocks: () => apiRequest<Blocks>('/api/chat/blocks'),
  people: (q: string) => apiRequest<{ people: Person[] }>(`/api/profiles${queryString({ q, limit: 20 })}`),
  profile: (username: string) => apiRequest<PublicProfile>(`/api/profiles/${enc(username)}`),
}

/** After a change to subscriptions, blocks, or conversations: refresh what lists them. */
export function invalidateChat(client: QueryClient) {
  for (const key of [chatKeys.summary, chatKeys.unread, chatKeys.blocks, ['chat', 'profile'], chatKeys.feed]) void client.invalidateQueries({ queryKey: key })
}
