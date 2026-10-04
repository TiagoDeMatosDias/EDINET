import { describe, expect, it } from 'vitest'

import {
  colorFor,
  companyTokens,
  continues,
  EMPTY_FILTER,
  matchesFilter,
  mergeMessages,
  segments,
  sidebarEntries,
  targetFromParam,
  targetParam,
  tseCode,
} from './chatModel'
import type { Channel, ChatMessage, Conversation } from './chatTypes'

function message(patch: Partial<ChatMessage>): ChatMessage {
  return {
    seq: 1, message_id: 'm1', kind: 'message', channel_id: 'topic:stocks', conversation_id: null,
    sender: { user_id: 'u1', username: 'alice' }, created_at: '2026-10-03T10:00:00Z', deleted: false,
    body: { text: 'hello', refs: {} }, ...patch,
  }
}

describe('chat text', () => {
  it('splits text into companies, mentions, links, and known channels', () => {
    const parts = segments('Look at $7203 (and $E01777), @bob: https://example.com/x. See #stocks, not #nope or a$1301', { 7203: { code: 'E02144', name: 'Toyota' } }, new Set(['stocks']))
    expect(parts).toEqual([
      { type: 'text', value: 'Look at ' },
      { type: 'company', token: '7203', ref: { code: 'E02144', name: 'Toyota' } },
      { type: 'text', value: ' (and ' },
      { type: 'company', token: 'E01777', ref: undefined },
      { type: 'text', value: '), ' },
      { type: 'mention', username: 'bob' },
      { type: 'text', value: ': ' },
      { type: 'link', url: 'https://example.com/x' },
      { type: 'text', value: '. See ' },
      { type: 'channel', slug: 'stocks' },
      { type: 'text', value: ', not #nope or a$1301' },
    ])
  })

  it('finds the same company tokens as the server, and reads TSE codes from stored tickers', () => {
    expect(companyTokens('$7203 $130A $E02144 $AAPL $72030 $7203')).toEqual(['7203', '130A', 'E02144'])
    expect([tseCode('72030'), tseCode('7203.T'), tseCode('285A0'), tseCode('AAPL')]).toEqual(['7203', '7203', '285A', null])
  })
})

describe('chat targets and lists', () => {
  it('round-trips the place shown through the URL', () => {
    for (const value of ['topic:stocks', 'company:E02144', 'conv:abc-123']) expect(targetParam(targetFromParam(value))).toBe(value)
    expect(targetFromParam(null)).toEqual({ type: 'feed' })
    expect(targetFromParam('javascript:alert(1)')).toEqual({ type: 'feed' })
  })

  it('colours the same id the same way every time', () => {
    expect(colorFor('topic:stocks')).toBe(colorFor('topic:stocks'))
  })

  it('merges live messages in order, applying deletions and hiding key events', () => {
    const first = message({ seq: 1, message_id: 'a' })
    const merged = mergeMessages([first], [
      message({ seq: 3, message_id: 'c' }),
      message({ seq: 2, message_id: 'b' }),
      message({ seq: 4, message_id: 'd', kind: 'system', system: { event: 'deleted', message_id: 'a' } }),
      message({ seq: 5, message_id: 'e', kind: 'system', system: { event: 'keys_shared', members: ['u2'] } }),
    ])
    expect(merged.map(item => item.message_id)).toEqual(['a', 'b', 'c'])
    expect(merged[0]).toMatchObject({ deleted: true, body: undefined })
  })

  it('joins consecutive messages from one person in one place within five minutes', () => {
    const a = message({ created_at: '2026-10-03T10:00:00Z' })
    expect(continues(a, message({ message_id: 'b', created_at: '2026-10-03T10:04:00Z' }))).toBe(true)
    expect(continues(a, message({ message_id: 'b', created_at: '2026-10-03T10:06:00Z' }))).toBe(false)
    expect(continues(a, message({ message_id: 'b', channel_id: 'topic:bonds' }))).toBe(false)
  })

  it('filters by kind, words, sender, place, mentions, and companies', () => {
    const conversations = new Map<string, Conversation>()
    const context = { where: '#stocks', me: 'bob', conversations }
    const withRef = message({ body: { text: 'Cheap $7203 @bob', refs: { 7203: { code: 'E02144', name: 'Toyota' } } } })
    const plain = message({ message_id: 'b', channel_id: 'company:E02144', body: { text: 'nothing', refs: {} } })
    expect(matchesFilter(withRef, withRef.body, { ...EMPTY_FILTER, text: 'toyota from:ali in:stocks' }, context)).toBe(true)
    expect(matchesFilter(withRef, withRef.body, { ...EMPTY_FILTER, text: 'from:carol' }, context)).toBe(false)
    expect(matchesFilter(withRef, withRef.body, { ...EMPTY_FILTER, mentionsMe: true, withCompanies: true }, context)).toBe(true)
    expect(matchesFilter(plain, plain.body, { ...EMPTY_FILTER, withCompanies: true }, context)).toBe(false)
    expect(matchesFilter(plain, plain.body, { ...EMPTY_FILTER, kind: 'companies' }, context)).toBe(true)
    expect(matchesFilter(plain, plain.body, { ...EMPTY_FILTER, kind: 'topics' }, context)).toBe(false)
  })

  it('lists the sidebar in the order [ and ] walk it, leaving out blocked channels', () => {
    const channels: Channel[] = [
      { channel_id: 'topic:general', kind: 'topic', name: 'General', slug: 'general', subscribed: true, unread: 2 },
      { channel_id: 'topic:fx', kind: 'topic', name: 'FX', slug: 'fx', blocked: true },
      { channel_id: 'company:E02144', kind: 'company', name: 'Toyota', subscribed: false, unread: 4 },
    ]
    const dm = { conversation_id: 'c1', kind: 'dm', my_status: 'active', unread: 1, hidden: false, members: [{ user_id: 'me', username: 'me', role: 'owner', status: 'active' }, { user_id: 'u2', username: 'bob', role: 'member', status: 'active' }] } as Conversation
    const invite = { ...dm, conversation_id: 'c2', kind: 'group', title: 'Club', my_status: 'invited', unread: 0 } as Conversation
    const entries = sidebarEntries(channels, [dm, invite], 'me', 3)
    expect(entries.map(entry => `${entry.icon}:${entry.label}:${entry.unread}`)).toEqual(['feed:All messages:3', 'invite:Club:1', 'topic:general:2', 'company:Toyota:0', 'dm:bob:1'])
  })
})
