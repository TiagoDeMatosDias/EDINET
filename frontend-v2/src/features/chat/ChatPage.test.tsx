import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { MemoryRouter, useLocation } from 'react-router-dom'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import ChatPage from './ChatPage'
import { GlobalHotkeys } from '../../components/GlobalHotkeys'

function json(value: unknown, status = 200) {
  return Promise.resolve(new Response(status === 204 ? null : JSON.stringify(value), { status, headers: { 'Content-Type': 'application/json' } }))
}

const SUMMARY = {
  me: { user_id: 'u1', username: 'alice', role: 'member', profile: null },
  key: null,
  channels: [
    { channel_id: 'topic:general', kind: 'topic', slug: 'general', name: 'General', description: 'Anything', subscribed: true, unread: 0, last_read_seq: 3, subscribers: 2 },
    { channel_id: 'topic:stocks', kind: 'topic', slug: 'stocks', name: 'Stocks', description: 'Equities', subscribed: false, unread: 0, subscribers: 1 },
  ],
  conversations: [],
  blocks: { users: [], channels: [] },
  cursor: 3,
}

const message = (seq: number, text: string, sender = { user_id: 'u2', username: 'bob' }, channel = 'topic:general') => ({
  seq, message_id: `m${seq}`, kind: 'message', channel_id: channel, conversation_id: null, sender, created_at: `2026-10-03T10:0${seq}:00Z`, deleted: false,
  body: { text, refs: text.includes('$7203') ? { 7203: { code: 'E02144', name: 'TOYOTA MOTOR' } } : {} },
})

const calls: Array<{ method: string; path: string; body?: unknown }> = []

beforeEach(() => {
  calls.length = 0
  window.localStorage.clear()
  vi.stubGlobal('fetch', vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
    const path = String(input)
    const method = init?.method ?? 'GET'
    calls.push({ method, path, body: init?.body ? JSON.parse(String(init.body)) : undefined })
    if (path.startsWith('/api/chat/updates')) {
      // The long poll waits until the test ends.
      return new Promise((_, reject) => init?.signal?.addEventListener('abort', () => reject(new DOMException('aborted', 'AbortError'))))
    }
    if (path === '/api/chat/summary') return json(SUMMARY)
    if (path.startsWith('/api/chat/feed')) return json({ messages: [message(1, 'Morning all'), message(2, 'Toyota at $7203 looks cheap'), message(3, 'I agree', { user_id: 'u1', username: 'alice' })], has_more: false })
    if (path.startsWith('/api/chat/channels/topic%3Astocks/messages') && method === 'GET') return json({ channel: SUMMARY.channels[1], messages: [message(4, 'Stocks talk', { user_id: 'u2', username: 'bob' }, 'topic:stocks')], has_more: false })
    if (path.startsWith('/api/chat/channels/topic%3Astocks/messages')) return json({ ...message(9, (JSON.parse(String(init?.body)) as { text: string }).text, { user_id: 'u1', username: 'alice' }, 'topic:stocks') }, 201)
    if (path.includes('/subscription') || path.includes('/read')) return json(null, 204)
    return json({})
  }))
})
afterEach(() => { cleanup(); vi.unstubAllGlobals() })

function Location() {
  const location = useLocation()
  return <output data-testid="location">{location.pathname === '/chat' ? location.search : location.pathname}</output>
}

function renderChat(search = '') {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return render(<QueryClientProvider client={client}><MemoryRouter initialEntries={[`/chat${search}`]}><ChatPage /><GlobalHotkeys isAdmin={false} /><Location /></MemoryRouter></QueryClientProvider>)
}

const press = (key: string, options: Partial<KeyboardEventInit> = {}) => fireEvent.keyDown(window, { key, ...options })
const rows = () => within(screen.getByRole('log')).getAllByText((_, element) => element?.classList.contains('chat-row') ?? false)

describe('ChatPage', () => {
  it('shows every followed channel in one colour-coded stream with company links', async () => {
    renderChat()
    await screen.findByText('Morning all')
    expect(screen.getByRole('heading', { name: 'All messages' })).toBeInTheDocument()
    expect(screen.getByRole('link', { name: /\$7203/ })).toHaveAttribute('href', '/analyze/E02144')
    expect(rows()[0]).toHaveTextContent('#general')
    // The newest message starts selected; K moves up.
    expect(rows()[2]).toHaveAttribute('aria-selected', 'true')
    press('k')
    expect(rows()[1]).toHaveAttribute('aria-selected', 'true')
    press('o')
    await waitFor(() => expect(screen.getByTestId('location').textContent).toBe('/analyze/E02144'))
  })

  it('filters with F, M, and $, and clears with the button', async () => {
    renderChat()
    await screen.findByText('Morning all')
    press('$')
    expect(rows()).toHaveLength(1)
    press('$')
    fireEvent.change(screen.getByRole('textbox', { name: 'Filter messages' }), { target: { value: 'from:alice' } })
    expect(rows()).toHaveLength(1)
    expect(rows()[0]).toHaveTextContent('I agree')
    fireEvent.click(screen.getByTitle('Clear the filter'))
    expect(rows()).toHaveLength(3)
  })

  it('walks channels with ] and subscribes with S', async () => {
    renderChat()
    await screen.findByText('Morning all')
    press(']')
    await waitFor(() => expect(screen.getByTestId('location').textContent).toBe('?c=topic%3Ageneral'))
    press(']')
    await waitFor(() => expect(screen.getByTestId('location').textContent).toBe('?c=topic%3Astocks'))
    await screen.findByText('Stocks talk')
    press('s')
    await waitFor(() => expect(calls.some(call => call.method === 'PUT' && call.path === '/api/chat/channels/topic%3Astocks/subscription')).toBe(true))
  })

  it('sends with Enter from the composer and shows the message', async () => {
    renderChat('?c=topic:stocks')
    await screen.findByText('Stocks talk')
    press('Enter')
    const box = screen.getByRole('textbox', { name: 'Message' })
    expect(box).toHaveFocus()
    fireEvent.change(box, { target: { value: 'Buying more', selectionStart: 11 } })
    fireEvent.keyDown(box, { key: 'Enter' })
    await screen.findByText('Buying more')
    const sent = calls.find(call => call.method === 'POST' && call.path === '/api/chat/channels/topic%3Astocks/messages')
    expect(sent?.body).toMatchObject({ text: 'Buying more', refs: {}, reply_to: null })
    expect(box).toHaveValue('')
  })

  it('asks for an encryption key before a private conversation, from N', async () => {
    renderChat()
    await screen.findByText('Morning all')
    press('n')
    expect(await screen.findByRole('dialog', { name: 'New direct message' })).toBeInTheDocument()
    fireEvent.keyDown(window, { key: 'Escape' })
    await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument())
    press('?')
    expect(await screen.findByRole('dialog', { name: /Keyboard shortcuts/ })).toHaveTextContent('reference a company')
  })
})
