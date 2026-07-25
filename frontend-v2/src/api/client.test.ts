import { afterEach, describe, expect, it, vi } from 'vitest'

import { apiRequest, queryString, setAccessToken } from './client'

afterEach(() => {
  setAccessToken(null)
  vi.unstubAllGlobals()
})

describe('API client', () => {
  it('surfaces backend detail messages', async () => {
    vi.stubGlobal('fetch', vi.fn(() => Promise.resolve(new Response(
      JSON.stringify({ detail: 'Database not configured' }),
      { status: 503, headers: { 'Content-Type': 'application/json' } },
    ))))

    await expect(apiRequest('/api/example')).rejects.toMatchObject({
      name: 'ApiError',
      status: 503,
      message: 'Database not configured',
    })
  })

  it('omits empty query parameters', () => {
    expect(queryString({ q: 'Toyota', limit: 20, empty: '', missing: undefined }))
      .toBe('?q=Toyota&limit=20')
  })

  it('refreshes once and retries a request with the new access token', async () => {
    setAccessToken('expired-token')
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(new Response(JSON.stringify({ detail: 'expired' }), { status: 401, headers: { 'Content-Type': 'application/json' } }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ access_token: 'fresh-token' }), { status: 200, headers: { 'Content-Type': 'application/json' } }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ ok: true }), { status: 200, headers: { 'Content-Type': 'application/json' } }))
    vi.stubGlobal('fetch', fetchMock)

    await expect(apiRequest<{ ok: boolean }>('/api/private')).resolves.toEqual({ ok: true })
    expect(fetchMock).toHaveBeenCalledTimes(3)
    expect(fetchMock.mock.calls[0][1]).toMatchObject({ credentials: 'include' })
    expect(new Headers(fetchMock.mock.calls[0][1]?.headers).get('Authorization')).toBe('Bearer expired-token')
    expect(fetchMock.mock.calls[1][0]).toBe('/api/auth/refresh')
    expect(new Headers(fetchMock.mock.calls[2][1]?.headers).get('Authorization')).toBe('Bearer fresh-token')
  })

  it('shares one refresh operation across concurrent expired requests', async () => {
    setAccessToken('expired-token')
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(new Response('', { status: 401 }))
      .mockResolvedValueOnce(new Response('', { status: 401 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ access_token: 'fresh-token' }), { status: 200, headers: { 'Content-Type': 'application/json' } }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ id: 1 }), { status: 200, headers: { 'Content-Type': 'application/json' } }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ id: 2 }), { status: 200, headers: { 'Content-Type': 'application/json' } }))
    vi.stubGlobal('fetch', fetchMock)

    await expect(Promise.all([
      apiRequest<{ id: number }>('/api/one'),
      apiRequest<{ id: number }>('/api/two'),
    ])).resolves.toEqual([{ id: 1 }, { id: 2 }])
    expect(fetchMock.mock.calls.filter(([path]) => path === '/api/auth/refresh')).toHaveLength(1)
  })
})
