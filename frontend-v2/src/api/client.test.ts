import { afterEach, describe, expect, it, vi } from 'vitest'

import { apiRequest, queryString } from './client'

afterEach(() => vi.unstubAllGlobals())

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
})

