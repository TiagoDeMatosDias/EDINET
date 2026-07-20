import { afterEach, describe, expect, it, vi } from 'vitest'

import { apiStream } from './stream'

afterEach(() => vi.unstubAllGlobals())

describe('SSE API client', () => {
  it('reports progress and returns the final result', async () => {
    const encoder = new TextEncoder()
    const body = new ReadableStream({
      start(controller) {
        controller.enqueue(encoder.encode('data: {"type":"progress","message":"Period 1"}\n\n'))
        controller.enqueue(encoder.encode('data: {"type":"result","id":"run-1","aggregate":{"successful":4}}\n\n'))
        controller.close()
      },
    })
    vi.stubGlobal('fetch', vi.fn(() => Promise.resolve(new Response(body, { status: 200 }))))
    const messages: string[] = []

    const result = await apiStream('/api/backtesting/run-rolling', {}, message => {
      messages.push(String(message.type))
    })

    expect(messages).toEqual(['progress', 'result'])
    expect(result).toMatchObject({ type: 'result', id: 'run-1' })
  })
})

