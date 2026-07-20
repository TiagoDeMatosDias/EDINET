import { ApiError } from './client'

export type StreamMessage = Record<string, unknown> & { type?: string; message?: string }

function parseEvent(block: string): StreamMessage | null {
  const data = block
    .split(/\r?\n/)
    .filter(line => line.startsWith('data:'))
    .map(line => line.slice(5).trimStart())
    .join('\n')
  if (!data) return null
  return JSON.parse(data) as StreamMessage
}

export async function apiStream(
  path: string,
  body: unknown,
  onMessage: (message: StreamMessage) => void,
  signal?: AbortSignal,
) {
  const response = await fetch(path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
    signal,
  })
  if (!response.ok || !response.body) {
    const payload = await response.text()
    throw new ApiError(payload || response.statusText, response.status, payload)
  }

  const reader = response.body.getReader()
  const decoder = new TextDecoder()
  let buffer = ''
  let result: StreamMessage | undefined
  while (true) {
    const { done, value } = await reader.read()
    buffer += decoder.decode(value, { stream: !done })
    const blocks = buffer.split(/\r?\n\r?\n/)
    buffer = blocks.pop() ?? ''
    for (const block of blocks) {
      const message = parseEvent(block)
      if (!message) continue
      onMessage(message)
      if (message.type === 'error') throw new Error(message.message || 'Streaming request failed')
      if (message.type === 'result') result = message
    }
    if (done) break
  }
  return result
}

