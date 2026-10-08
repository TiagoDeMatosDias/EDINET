import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import PipelinePage from './PipelinePage'

const STEPS = [
  { name: 'update_stock_prices', display_name: 'Update Stock Prices', description: 'Fetch prices', input_fields: [] },
  { name: 'detect_splits', display_name: 'Detect Splits', description: 'Find splits', input_fields: [{ name: 'mode', label: 'Detection Mode', choices: ['incremental', 'full'], default: 'incremental' }] },
]
const posted: Array<{ path: string; body: unknown }> = []

beforeEach(() => {
  posted.length = 0
  window.localStorage.clear()
  vi.stubGlobal('fetch', vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
    const path = String(input)
    if (init?.method === 'POST') posted.push({ path, body: JSON.parse(String(init.body)) })
    const body = path === '/api/steps' ? { steps: STEPS }
      : path.startsWith('/api/jobs') && !path.includes('/api/jobs/') ? [{ job_id: 'j1', status: 'failed', current_step: null, created_at: '2026-10-03T13:42:48Z', started_at: '2026-10-03T13:42:48Z', completed_at: '2026-10-03T13:42:52Z', error_message: "Step 'detect_splits' failed (ZeroDivisionError)", steps: [{ ordinal: 0, step_name: 'detect_splits', status: 'failed' }] }]
        : path.startsWith('/api/jobs/j1/output') ? { job_id: 'j1', status: 'failed', output: {} }
          : path.startsWith('/api/jobs/') ? { job_id: 'j1', status: 'failed', steps: [{ ordinal: 0, step_name: 'detect_splits', status: 'failed', error_message: 'boom' }] }
            : path === '/api/pipeline/run' ? { job_id: 'j2', status: 'queued', created_at: '' }
              : { max_upload_bytes: 1000 }
    return Promise.resolve(new Response(JSON.stringify(body), { status: 200, headers: { 'Content-Type': 'application/json' } }))
  }))
})
afterEach(() => { cleanup(); vi.unstubAllGlobals() })

const press = (key: string, target: Element | Window = window, options: Partial<KeyboardEventInit> = {}) => fireEvent.keyDown(target, { key, ...options })

describe('PipelinePage keyboard', () => {
  it('builds, configures, and runs a sequence without the mouse', async () => {
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    render(<QueryClientProvider client={client}><MemoryRouter><PipelinePage /></MemoryRouter></QueryClientProvider>)
    const library = await screen.findByRole('list', { name: 'Step library' })
    expect(await screen.findAllByText("Step 'detect_splits' failed (ZeroDivisionError)")).not.toHaveLength(0)

    press('1')
    const rows = within(library).getAllByRole('listitem')
    expect(rows[0]).toHaveFocus()
    press('Enter', rows[0])
    press('j', rows[0])
    expect(rows[1]).toHaveFocus()
    press(' ', rows[1])

    const sequence = screen.getByRole('list', { name: 'Sequence' })
    expect(within(sequence).getAllByRole('listitem').map(row => row.textContent)).toEqual([expect.stringContaining('Update Stock Prices'), expect.stringContaining('Detect Splits')])
    press('2')
    const first = within(sequence).getAllByRole('listitem')[0]
    expect(first).toHaveFocus()
    press('J', first, { shiftKey: true })
    expect(within(sequence).getAllByRole('listitem').map(row => row.textContent)).toEqual([expect.stringContaining('Detect Splits'), expect.stringContaining('Update Stock Prices')])
    // The moved step keeps the cursor: overwrite it, then configure the step above.
    press('o', document.activeElement!)
    press('k', document.activeElement!)
    press('Enter', document.activeElement!)
    fireEvent.change(screen.getByRole('combobox', { name: /Detection Mode/ }), { target: { value: 'full' } })

    press('r')
    await waitFor(() => expect(posted.find(item => item.path === '/api/pipeline/run')?.body).toEqual({
      steps: [{ name: 'detect_splits', overwrite: false }, { name: 'update_stock_prices', overwrite: true }],
      config: { detect_splits_config: { mode: 'full' } },
    }))
  })
})
