import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { act, cleanup, fireEvent, render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import type { JobSummary } from '../api/jobs'
import { JobTray } from './JobTray'
import { ToastProvider, useToasts } from './Toasts'

function job(overrides: Partial<JobSummary>): JobSummary {
  return {
    job_id: 'j1', kind: 'screening', kind_label: 'Screening', title: 'Screen A', status: 'in_progress', progress_percent: 40,
    status_message: null, progress: null, created_at: '2026-10-15T00:00:00Z', started_at: null, completed_at: null,
    updated_at: '2026-10-15T00:00:10Z', error_message: null, notify: true, link: '/screening', cancellable: true, waiting_for_worker: false,
    ...overrides,
  }
}

let jobs: JobSummary[] = []

beforeEach(() => {
  vi.useFakeTimers()
  jobs = []
  vi.stubGlobal('fetch', vi.fn(() => Promise.resolve(new Response(JSON.stringify({ jobs, server_time: '2026-10-15T00:00:00Z', workers: { online: 1, interactive_slots: 1, batch_slots: 2 } }), { status: 200, headers: { 'Content-Type': 'application/json' } }))))
})
afterEach(() => { cleanup(); vi.unstubAllGlobals(); vi.useRealTimers() })

function renderTray() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return render(<QueryClientProvider client={client}><MemoryRouter><ToastProvider><JobTray enabled /></ToastProvider></MemoryRouter></QueryClientProvider>)
}
const tick = (ms: number) => act(async () => { await vi.advanceTimersByTimeAsync(ms) })

describe('job toasts', () => {
  it('toasts when a notify job goes from active to completed, with an Open link', async () => {
    jobs = [job({})]
    renderTray()
    await tick(0)
    expect(screen.queryByRole('status')).toBeNull()
    jobs = [job({ status: 'completed' })]
    await tick(2100)
    expect(screen.getByRole('status')).toHaveTextContent('Screen A finished')
    expect(screen.getByRole('link', { name: 'Open' })).toHaveAttribute('href', '/screening')
  })

  it('reports failures with the error and cancellations', async () => {
    jobs = [job({}), job({ job_id: 'j2', title: 'Other' })]
    renderTray()
    await tick(0)
    jobs = [job({ status: 'failed', error_message: 'Bad input' }), job({ job_id: 'j2', title: 'Other', status: 'cancelled' })]
    await tick(2100)
    const text = screen.getAllByRole('status').map(item => item.textContent).join(' | ')
    expect(text).toContain('Screen A failed: Bad input')
    expect(text).toContain('Other was cancelled')
  })

  it('stays quiet for jobs already finished on first load and for notify=false', async () => {
    jobs = [job({ status: 'completed' }), job({ job_id: 'j2', notify: false })]
    renderTray()
    await tick(0)
    jobs = [job({ status: 'completed' }), job({ job_id: 'j2', notify: false, status: 'completed' })]
    await tick(2100)
    expect(screen.queryByRole('status')).toBeNull()
  })

  it('dismisses automatically after 8 s and from the keyboard', async () => {
    function Pusher() {
      const toasts = useToasts()
      return <button onClick={() => { toasts.push({ message: 'One' }); toasts.push({ message: 'Two' }) }}>push</button>
    }
    render(<MemoryRouter><ToastProvider><Pusher /></ToastProvider></MemoryRouter>)
    fireEvent.click(screen.getByText('push'))
    expect(screen.getAllByRole('status')).toHaveLength(2)
    fireEvent.keyDown(screen.getAllByRole('status')[0], { key: 'Escape' })
    expect(screen.getAllByRole('status')).toHaveLength(1)
    expect(screen.getByRole('status')).toHaveTextContent('Two')
    await tick(8100)
    expect(screen.queryByRole('status')).toBeNull()
    fireEvent.click(screen.getByText('push'))
    fireEvent.click(screen.getAllByRole('button', { name: 'Dismiss notification' })[0])
    expect(screen.getAllByRole('status')).toHaveLength(1)
  })
})
