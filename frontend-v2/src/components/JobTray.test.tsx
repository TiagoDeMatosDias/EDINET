import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { act, cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import type { JobSummary } from '../api/jobs'
import { JobTray } from './JobTray'
import { ToastProvider } from './Toasts'

function job(overrides: Partial<JobSummary>): JobSummary {
  return {
    job_id: 'j1', kind: 'screening', kind_label: 'Screening', title: 'Screen A', status: 'in_progress', progress_percent: 40,
    status_message: 'Scoring companies', progress: null, created_at: '2026-10-15T00:00:00Z', started_at: null, completed_at: null,
    updated_at: '2026-10-15T00:00:10Z', error_message: null, notify: true, link: null, cancellable: true, waiting_for_worker: false,
    ...overrides,
  }
}

let jobs: JobSummary[] = []
const calls: Array<{ method: string; path: string }> = []

beforeEach(() => {
  jobs = []
  calls.length = 0
  vi.stubGlobal('fetch', vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
    const path = String(input)
    calls.push({ method: init?.method ?? 'GET', path })
    const body = path.startsWith('/api/my-jobs?') ? { jobs, server_time: '2026-10-15T00:00:00Z', workers: { online: 1, interactive_slots: 1, batch_slots: 2 } } : {}
    return Promise.resolve(new Response(JSON.stringify(body), { status: 200, headers: { 'Content-Type': 'application/json' } }))
  }))
})
afterEach(() => { cleanup(); vi.unstubAllGlobals(); vi.useRealTimers() })

function renderTray() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return render(<QueryClientProvider client={client}><MemoryRouter><ToastProvider><JobTray enabled /></ToastProvider></MemoryRouter></QueryClientProvider>)
}
const listCalls = () => calls.filter(call => call.path.startsWith('/api/my-jobs?')).length

describe('JobTray', () => {
  it('shows the active count and lists active before recent jobs', async () => {
    jobs = [
      job({ job_id: 'a', title: 'Active one', waiting_for_worker: false }),
      job({ job_id: 'b', title: 'Queued one', status: 'open', progress_percent: 0, waiting_for_worker: true }),
      job({ job_id: 'c', title: 'Old failure', status: 'failed', error_message: 'Out of memory', cancellable: false }),
    ]
    renderTray()
    expect(await screen.findByTestId('job-count')).toHaveTextContent('2')
    const button = screen.getByRole('button', { name: /Jobs, 2 active/ })
    expect(button).toHaveAttribute('aria-expanded', 'false')
    fireEvent.click(button)
    expect(button).toHaveAttribute('aria-expanded', 'true')
    expect(screen.getByRole('progressbar', { name: 'Active one progress' })).toHaveAttribute('aria-valuenow', '40')
    expect(screen.getByText('40% · Scoring companies')).toBeInTheDocument()
    expect(screen.getByText('Waiting for a worker')).toBeInTheDocument()
    expect(screen.getByText('Out of memory')).toBeInTheDocument()
    const titles = screen.getAllByRole('listitem').map(item => item.querySelector('.job-row__title')?.textContent)
    expect(titles).toEqual(['Active one', 'Queued one', 'Old failure'])
  })

  it('hides the count when idle but still lists recent jobs, with an Open link', async () => {
    jobs = [job({ status: 'completed', cancellable: false, link: '/screening' })]
    renderTray()
    const button = await screen.findByRole('button', { name: 'Jobs' })
    await waitFor(() => expect(listCalls()).toBe(1))
    expect(screen.queryByTestId('job-count')).toBeNull()
    fireEvent.click(button)
    expect(screen.getByRole('link', { name: 'Open' })).toHaveAttribute('href', '/screening')
  })

  it('cancels a job and closes on Escape and outside click', async () => {
    jobs = [job({})]
    renderTray()
    const button = await screen.findByRole('button', { name: /Jobs/ })
    fireEvent.click(button)
    fireEvent.click(screen.getByRole('button', { name: 'Cancel' }))
    await waitFor(() => expect(calls).toContainEqual({ method: 'POST', path: '/api/my-jobs/j1/cancel' }))
    fireEvent.keyDown(document, { key: 'Escape' })
    expect(button).toHaveAttribute('aria-expanded', 'false')
    expect(button).toHaveFocus()
    fireEvent.click(button)
    fireEvent.mouseDown(document.body)
    expect(button).toHaveAttribute('aria-expanded', 'false')
  })

  it('polls every 2 s while a job is active and every 15 s otherwise', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: false })
    jobs = [job({})]
    renderTray()
    await act(async () => { await vi.advanceTimersByTimeAsync(0) })
    expect(listCalls()).toBe(1)
    await act(async () => { await vi.advanceTimersByTimeAsync(2100) })
    expect(listCalls()).toBe(2)
    jobs = [job({ status: 'completed' })]
    await act(async () => { await vi.advanceTimersByTimeAsync(2100) })
    expect(listCalls()).toBe(3)
    await act(async () => { await vi.advanceTimersByTimeAsync(10_000) })
    expect(listCalls()).toBe(3)
    await act(async () => { await vi.advanceTimersByTimeAsync(5_100) })
    expect(listCalls()).toBe(4)
  })
})
