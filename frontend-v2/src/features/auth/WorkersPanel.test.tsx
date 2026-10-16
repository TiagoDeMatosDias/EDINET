import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import type { WorkerView, WorkersAdminResponse } from '../../api/jobs'
import { WorkersPanel } from './WorkersPanel'

const base: WorkerView = {
  worker_id: 'local', name: 'Local worker', kind: 'local', enabled: true, interactive_slots: 1, batch_slots: 2, created_at: '2026-10-01T00:00:00Z',
  online: true, last_seen_at: new Date().toISOString(), version: '1.2.3', hostname: 'box', pid: 99, platform: 'linux', data_access: true,
  data_access_detail: 'reads market.db', interactive_busy: 1, batch_busy: 0,
  running_jobs: [{ job_id: 'j1', kind: 'screening', title: 'Screen A', started_at: null, progress_percent: 25 }],
}
const enrolled: WorkerView = { ...base, worker_id: 'w2', name: 'Desk', kind: 'enrolled', online: false, running_jobs: [], data_access: false, data_access_detail: null }
const state: WorkersAdminResponse = {
  workers: [base, enrolled],
  launcher: { state: 'running', pid: 4242, restarts: 2, last_exit_code: 1, command: null },
  settings: { launch_local: 'on' },
}
const calls: Array<{ method: string; path: string; body?: unknown }> = []
let echoStatus = 200

beforeEach(() => {
  calls.length = 0
  echoStatus = 200
  vi.stubGlobal('fetch', vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
    const path = String(input)
    const method = init?.method ?? 'GET'
    calls.push({ method, path, body: init?.body ? JSON.parse(String(init.body)) : undefined })
    let body: unknown = {}
    let status = 200
    if (path === '/api/admin/workers' && method === 'GET') body = state
    else if (path === '/api/admin/workers/enrollments') body = { enrollment: 'SRW1.abc', command: 'ShadeWorker enroll SRW1.abc', expires_at: '2026-10-15T12:30:00Z' }
    else if (path === '/api/admin/workers/local/restart') body = { ...state.launcher, restarts: 3 }
    else if (path === '/api/admin/workers/echo') {
      status = echoStatus
      body = echoStatus === 202 ? { code: 'job_pending', job_id: 'e1', waiting_for_worker: true, detail: 'No worker is online; the job will start when one connects.' } : { job_id: 'e1', status: 'completed' }
    }
    return Promise.resolve(new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } }))
  }))
})
afterEach(() => { cleanup(); vi.unstubAllGlobals() })

function renderPanel() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return render(<QueryClientProvider client={client}><WorkersPanel /></QueryClientProvider>)
}

describe('WorkersPanel', () => {
  it('shows workers, slot use, running jobs and the local launcher', async () => {
    renderPanel()
    expect(await screen.findByText('Desk')).toBeInTheDocument()
    expect(screen.getAllByText('1 of 1 busy').length).toBeGreaterThan(0)
    expect(screen.getByText('reads market.db')).toBeInTheDocument()
    expect(screen.getByText(/Screen A/)).toBeInTheDocument()
    expect(screen.getByText('Running')).toBeInTheDocument()
    expect(screen.getByText(/pid 4242 · 2 restarts · last exit code 1/)).toBeInTheDocument()
    expect(screen.getAllByRole('img', { name: 'Online' })).toHaveLength(1)
    expect(screen.getAllByRole('button', { name: /Revoke/ })).toHaveLength(1)
  })

  it('patches name and slots, toggles enabled, and revokes after confirmation', async () => {
    renderPanel()
    await screen.findByText('Desk')
    const card = screen.getByLabelText('Name of Desk').closest('li') as HTMLElement
    fireEvent.change(screen.getByLabelText('Batch slots of Desk'), { target: { value: '5' } })
    fireEvent.change(screen.getByLabelText('Name of Desk'), { target: { value: 'Desk 2' } })
    fireEvent.click(within(card).getByRole('button', { name: 'Save' }))
    await waitFor(() => expect(calls).toContainEqual({ method: 'PATCH', path: '/api/admin/workers/w2', body: { name: 'Desk 2', interactive_slots: 1, batch_slots: 5 } }))
    fireEvent.click(within(card).getByRole('button', { name: 'Disable' }))
    await waitFor(() => expect(calls).toContainEqual({ method: 'PATCH', path: '/api/admin/workers/w2', body: { enabled: false } }))
    fireEvent.click(within(card).getByRole('button', { name: 'Revoke' }))
    expect(calls.some(call => call.method === 'DELETE')).toBe(false)
    fireEvent.click(within(card).getByRole('button', { name: 'Revoke: sure?' }))
    await waitFor(() => expect(calls).toContainEqual({ method: 'DELETE', path: '/api/admin/workers/w2', body: undefined }))
  })

  it('rejects slot counts out of range', async () => {
    renderPanel()
    await screen.findByText('Desk')
    fireEvent.change(screen.getByLabelText('Interactive slots of Desk'), { target: { value: '5' } })
    expect(within(screen.getByLabelText('Name of Desk').closest('li') as HTMLElement).getByRole('button', { name: 'Save' })).toBeDisabled()
  })

  it('creates an enrollment and shows the command', async () => {
    renderPanel()
    await screen.findByText('Desk')
    fireEvent.change(screen.getByPlaceholderText('Worker'), { target: { value: 'Laptop' } })
    fireEvent.click(screen.getByRole('button', { name: /Add a worker/ }))
    expect(await screen.findByText('ShadeWorker enroll SRW1.abc')).toBeInTheDocument()
    expect(calls).toContainEqual({ method: 'POST', path: '/api/admin/workers/enrollments', body: { name: 'Laptop' } })
    expect(screen.getByRole('button', { name: /Copy/ })).toBeInTheDocument()
  })

  it('restarts the local worker and saves the launch setting', async () => {
    renderPanel()
    await screen.findByText('Desk')
    fireEvent.click(screen.getByRole('button', { name: /Restart/ }))
    await waitFor(() => expect(calls).toContainEqual({ method: 'POST', path: '/api/admin/workers/local/restart', body: undefined }))
    fireEvent.click(screen.getByLabelText(/Start the local worker with the server/))
    await waitFor(() => expect(calls).toContainEqual({ method: 'PUT', path: '/api/admin/settings/workers.launch_local', body: { value: 'off' } }))
    expect(await screen.findByText('Saved. This applies after the server restarts.')).toBeInTheDocument()
  })

  it('runs a test job and reports completion or the pending detail', async () => {
    renderPanel()
    await screen.findByText('Desk')
    fireEvent.click(screen.getByRole('button', { name: /Run test job/ }))
    expect(await screen.findByText(/Test job completed in \d+ ms/)).toBeInTheDocument()
    expect(calls).toContainEqual({ method: 'POST', path: '/api/admin/workers/echo', body: { wait: true, result: 'ok' } })
    echoStatus = 202
    fireEvent.click(screen.getByRole('button', { name: /Run test job/ }))
    expect(await screen.findByText('No worker is online; the job will start when one connects.')).toBeInTheDocument()
  })
})
