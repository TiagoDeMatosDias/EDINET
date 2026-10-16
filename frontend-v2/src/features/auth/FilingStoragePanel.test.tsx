import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { filingStorageNoticeText, gigabytes, type FilingStorageResponse } from './filingStorage'
import { FilingStoragePanel } from './FilingStoragePanel'

const LEGACY: FilingStorageResponse = {
  layout: 'legacy', state: 'legacy', index: { path: '/data/filings.db', bytes: 77_567_938_560 }, shards_folder: '/data/filings', shards: [], jobs: [],
  preflight: {
    layout: 'legacy', needed: true, filings: 72514, needed_bytes: 81_446_335_488, free_bytes: 368_000_000_000, enough_space: true, undated: [], undated_count: 0,
    target_folder: '/data/filings', default_folder: true,
    years: [{ shard: '2016', filings: 489, estimated_bytes: 500_000_000, path: '/data/filings/filings_2016.db' }, { shard: '2026', filings: 7018, estimated_bytes: 7_000_000_000, path: '/data/filings/filings_2026.db' }],
  },
}
const SPLIT: FilingStorageResponse = {
  layout: 'split-v1', state: 'split', index: { path: '/data/filings.db', bytes: 60_000_000 }, shards_folder: '/data/filings', jobs: [],
  shards: [
    { shard: '2019', path: 'filings/filings_2019.db', file: '/data/filings/filings_2019.db', state: 'closed', filings: 4116, archive_bytes: 4_000_000_000, file_bytes: 4_400_000_000, checked_at: '2026-10-16T08:00:00Z', check_result: 'ok' },
    { shard: '2020', path: 'filings/filings_2020.db', file: '/data/filings/filings_2020.db', state: 'missing', filings: 4154, archive_bytes: 4_100_000_000, file_bytes: null, checked_at: null, check_result: null },
  ],
  retired: { path: '/data/filings.db.migrated', bytes: 77_567_938_560 },
}
const calls: Array<{ method: string; path: string; body?: unknown }> = []
let current: FilingStorageResponse = LEGACY

beforeEach(() => {
  calls.length = 0
  current = LEGACY
  vi.stubGlobal('fetch', vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
    const path = String(input)
    const method = init?.method ?? 'GET'
    calls.push({ method, path, body: init?.body ? JSON.parse(String(init.body)) : undefined })
    let body: unknown = current
    let status = 200
    if (path === '/api/admin/filing-storage/split') { status = 202; body = { job_id: 'j1', status: 'open', kind: 'filings_split', title: 'Split filing storage' } }
    else if (path === '/api/admin/filing-storage/check') { status = 202; body = { job_id: 'j2', status: 'open', kind: 'filing_storage_check', title: 'Check filing storage' } }
    else if (path === '/api/admin/filing-storage/compact') { status = 202; body = { job_id: 'j3', status: 'open', kind: 'filing_storage_compact', title: 'Compact filings 2019' } }
    else if (path === '/api/admin/filing-storage/retired') { status = 202; body = { job_id: 'j4', status: 'open', kind: 'filing_storage_cleanup', title: 'Delete the old filings file' } }
    return Promise.resolve(new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } }))
  }))
})
afterEach(() => { cleanup(); vi.unstubAllGlobals() })

function renderPanel() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return render(<QueryClientProvider client={client}><FilingStoragePanel /></QueryClientProvider>)
}

describe('FilingStoragePanel', () => {
  it('words the notice for each state', () => {
    expect(filingStorageNoticeText({ state: 'legacy', needed_bytes: 81_446_335_488 })).toBe('Filing storage needs reorganising — ≈81 GB free space needed')
    expect(filingStorageNoticeText({ state: 'ready', needed_bytes: 0 })).toMatch(/restart the server to finish/)
    expect(gigabytes(4_400_000_000)).toBe('4.4 GB')
    expect(gigabytes(60_000_000)).toBe('60 MB')
  })

  it('shows the preflight and starts the split after confirmation', async () => {
    renderPanel()
    expect(await screen.findByText('72,514')).toBeInTheDocument()
    expect(screen.getByText('81 GB')).toBeInTheDocument()
    expect(screen.getByText('2016–2026')).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: /Split filing storage/ }))
    expect(calls.some(call => call.method === 'POST')).toBe(false)
    fireEvent.change(screen.getByLabelText('Folder for the yearly files'), { target: { value: '/mnt/big/filings' } })
    fireEvent.click(screen.getByRole('button', { name: /Split filing storage/ }))
    fireEvent.click(screen.getByRole('button', { name: /Start the split: sure\?/ }))
    await waitFor(() => expect(calls).toContainEqual({ method: 'POST', path: '/api/admin/filing-storage/split', body: { target_folder: '/mnt/big/filings' } }))
    expect(await screen.findByText(/Split filing storage: queued/)).toBeInTheDocument()
  })

  it('refuses the split while filings have no submission date or space is short', async () => {
    current = { ...LEGACY, preflight: { ...LEGACY.preflight!, enough_space: false, free_bytes: 10_000_000_000, undated: ['S100NODATE'], undated_count: 1 } }
    renderPanel()
    expect(await screen.findByText(/no submission date/)).toHaveTextContent('S100NODATE')
    expect(screen.getByText(/not enough/)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /Split filing storage/ })).toBeDisabled()
  })

  it('asks for a restart when the split is ready', async () => {
    current = { ...LEGACY, state: 'ready' }
    renderPanel()
    expect(await screen.findByText(/Restart the server/)).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /Split filing storage/ })).not.toBeInTheDocument()
  })

  it('lists one row per year with check and compact, and deletes the old file after confirmation', async () => {
    current = SPLIT
    renderPanel()
    const row = (await screen.findByText('2019')).closest('tr') as HTMLElement
    expect(within(row).getByText('4,116')).toBeInTheDocument()
    expect(within(row).getByText('4.4 GB')).toBeInTheDocument()
    expect(within(row).getByText(/^OK · /)).toBeInTheDocument()
    const missing = screen.getByText('2020').closest('tr') as HTMLElement
    expect(missing).toHaveClass('is-alarm')
    expect(within(missing).getByRole('button', { name: 'Check 2020' })).toBeDisabled()

    fireEvent.click(within(row).getByRole('button', { name: 'Check 2019' }))
    await waitFor(() => expect(calls).toContainEqual({ method: 'POST', path: '/api/admin/filing-storage/check', body: { repair: false, shard: '2019' } }))
    fireEvent.click(within(row).getByRole('button', { name: 'Compact 2019' }))
    await waitFor(() => expect(calls).toContainEqual({ method: 'POST', path: '/api/admin/filing-storage/compact', body: { shard: '2019' } }))
    fireEvent.click(screen.getByRole('button', { name: /Check and repair the index/ }))
    await waitFor(() => expect(calls).toContainEqual({ method: 'POST', path: '/api/admin/filing-storage/check', body: { repair: true } }))

    expect(screen.getByText('/data/filings.db.migrated')).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: /^Delete$/ }))
    expect(calls.some(call => call.method === 'DELETE')).toBe(false)
    fireEvent.click(screen.getByRole('button', { name: /Delete it: sure\?/ }))
    await waitFor(() => expect(calls).toContainEqual({ method: 'DELETE', path: '/api/admin/filing-storage/retired', body: undefined }))
  })

  it('shows running jobs and disables new actions meanwhile', async () => {
    current = { ...SPLIT, jobs: [{ job_id: 'j9', kind: 'filing_storage_check', status: 'in_progress', title: 'Check filing storage', progress_percent: 40, status_message: 'Checking 2019', error_message: null, created_at: '', completed_at: null }] }
    renderPanel()
    expect(await screen.findByText('Check filing storage')).toBeInTheDocument()
    expect(screen.getByText(/40%/)).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /Check all/ })).toBeDisabled()
  })
})
