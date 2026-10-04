import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { cleanup, fireEvent, render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { AuthContext, type AuthContextValue } from '../auth/authContext'
import OverviewPage from './OverviewPage'

const OVERVIEW = {
  today: '2026-10-04',
  portfolio: { currency: 'EUR', valuation_date: '2026-10-03', first_date: '2020-01-02', total_value: 277233, cash: 178, day_return: 0.004, ytd_return: 0.12, total_return: 0.85, holdings_count: 14, top_holdings: [{ symbol: 'VWCE', asset_category: 'STK', value: 157962, weight: 0.57, currency: 'EUR' }, { symbol: 'AFL', asset_category: 'STK', value: 19873, weight: 0.07, currency: 'USD' }] },
  research: { followed: 5, alerts: 2, notes: 3, reviews_due: [{ edinet_code: 'E02144', review_on: '2026-09-01', thesis_status: 'watch' }], recent_notes: [{ note_id: 'n1', title: 'Margins held', edinet_code: 'E02144', updated_at: '2026-10-02T10:00:00Z' }], theses: { watch: 2 } },
  data: { latest_price_date: '2026-10-02', priced_securities: 3801, filings: { unique_filings: 72515, unique_companies: 5145, last_submitted: '2026-07-27 16:30', first_submitted: '2016-08-04 10:33', filings_with_issues: 4811 } },
}

const requests: string[] = []

beforeEach(() => {
  requests.length = 0
  vi.stubGlobal('fetch', vi.fn((input: RequestInfo | URL) => {
    const path = String(input)
    requests.push(path)
    const body = path === '/api/overview' ? OVERVIEW
      : path.startsWith('/api/research/recent-work') ? { items: [{ work_id: 'w1', kind: 'screen', title: 'Cheap banks', href: '/screen?run=1', occurred_at: '2026-10-03T09:00:00Z' }] }
        : path.startsWith('/api/chat/feed') ? { messages: [], has_more: false }
          : path === '/api/chat/unread' ? { total: 3, channels: 2, conversations: 1, invitations: 0 }
            : path === '/api/system/status' ? { version: '1.0.0', timestamp: '', jobs: { queue_depth: 1, active: 1, counts_by_status: {} } }
              : path.startsWith('/api/jobs') ? [{ job_id: 'j1', status: 'failed', current_step: 'detect_splits', error_message: 'boom', created_at: '2026-10-03T13:42:48Z' }]
                : {}
    return Promise.resolve(new Response(JSON.stringify(body), { status: 200, headers: { 'Content-Type': 'application/json' } }))
  }))
})
afterEach(() => { cleanup(); vi.unstubAllGlobals() })

function renderAs(role: 'admin' | 'member') {
  const auth = { user: { user_id: 'u1', username: 'alice', role, status: 'active' }, status: { mode: 'accounts' }, loading: false } as unknown as AuthContextValue
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return render(<QueryClientProvider client={client}><AuthContext.Provider value={auth}><MemoryRouter><OverviewPage /></MemoryRouter></AuthContext.Provider></QueryClientProvider>)
}

describe('OverviewPage', () => {
  it('packs portfolio, research, chat, and data into one screen for members, without operator details', async () => {
    renderAs('member')
    await screen.findByRole('heading', { name: 'Overview' })
    expect(screen.getByText('€277,233')).toBeInTheDocument()
    expect(screen.getByText('+12.0%')).toBeInTheDocument()
    expect(screen.getByRole('link', { name: 'VWCE' })).toHaveAttribute('href', '/analyze?ticker=VWCE&from=portfolio')
    expect(screen.getByRole('link', { name: 'Margins held' })).toBeInTheDocument()
    expect(await screen.findByRole('link', { name: 'Cheap banks' })).toHaveAttribute('href', '/screen?run=1')
    expect(screen.queryByText('Refresh data')).not.toBeInTheDocument()
    expect(screen.queryByText('Recent pipeline runs')).not.toBeInTheDocument()
    expect(requests.some(path => path.startsWith('/api/jobs') || path === '/api/system/status')).toBe(false)
  })

  it('adds the pipeline status and runs for administrators', async () => {
    renderAs('admin')
    expect(await screen.findByText('1 active')).toBeInTheDocument()
    expect(screen.getByRole('link', { name: 'Refresh data' })).toHaveAttribute('href', '/pipeline')
    expect(await screen.findByText('Recent pipeline runs')).toBeInTheDocument()
    expect(await screen.findByText('boom')).toBeInTheDocument()
  })

  it('moves through every panel from the keyboard', async () => {
    renderAs('member')
    await screen.findByRole('link', { name: 'VWCE' })
    fireEvent.keyDown(window, { key: 'j' })
    expect(screen.getByRole('link', { name: 'VWCE' })).toHaveFocus()
    fireEvent.keyDown(window, { key: 'j' })
    expect(screen.getByRole('link', { name: 'AFL' })).toHaveFocus()
    fireEvent.keyDown(window, { key: ']' })
    expect(screen.getByRole('link', { name: 'E02144' })).toHaveFocus()
    fireEvent.keyDown(window, { key: '1' })
    expect(screen.getByRole('link', { name: 'VWCE' })).toHaveFocus()
  })
})
