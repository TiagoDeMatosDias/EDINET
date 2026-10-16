import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { MemoryRouter, Route, Routes, useLocation } from 'react-router-dom'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { AuthContext, type AuthContextValue } from '../auth/authContext'
import ApiDocsPage, { DocText } from './ApiDocsPage'
import type { DocEndpoint, DocSection, DocsIndex } from './apiDocsTypes'
import { curlExample, initialValues, javascriptExample, pythonExample, requestPath, sectionForPath } from './examples'

function endpoint(overrides: Partial<DocEndpoint>): DocEndpoint {
  return {
    id: 'get-api-example',
    method: 'GET',
    path: '/api/example',
    summary: 'An example.',
    details: [],
    access: 'member',
    access_label: 'Any signed-in user',
    read_only_token: true,
    parameters: [],
    body: null,
    response: { status: 200, content_type: 'application/json', type: 'object', fields: [], models: [], description: '' },
    try_it: true,
    streams: false,
    ...overrides,
  }
}

const PRICES = endpoint({
  id: 'get-api-security-price-history',
  path: '/api/security/price-history',
  summary: 'Daily prices of a ticker; `adjusted=true` adjusts for splits.',
  parameters: [
    { name: 'ticker', in: 'query', required: true, type: 'string', default: null, description: '', enum: null, example: '72030' },
    { name: 'start_date', in: 'query', required: false, type: 'string or null', default: null, description: '', enum: null, example: '2024-01-01' },
    { name: 'adjusted', in: 'query', required: false, type: 'boolean', default: false, description: '', enum: null, example: null },
  ],
})

const RUN = endpoint({
  id: 'post-api-screening-run',
  method: 'POST',
  path: '/api/screening/run',
  summary: 'Run a screening query on a worker and return the matching rows.',
  read_only_token: false,
  body: {
    content_type: 'application/json',
    required: true,
    model: 'ScreeningRunRequest',
    fields: [{ name: 'criteria_match', type: 'string', required: false, default: 'all', description: 'Whether all or any criteria terms must hold', enum: ['all', 'any'] }],
    models: [],
    example: { criteria: [{ table: 'Financial_Ratios', column: 'Return on Equity', operator: '>', value: 0.15 }], sort_order: 'DESC', include_names: true, limit: null },
    form: null,
  },
  response: { status: 200, content_type: 'application/json', type: 'object', fields: [], models: [], description: '`columns` and `rows`.' },
})

const DOWNLOAD = endpoint({
  id: 'get-api-backtesting-download-backtest-id',
  path: '/api/backtesting/download/{backtest_id}',
  parameters: [{ name: 'backtest_id', in: 'path', required: true, type: 'string', default: null, description: '', enum: null, example: 'bt 1/2' }],
  response: { status: 200, content_type: 'application/zip', type: null, fields: [], models: [], description: '' },
})

const DELETE_SAVED = endpoint({
  id: 'delete-api-screening-saved-identifier',
  method: 'DELETE',
  path: '/api/screening/saved/{identifier}',
  summary: 'Delete a saved screen.',
  read_only_token: false,
  try_it: false,
  parameters: [{ name: 'identifier', in: 'path', required: true, type: 'string', default: null, description: '', enum: null, example: 'Cheap and profitable' }],
  response: { status: 204, content_type: null, type: null, fields: [], models: [], description: '' },
})

const UPLOAD = endpoint({
  id: 'post-api-backtesting-run-csv',
  method: 'POST',
  path: '/api/backtesting/run-csv',
  read_only_token: false,
  try_it: false,
  body: { content_type: 'multipart/form-data', fields: [], models: [], example: null, form: { file: '@portfolio.csv', benchmark: 'TPX' } },
})

describe('example generation', () => {
  it('fills the path and query and keeps readable separators', () => {
    expect(requestPath(PRICES, initialValues(PRICES))).toBe('/api/security/price-history?ticker=72030&start_date=2024-01-01')
    expect(requestPath(DOWNLOAD, initialValues(DOWNLOAD))).toBe('/api/backtesting/download/bt%201%2F2')
    expect(requestPath(PRICES, { path: {}, query: { ticker: 'company:E02144,72030' }, body: '' })).toBe('/api/security/price-history?ticker=company:E02144,72030')
  })

  it('writes curl with the token header, a JSON body, and downloads to a file', () => {
    expect(curlExample(PRICES, initialValues(PRICES))).toBe('curl -s "$SHADE_URL/api/security/price-history?ticker=72030&start_date=2024-01-01" -H "Authorization: Bearer $SHADE_TOKEN"')
    const run = curlExample(RUN, initialValues(RUN))
    expect(run).toContain('curl -s "$SHADE_URL/api/screening/run" \\\n  -H "Authorization: Bearer $SHADE_TOKEN" \\\n  --json \'{')
    expect(run).not.toContain('-X POST')
    expect(curlExample(DOWNLOAD, initialValues(DOWNLOAD))).toContain('-o download.zip')
    expect(curlExample(DELETE_SAVED, initialValues(DELETE_SAVED))).toBe('curl -s -X DELETE "$SHADE_URL/api/screening/saved/Cheap%20and%20profitable" -H "Authorization: Bearer $SHADE_TOKEN"')
    expect(curlExample(UPLOAD, initialValues(UPLOAD))).toContain('-F "file=@portfolio.csv" \\\n  -F "benchmark=TPX"')
    // A quote in the body survives the shell.
    expect(curlExample(RUN, { path: {}, query: {}, body: '{"name": "Bob\'s screen"}' })).toContain(`--json '{"name": "Bob'\\''s screen"}'`)
  })

  it('writes Python with the shared call() helper and Python literals', () => {
    expect(pythonExample(PRICES, initialValues(PRICES))).toBe('result = call("GET", "/api/security/price-history", params={"ticker": "72030", "start_date": "2024-01-01"})\nprint(result)')
    const run = pythonExample(RUN, initialValues(RUN))
    expect(run).toContain('result = call("POST", "/api/screening/run", json={\n    "criteria": [\n        {\n            "table": "Financial_Ratios",')
    expect(run).toContain('"value": 0.15,')
    expect(run).toContain('"sort_order": "DESC",')
    expect(run).toContain('"include_names": True,')
    expect(run).toContain('"limit": None,')
    expect(pythonExample(PRICES, { path: {}, query: { ticker: '72030', adjusted: 'true', limit: '5' }, body: '' })).toContain('params={"ticker": "72030", "adjusted": True, "limit": "5"}')
    expect(pythonExample(DELETE_SAVED, initialValues(DELETE_SAVED))).toBe('call("DELETE", "/api/screening/saved/Cheap%20and%20profitable")')
    expect(pythonExample(DOWNLOAD, initialValues(DOWNLOAD))).toBe('response = api.request("GET", BASE + "/api/backtesting/download/bt%201%2F2")\nresponse.raise_for_status()\nopen("download.zip", "wb").write(response.content)')
    expect(pythonExample(UPLOAD, initialValues(UPLOAD))).toBe('result = call("POST", "/api/backtesting/run-csv", files={"file": open("portfolio.csv", "rb")}, data={"benchmark": "TPX"})\nprint(result)')
  })

  it('writes JavaScript with fetch, the token, and node imports only when needed', () => {
    const run = javascriptExample(RUN, initialValues(RUN))
    expect(run.startsWith('const { SHADE_URL, SHADE_TOKEN } = process.env')).toBe(true)
    expect(run).toContain("method: 'POST'")
    expect(run).toContain("'Content-Type': 'application/json'")
    expect(run).toContain('console.log(await response.json())')
    const download = javascriptExample(DOWNLOAD, initialValues(DOWNLOAD))
    expect(download.split('\n')[0]).toBe("import { writeFile } from 'node:fs/promises'")
    expect(download).toContain('await writeFile("download.zip"')
    expect(javascriptExample(UPLOAD, initialValues(UPLOAD)).split('\n')[0]).toBe("import { readFile } from 'node:fs/promises'")
    expect(javascriptExample(DELETE_SAVED, initialValues(DELETE_SAVED))).not.toContain('response.json()')
  })

  it('maps a workspace address to its docs section', () => {
    const sections = [
      { id: 'analysis', title: 'Analysis', screen_paths: ['/analyze', '/analyze/:companyCode'], summary: '', endpoint_count: 1 },
      { id: 'people', title: 'People', screen_paths: ['/people/:username'], summary: '', endpoint_count: 1 },
    ]
    expect(sectionForPath(sections, '/analyze/E02144')).toBe('analysis')
    expect(sectionForPath(sections, '/analyze/')).toBe('analysis')
    expect(sectionForPath(sections, '/people/bob')).toBe('people')
    expect(sectionForPath(sections, '/people/bob/extra')).toBeNull()
    expect(sectionForPath(sections, '/overview')).toBeNull()
  })
})

describe('DocText', () => {
  it('renders paragraphs, lists, code, and bold', () => {
    const { container } = render(<DocText text={'Send **one** request to `/api/x`.\n\n- first\n- second `y`'} />)
    expect(container.querySelector('p strong')?.textContent).toBe('one')
    expect(container.querySelector('p code')?.textContent).toBe('/api/x')
    expect([...container.querySelectorAll('li')].map(item => item.textContent)).toEqual(['first', 'second y'])
  })
})

const INDEX: DocsIndex = {
  auth_mode: 'accounts',
  sections: [
    { id: 'start', title: 'Getting started', screen_paths: [], summary: 'Tokens and requests.', endpoint_count: 0 },
    { id: 'screening', title: 'Screening', screen_paths: ['/screen'], summary: 'Find companies.', endpoint_count: 2 },
    { id: 'backtest', title: 'Backtest', screen_paths: ['/backtest'], summary: 'Test portfolios.', endpoint_count: 1 },
  ],
  python_setup: 'import requests\nBASE = "x"',
  openapi_url: '/api/docs/openapi.json',
  explorer_url: null,
}

function section(id: string, overrides: Partial<DocSection> = {}): DocSection {
  const summary = INDEX.sections.find(item => item.id === id)!
  return {
    ...summary,
    intro: `About ${summary.title}.`,
    guides: [],
    groups: [],
    related: [],
    auth_mode: 'accounts',
    python_setup: INDEX.python_setup,
    ...overrides,
  }
}

const SECTIONS: Record<string, DocSection> = {
  start: section('start', { guides: [{ title: 'Create a token', text: 'Open **Account**.\n\n- Copy it.', curl: 'curl -s "$SHADE_URL/api/auth/me"', python: 'print(call("GET", "/api/auth/me"))', response: '{"username": "you"}' }] }),
  screening: section('screening', {
    guides: [{ title: 'Run a screen', text: 'Post the criteria.', curl: 'curl screening', python: 'call("POST", "/api/screening/run")', response: '' }],
    groups: [{ title: 'Run', intro: 'Run screens.', endpoints: [RUN, DELETE_SAVED] }],
    related: [{ ...PRICES, section: 'analysis', section_title: 'Analysis' }],
  }),
  backtest: section('backtest', { groups: [{ title: 'Results', intro: '', endpoints: [DOWNLOAD] }] }),
}

const calls: Array<{ method: string; path: string; body?: string; auth: string | null }> = []
let authMode: 'accounts' | 'disabled' = 'accounts'

beforeEach(() => {
  calls.length = 0
  authMode = 'accounts'
  vi.stubGlobal('fetch', vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
    const path = String(input)
    const headers = new Headers(init?.headers)
    calls.push({ method: init?.method ?? 'GET', path, body: init?.body ? String(init.body) : undefined, auth: headers.get('Authorization') })
    const json = (body: unknown, status = 200) => Promise.resolve(new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } }))
    if (path === '/api/docs') return json({ ...INDEX, auth_mode: authMode, explorer_url: authMode === 'disabled' ? '/docs' : null })
    if (path === '/api/docs/openapi.json') return json({ openapi: '3.1.0', paths: {} })
    const match = /^\/api\/docs\/([a-z]+)$/.exec(path)
    if (match) return SECTIONS[match[1]] ? json({ ...SECTIONS[match[1]], auth_mode: authMode }) : json({ detail: 'Unknown section' }, 404)
    if (path === '/api/screening/run') return json({ columns: ['Company_Name'], rows: [['Toyota Motor Corporation']], row_count: 1, error: null })
    return json({})
  }))
  Object.assign(navigator, { clipboard: { writeText: vi.fn(() => Promise.resolve()) } })
})
afterEach(() => { cleanup(); vi.unstubAllGlobals() })

function Address() {
  const location = useLocation()
  return <output data-testid="address">{location.pathname}{location.hash}</output>
}

function renderDocs(entry: string) {
  const auth = { user: { user_id: 'u1', username: 'ana', role: 'member', status: 'active' }, status: { mode: authMode }, loading: false } as unknown as AuthContextValue
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return render(<QueryClientProvider client={client}><AuthContext.Provider value={auth}><MemoryRouter initialEntries={[entry]}>
    <Routes><Route path="/api-docs" element={<ApiDocsPage />} /><Route path="/api-docs/:sectionId" element={<ApiDocsPage />} /></Routes>
    <Address />
  </MemoryRouter></AuthContext.Provider></QueryClientProvider>)
}

describe('ApiDocsPage', () => {
  it('opens the docs of the screen it was called from', async () => {
    renderDocs('/api-docs?from=%2Fscreen')
    expect(await screen.findByRole('heading', { level: 1, name: 'Screening' })).toBeTruthy()
    expect(screen.getByTestId('address').textContent).toBe('/api-docs/screening')
    expect(screen.getByText('Run a screen')).toBeTruthy()
    expect(screen.getByRole('link', { name: 'Open the screen' }).getAttribute('href')).toBe('/screen')
    expect(screen.getByText(/create one on your Account page/)).toBeTruthy()
  })

  it('falls back to Getting started for a screen without docs', async () => {
    renderDocs('/api-docs?from=%2Fsomewhere')
    expect(await screen.findByRole('heading', { level: 1, name: 'Getting started' })).toBeTruthy()
    expect(screen.getByTestId('address').textContent).toBe('/api-docs/start')
    expect(screen.getByText('Server address')).toBeTruthy()
    expect(screen.getByRole('link', { name: /Create a token/ }).getAttribute('href')).toBe('/account#tokens')
    expect(screen.queryByRole('link', { name: /OpenAPI explorer/ })).toBeNull()
    expect(screen.getByRole('button', { name: /Download certificate/ })).toBeTruthy()
    vi.stubGlobal('URL', Object.assign(URL, { createObjectURL: vi.fn(() => 'blob:x'), revokeObjectURL: vi.fn() }))
    fireEvent.click(screen.getByRole('button', { name: /Download OpenAPI schema/ }))
    await waitFor(() => expect(calls.some(call => call.path === '/api/docs/openapi.json')).toBe(true))
  })

  it('says a section it cannot show is not there, without offering a retry', async () => {
    renderDocs('/api-docs/admin')
    const panel = (await screen.findByText('No API docs here')).closest('.state-panel') as HTMLElement
    expect(within(panel).getByRole('link', { name: 'Getting started' }).getAttribute('href')).toBe('/api-docs/start')
    expect(screen.queryByRole('button', { name: 'Try again' })).toBeNull()
  })

  it('shows endpoints with access marks, examples, and a working Try it', async () => {
    renderDocs('/api-docs/screening')
    const row = await screen.findByRole('button', { name: /POST \/api\/screening\/run/ })
    expect(within(row).queryByText('Read-only token')).toBeNull()
    expect(within(screen.getByRole('button', { name: /GET \/api\/security\/price-history/ })).getByText('Read-only token')).toBeTruthy()
    fireEvent.click(row)
    const curl = screen.getByLabelText('POST /api/screening/run (curl)')
    expect(curl.textContent).toContain('"$SHADE_URL/api/screening/run"')
    fireEvent.click(within(document.getElementById('post-api-screening-run')!).getByRole('tab', { name: /Python/ }))
    expect(screen.getByLabelText('POST /api/screening/run (python)').textContent).toContain('result = call("POST", "/api/screening/run", json={')
    fireEvent.click(screen.getByRole('button', { name: /^Send/ }))
    expect(await screen.findByText(/Toyota Motor Corporation/)).toBeTruthy()
    const sent = calls.find(call => call.path === '/api/screening/run')!
    expect(sent.method).toBe('POST')
    expect(JSON.parse(sent.body!).sort_order).toBe('DESC')
  })

  it('edits a parameter into the examples, and does not send calls that change data', async () => {
    renderDocs('/api-docs/screening')
    fireEvent.click(await screen.findByRole('button', { name: /DELETE \/api\/screening\/saved/ }))
    expect(screen.queryByRole('button', { name: /^Send/ })).toBeNull()
    expect(screen.getByText(/This call changes data/)).toBeTruthy()
    fireEvent.change(screen.getByRole('textbox', { name: /identifier/ }), { target: { value: 'Value' } })
    expect(screen.getByLabelText(/DELETE \/api\/screening\/saved\/\{identifier\} \(/).textContent).toContain('/api/screening/saved/Value"')
  })

  it('filters the endpoints and opens one named in the address', async () => {
    renderDocs('/api-docs/screening#delete-api-screening-saved-identifier')
    expect(await screen.findByText(/This call changes data/)).toBeTruthy()
    fireEvent.change(screen.getByPlaceholderText('Filter by path or description'), { target: { value: 'price' } })
    expect(screen.queryByRole('button', { name: /POST \/api\/screening\/run/ })).toBeNull()
    expect(screen.getByRole('button', { name: /GET \/api\/security\/price-history/ })).toBeTruthy()
    expect(screen.queryByText('Run a screen')).toBeNull()
    fireEvent.change(screen.getByPlaceholderText('Filter by path or description'), { target: { value: 'zzz' } })
    expect(screen.getByText(/No endpoint matches/)).toBeTruthy()
  })

  it('moves between screens and endpoints from the keyboard', async () => {
    renderDocs('/api-docs/screening')
    await screen.findByRole('heading', { level: 1, name: 'Screening' })
    fireEvent.keyDown(window, { key: 'j' })
    expect(document.activeElement?.id).toBe('toggle-post-api-screening-run')
    fireEvent.keyDown(window, { key: '2' })
    fireEvent.click(document.activeElement!)
    expect(screen.getByLabelText('POST /api/screening/run (python)')).toBeTruthy()
    fireEvent.keyDown(window, { key: ']' })
    expect(await screen.findByRole('heading', { level: 1, name: 'Backtest' })).toBeTruthy()
    expect(screen.getByTestId('address').textContent).toBe('/api-docs/backtest')
  })

  it('tells a server without sign-in that no token is needed', async () => {
    authMode = 'disabled'
    renderDocs('/api-docs/screening')
    expect(await screen.findByText(/runs without sign-in/)).toBeTruthy()
    expect(screen.queryByText(/create one on your Account page/)).toBeNull()
    fireEvent.click(screen.getByRole('link', { name: 'Getting started' }))
    expect((await screen.findByRole('link', { name: /OpenAPI explorer/ })).getAttribute('href')).toBe('/docs')
  })

  it('shows a download answer as a file to save', async () => {
    vi.stubGlobal('fetch', vi.fn((input: RequestInfo | URL) => {
      const path = String(input)
      const json = (body: unknown) => Promise.resolve(new Response(JSON.stringify(body), { headers: { 'Content-Type': 'application/json' } }))
      if (path === '/api/docs') return json(INDEX)
      if (path === '/api/docs/backtest') return json(SECTIONS.backtest)
      return Promise.resolve(new Response('PK zip bytes', { headers: { 'Content-Type': 'application/zip' } }))
    }))
    renderDocs('/api-docs/backtest')
    fireEvent.click(await screen.findByRole('button', { name: /GET \/api\/backtesting\/download/ }))
    fireEvent.click(screen.getByRole('button', { name: /^Send/ }))
    await waitFor(() => expect(screen.getByText(/12 bytes file/)).toBeTruthy())
    expect(screen.getByRole('button', { name: /Save it/ })).toBeTruthy()
  })
})
