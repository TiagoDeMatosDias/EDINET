import { useQuery } from '@tanstack/react-query'
import { Check, ChevronDown, ChevronRight, Copy, Download, ExternalLink, KeyRound, Play } from 'lucide-react'
import { Fragment, useEffect, useMemo, useRef, useState, type ReactNode } from 'react'
import { Link, useLocation, useNavigate, useParams, useSearchParams } from 'react-router-dom'

import { ApiError, apiRequest, authenticatedFetch } from '../../api/client'
import { downloadApiFile, downloadBlob } from '../../api/download'
import { EmptyState, ErrorState, LoadingState } from '../../components/Feedback'
import { HotkeyHelpButton } from '../../hotkeys/HotkeyHelpButton'
import { HotkeyKbd } from '../../hotkeys/HotkeyKbd'
import { useHotkeyScope } from '../../hotkeys/useHotkeyScope'
import { apiDocsScope, LANGUAGES } from './apiDocsHotkeys'
import type { DocEndpoint, DocField, DocGuide, DocModel, DocSection, DocsIndex, Language } from './apiDocsTypes'
import { downloadName, example, initialValues, requestPath, sectionForPath, type CallValues } from './examples'
import './apidocs.css'

const LANGUAGE_KEY = 'shade.api-docs.language'
const MAX_SHOWN = 60_000

function storedLanguage(): Language {
  try {
    const value = window.localStorage.getItem(LANGUAGE_KEY)
    if (value === 'curl' || value === 'python' || value === 'javascript') return value
  } catch {
    // Storage can be unavailable (private windows); curl is the default.
  }
  return 'curl'
}

/** Inline Markdown of the docs text: `code` and **bold**. */
function Inline({ text }: { text: string }) {
  const parts = text.split(/(`[^`]+`|\*\*[^*]+\*\*)/g)
  return <>{parts.map((part, index) => part.startsWith('`') && part.endsWith('`') && part.length > 1
    ? <code key={index}>{part.slice(1, -1)}</code>
    : part.startsWith('**') && part.endsWith('**') && part.length > 4 ? <strong key={index}>{part.slice(2, -2)}</strong> : <Fragment key={index}>{part}</Fragment>)}</>
}

/** The docs' Markdown subset: paragraphs and "- " lists. */
export function DocText({ text }: { text: string }) {
  if (!text) return null
  return <>{text.split(/\n\s*\n/).map((block, index) => {
    const lines = block.split('\n').filter(line => line.trim())
    if (lines.length && lines.every(line => line.trimStart().startsWith('- '))) {
      return <ul key={index}>{lines.map((line, item) => <li key={item}><Inline text={line.trimStart().slice(2)} /></li>)}</ul>
    }
    return <p key={index}><Inline text={lines.join(' ')} /></p>
  })}</>
}

function CopyButton({ text, label = 'Copy' }: { text: string; label?: string }) {
  const [copied, setCopied] = useState(false)
  useEffect(() => {
    if (!copied) return
    const timer = window.setTimeout(() => setCopied(false), 1500)
    return () => window.clearTimeout(timer)
  }, [copied])
  return <button type="button" className="apidocs-copy" onClick={() => { void navigator.clipboard?.writeText(text).then(() => setCopied(true), () => setCopied(false)) }}>
    {copied ? <Check aria-hidden="true" /> : <Copy aria-hidden="true" />}{copied ? 'Copied' : label}
  </button>
}

function CodeBlock({ code, label }: { code: string; label?: string }) {
  return <div className="apidocs-code"><CopyButton text={code} /><pre aria-label={label}><code>{code}</code></pre></div>
}

function LanguageTabs({ language, onChange, available = ['curl', 'python', 'javascript'] }: { language: Language; onChange: (language: Language) => void; available?: Language[] }) {
  return <div className="segmented apidocs-languages" role="tablist" aria-label="Example language">
    {LANGUAGES.filter(item => available.includes(item.id)).map(item => <button key={item.id} type="button" role="tab" aria-selected={language === item.id} className={language === item.id ? 'active' : undefined} onClick={() => onChange(item.id)}>
      {item.label} <HotkeyKbd hotkey={apiDocsScope.byId[`language-${item.id}`]} />
    </button>)}
  </div>
}

function GuideCard({ guide, language, onLanguage }: { guide: DocGuide; language: Language; onLanguage: (language: Language) => void }) {
  const available = (['curl', 'python'] as const).filter(item => (item === 'curl' ? guide.curl : guide.python))
  const shown: Language = available.includes(language as 'curl' | 'python') ? language : available[0] ?? 'curl'
  const code = shown === 'python' ? guide.python : guide.curl
  return <article className="apidocs-guide">
    <h3>{guide.title}</h3>
    <DocText text={guide.text} />
    {available.length > 0 && <>
      {available.length > 1 && <LanguageTabs language={shown} onChange={onLanguage} available={[...available]} />}
      <CodeBlock code={code} label={`${guide.title} (${shown})`} />
    </>}
    {guide.response && <details className="apidocs-response-example"><summary>Example answer</summary><pre><code>{guide.response}</code></pre></details>}
  </article>
}

function FieldTable({ fields, caption }: { fields: DocField[]; caption?: string }) {
  if (!fields.length) return null
  return <table className="apidocs-fields">
    {caption && <caption>{caption}</caption>}
    <thead><tr><th>Field</th><th>Type</th><th>Description</th></tr></thead>
    <tbody>{fields.map(field => <tr key={field.name}>
      <td><code>{field.name}</code>{field.required && <span className="apidocs-required" title="Required"> required</span>}</td>
      <td className="apidocs-type">{field.type}</td>
      <td>
        <Inline text={field.description} />
        {field.enum && field.enum.length > 0 && <span className="apidocs-enum"> One of {field.enum.map(item => <code key={String(item)}>{String(item)}</code>)}.</span>}
        {field.default !== null && field.default !== undefined && field.default !== '' && <span className="apidocs-default"> Default <code>{JSON.stringify(field.default)}</code>.</span>}
      </td>
    </tr>)}</tbody>
  </table>
}

function Models({ models }: { models: DocModel[] }) {
  if (!models.length) return null
  return <div className="apidocs-models">{models.map(model => <details key={model.name}><summary><code>{model.name}</code></summary><FieldTable fields={model.fields} /></details>)}</div>
}

interface TryResult {
  status: number
  ok: boolean
  milliseconds: number
  contentType: string
  text?: string
  blob?: Blob
}

function prettyBody(text: string, contentType: string) {
  if (!contentType.includes('json')) return text
  try {
    return JSON.stringify(JSON.parse(text), null, 2)
  } catch {
    return text
  }
}

function formatBytes(size: number) {
  return size >= 1_048_576 ? `${(size / 1_048_576).toFixed(1)} MB` : size >= 1024 ? `${(size / 1024).toFixed(1)} KB` : `${size} bytes`
}

function TryIt({ endpoint, values, onChange }: { endpoint: DocEndpoint; values: CallValues; onChange: (values: CallValues) => void }) {
  const [result, setResult] = useState<TryResult | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)
  const pathParameters = endpoint.parameters.filter(parameter => parameter.in === 'path')
  const queryParameters = endpoint.parameters.filter(parameter => parameter.in === 'query')
  const jsonBody = Boolean(endpoint.body && !endpoint.body.form)
  const send = async () => {
    setBusy(true)
    setError(null)
    const started = performance.now()
    try {
      const init: RequestInit = { method: endpoint.method }
      if (jsonBody && values.body.trim()) {
        JSON.parse(values.body)
        init.body = values.body
      }
      const response = await authenticatedFetch(requestPath(endpoint, values), init)
      const contentType = response.headers.get('content-type') ?? ''
      const textual = !contentType || contentType.includes('json') || contentType.startsWith('text/plain')
      const next: TryResult = { status: response.status, ok: response.ok, milliseconds: Math.round(performance.now() - started), contentType }
      if (textual) next.text = await response.text()
      else next.blob = await response.blob()
      setResult(next)
    } catch (err) {
      setResult(null)
      setError(err instanceof SyntaxError ? 'The body is not valid JSON.' : err instanceof Error ? err.message : String(err))
    } finally {
      setBusy(false)
    }
  }
  const set = (kind: 'path' | 'query', name: string, value: string) => onChange({ ...values, [kind]: { ...values[kind], [name]: value } })
  const shown = result?.text !== undefined ? prettyBody(result.text, result.contentType) : ''
  return <div className="apidocs-try">
    {(pathParameters.length > 0 || queryParameters.length > 0) && <div className="apidocs-try__fields">
      {[...pathParameters, ...queryParameters].map(parameter => <label key={`${parameter.in}-${parameter.name}`} className="field">
        <span>{parameter.name} <small>{parameter.in}{parameter.required ? ', required' : ''} · {parameter.type}</small></span>
        <input className="input" value={(parameter.in === 'path' ? values.path : values.query)[parameter.name] ?? ''} placeholder={parameter.default !== null && parameter.default !== undefined ? `default ${String(parameter.default)}` : ''} onChange={event => set(parameter.in === 'path' ? 'path' : 'query', parameter.name, event.target.value)} />
        {parameter.description && <small><Inline text={parameter.description} /></small>}
      </label>)}
    </div>}
    {jsonBody && <label className="field apidocs-try__body"><span>Request body <small>JSON</small></span>
      <textarea className="input" spellCheck={false} rows={Math.min(16, Math.max(3, values.body.split('\n').length))} value={values.body} onChange={event => onChange({ ...values, body: event.target.value })} />
    </label>}
    <div className="apidocs-try__actions">
      {endpoint.try_it && !endpoint.streams
        ? <button id={`send-${endpoint.id}`} type="button" className="button button--primary button--small" disabled={busy} onClick={() => void send()}><Play aria-hidden="true" />{busy ? 'Sending…' : 'Send'} <HotkeyKbd hotkey={apiDocsScope.byId.send} /></button>
        : <small className="muted">{endpoint.streams ? 'This endpoint streams its answer; try it from a client.' : 'This call changes data, so the page does not send it. The examples above use the values you enter here.'}</small>}
      <button type="button" className="text-button" onClick={() => { onChange(initialValues(endpoint)); setResult(null); setError(null) }}>Reset values</button>
    </div>
    {error && <p className="form-error" role="alert">{error}</p>}
    {result && <div className="apidocs-result" role="status">
      <p className={result.ok ? 'apidocs-result__status' : 'apidocs-result__status apidocs-result__status--error'}><strong>{result.status}</strong> · {result.milliseconds} ms · {result.contentType || 'no content'}</p>
      {result.blob ? <p>{formatBytes(result.blob.size)} file. <button type="button" className="text-button" onClick={() => downloadBlob(downloadName(endpoint) ?? 'download', result.blob!)}><Download aria-hidden="true" />Save it</button></p>
        : shown ? <pre><code>{shown.length > MAX_SHOWN ? `${shown.slice(0, MAX_SHOWN)}\n… (${formatBytes(shown.length)} in all; showing the start)` : shown}</code></pre> : null}
    </div>}
  </div>
}

const ACCESS_BADGES: Record<string, string> = { public: 'No token', operator: 'Operators', admin: 'Administrators' }

function EndpointRow({ endpoint, open, active, values, language, onToggle, onFocus, onValues, onLanguage, related }: {
  endpoint: DocEndpoint
  open: boolean
  active: boolean
  values: CallValues
  language: Language
  onToggle: () => void
  onFocus: () => void
  onValues: (values: CallValues) => void
  onLanguage: (language: Language) => void
  related?: { section: string; title: string }
}) {
  const code = example(language, endpoint, values)
  return <article id={endpoint.id} className={`apidocs-endpoint${open ? ' apidocs-endpoint--open' : ''}${active ? ' is-cursor' : ''}`}>
    <button type="button" id={`toggle-${endpoint.id}`} className="apidocs-endpoint__head" aria-expanded={open} onClick={onToggle} onFocus={onFocus}>
      {open ? <ChevronDown aria-hidden="true" /> : <ChevronRight aria-hidden="true" />}
      <span className={`apidocs-method apidocs-method--${endpoint.method.toLowerCase()}`}>{endpoint.method}</span>{' '}
      <code className="apidocs-path">{endpoint.path}</code>{' '}
      <span className="apidocs-summary"><Inline text={endpoint.summary} /></span>{' '}
      <span className="apidocs-badges">
        {ACCESS_BADGES[endpoint.access] && <span className={`badge apidocs-access apidocs-access--${endpoint.access}`}>{ACCESS_BADGES[endpoint.access]}</span>}
        {endpoint.read_only_token && endpoint.access !== 'public' && <span className="badge apidocs-readonly" title="A read-only token can call this">Read-only token</span>}
      </span>
    </button>
    {open && <div className="apidocs-endpoint__body">
      {related && <p className="apidocs-related-note">Documented under <Link to={`/api-docs/${related.section}#${endpoint.id}`}>{related.title}</Link>.</p>}
      {endpoint.details.map((detail, index) => <p key={index}><Inline text={detail} /></p>)}
      <dl className="apidocs-facts">
        <div><dt>Who</dt><dd>{endpoint.access_label}</dd></div>
        <div><dt>Read-only token</dt><dd>{endpoint.read_only_token ? 'Yes' : 'No, needs full access'}</dd></div>
        <div><dt>Answers</dt><dd>{endpoint.response.status} {endpoint.response.content_type ?? ''}{endpoint.response.type ? ` · ${endpoint.response.type}` : ''}</dd></div>
      </dl>
      {endpoint.body && <section className="apidocs-part">
        <h4>Request body <small>{endpoint.body.content_type}{endpoint.body.model ? ` · ${endpoint.body.model}` : ''}</small></h4>
        {endpoint.body.form && <p>Form fields: {Object.entries(endpoint.body.form).map(([name, value]) => <code key={name}>{name}{value.startsWith('@') ? ' (file)' : ''}</code>)}</p>}
        <FieldTable fields={endpoint.body.fields} />
        <Models models={endpoint.body.models} />
      </section>}
      <section className="apidocs-part">
        <h4>Response</h4>
        {endpoint.response.description && <p><Inline text={endpoint.response.description} /></p>}
        <FieldTable fields={endpoint.response.fields} />
        <Models models={endpoint.response.models} />
        {!endpoint.response.description && !endpoint.response.fields.length && <p className="muted">{endpoint.response.status === 204 ? 'No content.' : 'JSON; send it to see the shape.'}</p>}
      </section>
      <section className="apidocs-part">
        <div className="apidocs-part__head"><h4>Example</h4><LanguageTabs language={language} onChange={onLanguage} /></div>
        <CodeBlock code={code} label={`${endpoint.method} ${endpoint.path} (${language})`} />
      </section>
      <section className="apidocs-part">
        <h4>{endpoint.try_it && !endpoint.streams ? 'Try it' : 'Values'}</h4>
        <TryIt endpoint={endpoint} values={values} onChange={onValues} />
      </section>
    </div>}
  </article>
}

function matches(endpoint: DocEndpoint, filter: string) {
  if (!filter) return true
  const needle = filter.toLowerCase()
  return `${endpoint.method} ${endpoint.path} ${endpoint.summary}`.toLowerCase().includes(needle)
}

function ServerFacts({ index }: { index: DocsIndex }) {
  const origin = window.location.origin
  const [error, setError] = useState<string | null>(null)
  const download = (path: string, filename: string) => {
    setError(null)
    downloadApiFile(path, filename).catch((err: unknown) => setError(err instanceof Error ? err.message : String(err)))
  }
  return <div className="apidocs-server">
    <dl className="apidocs-facts">
      <div><dt>Server address</dt><dd><code>{origin}</code> <CopyButton text={`export SHADE_URL=${origin}`} label="Copy export" /></dd></div>
      <div><dt>Sign-in</dt><dd>{index.auth_mode === 'accounts' ? 'Every request needs a token' : 'Not required on this server'}</dd></div>
    </dl>
    <div className="apidocs-server__actions">
      {index.auth_mode === 'accounts' && <Link className="button button--primary button--small" to="/account#tokens"><KeyRound aria-hidden="true" />Create a token</Link>}
      <button type="button" className="button button--secondary button--small" onClick={() => download('/api/docs/certificate', 'shade.pem')}><Download aria-hidden="true" />Download certificate</button>
      {index.openapi_url && <button type="button" className="button button--secondary button--small" onClick={() => download(index.openapi_url!, 'shade-openapi.json')}><Download aria-hidden="true" />Download OpenAPI schema</button>}
      {index.explorer_url && <a className="button button--ghost button--small" href={index.explorer_url} target="_blank" rel="noreferrer"><ExternalLink aria-hidden="true" />OpenAPI explorer</a>}
    </div>
    {error && <p className="form-error" role="alert">{error}</p>}
  </div>
}

export default function ApiDocsPage() {
  const { sectionId } = useParams()
  const [params] = useSearchParams()
  const navigate = useNavigate()
  const location = useLocation()
  const [language, setLanguageState] = useState<Language>(storedLanguage)
  const [filter, setFilter] = useState('')
  const [expanded, setExpanded] = useState<Set<string>>(new Set())
  const [edits, setEdits] = useState<Record<string, CallValues>>({})
  const [cursor, setCursor] = useState(-1)
  const filterRef = useRef<HTMLInputElement>(null)
  const index = useQuery({ queryKey: ['api-docs'], queryFn: () => apiRequest<DocsIndex>('/api/docs'), staleTime: Infinity })
  const from = params.get('from')
  const resolved = sectionId ?? (from && index.data ? sectionForPath(index.data.sections, from) ?? 'start' : from ? undefined : 'start')
  const section = useQuery({ queryKey: ['api-docs', resolved], queryFn: () => apiRequest<DocSection>(`/api/docs/${encodeURIComponent(resolved!)}`), enabled: Boolean(resolved), staleTime: Infinity })

  // ``?from=/screen`` (the top bar's link) settles on that screen's own address.
  useEffect(() => {
    if (!sectionId && resolved) navigate(`/api-docs/${resolved}`, { replace: true })
  }, [sectionId, resolved, navigate])

  const setLanguage = (next: Language) => {
    setLanguageState(next)
    try { window.localStorage.setItem(LANGUAGE_KEY, next) } catch { /* the choice just is not remembered */ }
  }

  const data = section.data
  const related = useMemo(() => data?.related ?? [], [data])
  const visible = useMemo(() => [
    ...(data?.groups ?? []).flatMap(group => group.endpoints.filter(endpoint => matches(endpoint, filter))),
    ...related.filter(endpoint => matches(endpoint, filter)),
  ], [data, related, filter])

  // Open an endpoint named in the address (``#post-api-screening-run``), once
  // per section and address rather than on every filter keystroke.
  const hash = location.hash.replace('#', '')
  const hashKey = data && hash ? `${data.id}#${hash}` : ''
  const [openedFor, setOpenedFor] = useState('')
  if (hashKey !== openedFor) {
    setOpenedFor(hashKey)
    const position = visible.findIndex(endpoint => endpoint.id === hash)
    if (hashKey && position >= 0) {
      setExpanded(current => new Set(current).add(hash))
      setCursor(position)
    }
  }
  useEffect(() => {
    if (!hashKey) return
    const timer = window.setTimeout(() => document.getElementById(hashKey.split('#')[1])?.scrollIntoView?.({ block: 'start' }), 50)
    return () => window.clearTimeout(timer)
  }, [hashKey])

  const valuesFor = (endpoint: DocEndpoint) => edits[endpoint.id] ?? initialValues(endpoint)
  const toggle = (id: string) => setExpanded(current => {
    const next = new Set(current)
    if (next.has(id)) next.delete(id)
    else next.add(id)
    return next
  })
  const moveTo = (position: number) => {
    const endpoint = visible[position]
    if (!endpoint) return
    setCursor(position)
    const button = document.getElementById(`toggle-${endpoint.id}`)
    button?.focus({ preventScroll: true })
    button?.scrollIntoView?.({ block: 'nearest' })
  }
  const step = (delta: number) => {
    const sections = index.data?.sections ?? []
    const position = sections.findIndex(item => item.id === resolved)
    const next = sections[position + delta]
    if (next) { setFilter(''); setExpanded(new Set()); setCursor(-1); navigate(`/api-docs/${next.id}`) }
  }
  const current = cursor >= 0 ? visible[cursor] : undefined
  useHotkeyScope(apiDocsScope, {
    'previous-section': () => step(-1),
    'next-section': () => step(1),
    next: () => moveTo(Math.min(visible.length - 1, cursor + 1)),
    previous: () => moveTo(Math.max(0, cursor - 1)),
    filter: () => filterRef.current?.focus(),
    'language-curl': () => setLanguage('curl'),
    'language-python': () => setLanguage('python'),
    'language-javascript': () => setLanguage('javascript'),
    copy: () => { if (current) void navigator.clipboard?.writeText(example(language, current, valuesFor(current))) },
    send: () => {
      if (!current?.try_it) return
      setExpanded(set => new Set(set).add(current.id))
      window.setTimeout(() => document.getElementById(`send-${current.id}`)?.click(), 0)
    },
  })

  const sections = index.data?.sections ?? []
  const nav = <nav className="apidocs-nav" aria-label="API docs by screen">
    <select className="select apidocs-nav__select" aria-label="Screen" value={resolved ?? ''} onChange={event => navigate(`/api-docs/${event.target.value}`)}>
      {sections.map(item => <option key={item.id} value={item.id}>{item.title}</option>)}
    </select>
    <ul>{sections.map(item => <li key={item.id}><Link to={`/api-docs/${item.id}`} className={item.id === resolved ? 'active' : undefined} aria-current={item.id === resolved ? 'page' : undefined}>
      <span>{item.title}</span>{item.endpoint_count > 0 && <small>{item.endpoint_count}</small>}
    </Link></li>)}</ul>
  </nav>

  let body: ReactNode
  if (index.isError) body = <ErrorState error={index.error} retry={() => void index.refetch()} />
  // Unknown sections and sections for a higher role answer the same 404.
  else if (section.error instanceof ApiError && section.error.status === 404) body = <EmptyState title="No API docs here" description="There is no documentation at this address for your account." action={<Link className="button button--secondary button--small" to="/api-docs/start">Getting started</Link>} />
  else if (section.isError) body = <ErrorState error={section.error} retry={() => void section.refetch()} />
  else if (!data || !index.data) body = <LoadingState label="Loading the API docs" />
  else {
    const rowProps = (endpoint: DocEndpoint) => ({
      endpoint,
      open: expanded.has(endpoint.id),
      active: current?.id === endpoint.id,
      values: valuesFor(endpoint),
      language,
      onToggle: () => toggle(endpoint.id),
      onFocus: () => setCursor(visible.findIndex(item => item.id === endpoint.id)),
      onValues: (values: CallValues) => setEdits(all => ({ ...all, [endpoint.id]: values })),
      onLanguage: setLanguage,
    })
    const groups = data.groups.map(group => ({ ...group, endpoints: group.endpoints.filter(endpoint => matches(endpoint, filter)) })).filter(group => group.endpoints.length)
    const relatedShown = related.filter(endpoint => matches(endpoint, filter))
    body = <>
      {data.intro && <div className="apidocs-intro"><DocText text={data.intro} /></div>}
      {data.id === 'start' && <ServerFacts index={index.data} />}
      {data.auth_mode === 'disabled'
        ? <p className="callout callout--info">This server runs without sign-in, so it answers requests from this computer without a token; the <code>Authorization</code> header in the examples is ignored (set <code>SHADE_TOKEN</code> to anything).</p>
        : data.id !== 'start' && <p className="callout callout--info">Every request needs a personal API token: <Link to="/account#tokens">create one on your Account page</Link>, then follow <Link to="/api-docs/start">Getting started</Link>.</p>}
      {!filter && data.guides.length > 0 && <section className="apidocs-guides" aria-label="Guides">
        <h2>{data.id === 'start' ? 'Getting started' : 'Guides'}</h2>
        <details className="apidocs-setup" open={data.id === 'start'}>
          <summary>Python setup: every Python example starts with this</summary>
          <CodeBlock code={data.python_setup} label="Python setup" />
        </details>
        {data.guides.map(guide => <GuideCard key={guide.title} guide={guide} language={language} onLanguage={setLanguage} />)}
      </section>}
      <section className="apidocs-reference" aria-label="Endpoints">
        <div className="apidocs-reference__head">
          <h2>Endpoints</h2>
          <label className="apidocs-filter"><span className="sr-only">Filter endpoints</span>
            <input ref={filterRef} className="input" type="search" placeholder="Filter by path or description" value={filter} onChange={event => { setFilter(event.target.value); setCursor(-1) }} />
            <HotkeyKbd hotkey={apiDocsScope.byId.filter} />
          </label>
        </div>
        {groups.map(group => <section key={group.title} className="apidocs-group">
          <h3>{group.title}</h3>
          {group.intro && <p>{group.intro}</p>}
          {group.endpoints.map(endpoint => <EndpointRow key={endpoint.id} {...rowProps(endpoint)} />)}
        </section>)}
        {relatedShown.length > 0 && <section className="apidocs-group">
          <h3>Also used on this screen</h3>
          <p>Documented with other screens.</p>
          {relatedShown.map(endpoint => <EndpointRow key={`related-${endpoint.id}`} {...rowProps(endpoint)} related={{ section: endpoint.section, title: endpoint.section_title }} />)}
        </section>}
        {!groups.length && !relatedShown.length && <p className="muted">No endpoint matches “{filter}”.</p>}
      </section>
    </>
  }

  const title = data?.title ?? 'API'
  return <div className="apidocs-page dense-page">
    <header className="page-header">
      <div><span className="eyebrow">API docs</span><h1>{title}</h1><p>{data?.summary ?? 'Use everything the workspace does from your own scripts and apps.'}</p></div>
      <div className="page-actions">
        {data && data.screen_paths.length > 0 && !data.screen_paths[0].includes(':') && <Link className="button button--secondary button--small" to={data.screen_paths[0]}>Open the screen</Link>}
        <HotkeyHelpButton />
      </div>
    </header>
    <div className="apidocs-layout">
      {nav}
      <div className="apidocs-main">{body}</div>
    </div>
  </div>
}
