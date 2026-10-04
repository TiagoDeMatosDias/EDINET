import { useQuery } from '@tanstack/react-query'
import { ArrowRight, Keyboard } from 'lucide-react'
import { useCallback, useState, type ReactNode } from 'react'
import { Link, useNavigate } from 'react-router-dom'

import { apiRequest } from '../../api/client'
import type { Job } from '../../api/types'
import { ErrorState, LoadingState } from '../../components/Feedback'
import { ShortcutsDialog, type ShortcutGroup } from '../../components/ShortcutsDialog'
import { useSystemStatus } from '../../hooks/useHealth'
import { useHotkeys } from '../../hooks/useHotkeys'
import { useAuth } from '../auth/authContext'
import { chatApi, chatKeys, useChatUnread } from '../chat/chatApi'
import { colorFor, messageTime, personName, relativeTime } from '../chat/chatModel'
import { recentWorkTitle } from './recentWork'
import './overview.css'

interface RecentWorkItem {
  work_id: string
  kind: string
  title: string
  subtitle?: string | null
  href: string
  details_json?: string | null
  occurred_at: string
}

interface OverviewData {
  today: string
  portfolio: null | {
    currency: string
    valuation_date: string
    first_date: string | null
    total_value: number
    cash: number | null
    day_return: number | null
    ytd_return: number | null
    total_return: number | null
    holdings_count: number
    top_holdings: Array<{ symbol: string; asset_category: string; value: number | null; weight: number | null; currency: string }>
  }
  research: {
    followed: number
    alerts: number
    notes: number
    reviews_due: Array<{ edinet_code: string; review_on: string; thesis_status: string | null }>
    recent_notes: Array<{ note_id: string; title: string; edinet_code: string | null; updated_at: string }>
    theses: Record<string, number>
  }
  data: {
    latest_price_date: string | null
    priced_securities: number | null
    filings: null | { unique_filings: number; unique_companies: number; last_submitted: string | null; first_submitted: string | null; filings_with_issues: number }
  }
}

const EMPTY_RESEARCH: OverviewData['research'] = { followed: 0, alerts: 0, notes: 0, reviews_due: [], recent_notes: [], theses: {} }

const KIND_LABELS: Record<string, string> = { screen: 'Screen', company: 'Company', comparison: 'Compare', filing: 'Filing', backtest: 'Backtest' }

const SHORTCUTS: ShortcutGroup[] = [
  { title: 'Overview', shortcuts: [
    { keys: ['J', 'K'], label: 'Next or previous item in any panel' },
    { keys: ['[', ']'], label: 'Previous or next panel' },
    { keys: ['Enter'], label: 'Open the item' },
    { keys: ['1', '2', '3', '4', '5', '6'], label: 'Portfolio, Research, Chat, Recent work, Data, Start' },
    { keys: ['?'], label: 'Show or hide this list' },
  ] },
  { title: 'Start something', shortcuts: [
    { keys: ['N'], label: 'New screen' },
    { keys: ['/'], label: 'Analyze a company (search)' },
    { keys: ['T'], label: 'Test an idea in Backtest' },
    { keys: ['U'], label: 'Read unread chat' },
  ] },
]

const money = new Intl.NumberFormat('en-GB', { maximumFractionDigits: 0 })
const dayFormat = new Intl.DateTimeFormat('en-GB', { day: 'numeric', month: 'short', year: 'numeric' })

function percent(value: number | null | undefined, digits = 1) {
  if (value == null || !Number.isFinite(value)) return '—'
  const text = Math.abs(value * 100).toFixed(digits)
  return `${value > 0 ? '+' : value < 0 ? '−' : ''}${text}%`
}

function day(value: string | null | undefined) {
  if (!value) return '—'
  const parsed = new Date(value.length <= 10 ? `${value}T00:00:00` : value.replace(' ', 'T'))
  return Number.isNaN(parsed.getTime()) ? value : dayFormat.format(parsed)
}

function tone(value: number | null | undefined) {
  return value == null ? undefined : value < 0 ? 'is-down' : value > 0 ? 'is-up' : undefined
}

/** Move focus through the items of every panel, or jump between panels. */
function moveFocus(step: number, byPanel = false) {
  const panels = [...document.querySelectorAll<HTMLElement>('.ov-panel')]
  const items = [...document.querySelectorAll<HTMLElement>('.ov-panel .ov-item')]
  const active = document.activeElement as HTMLElement | null
  if (byPanel) {
    const current = panels.findIndex(panel => panel.contains(active))
    const next = panels[(current + step + panels.length) % panels.length]
    ;(next?.querySelector<HTMLElement>('.ov-item') ?? next?.querySelector<HTMLElement>('h2'))?.focus()
    return
  }
  const index = items.indexOf(active as HTMLElement)
  const next = items[index < 0 ? (step > 0 ? 0 : items.length - 1) : Math.max(0, Math.min(items.length - 1, index + step))]
  next?.focus()
  next?.scrollIntoView?.({ block: 'nearest' })
}

function focusPanel(index: number) {
  const panel = document.querySelectorAll<HTMLElement>('.ov-panel')[index]
  ;(panel?.querySelector<HTMLElement>('.ov-item') ?? panel?.querySelector<HTMLElement>('h2'))?.focus()
}

function Panel({ index, title, link, meta, children, className = '' }: { index: number; title: string; link?: { to: string; label: string }; meta?: ReactNode; children: ReactNode; className?: string }) {
  return <section className={`ov-panel ${className}`} aria-labelledby={`ov-panel-${index}`}>
    <header>
      <kbd aria-hidden="true">{index}</kbd>
      <h2 id={`ov-panel-${index}`} tabIndex={-1}>{title}</h2>
      {meta && <span className="ov-panel__meta">{meta}</span>}
      {link && <Link className="ov-panel__link" to={link.to}>{link.label}<ArrowRight aria-hidden="true" /></Link>}
    </header>
    {children}
  </section>
}

function Kpi({ label, value, detail, className }: { label: string; value: ReactNode; detail?: ReactNode; className?: string }) {
  return <div className={`ov-kpi ${className ?? ''}`}><span>{label}</span><strong>{value}</strong>{detail && <small>{detail}</small>}</div>
}

export default function OverviewPage() {
  const auth = useAuth()
  const navigate = useNavigate()
  const isAdmin = auth.user?.role === 'admin' || auth.user?.role === 'operator'
  const [help, setHelp] = useState(false)
  const closeHelp = useCallback(() => setHelp(false), [])
  const overview = useQuery({ queryKey: ['overview'], queryFn: () => apiRequest<OverviewData>('/api/overview'), refetchInterval: 120_000 })
  const recent = useQuery({ queryKey: ['recent-work'], queryFn: () => apiRequest<{ items: RecentWorkItem[] }>('/api/research/recent-work?limit=50') })
  const feed = useQuery({ queryKey: chatKeys.feed, queryFn: () => chatApi.feed(), retry: false, staleTime: 30_000 })
  const unread = useChatUnread()
  const status = useSystemStatus(isAdmin)
  const jobs = useQuery({ queryKey: ['jobs'], queryFn: () => apiRequest<Job[]>('/api/jobs?limit=8'), enabled: isAdmin, retry: false })

  useHotkeys({
    j: () => moveFocus(1),
    k: () => moveFocus(-1),
    ']': () => moveFocus(1, true),
    '[': () => moveFocus(-1, true),
    ...Object.fromEntries([1, 2, 3, 4, 5, 6].map(index => [String(index), () => focusPanel(index - 1)])),
    n: () => navigate('/screen'),
    t: () => navigate('/backtest'),
    u: () => navigate('/chat'),
    '?': () => setHelp(true),
  }, !help)

  if (overview.isLoading) return <LoadingState label="Loading your overview" />
  if (overview.isError || !overview.data) return <ErrorState error={overview.error} retry={() => overview.refetch()} />
  const portfolio = overview.data.portfolio ?? null
  const research = { ...EMPTY_RESEARCH, ...overview.data.research }
  const data: OverviewData['data'] = overview.data.data ?? { latest_price_date: null, priced_securities: null, filings: null }
  const items = recent.data?.items ?? []
  const messages = (feed.data?.messages ?? []).filter(message => message.kind === 'message' && !message.deleted).slice(-8).reverse()
  const thesisText = Object.entries(research.theses).map(([key, count]) => `${count} ${key}`).join(' · ')

  return <div className="ov-page">
    <header className="ov-head">
      <div><span className="eyebrow">Research workspace</span><h1>Overview</h1></div>
      <p>{day(overview.data.today)} · {auth.user ? `signed in as ${auth.user.username}` : 'local workspace'}</p>
      <nav className="ov-actions" aria-label="Start something">
        <Link className="button button--primary button--small" to="/screen">Find companies <kbd>N</kbd></Link>
        <button type="button" className="button button--secondary button--small" onClick={() => document.querySelector<HTMLInputElement>('.global-search input')?.focus()}>Analyze <kbd>/</kbd></button>
        <Link className="button button--secondary button--small" to="/backtest">Test an idea <kbd>T</kbd></Link>
        <Link className="button button--secondary button--small" to="/compare">Compare</Link>
        {auth.user?.role === 'admin' && <Link className="button button--secondary button--small" to="/pipeline">Refresh data</Link>}
        <button type="button" className="icon-button" onClick={() => setHelp(true)} title="Keyboard shortcuts (?)" aria-label="Keyboard shortcuts"><Keyboard /></button>
      </nav>
    </header>

    <div className="ov-kpis">
      <Kpi label="Portfolio" value={portfolio ? `€${money.format(portfolio.total_value)}` : '—'} detail={portfolio ? `as of ${day(portfolio.valuation_date)}` : 'Import activity in Portfolio'} />
      <Kpi label="Day" value={percent(portfolio?.day_return, 2)} className={tone(portfolio?.day_return)} />
      <Kpi label="Year to date" value={percent(portfolio?.ytd_return)} className={tone(portfolio?.ytd_return)} />
      <Kpi label="Since start" value={percent(portfolio?.total_return)} className={tone(portfolio?.total_return)} detail={portfolio?.first_date ? `from ${day(portfolio.first_date)}` : undefined} />
      <Kpi label="Holdings" value={portfolio?.holdings_count ?? '—'} detail={portfolio?.cash != null ? `€${money.format(portfolio.cash)} cash` : undefined} />
      <Kpi label="Followed" value={research.followed} detail={thesisText || `${research.notes} notes`} />
      <Kpi label="Reviews due" value={research.reviews_due.length} className={research.reviews_due.length ? 'is-down' : undefined} detail={`${research.alerts} alerts set`} />
      <Kpi label="Unread chat" value={unread.data?.total ?? '—'} className={unread.data?.total ? 'is-down' : undefined} detail={unread.data?.invitations ? `${unread.data.invitations} invitations` : 'channels and messages'} />
      {isAdmin && <Kpi label="Pipeline" value={status.data ? `${status.data.jobs.active} active` : '—'} detail={status.data ? `${status.data.jobs.queue_depth} queued · v${status.data.version}` : 'checking'} />}
    </div>

    <div className="ov-grid">
      <Panel index={1} title="Portfolio" link={{ to: '/portfolio', label: 'Open' }} meta={portfolio ? `top ${portfolio.top_holdings.length} of ${portfolio.holdings_count}` : undefined}>
        {portfolio?.top_holdings.length ? <table className="ov-table">
          <thead><tr><th>Holding</th><th className="num">Weight</th><th className="num">Value €</th></tr></thead>
          <tbody>{portfolio.top_holdings.map(item => <tr key={`${item.symbol}-${item.asset_category}`}>
            <td><Link className="ov-item" to={`/analyze?ticker=${encodeURIComponent(item.symbol)}&from=portfolio`}>{item.symbol}</Link><small>{item.currency}</small></td>
            <td className="num"><span className="ov-bar" style={{ width: `${Math.max(2, (item.weight ?? 0) * 100)}%` }} />{percent(item.weight).replace('+', '')}</td>
            <td className="num">{item.value != null ? money.format(item.value) : '—'}</td>
          </tr>)}</tbody>
        </table> : <p className="ov-empty">No holdings yet. <Link className="ov-item" to="/portfolio">Import an IBKR Flex Query or add transactions</Link>.</p>}
      </Panel>

      <Panel index={2} title="Research" link={{ to: '/research', label: 'Open' }} meta={`${research.followed} companies · ${research.notes} notes`}>
        <h3>Reviews due</h3>
        {research.reviews_due.length ? <ul className="ov-list">{research.reviews_due.map(item => <li key={item.edinet_code}>
          <Link className="ov-item" to={`/research?company=${encodeURIComponent(item.edinet_code)}`}>{item.edinet_code}</Link><small>{item.thesis_status ?? 'no status'}</small><time>{day(item.review_on)}</time>
        </li>)}</ul> : <p className="ov-empty">Nothing due. Set review dates on a company’s research.</p>}
        <h3>Latest notes</h3>
        {research.recent_notes.length ? <ul className="ov-list">{research.recent_notes.map(note => <li key={note.note_id}>
          <Link className="ov-item" to={`/research?tab=notes${note.edinet_code ? `&company=${encodeURIComponent(note.edinet_code)}` : ''}`}>{note.title}</Link><small>{note.edinet_code ?? ''}</small><time>{relativeTime(note.updated_at)}</time>
        </li>)}</ul> : <p className="ov-empty">No notes yet.</p>}
      </Panel>

      <Panel index={3} title="Chat" link={{ to: '/chat', label: 'Open' }} meta={unread.data ? `${unread.data.total} unread` : undefined}>
        {messages.length ? <ul className="ov-list ov-list--chat">{messages.map(message => <li key={message.message_id}>
          <span className="chat-dot" style={{ background: colorFor(message.channel_id ?? message.conversation_id ?? '') }} />
          <Link className="ov-item" to={message.channel_id ? `/chat?c=${encodeURIComponent(message.channel_id)}` : `/chat?c=conv:${message.conversation_id}`}>
            <strong>{personName(message.sender)}</strong> {message.body?.text ?? (message.encrypted ? '🔒 encrypted message' : '')}
          </Link>
          <time>{messageTime(message.created_at)}</time>
        </li>)}</ul> : <p className="ov-empty">Subscribe to channels in <Link className="ov-item" to="/chat">Chat</Link> to see their latest messages here.</p>}
      </Panel>

      <Panel index={4} title="Recent work" meta="saved to your account" className="ov-panel--wide">
        {recent.isLoading ? <LoadingState label="Loading recent work" /> : items.length ? <ul className="ov-list ov-list--work">{items.slice(0, 16).map(item => <li key={item.work_id}>
          <span className={`ov-kind ov-kind--${item.kind}`}>{KIND_LABELS[item.kind] ?? item.kind}</span>
          <Link className="ov-item" to={item.href}>{recentWorkTitle(item)}</Link>
          <small>{item.subtitle ?? ''}</small>
          <time>{relativeTime(item.occurred_at)}</time>
        </li>)}</ul> : <p className="ov-empty">Screens, companies, comparisons, filings, and backtests you open are kept here.</p>}
      </Panel>

      <Panel index={5} title="Data" meta="shared research data">
        <dl className="ov-facts">
          <div><dt>Prices through</dt><dd>{day(data.latest_price_date)}</dd><small>{data.priced_securities != null ? `${data.priced_securities.toLocaleString()} securities that day` : ''}</small></div>
          <div><dt>Filings</dt><dd>{data.filings ? data.filings.unique_filings.toLocaleString() : '—'}</dd><small>{data.filings ? `${data.filings.unique_companies.toLocaleString()} companies` : ''}</small></div>
          <div><dt>Latest filing</dt><dd>{day(data.filings?.last_submitted)}</dd><small>{data.filings?.first_submitted ? `since ${day(data.filings.first_submitted)}` : ''}</small></div>
          <div><dt>With issues</dt><dd>{data.filings ? data.filings.filings_with_issues.toLocaleString() : '—'}</dd><small><Link className="ov-item" to="/filings">Filing Explorer</Link></small></div>
        </dl>
        {isAdmin && <>
          <h3>Recent pipeline runs</h3>
          {jobs.data?.length ? <ul className="ov-list">{jobs.data.slice(0, 6).map(job => <li key={job.job_id}>
            <span className={`ov-status ov-status--${job.status}`}>{job.status}</span>
            <Link className="ov-item" to="/pipeline">{job.current_step ?? job.steps?.map(step => step.step_name).join(', ') ?? 'pipeline run'}</Link>
            <small>{job.error_message ?? `${Math.round(job.progress_percent ?? 0)}%`}</small>
            <time>{job.created_at ? relativeTime(job.created_at) : ''}</time>
          </li>)}</ul> : <p className="ov-empty">No runs yet.</p>}
        </>}
      </Panel>

      <Panel index={6} title="Start" meta="where to go next">
        <ul className="ov-list ov-list--start">
          <li><Link className="ov-item" to="/screen">Find companies that match rules</Link><kbd>N</kbd></li>
          <li><Link className="ov-item" to="/backtest">Backtest a portfolio or a screen</Link><kbd>T</kbd></li>
          <li><Link className="ov-item" to="/compare">Compare companies, a tag, or your portfolio</Link></li>
          <li><Link className="ov-item" to="/research?tab=alerts">Set an alert on a price or ratio</Link></li>
          <li><Link className="ov-item" to="/chat">Discuss ideas in Chat</Link><kbd>U</kbd></li>
          <li><Link className="ov-item" to="/filings">Read filings in Japanese and English</Link></li>
        </ul>
      </Panel>
    </div>
    {help && <ShortcutsDialog groups={SHORTCUTS} onClose={closeHelp} />}
  </div>
}
