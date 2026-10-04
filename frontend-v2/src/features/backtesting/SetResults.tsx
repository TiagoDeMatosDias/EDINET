import { forwardRef, useImperativeHandle, useMemo, useState, type KeyboardEvent } from 'react'
import { Bar, Line } from 'react-chartjs-2'
import { Link } from 'react-router-dom'

import { DownloadButton } from '../../components/DownloadButton'
import { chainRuns, dec, fanBands, finite, heatColor, heatText, histogram, month, pct, runKey, sortRuns, summarize, tone, type PathPoint, type RunRow, type SetResult } from './backtestModel'
import { Kpi, Warnings } from './BacktestResults'
import { asPercent, BENCHMARK_COLOR, DURATION_COLORS, PORTFOLIO_COLOR, percentOptions } from './charts'

const SET_TABS = ['time', 'distribution', 'heatmap', 'paths', 'runs'] as const
type SetTab = typeof SET_TABS[number]
const TAB_LABELS: Record<SetTab, string> = { time: 'Over time', distribution: 'Distribution', heatmap: 'Start-month heatmap', paths: 'Paths', runs: 'Runs' }
const STATUS_LABELS: Record<string, string> = { ok: 'Complete', truncated: 'Truncated', no_data: 'No prices', failed: 'Failed' }
const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

export interface SetResultsHandle { cycleTab: (step: number) => void; cycleDuration: (step: number) => void; cycleWeighting: () => void; toggleMetric: () => void; closeDetail: () => boolean }

function weightingLabel(value: string) { return value === 'market_cap' ? 'Market cap' : value === 'equal' ? 'Equal' : value === 'csv' ? 'CSV' : value }

function Matrix({ rows, durations, weightings, selected, onSelect }: { rows: RunRow[]; durations: string[]; weightings: string[]; selected: { duration: string; weighting: string }; onSelect: (weighting: string, duration: string) => void }) {
  const onKeyDown = (event: KeyboardEvent<HTMLTableRowElement>) => {
    if (event.key !== 'j' && event.key !== 'k' && event.key !== 'ArrowDown' && event.key !== 'ArrowUp') return
    const next = (event.key === 'j' || event.key === 'ArrowDown' ? event.currentTarget.nextElementSibling : event.currentTarget.previousElementSibling) as HTMLElement | null
    if (next) { event.preventDefault(); next.focus() }
  }
  return <div className="bt-scroll">
    <table className="bt-table bt-matrix">
      <thead><tr><th>Weighting</th><th>Hold</th><th className="num" title="Complete runs (truncated runs in brackets)">Runs</th><th className="num">Mean ann.</th><th className="num">Median</th><th className="num">Best</th><th className="num">Worst</th><th className="num">Positive</th><th className="num">Beat bench</th><th className="num">Mean excess</th><th className="num">Bench ann.</th><th className="num">Sharpe</th><th className="num">Max DD</th></tr></thead>
      <tbody>{weightings.flatMap(weighting => durations.map(duration => {
        const group = rows.filter(row => row.weighting === weighting && row.duration === duration)
        const stats = summarize(group)
        const truncated = group.filter(row => row.status === 'truncated').length
        const active = selected.duration === duration && selected.weighting === weighting
        return <tr key={`${weighting}|${duration}`} tabIndex={0} className={active ? 'is-selected' : ''} aria-selected={active} onClick={() => onSelect(weighting, duration)} onKeyDown={event => { if (event.key === 'Enter') onSelect(weighting, duration); else onKeyDown(event) }}>
          <td>{weightingLabel(weighting)}</td><td>{duration}</td>
          <td className="num">{stats.count}{truncated ? <small> ({truncated})</small> : null}</td>
          <td className={`num ${tone(stats.mean)}`}><b>{pct(stats.mean, 1, true)}</b></td>
          <td className={`num ${tone(stats.median)}`}>{pct(stats.median, 1, true)}</td>
          <td className="num">{pct(stats.best, 1, true)}</td><td className="num">{pct(stats.worst, 1, true)}</td>
          <td className="num">{pct(stats.positive, 0)}</td>
          <td className="num"><span className="bt-bar" style={{ width: `${(stats.winRate ?? 0) * 100}%` }} />{pct(stats.winRate, 0)}</td>
          <td className={`num ${tone(stats.excess)}`}>{pct(stats.excess, 1, true)}</td>
          <td className="num">{pct(stats.benchmark, 1, true)}</td>
          <td className="num">{dec(stats.sharpe)}</td><td className="num">{pct(stats.drawdown)}</td>
        </tr>
      }))}</tbody>
    </table>
  </div>
}

function OverTime({ rows, durations, weighting, duration, onPick }: { rows: RunRow[]; durations: string[]; weighting: string; duration: string; onPick: (row: RunRow) => void }) {
  const periods = useMemo(() => [...new Set(rows.map(row => row.period))].sort(), [rows])
  const byKey = useMemo(() => new Map(rows.map(row => [runKey(row), row])), [rows])
  const shown = rows.filter(row => row.weighting === weighting && row.duration === duration)
  const chain = useMemo(() => chainRuns(shown), [shown])
  const last = chain[chain.length - 1]
  const firstDate = chain[0]?.date
  const years = last && firstDate ? (new Date(last.date).getTime() - new Date(firstDate).getTime()) / (365.25 * 864e5) : 0
  const cagr = (value: number | null | undefined) => value == null || years <= 0 ? null : value ** (1 / years) - 1
  const value = (row: RunRow | undefined) => row && (row.status === 'ok' || row.status === 'truncated') ? asPercent(row.annualized_return) : null
  return <div className="bt-grid bt-grid--two">
    <figure className="bt-panel">
      <figcaption>Annualized return by start month · {weightingLabel(weighting)} · hollow points are truncated runs · click a point for the run</figcaption>
      <div className="bt-chart bt-chart--tall"><Line data={{ labels: periods.map(month), datasets: [
        ...durations.map(item => ({
          label: item,
          data: periods.map(period => value(byKey.get(`${period}|${weighting}|${item}`))),
          borderColor: DURATION_COLORS[item] ?? PORTFOLIO_COLOR,
          backgroundColor: periods.map(period => byKey.get(`${period}|${weighting}|${item}`)?.status === 'truncated' ? 'transparent' : DURATION_COLORS[item] ?? PORTFOLIO_COLOR),
          borderWidth: item === duration ? 2 : 1,
          pointRadius: item === duration ? 2.5 : 1.2,
          hidden: item !== duration && durations.length > 2,
          spanGaps: false,
        })),
        { label: `Benchmark ${duration}`, data: periods.map(period => asPercent(byKey.get(`${period}|${weighting}|${duration}`)?.benchmark_annualized_return)), borderColor: BENCHMARK_COLOR, borderDash: [4, 3], borderWidth: 1.2, pointRadius: 0, spanGaps: false },
      ] }} options={percentOptions<'line'>({ xLabels: true, onClick: index => { const row = byKey.get(`${periods[index]}|${weighting}|${duration}`); if (row) onPick(row) } })} /></div>
    </figure>
    <figure className="bt-panel">
      <figcaption>Back-to-back {duration} holds · growth of 1 · CAGR {pct(cagr(last?.portfolio), 1, true)} vs {pct(cagr(last?.benchmark), 1, true)}</figcaption>
      <div className="bt-chart bt-chart--tall">{chain.length > 1 ? <Line data={{ labels: chain.map(point => month(point.date)), datasets: [
        { label: 'Strategy', data: chain.map(point => asPercent(point.portfolio - 1)), borderColor: PORTFOLIO_COLOR, borderWidth: 2, pointRadius: 2 },
        { label: 'Benchmark', data: chain.map(point => point.benchmark == null ? null : asPercent(point.benchmark - 1)), borderColor: BENCHMARK_COLOR, borderDash: [4, 3], borderWidth: 1.2, pointRadius: 1.5 },
      ] }} options={percentOptions<'line'>({ xLabels: true })} /> : <p className="muted bt-empty">Too few runs to chain.</p>}</div>
    </figure>
  </div>
}

function Distribution({ rows }: { rows: RunRow[] }) {
  const complete = rows.filter(row => row.status === 'ok')
  const values = complete.map(row => finite(row.annualized_return)).filter((value): value is number => value != null)
  const bench = complete.map(row => finite(row.benchmark_annualized_return)).filter((value): value is number => value != null)
  const excess = complete.map(row => finite(row.excess_annualized_return)).filter((value): value is number => value != null)
  const spread = values.length ? Math.max(...values) - Math.min(...values) : 0
  const width = spread > 1 ? 0.1 : spread > 0.4 ? 0.05 : spread > 0.15 ? 0.02 : 0.01
  const bins = histogram(values, bench, width)
  const excessBins = histogram(excess, [], width)
  const label = (bin: { from: number; to: number }) => `${(bin.from * 100).toFixed(0)}…${(bin.to * 100).toFixed(0)}%`
  const countOptions = { ...percentOptions<'bar'>({ xLabels: true, tooltipLabel: (value, name) => `${name}: ${value} runs` }), scales: { x: { ticks: { font: { size: 9 }, maxRotation: 0, autoSkipPadding: 8 }, grid: { display: false } }, y: { position: 'right' as const, ticks: { precision: 0, font: { size: 9 } } } } }
  return <div className="bt-grid bt-grid--two">
    <figure className="bt-panel">
      <figcaption>Annualized return of complete runs · {values.length} runs</figcaption>
      <div className="bt-chart bt-chart--tall"><Bar data={{ labels: bins.map(label), datasets: [
        { label: 'Strategy', data: bins.map(bin => bin.portfolio), backgroundColor: `${PORTFOLIO_COLOR}cc` },
        ...(bench.length ? [{ label: 'Benchmark, same windows', data: bins.map(bin => bin.benchmark), backgroundColor: `${BENCHMARK_COLOR}77` }] : []),
      ] }} options={countOptions} /></div>
    </figure>
    <figure className="bt-panel">
      <figcaption>Excess over the benchmark, per run · {excess.filter(value => value > 0).length} of {excess.length} ahead</figcaption>
      <div className="bt-chart bt-chart--tall">{excess.length ? <Bar data={{ labels: excessBins.map(label), datasets: [
        { label: 'Runs', data: excessBins.map(bin => bin.portfolio), backgroundColor: excessBins.map(bin => bin.from >= 0 ? `${PORTFOLIO_COLOR}cc` : '#C4462Ccc') },
      ] }} options={countOptions} /> : <p className="muted bt-empty">No benchmark in these runs.</p>}</div>
    </figure>
  </div>
}

function Heatmap({ rows, metric, onPick, selectedKey }: { rows: RunRow[]; metric: 'return' | 'excess'; onPick: (row: RunRow) => void; selectedKey: string | null }) {
  const cells = new Map(rows.map(row => [row.period.slice(0, 7), row]))
  const years = [...new Set(rows.map(row => row.period.slice(0, 4)))].sort()
  const valueOf = (row: RunRow) => metric === 'excess' ? finite(row.excess_annualized_return) : finite(row.annualized_return)
  const scale = Math.max(0.05, ...rows.map(row => Math.abs(valueOf(row) ?? 0))) * 0.8
  return <div className="bt-scroll">
    <table className="bt-table bt-heat bt-heat--cells">
      <thead><tr><th>Start</th>{MONTHS.map(name => <th key={name} className="num">{name}</th>)}</tr></thead>
      <tbody>{years.map(year => <tr key={year}>
        <th scope="row">{year}</th>
        {MONTHS.map((_, index) => {
          const row = cells.get(`${year}-${String(index + 1).padStart(2, '0')}`)
          if (!row) return <td key={index} />
          const value = row.status === 'ok' || row.status === 'truncated' ? valueOf(row) : null
          const title = `${month(row.period)} · ${STATUS_LABELS[row.status]} · ann. ${pct(row.annualized_return, 1, true)} · bench ${pct(row.benchmark_annualized_return, 1, true)} · excess ${pct(row.excess_annualized_return, 1, true)}`
          return <td key={index} className={`num ${row.status === 'truncated' ? 'is-truncated' : ''} ${selectedKey === runKey(row) ? 'is-selected' : ''}`} style={{ background: heatColor(value, scale), color: heatText(value, scale) }}>
            <button type="button" title={title} onClick={() => onPick(row)}>{value == null ? '·' : (value * 100).toFixed(1)}</button>
          </td>
        })}
      </tr>)}</tbody>
    </table>
  </div>
}

function Paths({ rows, paths, selected }: { rows: RunRow[]; paths: SetResult['paths']; selected: RunRow | null }) {
  const list = rows.filter(row => row.status === 'ok').map(row => paths?.[runKey(row)]).filter((path): path is NonNullable<typeof path> => Boolean(path?.length))
  const bands = fanBands(list)
  const own = selected ? paths?.[runKey(selected)] ?? [] : []
  if (bands.length < 2) return <p className="muted bt-empty">Paths need at least three complete runs.</p>
  const labels = bands.map(band => `${band.month}m`)
  const line = (key: 'p10' | 'p25' | 'p50' | 'p75' | 'p90') => bands.map(band => asPercent(band[key] - 1))
  return <figure className="bt-panel">
    <figcaption>Growth by months held, across {list.length} complete runs · bands are the 10–90th and 25–75th percentiles{selected ? ` · highlighted: ${month(selected.period)}` : ''}</figcaption>
    <div className="bt-chart bt-chart--tall"><Line data={{ labels, datasets: [
      { label: '10th', data: line('p10'), borderColor: 'transparent', pointRadius: 0, fill: false },
      { label: '10–90th', data: line('p90'), borderColor: 'transparent', backgroundColor: `${PORTFOLIO_COLOR}1f`, pointRadius: 0, fill: '-1' },
      { label: '25th', data: line('p25'), borderColor: 'transparent', pointRadius: 0, fill: false },
      { label: '25–75th', data: line('p75'), borderColor: 'transparent', backgroundColor: `${PORTFOLIO_COLOR}38`, pointRadius: 0, fill: '-1' },
      { label: 'Median', data: line('p50'), borderColor: PORTFOLIO_COLOR, borderWidth: 2, pointRadius: 0 },
      { label: 'Benchmark median', data: bands.map(band => band.bench == null ? null : asPercent(band.bench - 1)), borderColor: BENCHMARK_COLOR, borderDash: [4, 3], borderWidth: 1.2, pointRadius: 0 },
      ...(own.length ? [{ label: month(selected?.period ?? ''), data: labels.map((_, index) => asPercent(own[index]?.[1])), borderColor: '#C4462C', borderWidth: 1.5, pointRadius: 0 }] : []),
    ] }} options={percentOptions<'line'>({ xLabels: true })} /></div>
  </figure>
}

type RunSort = { key: keyof RunRow; desc: boolean }

function RunsTable({ rows, selectedKey, onPick }: { rows: RunRow[]; selectedKey: string | null; onPick: (row: RunRow) => void }) {
  const [sort, setSort] = useState<RunSort>({ key: 'period', desc: false })
  const [status, setStatus] = useState('all')
  const shown = useMemo(() => sortRuns(rows.filter(row => status === 'all' || row.status === status), sort.key, sort.desc), [rows, sort, status])
  const header = (key: keyof RunRow, label: string, numeric = true) => <th className={numeric ? 'num' : ''} aria-sort={sort.key === key ? (sort.desc ? 'descending' : 'ascending') : 'none'}>
    <button type="button" onClick={() => setSort(current => ({ key, desc: current.key === key ? !current.desc : numeric }))}>{label}{sort.key === key ? (sort.desc ? ' ↓' : ' ↑') : ''}</button>
  </th>
  const onKeyDown = (event: KeyboardEvent<HTMLTableRowElement>, row: RunRow) => {
    if (event.key === 'Enter') { event.preventDefault(); onPick(row); return }
    if (!['j', 'k', 'ArrowDown', 'ArrowUp'].includes(event.key)) return
    const next = (event.key === 'j' || event.key === 'ArrowDown' ? event.currentTarget.nextElementSibling : event.currentTarget.previousElementSibling) as HTMLElement | null
    if (next) { event.preventDefault(); next.focus() }
  }
  return <div className="bt-runs">
    <div className="bt-toolbar">
      <label className="bt-inline">Status <select className="select select--small" value={status} onChange={event => setStatus(event.target.value)}><option value="all">All · {rows.length}</option>{Object.entries(STATUS_LABELS).map(([value, label]) => <option key={value} value={value}>{label} · {rows.filter(row => row.status === value).length}</option>)}</select></label>
      <span className="muted">J/K to move, Enter for details</span>
    </div>
    <div className="bt-scroll bt-scroll--tall">
      <table className="bt-table">
        <thead><tr>{header('period', 'Start', false)}{header('status', 'Status', false)}{header('companies', 'Held')}{header('annualized_return', 'Ann.')}{header('total_return', 'Total')}{header('benchmark_annualized_return', 'Bench ann.')}{header('excess_annualized_return', 'Excess ann.')}{header('sharpe_ratio', 'Sharpe')}{header('max_drawdown', 'Max DD')}{header('volatility', 'Vol')}{header('end', 'Prices to', false)}</tr></thead>
        <tbody>{shown.map(row => {
          const key = runKey(row)
          return <tr key={key} tabIndex={0} className={`${selectedKey === key ? 'is-selected' : ''} ${row.status !== 'ok' ? 'is-dim' : ''}`} onClick={() => onPick(row)} onKeyDown={event => onKeyDown(event, row)}>
            <td>{month(row.period)}</td><td><span className={`bt-status bt-status--${row.status}`}>{STATUS_LABELS[row.status]}</span></td>
            <td className="num" title={row.matches ? `${row.companies} held of ${row.matches} matches` : undefined}>{row.companies ?? '—'}{row.matches ? <small>/{row.matches}</small> : null}</td>
            <td className={`num ${tone(row.annualized_return)}`}>{pct(row.annualized_return, 1, true)}</td>
            <td className={`num ${tone(row.total_return)}`}>{pct(row.total_return, 1, true)}</td>
            <td className="num">{pct(row.benchmark_annualized_return, 1, true)}</td>
            <td className={`num ${tone(row.excess_annualized_return)}`}>{pct(row.excess_annualized_return, 1, true)}</td>
            <td className="num">{dec(row.sharpe_ratio)}</td><td className="num">{pct(row.max_drawdown)}</td><td className="num">{pct(row.volatility)}</td>
            <td className="muted">{row.end ?? '—'}</td>
          </tr>
        })}</tbody>
      </table>
    </div>
  </div>
}

function RunDetail({ row, path, holdings, onClose }: { row: RunRow; path?: PathPoint[]; holdings: string[]; onClose: () => void }) {
  return <aside className="bt-detail" aria-label="Selected run">
    <header><h3>{month(row.period)} · {weightingLabel(row.weighting)} · {row.duration}</h3><span className={`bt-status bt-status--${row.status}`}>{STATUS_LABELS[row.status]}</span><button type="button" className="text-button" onClick={onClose}>Close <kbd>Esc</kbd></button></header>
    <dl className="bt-facts">
      <div><dt>Annualized</dt><dd className={tone(row.annualized_return)}>{pct(row.annualized_return, 2, true)}</dd></div>
      <div><dt>Total</dt><dd className={tone(row.total_return)}>{pct(row.total_return, 1, true)}</dd></div>
      <div><dt>Benchmark ann.</dt><dd>{pct(row.benchmark_annualized_return, 2, true)}</dd></div>
      <div><dt>Excess ann.</dt><dd className={tone(row.excess_annualized_return)}>{pct(row.excess_annualized_return, 2, true)}</dd></div>
      <div><dt>Sharpe</dt><dd>{dec(row.sharpe_ratio)}</dd></div>
      <div><dt>Max drawdown</dt><dd>{pct(row.max_drawdown)}</dd></div>
      <div><dt>Price · dividend</dt><dd>{pct(row.price_return, 1, true)} · {pct(row.dividend_return)}</dd></div>
      <div><dt>Held</dt><dd>{row.start ?? '—'} → {row.end ?? '—'}</dd></div>
    </dl>
    {path && path.length > 1 && <div className="bt-chart"><Line data={{ labels: path.map(point => month(point[0])), datasets: [
      { label: 'Run', data: path.map(point => asPercent(point[1])), borderColor: PORTFOLIO_COLOR, borderWidth: 1.5, pointRadius: 0 },
      { label: 'Benchmark', data: path.map(point => asPercent(point[2])), borderColor: BENCHMARK_COLOR, borderDash: [4, 3], borderWidth: 1.2, pointRadius: 0, spanGaps: true },
    ] }} options={percentOptions<'line'>({ xLabels: true })} /></div>}
    {row.warnings && row.warnings.length > 0 && <Warnings items={row.warnings} />}
    <h4>Held · {holdings.length}</h4>
    <ul className="bt-holdings">{holdings.map(ticker => <li key={ticker}><Link to={`/analyze?ticker=${encodeURIComponent(ticker)}&from=backtest`}>{ticker}</Link></li>)}</ul>
  </aside>
}

/** Many backtests of one strategy (a rolling screen or a CSV set): the spread of outcomes. */
export const SetResults = forwardRef<SetResultsHandle, { data: SetResult }>(function SetResults({ data }, ref) {
  const runs = useMemo(() => data.runs ?? [], [data.runs])
  const aggregate = data.aggregate ?? {}
  const config = data.config ?? {}
  const durations = useMemo(() => [...new Set(runs.map(row => row.duration))].sort((a, b) => parseFloat(a) - parseFloat(b)), [runs])
  const weightings = useMemo(() => [...new Set(runs.map(row => row.weighting))], [runs])
  const [tab, setTab] = useState<SetTab>('time')
  const [duration, setDuration] = useState(() => durations[0] ?? '1yr')
  const [weighting, setWeighting] = useState(() => weightings[0] ?? 'equal')
  const [metric, setMetric] = useState<'return' | 'excess'>('return')
  const [selectedKey, setSelectedKey] = useState<string | null>(null)
  const activeDuration = durations.includes(duration) ? duration : durations[0] ?? duration
  const activeWeighting = weightings.includes(weighting) ? weighting : weightings[0] ?? weighting
  const group = useMemo(() => runs.filter(row => row.duration === activeDuration && row.weighting === activeWeighting), [runs, activeDuration, activeWeighting])
  const selected = selectedKey ? runs.find(row => runKey(row) === selectedKey) ?? null : null
  const overall = summarize(runs)
  const counts = { complete: runs.filter(row => row.status === 'ok').length, truncated: runs.filter(row => row.status === 'truncated').length, empty: runs.filter(row => row.status === 'no_data' || row.status === 'failed').length }
  const kind = data.kind ?? 'rolling'

  useImperativeHandle(ref, () => ({
    cycleTab: step => setTab(current => SET_TABS[(SET_TABS.indexOf(current) + step + SET_TABS.length) % SET_TABS.length]),
    cycleDuration: step => setDuration(() => durations[(durations.indexOf(activeDuration) + step + durations.length) % Math.max(1, durations.length)] ?? activeDuration),
    cycleWeighting: () => setWeighting(() => weightings[(weightings.indexOf(activeWeighting) + 1) % Math.max(1, weightings.length)] ?? activeWeighting),
    toggleMetric: () => setMetric(current => current === 'return' ? 'excess' : 'return'),
    closeDetail: () => {
      if (!selectedKey) return false
      setSelectedKey(null)
      return true
    },
  }), [durations, weightings, activeDuration, activeWeighting, selectedKey])

  const pick = (row: RunRow) => setSelectedKey(runKey(row))
  const ranking = String(config.ranking_algorithm ?? 'none')
  const notes: string[] = []
  if (kind === 'rolling' && ranking === 'none' && Number(config.max_companies) > 0) notes.push(`This screen has no ranking: where more than ${String(config.max_companies)} companies matched, which ones were held was arbitrary. Add ranking rules in Screening for a reproducible selection.`)
  if (counts.truncated) notes.push(`${counts.truncated} runs end before their holding period does (prices run out) and are left out of the statistics.`)
  if (counts.empty) notes.push(`${counts.empty} runs had no prices or failed, and are left out.`)

  if (!runs.length) {
    return <section className="bt-result"><header className="bt-result__head"><h2>{kind === 'csv' ? 'CSV set' : 'Rolling screen'}</h2><span className="muted">saved {data.id}</span></header>
      <p className="bt-empty">This result has only summary statistics (it was saved before per-run results were kept).</p>
      <div className="bt-kpis"><Kpi label="Runs" value={String(aggregate.total_runs ?? 0)} /><Kpi label="Mean return" value={pct((aggregate.stats as Record<string, Record<string, number>> | undefined)?.total_return?.mean, 1, true)} /></div>
    </section>
  }

  return <section className="bt-result" aria-label="Backtest set result">
    <header className="bt-result__head">
      <h2>{kind === 'csv' ? 'CSV portfolio set' : `Rolling screen · ${String(config.cadence ?? '')}`}</h2>
      <span className="muted">{kind === 'rolling' ? `top ${String(config.max_companies ?? '—')} · ranking ${ranking} · ` : ''}{config.benchmark_mode === 'portfolio' ? 'vs own portfolio' : config.benchmark_ticker ? `vs ${String(config.benchmark_ticker)}` : 'no benchmark'} · {String(config.start_period ?? runs[0]?.period ?? '').slice(0, 7)} → {String(config.end_period ?? runs[runs.length - 1]?.period ?? '').slice(0, 7)} · saved {data.id}</span>
      <DownloadButton className="button button--ghost button--small" path={`/api/backtesting/download/${encodeURIComponent(data.id)}`} filename={`backtest_${data.id}.zip`}>Download</DownloadButton>
    </header>
    <Warnings items={notes} />
    <div className="bt-kpis">
      <Kpi label="Complete runs" value={String(counts.complete)} detail={`${counts.truncated} truncated · ${counts.empty} empty`} />
      <Kpi label="Mean annualized" value={pct(overall.mean, 1, true)} className={tone(overall.mean)} detail="all holds" />
      <Kpi label="Median" value={pct(overall.median, 1, true)} className={tone(overall.median)} />
      <Kpi label="Best · worst" value={`${pct(overall.best, 0, true)} · ${pct(overall.worst, 0, true)}`} />
      <Kpi label="Positive" value={pct(overall.positive, 0)} />
      <Kpi label="Beat benchmark" value={pct(overall.winRate, 0)} className={overall.winRate == null ? '' : overall.winRate >= 0.5 ? 'is-up' : 'is-down'} />
      <Kpi label="Mean excess" value={pct(overall.excess, 1, true)} className={tone(overall.excess)} detail="annualized" />
      <Kpi label="Mean Sharpe" value={dec(overall.sharpe)} />
      <Kpi label="Mean max DD" value={pct(overall.drawdown)} />
    </div>
    <Matrix rows={runs} durations={durations} weightings={weightings} selected={{ duration: activeDuration, weighting: activeWeighting }} onSelect={(nextWeighting, nextDuration) => { setWeighting(nextWeighting); setDuration(nextDuration) }} />
    <div className="bt-tabs-row">
      <div className="bt-tabs" role="tablist" aria-label="Result views">{SET_TABS.map(item => <button key={item} type="button" role="tab" aria-selected={tab === item} className={tab === item ? 'active' : ''} onClick={() => setTab(item)}>{TAB_LABELS[item]}</button>)}</div>
      <div className="bt-chips" aria-label="Holding period">{durations.map(item => <button key={item} type="button" aria-pressed={activeDuration === item} className={activeDuration === item ? 'active' : ''} onClick={() => setDuration(item)}>{item}</button>)}</div>
      {weightings.length > 1 && <div className="bt-chips" aria-label="Weighting">{weightings.map(item => <button key={item} type="button" aria-pressed={activeWeighting === item} className={activeWeighting === item ? 'active' : ''} onClick={() => setWeighting(item)}>{weightingLabel(item)}</button>)}</div>}
      {tab === 'heatmap' && <div className="bt-chips" aria-label="Measure"><button type="button" aria-pressed={metric === 'return'} className={metric === 'return' ? 'active' : ''} onClick={() => setMetric('return')}>Return</button><button type="button" aria-pressed={metric === 'excess'} className={metric === 'excess' ? 'active' : ''} onClick={() => setMetric('excess')}>Excess</button></div>}
      <span className="muted bt-keys"><kbd>[</kbd><kbd>]</kbd> view · <kbd>D</kbd> hold · {weightings.length > 1 ? <><kbd>W</kbd> weighting · </> : null}<kbd>M</kbd> measure</span>
    </div>
    <div className={selected ? 'bt-split' : ''}>
      <div role="tabpanel" aria-label={TAB_LABELS[tab]} className="bt-tabpanel">
        {tab === 'time' && <OverTime rows={runs} durations={durations} weighting={activeWeighting} duration={activeDuration} onPick={pick} />}
        {tab === 'distribution' && <Distribution rows={group} />}
        {tab === 'heatmap' && <figure className="bt-panel"><figcaption>{metric === 'excess' ? 'Annualized excess over the benchmark' : 'Annualized return'} by start month · {activeDuration} · {weightingLabel(activeWeighting)} · outlined cells are truncated</figcaption><Heatmap rows={group} metric={metric} onPick={pick} selectedKey={selectedKey} /></figure>}
        {tab === 'paths' && <Paths rows={group} paths={data.paths} selected={selected && selected.duration === activeDuration ? selected : null} />}
        {tab === 'runs' && <RunsTable rows={group} selectedKey={selectedKey} onPick={pick} />}
      </div>
      {selected && <RunDetail row={selected} path={data.paths?.[runKey(selected)]} holdings={data.period_holdings?.[selected.period] ?? []} onClose={() => setSelectedKey(null)} />}
    </div>
  </section>
})
