import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { HardDrive, ShieldCheck, Trash2, Wrench } from 'lucide-react'
import { useState, type FormEvent } from 'react'

import { apiRequest } from '../../api/client'
import { ACTIVE_JOB_STATUSES } from '../../api/jobs'
import { LoadingState } from '../../components/Feedback'
import { FILING_STORAGE_KEY, gigabytes, type FilingStorageJob, type FilingStorageResponse, type FilingStorageYear } from './filingStorage'

function errorText(error: unknown): string {
  return error instanceof Error ? error.message : 'Something went wrong.'
}

// The table shows the day only, so the row's actions fit beside it; the tooltip has the time.
function checkedDay(iso: string): string {
  return new Date(iso).toLocaleDateString()
}

function checkedTitle(result: string | null, iso: string | null): string | undefined {
  if (!iso) return undefined
  return `${result ?? 'No result'} · ${new Date(iso).toLocaleString()}`
}

function checkText(result: string | null): string {
  if (!result) return '—'
  return result === 'ok' ? 'OK' : result
}

function useStorageAction<T>(path: string, method: 'POST' | 'DELETE', onDone: (message: string | null) => void) {
  const client = useQueryClient()
  return useMutation({
    mutationFn: (body: T) => apiRequest<{ job_id: string; title: string }>(path, { method, body: body === undefined ? undefined : JSON.stringify(body) }),
    onSuccess: job => { onDone(`${job.title}: queued.`); void client.invalidateQueries({ queryKey: FILING_STORAGE_KEY }) },
    onError: (error: Error) => onDone(errorText(error)),
  })
}

function SplitForm({ data, running, onMessage }: { data: FilingStorageResponse; running: boolean; onMessage: (message: string | null) => void }) {
  const plan = data.preflight
  const [folder, setFolder] = useState('')
  const [armed, setArmed] = useState(false)
  const split = useStorageAction<{ target_folder?: string }>('/api/admin/filing-storage/split', 'POST', onMessage)
  const resuming = data.state === 'splitting'
  const submit = (event: FormEvent) => {
    event.preventDefault()
    if (!armed) { setArmed(true); return }
    setArmed(false)
    split.mutate(folder.trim() ? { target_folder: folder.trim() } : {})
  }
  return <div className="filing-storage__split">
    <p className="console-muted">
      All filings are still in one file, <code>{data.index.path}</code> ({gigabytes(data.index.bytes)}). The split copies them into a small index
      and one file per submission year, so a damaged year no longer affects the rest and a backup can skip years that have not changed.
      It runs as a worker job; the old file stays untouched until the server restarts, and is then kept as <code>filings.db.migrated</code> until you delete it.
    </p>
    {plan?.needed && <dl className="filing-storage__facts">
      <div><dt>Filings</dt><dd>{plan.filings?.toLocaleString() ?? '—'}</dd></div>
      <div><dt>Yearly files</dt><dd>{plan.years?.length ?? 0} <small className="console-muted">{plan.years?.length ? `${plan.years[0].shard}–${plan.years[plan.years.length - 1].shard}` : ''}</small></dd></div>
      <div><dt>Space needed</dt><dd>{gigabytes(plan.needed_bytes)}</dd></div>
      <div><dt>Free space</dt><dd className={plan.enough_space ? undefined : 'console-warn'}>{gigabytes(plan.free_bytes)}{plan.enough_space ? '' : ' — not enough'}</dd></div>
      <div><dt>Yearly files go to</dt><dd className="console-mono">{plan.target_folder}</dd></div>
    </dl>}
    {plan?.undated_count ? <p className="form-error" role="alert">{plan.undated_count} filing{plan.undated_count === 1 ? ' has' : 's have'} no submission date, so the split cannot choose their year: {plan.undated?.join(', ')}. Run Acquire filings for them first.</p> : null}
    {resuming && data.next && <p className="console-muted">Copied so far: {data.next.years.map(year => `${year.shard} (${year.filing_count.toLocaleString()})`).join(', ') || 'nothing yet'}.</p>}
    <form className="console-inline" onSubmit={submit}>
      <label className="console-field console-field--grow"><span>Another folder for the yearly files (optional)</span><input className="input" aria-label="Folder for the yearly files" value={folder} placeholder={data.shards_folder} maxLength={1024} disabled={resuming} onChange={event => { setFolder(event.target.value); setArmed(false) }} /></label>
      <button type="submit" className="button button--primary button--small" disabled={running || split.isPending || Boolean(plan?.undated_count)} onBlur={() => setArmed(false)}>
        <HardDrive aria-hidden="true" />{armed ? 'Start the split: sure?' : resuming ? 'Resume the split' : 'Split filing storage'}
      </button>
    </form>
  </div>
}

function YearRow({ year, busy, onMessage }: { year: FilingStorageYear; busy: boolean; onMessage: (message: string | null) => void }) {
  const check = useStorageAction<{ repair: boolean; shard: string }>('/api/admin/filing-storage/check', 'POST', onMessage)
  const compact = useStorageAction<{ shard: string }>('/api/admin/filing-storage/compact', 'POST', onMessage)
  const problem = year.state === 'missing' || (year.check_result != null && year.check_result !== 'ok')
  return <tr className={problem ? 'is-alarm' : undefined}>
    <td><span>{year.shard}</span>{year.state !== 'active' && <span className="filing-storage__state">{year.state}</span>}</td>
    <td className="num">{year.filings.toLocaleString()}</td>
    <td className="num">{gigabytes(year.archive_bytes)}</td>
    <td className="num">{gigabytes(year.file_bytes)}</td>
    <td title={checkedTitle(year.check_result, year.checked_at)}>{year.checked_at ? `${checkText(year.check_result)} · ${checkedDay(year.checked_at)}` : 'not checked'}</td>
    <td className="filing-storage__row-actions">
      <button type="button" className="text-button" aria-label={`Check ${year.shard}`} disabled={busy || check.isPending || year.state === 'missing'} onClick={() => check.mutate({ repair: false, shard: year.shard })}>Check</button>
      <button type="button" className="text-button" aria-label={`Compact ${year.shard}`} disabled={busy || compact.isPending || year.state === 'missing'} onClick={() => compact.mutate({ shard: year.shard })}>Compact</button>
    </td>
  </tr>
}

function RetiredFile({ retired, busy, onMessage }: { retired: { path: string; bytes: number }; busy: boolean; onMessage: (message: string | null) => void }) {
  const [armed, setArmed] = useState(false)
  const remove = useStorageAction<undefined>('/api/admin/filing-storage/retired', 'DELETE', onMessage)
  return <div className="filing-storage__retired">
    <span>Old single file kept after the split: <code>{retired.path}</code> ({gigabytes(retired.bytes)}). Delete it once the new files have been checked and backed up.</span>
    <button type="button" className={armed ? 'button button--secondary button--small is-danger' : 'button button--secondary button--small'} disabled={busy || remove.isPending} onBlur={() => setArmed(false)} onClick={() => { if (armed) { setArmed(false); remove.mutate(undefined) } else setArmed(true) }}>
      <Trash2 aria-hidden="true" />{armed ? 'Delete it: sure?' : 'Delete'}
    </button>
  </div>
}

function SplitStorage({ data, busy, onMessage }: { data: FilingStorageResponse; busy: boolean; onMessage: (message: string | null) => void }) {
  const check = useStorageAction<{ repair: boolean }>('/api/admin/filing-storage/check', 'POST', onMessage)
  const filings = data.shards.reduce((sum, year) => sum + year.filings, 0)
  const bytes = data.shards.reduce((sum, year) => sum + (year.file_bytes ?? 0), 0)
  return <>
    <p className="console-muted">Index <code>{data.index.path}</code> ({gigabytes(data.index.bytes)}) and {data.shards.length} yearly file{data.shards.length === 1 ? '' : 's'} in <code>{data.shards_folder}</code>: {filings.toLocaleString()} filings, {gigabytes(bytes)}.</p>
    {data.shards.length ? <div className="console-scroll"><table className="console-table filing-storage__table">
      <thead><tr><th>Year</th><th className="num">Filings</th><th className="num">Archives</th><th className="num">File</th><th>Last check</th><th><span className="sr-only">Actions</span></th></tr></thead>
      <tbody>{data.shards.map(year => <YearRow key={year.shard} year={year} busy={busy} onMessage={onMessage} />)}</tbody>
    </table></div> : <p className="console-muted">No filings yet; yearly files are created as filings arrive.</p>}
    <div className="console-actions">
      <button type="button" className="button button--secondary button--small" disabled={busy || check.isPending} onClick={() => check.mutate({ repair: false })}><ShieldCheck aria-hidden="true" />Check all</button>
      <button type="button" className="button button--secondary button--small" disabled={busy || check.isPending} onClick={() => check.mutate({ repair: true })}><Wrench aria-hidden="true" />Check and repair the index</button>
    </div>
    {data.retired && <RetiredFile retired={data.retired} busy={busy} onMessage={onMessage} />}
  </>
}

function Jobs({ jobs }: { jobs: FilingStorageJob[] }) {
  if (!jobs.length) return null
  return <ul className="filing-storage__jobs" aria-label="Filing storage jobs">
    {jobs.slice(0, 4).map(job => <li key={job.job_id}>
      <strong>{job.title}</strong> <span className={job.status === 'failed' ? 'console-pill console-pill--off' : 'console-pill'}>{job.status.replace('_', ' ')}</span>
      {ACTIVE_JOB_STATUSES.has(job.status) && job.progress_percent != null && <small className="console-muted"> {Math.round(job.progress_percent)}%</small>}
      {(job.error_message || job.status_message) && <small className={job.error_message ? 'console-warn' : 'console-muted'}> {job.error_message || job.status_message}</small>}
    </li>)}
  </ul>
}

/** Admin → Filing storage: the split from the old single file, then one row per submission year. */
export function FilingStoragePanel() {
  const [message, setMessage] = useState<string | null>(null)
  const storage = useQuery({
    queryKey: FILING_STORAGE_KEY,
    queryFn: () => apiRequest<FilingStorageResponse>('/api/admin/filing-storage'),
    refetchInterval: query => (query.state.data?.jobs.some(job => ACTIVE_JOB_STATUSES.has(job.status)) ? 3_000 : 30_000),
  })
  if (storage.isError) return <p className="form-error" role="alert">{errorText(storage.error)}</p>
  if (!storage.data) return <LoadingState label="Loading filing storage" />
  const data = storage.data
  const busy = data.jobs.some(job => ACTIVE_JOB_STATUSES.has(job.status))
  return <div className="filing-storage">
    {message && <p className="console-muted" role="status">{message} <button type="button" className="text-button" onClick={() => setMessage(null)}>Dismiss</button></p>}
    {data.state === 'legacy' || data.state === 'splitting' ? <SplitForm data={data} running={busy} onMessage={setMessage} /> : null}
    {data.state === 'ready' && <p className="console-muted">The yearly files are complete. Restart the server (Server settings → Shut down, then start it again) to switch over; the old file is then kept as <code>filings.db.migrated</code>.</p>}
    {data.state === 'split' && <SplitStorage data={data} busy={busy} onMessage={setMessage} />}
    <Jobs jobs={data.jobs} />
  </div>
}
