import { useQuery } from '@tanstack/react-query'
import { Search } from 'lucide-react'
import { useState } from 'react'

import { apiRequest } from '../../../api/client'
import { LoadingState } from '../../../components/Feedback'
import { Tip } from '../../../components/Tooltip'
import { formatBytes } from '../filingFormat'
import type { Artifact, QualityIssue, TaxonomyEntry } from './viewerTypes'

const TAXONOMY_PAGE = 100

/** Short, recognisable names for the taxonomies a filing draws its concepts from. */
function namespaceName(uri?: string) {
  if (!uri) return 'Unknown'
  // Filer extensions live under the filing's own path, which also names its form (jpcrp030000).
  if (/\/E\d{5}-\d{3}\//.test(uri)) return 'Company extension'
  if (/jpcrp/.test(uri)) return 'Corporate disclosure (jpcrp)'
  if (/jppfs/.test(uri)) return 'Japanese GAAP statements (jppfs)'
  if (/jpigp/.test(uri)) return 'Designated IFRS (jpigp)'
  if (/jpdei/.test(uri)) return 'Document information (jpdei)'
  if (/ifrs/.test(uri)) return 'IFRS'
  if (/xbrl\.org/.test(uri)) return 'XBRL core'
  return uri.replace(/^https?:\/\//, '').split('/').slice(0, 3).join('/')
}

function folderOf(path: string) {
  const parts = path.split('/')
  return parts.length > 1 ? parts.slice(0, -1).join('/') : 'Package root'
}

export function DetailsTab({ docId, artifacts, issues, issuesLoading }: { docId: string; artifacts: Artifact[]; issues: QualityIssue[]; issuesLoading: boolean }) {
  const [filter, setFilter] = useState('')
  const [limit, setLimit] = useState(TAXONOMY_PAGE)
  const taxonomy = useQuery({ queryKey: ['filing-taxonomy', docId], queryFn: () => apiRequest<{ taxonomy: TaxonomyEntry[] }>(`/api/filings/${encodeURIComponent(docId)}/taxonomy`) })
  const parseRun = useQuery({ queryKey: ['filing-parse-run', docId], retry: false, queryFn: () => apiRequest<{ latest: Record<string, unknown> | null }>(`/api/filings/${encodeURIComponent(docId)}/parse-runs`) })
  const concepts = taxonomy.data?.taxonomy ?? []
  const namespaces = new Map<string, number>()
  for (const entry of concepts) namespaces.set(namespaceName(entry.namespace_uri), (namespaces.get(namespaceName(entry.namespace_uri)) ?? 0) + 1)
  const needle = filter.trim().toLowerCase()
  const matching = needle ? concepts.filter(entry => `${entry.concept ?? ''} ${namespaceName(entry.namespace_uri)}`.toLowerCase().includes(needle)) : concepts
  const folders = new Map<string, Artifact[]>()
  for (const artifact of artifacts) folders.set(folderOf(artifact.member_path), [...(folders.get(folderOf(artifact.member_path)) ?? []), artifact])
  const totalSize = artifacts.reduce((sum, artifact) => sum + (artifact.size_bytes ?? 0), 0)
  const run = parseRun.data?.latest
  return <div className="details-grid">
    <section className="panel details-panel" aria-labelledby="quality-title">
      <h3 id="quality-title">Quality notes</h3>
      {issuesLoading ? <LoadingState label="Loading quality notes" /> : issues.length ? <table className="details-table">
        <thead><tr><th scope="col">Severity</th><th scope="col">Check</th><th scope="col">Detail</th></tr></thead>
        <tbody>{issues.map(issue => <tr key={issue.issue_id}><td><span className={`severity severity--${issue.severity}`}>{issue.severity}</span></td><td className="mono">{issue.code}</td><td>{issue.message}</td></tr>)}</tbody>
      </table> : <p className="details-panel__empty">No data-quality issues were recorded when this filing was parsed.</p>}
      <h3>Parsing</h3>
      {run ? <dl className="details-facts">
        <div><dt>Parser</dt><dd>{String(run.parser_version ?? '—')}</dd></div>
        <div><dt>Status</dt><dd>{String(run.status ?? '—')}</dd></div>
        <div><dt>Facts</dt><dd>{Number(run.fact_count ?? 0).toLocaleString()}</dd></div>
        <div><dt>Sections</dt><dd>{Number(run.section_count ?? 0).toLocaleString()}</dd></div>
        <div><dt>Warnings</dt><dd>{Number(run.warning_count ?? 0).toLocaleString()}</dd></div>
        {Boolean(run.error_message) && <div><dt>Error</dt><dd>{String(run.error_message)}</dd></div>}
      </dl> : <p className="details-panel__empty">{parseRun.isLoading ? 'Loading…' : 'No parse run is recorded for this filing.'}</p>}
    </section>

    <section className="panel details-panel" aria-labelledby="package-title">
      <h3 id="package-title">Package contents <small>{artifacts.length} files · {formatBytes(totalSize)} extracted</small></h3>
      {[...folders].map(([folder, items]) => <div key={folder} className="details-folder">
        <h4 className="mono">{folder}</h4>
        <ul>{items.map(artifact => <li key={artifact.artifact_id}><span className="mono" title={artifact.member_path}>{artifact.member_path.split('/').pop()}</span><small>{artifact.kind} · {formatBytes(artifact.size_bytes)}</small></li>)}</ul>
      </div>)}
    </section>

    <section className="panel details-panel details-panel--wide" aria-labelledby="taxonomy-title">
      <h3 id="taxonomy-title"><Tip content="XBRL concepts this filing reports values for, by the taxonomy that defines them. Company extensions are concepts the filer defined itself.">Taxonomy concepts</Tip> <small>{concepts.length.toLocaleString()} concepts</small></h3>
      {taxonomy.isLoading ? <LoadingState label="Loading taxonomy concepts" /> : <>
        <ul className="details-namespaces">{[...namespaces].sort((a, b) => b[1] - a[1]).map(([name, count]) => <li key={name}><button type="button" className="text-button" onClick={() => { setFilter(name); setLimit(TAXONOMY_PAGE) }}>{name}</button><small>{count.toLocaleString()}</small></li>)}</ul>
        <label className="viewer-search details-search">
          <Search aria-hidden="true" />
          <input value={filter} onChange={event => { setFilter(event.target.value); setLimit(TAXONOMY_PAGE) }} placeholder="Filter concepts" aria-label="Filter taxonomy concepts" />
        </label>
        <table className="details-table">
          <thead><tr><th scope="col">Concept</th><th scope="col">Taxonomy</th></tr></thead>
          <tbody>{matching.slice(0, limit).map((entry, index) => <tr key={`${entry.namespace_uri}-${entry.concept}-${index}`}><td className="mono details-concept">{entry.concept}</td><td title={entry.namespace_uri}>{namespaceName(entry.namespace_uri)}</td></tr>)}</tbody>
        </table>
        <p className="details-panel__foot">{Math.min(limit, matching.length).toLocaleString()} of {matching.length.toLocaleString()} shown{matching.length > limit && <> · <button type="button" className="text-button" onClick={() => setLimit(limit + TAXONOMY_PAGE)}>Show {TAXONOMY_PAGE} more</button></>}</p>
      </>}
    </section>
  </div>
}
