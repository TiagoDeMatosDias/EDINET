import type { JobStep } from '../../api/types'

function statusBadge(status: string) {
  if (status === 'completed') return 'badge badge--success'
  if (status === 'failed' || status === 'interrupted' || status === 'cancelled') return 'badge badge--danger'
  return 'badge'
}

function formatDuration(ms: number | null | undefined) {
  if (ms == null) return '—'
  if (ms < 1_000) return `${ms} ms`
  const seconds = ms / 1_000
  if (seconds < 60) return `${seconds.toFixed(1)} s`
  const minutes = Math.floor(seconds / 60)
  return `${minutes} min ${Math.round(seconds % 60)} s`
}

function humanize(key: string) {
  const spaced = key.replace(/[_-]+/g, ' ').replace(/([a-z])([A-Z])/g, '$1 $2').trim()
  return spaced.charAt(0).toUpperCase() + spaced.slice(1)
}

const MAX_LIST_ITEMS = 8

function formatOutputValue(value: unknown): string {
  if (value == null || value === '') return '—'
  if (typeof value === 'boolean') return value ? 'Yes' : 'No'
  if (typeof value === 'number') return value.toLocaleString()
  if (typeof value === 'string') return value
  if (Array.isArray(value)) {
    if (!value.length) return 'None'
    if (value.every(item => item == null || typeof item !== 'object')) {
      const shown = value.slice(0, MAX_LIST_ITEMS).map(formatOutputValue).join(', ')
      return value.length > MAX_LIST_ITEMS ? `${shown} +${value.length - MAX_LIST_ITEMS} more` : shown
    }
    return `${value.length.toLocaleString()} items`
  }
  return JSON.stringify(value)
}

export function StepStateTable({ steps }: { steps: JobStep[] }) {
  if (!steps.length) return <p className="muted">No step state recorded.</p>
  return (
    <div className="table-scroll">
      <table className="data-grid job-steps">
        <thead><tr><th scope="col">#</th><th scope="col">Step</th><th scope="col">Status</th><th scope="col">Duration</th><th scope="col">Details</th></tr></thead>
        <tbody>
          {[...steps].sort((a, b) => a.ordinal - b.ordinal).map(step => (
            <tr key={`${step.ordinal}-${step.step_name}`}>
              <td>{step.ordinal + 1}</td>
              <td><strong>{step.step_name}</strong>{step.overwrite && <small className="muted"> · overwrite</small>}</td>
              <td><span className={statusBadge(step.status)}>{step.status}</span></td>
              <td>{formatDuration(step.duration_ms)}</td>
              <td className={step.error_message ? 'form-error' : undefined}>{step.error_message || step.status_message || '—'}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

export function RunOutput({ output }: { output: Record<string, unknown> }) {
  const steps = Object.entries(output)
  if (!steps.length) return <p className="muted">No completed step reported output.</p>
  return (
    <div className="stack run-output">
      {steps.map(([stepName, result]) => (
        <section key={stepName} className="run-output-step">
          <h3>{stepName}</h3>
          {result && typeof result === 'object' && !Array.isArray(result) ? (
            <dl className="run-output-list">
              {Object.entries(result as Record<string, unknown>).map(([key, value]) => (
                <div key={key}><dt>{humanize(key)}</dt><dd>{formatOutputValue(value)}</dd></div>
              ))}
            </dl>
          ) : <p>{formatOutputValue(result)}</p>}
        </section>
      ))}
      <details className="details">
        <summary>View raw output</summary>
        <pre>{JSON.stringify(output, null, 2)}</pre>
      </details>
    </div>
  )
}
