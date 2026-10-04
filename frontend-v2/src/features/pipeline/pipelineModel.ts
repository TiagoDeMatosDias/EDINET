import type { Job, PipelineStep } from '../../api/types'

export type SelectedStep = { id: string; name: string; overwrite: boolean }
export type SavedSetup = { name: string; steps: SelectedStep[]; config: Record<string, unknown> }

export const SETUPS_KEY = 'shade.pipeline.setups'
const MAX_SAVED_SETUP_CHARS = 1_000_000
const TERMINAL_JOB_STATUSES = new Set(['cancelled', 'completed', 'failed', 'interrupted'])
export const DAILY_RECIPE = ['download_documents', 'generate_financial_statements', 'generate_ratios', 'update_stock_prices']

export function isTerminalJob(status?: string) {
  return status ? TERMINAL_JOB_STATUSES.has(status) : false
}

export function isBrowserFile(value: unknown): value is File {
  return typeof File !== 'undefined' && value instanceof File
}

function isEmbeddedFile(value: unknown) {
  return typeof value === 'object' && value !== null && 'filename' in value && 'content' in value && typeof value.filename === 'string' && typeof value.content === 'string'
}

/** Saved recipes never keep file contents. */
export function stripFileUploads(config: Record<string, unknown>) {
  return Object.fromEntries(Object.entries(config).filter(([, value]) => !isBrowserFile(value) && !isEmbeddedFile(value)))
}

export function readSetups(): SavedSetup[] {
  try {
    const raw = localStorage.getItem(SETUPS_KEY) ?? ''
    if (raw.length > MAX_SAVED_SETUP_CHARS) {
      localStorage.removeItem(SETUPS_KEY)
      return []
    }
    const parsed = JSON.parse(raw || '[]')
    if (!Array.isArray(parsed)) return []
    return parsed.filter(item => item && typeof item === 'object').map(item => ({
      ...(item as SavedSetup),
      config: stripFileUploads((item as SavedSetup).config ?? {}),
    }))
  } catch {
    try { localStorage.removeItem(SETUPS_KEY) } catch { /* ignore storage cleanup failures */ }
    return []
  }
}

export function stepLabel(step: Pick<PipelineStep, 'name' | 'display_name'>) {
  return step.display_name || step.name.replaceAll('_', ' ').replace(/\b\w/g, char => char.toUpperCase())
}

export function configKey(stepName: string, fieldName: string) {
  return `${stepName}_config.${fieldName}`
}

/** The run request: flat "step_config.field" values nested per step, files as multipart parts. */
export function runPayload(selected: SelectedStep[], config: Record<string, unknown>, steps: PipelineStep[]) {
  const nested: Record<string, unknown> = {}
  const files: Array<{ key: string; file: File }> = []
  const selectedConfigKeys = new Set(selected.map(step => steps.find(item => item.name === step.name)?.config_key ?? `${step.name}_config`))
  for (const [key, value] of Object.entries(config)) {
    const dot = key.indexOf('.')
    if (dot === -1) { nested[key] = value; continue }
    const stepKey = key.slice(0, dot)
    if (!selectedConfigKeys.has(stepKey)) continue
    const fieldKey = key.slice(dot + 1)
    if (!nested[stepKey]) nested[stepKey] = {}
    if (isBrowserFile(value)) {
      const uploadKey = `${stepKey}.${fieldKey}`
      files.push({ key: uploadKey, file: value })
      ;(nested[stepKey] as Record<string, unknown>)[fieldKey] = { __pipeline_upload__: uploadKey }
    } else {
      ;(nested[stepKey] as Record<string, unknown>)[fieldKey] = value
    }
  }
  return { payload: { steps: selected.map(step => ({ name: step.name, overwrite: step.overwrite })), config: nested }, files }
}

export function jobDuration(job: Pick<Job, 'started_at' | 'completed_at'>, now = Date.now()) {
  if (!job.started_at) return null
  const end = job.completed_at ? Date.parse(job.completed_at) : now
  return Math.max(0, end - Date.parse(job.started_at))
}

export function formatMs(ms: number | null | undefined) {
  if (ms == null) return '—'
  if (ms < 1_000) return `${ms} ms`
  const seconds = ms / 1_000
  if (seconds < 60) return `${seconds.toFixed(1)} s`
  const minutes = Math.floor(seconds / 60)
  if (minutes < 60) return `${minutes} min ${Math.round(seconds % 60)} s`
  return `${Math.floor(minutes / 60)} h ${minutes % 60} min`
}
