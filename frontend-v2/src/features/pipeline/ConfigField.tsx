import { useRef, useState } from 'react'

import type { PipelineField } from '../../api/types'
import { Field } from '../../components/Page'
import { isBrowserFile } from './pipelineModel'

const DEFAULT_UPLOAD_BYTES = 10 * 1024 * 1024

function fileNameFromValue(value: unknown) {
  if (typeof value === 'string') return value.split(/[\\/]/).pop() ?? value
  if (isBrowserFile(value)) return value.name
  if (typeof value === 'object' && value !== null && 'filename' in value && typeof value.filename === 'string') return value.filename
  return ''
}

function fileAccept(field: PipelineField) {
  const patterns = (field.filetypes ?? [])
    .flatMap(([, pattern]) => pattern.split(','))
    .map(pattern => pattern.trim())
    .filter(pattern => pattern && pattern !== '*.*')
    .map(pattern => pattern.replace(/^\*\./, '.'))
  return patterns.length ? patterns.join(',') : undefined
}

export function ConfigField({ field, value, onChange, maxUploadBytes }: { field: PipelineField; value: unknown; onChange: (value: unknown) => void; maxUploadBytes?: number }) {
  const fieldLabel = field.label || field.name
  const fieldDesc = field.description
  const inputType = field.type?.toLowerCase() ?? 'text'
  const maxBytes = field.max_bytes ?? maxUploadBytes ?? DEFAULT_UPLOAD_BYTES
  const [fileError, setFileError] = useState('')
  const fileInputRef = useRef<HTMLInputElement>(null)
  if (field.choices?.length) return <Field label={fieldLabel} hint={fieldDesc}><select className="select" value={String(value ?? field.default ?? '')} onChange={event => onChange(event.target.value)}>{field.choices.map(choice => <option key={choice} value={choice}>{choice}</option>)}</select></Field>
  if (inputType.includes('bool')) return <label className="check"><input type="checkbox" checked={Boolean(value ?? field.default)} onChange={event => onChange(event.target.checked)} />{fieldLabel}</label>
  if (inputType.includes('int') || inputType.includes('float') || inputType.includes('number')) return <Field label={fieldLabel} hint={fieldDesc}><input className="input" type="number" value={Number(value ?? field.default ?? 0)} onChange={event => onChange(Number(event.target.value))} /></Field>
  if (inputType.includes('file')) {
    const selectedFileName = fileNameFromValue(value)
    const handleFile = (file?: File) => {
      if (!file) return
      setFileError('')
      if (maxBytes > 0 && file.size > maxBytes) {
        setFileError(`File is too large. Maximum size is ${Math.round(maxBytes / (1024 * 1024))} MiB.`)
        if (fileInputRef.current) fileInputRef.current.value = ''
        return
      }
      try {
        onChange(file)
      } catch {
        setFileError('The selected file could not be read.')
        if (fileInputRef.current) fileInputRef.current.value = ''
      }
    }
    return <Field label={fieldLabel} hint={fieldDesc}>
      <input ref={fileInputRef} className="input" type="file" accept={fileAccept(field)} onChange={event => void handleFile(event.target.files?.[0])} />
      {selectedFileName && <div className="pipeline-file-selection"><small>Selected: {selectedFileName}</small><button type="button" className="button button--ghost" onClick={() => { onChange(''); if (fileInputRef.current) fileInputRef.current.value = '' }}>Clear</button></div>}
      {fileError && <small className="form-error" role="alert">{fileError}</small>}
    </Field>
  }
  return <Field label={fieldLabel} hint={fieldDesc}><input className="input" value={String(value ?? field.default ?? '')} onChange={event => onChange(event.target.value)} /></Field>
}

