import { useState, type ReactNode, type Ref } from 'react'

/**
 * A compact numeric input that keeps what is typed while focused and shows the
 * rounded value otherwise. ``scale: 100`` edits a fraction as a percentage.
 * ↑ and ↓ step the value (Shift for ten steps).
 */
export function NumberField({ label, value, onChange, step = 1, scale = 1, digits = 2, min, max, suffix, hint, inputRef, children, className = '' }: {
  label: string
  value: number | null
  onChange: (value: number) => void
  step?: number
  scale?: number
  digits?: number
  min?: number
  max?: number
  suffix?: string
  hint?: ReactNode
  inputRef?: Ref<HTMLInputElement>
  children?: ReactNode
  className?: string
}) {
  const [text, setText] = useState<string | null>(null)
  const clamp = (next: number) => Math.min(max ?? Infinity, Math.max(min ?? -Infinity, next))
  const shown = text ?? (value == null || !Number.isFinite(value) ? '' : String(Number((value * scale).toFixed(digits))))
  const commit = (raw: string) => {
    const parsed = Number(raw.replace(/,/g, '').trim())
    if (raw.trim() !== '' && Number.isFinite(parsed)) onChange(clamp(parsed / scale))
  }
  return <label className={`rs-field ${className}`}>
    <span className="rs-field__label">{label}</span>
    <span className="rs-field__control">
      <input
        ref={inputRef}
        className="input"
        aria-label={label}
        inputMode="decimal"
        value={shown}
        onChange={event => { setText(event.target.value); commit(event.target.value) }}
        onBlur={() => setText(null)}
        onKeyDown={event => {
          if (event.key === 'Escape') { setText(null); event.currentTarget.blur(); return }
          if (event.key === 'Enter') { setText(null); return }
          if (event.key !== 'ArrowUp' && event.key !== 'ArrowDown') return
          event.preventDefault()
          const base = value ?? 0
          const delta = (event.key === 'ArrowUp' ? 1 : -1) * step * (event.shiftKey ? 10 : 1)
          setText(null)
          onChange(clamp(Number((base + delta / scale).toFixed(10))))
        }}
      />
      {suffix && <span className="rs-field__suffix">{suffix}</span>}
      {children}
    </span>
    {hint && <small className="rs-field__hint">{hint}</small>}
  </label>
}
