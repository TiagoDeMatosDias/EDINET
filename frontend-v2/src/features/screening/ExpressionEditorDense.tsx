import { X } from 'lucide-react'
import { useState } from 'react'

import { metricKey, type MetricOption } from './metricCatalog'
import { MetricPicker } from './MetricPicker'
import type { Criterion, ExpressionToken } from './types'

const ARITHMETIC = ['+', '-', '*', '/'] as const
const ARITHMETIC_LABELS: Record<typeof ARITHMETIC[number], string> = { '+': '+', '-': '−', '*': '×', '/': '÷' }

function Token({ token, options, tagNames, valueType, autoOpen, onChange, onRemove }: { token: ExpressionToken; options: MetricOption[]; tagNames: string[]; valueType?: 'date'; autoOpen?: boolean; onChange: (token: ExpressionToken) => void; onRemove: () => void }) {
  const chosen = token.type === 'column' ? options.find(option => option.key === metricKey(token.table, token.column)) ?? (token.table && token.column ? { key: metricKey(token.table, token.column), label: token.column, tableLabel: token.table, order: 99, search: '' } : undefined) : undefined
  return <span className={`expr-token expr-token--${token.type}`}>
    {token.type === 'column' && <MetricPicker compact autoOpen={autoOpen} options={options.filter(option => !option.preset)} value={chosen} label="Expression metric" onSelect={option => onChange({ type: 'column', table: option.table ?? '', column: option.column ?? '' })} />}
    {token.type === 'value' && <input className="expr-value" type={valueType === 'date' ? 'date' : 'text'} inputMode={valueType === 'date' ? undefined : 'decimal'} value={String(token.value ?? '')} onChange={event => onChange({ type: 'value', value: event.target.value })} aria-label={valueType === 'date' ? 'Expression date value' : 'Expression value'} />}
    {token.type === 'tag' && <select className="expr-tag" value={token.value} onChange={event => onChange({ type: 'tag', value: event.target.value })} aria-label="Tag value"><option value="">— tag —</option>{tagNames.map(t => <option key={t} value={t}>{t}</option>)}</select>}
    {token.type === 'op' && <select className="expr-op" value={token.op} aria-label="Arithmetic operator" onChange={event => onChange({ type: 'op', op: event.target.value as typeof ARITHMETIC[number] })}>{ARITHMETIC.map(operator => <option key={operator} value={operator}>{ARITHMETIC_LABELS[operator]}</option>)}</select>}
    {token.type === 'paren' && <span className="expr-paren" aria-label={token.value === '(' ? 'Open parenthesis' : 'Close parenthesis'}>{token.value}</span>}
    <button className="expr-remove" type="button" onClick={onRemove} aria-label="Remove expression token"><X /></button>
  </span>
}

/**
 * Arithmetic over metrics, values, and tags with explicit parentheses. Metrics
 * use the searchable picker; a freshly added metric opens it straight away.
 */
export function ExpressionTokenList({ value, options, tagNames, valueType, onChange, label }: { value: ExpressionToken[]; options: MetricOption[]; tagNames: string[]; valueType?: 'date'; onChange: (tokens: ExpressionToken[]) => void; label: string }) {
  const [openIndex, setOpenIndex] = useState<number | null>(null)
  const replace = (index: number, token: ExpressionToken) => onChange(value.map((item, itemIndex) => itemIndex === index ? token : item))
  const append = (kind: string) => {
    if (kind === 'column') { setOpenIndex(value.length); onChange([...value, { type: 'column', table: '', column: '' }]) }
    if (kind === 'value' || kind === 'date') onChange([...value, { type: 'value', value: valueType === 'date' ? '' : 0 }])
    if (kind === 'tag') onChange([...value, { type: 'tag', value: tagNames[0] ?? '' }])
    if (kind === 'op') onChange([...value, { type: 'op', op: '*' }])
    if (kind === 'lparen') onChange([...value, { type: 'paren', value: '(' }])
    if (kind === 'rparen') onChange([...value, { type: 'paren', value: ')' }])
  }
  return <div className="expression-side">
    <span className="expression-label">{label}</span>
    <div className="expression-tokens">
      {value.map((token, index) => <Token key={`${index}-${token.type}`} token={token} options={options} tagNames={tagNames} valueType={valueType} autoOpen={index === openIndex} onChange={next => replace(index, next)} onRemove={() => onChange(value.filter((_, itemIndex) => itemIndex !== index))} />)}
      <select className="expression-add-select" value="" onChange={event => append(event.target.value)} aria-label={`Add ${label.toLowerCase()} expression token`} title="Add a metric, value, operator, or parenthesis">
        <option value="">+ Add</option>
        <option value="column">Metric</option>
        <option value="value">{valueType === 'date' ? 'Date' : 'Value'}</option>
        <option value="tag">Tag</option>
        <option value="op">Math (+ − × ÷)</option>
        <option value="lparen">(</option>
        <option value="rparen">)</option>
      </select>
    </div>
  </div>
}

const SPLIT_STATUS_OPTIONS: Array<{ value: string; label: string; hint: string }> = [
  { value: 'confirmed', label: 'Confirmed', hint: 'Verified against share counts — safe to filter on' },
  { value: 'pending', label: 'Pending', hint: 'Detected by price heuristics, not yet verified' },
  { value: 'rejected', label: 'Rejected', hint: 'Detection false positive' },
  { value: 'any', label: 'Any', hint: 'Ignore verification status' },
]

export function RecentSplitCriterion({ criterion, onChange }: { criterion: Criterion; onChange: (next: Criterion) => void }) {
  const [advanced, setAdvanced] = useState(false)
  const action = criterion.split_action === 'include' ? 'include' : 'exclude'
  const status = SPLIT_STATUS_OPTIONS.some(option => option.value === criterion.split_status) ? criterion.split_status : 'confirmed'
  const dateOperator = criterion.split_date_operator === 'on_or_before' ? 'on_or_before' : 'on_or_after'
  const rawWindow = Number(criterion.split_window_days)
  const windowed = Number.isFinite(rawWindow) && rawWindow >= 1
  const windowDays = windowed ? rawWindow : 365
  const setWindowDays = (raw: string) => {
    const days = Math.floor(Number(raw))
    onChange({ ...criterion, split_window_days: Number.isFinite(days) && days >= 1 ? days : null })
  }
  return <div className="recent-split">
    <div className="simple-rule recent-split-rule">
      <select className="input" aria-label="Split match action" value={action} onChange={event => onChange({ ...criterion, split_action: event.target.value })}>
        <option value="exclude">Exclude</option>
        <option value="include">Only</option>
      </select>
      <span>companies with a split</span>
      {windowed ? <>
        <span>in the last</span>
        <input className="input" type="number" min={1} aria-label="Split window days" title="Counted back from the as-of date, or today when no as-of date is set" value={windowDays} onChange={event => setWindowDays(event.target.value)} />
        <span>days</span>
      </> : <>
        <select className="input" aria-label="Split date comparison" value={dateOperator} onChange={event => onChange({ ...criterion, split_date_operator: event.target.value })}>
          <option value="on_or_after">on or after</option>
          <option value="on_or_before">on or before</option>
        </select>
        <input className="input" type="date" aria-label="Recent split cutoff date" value={String(criterion.value ?? '')} onChange={event => onChange({ ...criterion, value: event.target.value, field_type: 'date' })} />
      </>}
      <button type="button" className="text-button recent-split-toggle" aria-expanded={advanced} onClick={() => setAdvanced(value => !value)}>Advanced</button>
    </div>
    {advanced && <div className="recent-split-advanced">
      <label>Split status
        <select className="input" aria-label="Split confirmation status" value={status} onChange={event => onChange({ ...criterion, split_status: event.target.value })}>
          {SPLIT_STATUS_OPTIONS.map(option => <option key={option.value} value={option.value} title={option.hint}>{option.label}</option>)}
        </select>
      </label>
      <label>Date match
        <select className="input" aria-label="Split date mode" value={windowed ? 'window' : 'exact'} onChange={event => onChange({ ...criterion, split_window_days: event.target.value === 'window' ? 365 : null })}>
          <option value="window">Within a window of the as-of date</option>
          <option value="exact">Exact cutoff date</option>
        </select>
      </label>
    </div>}
  </div>
}
