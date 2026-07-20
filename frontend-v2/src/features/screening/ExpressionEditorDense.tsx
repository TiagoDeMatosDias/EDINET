import { X } from 'lucide-react'

import { newExpressionCriterion } from './expression-model'
import { MetricSelect } from './MetricSelect'
import type { Criterion, ExpressionToken, MetricCatalog } from './types'

const ARITHMETIC = ['+', '-', '*', '/'] as const
const COMPARISONS = ['>', '>=', '<', '<=', '=', '!=', 'IS', 'IS NOT']

function numericValue(value: string) {
  const trimmed = value.trim()
  return trimmed !== '' && Number.isFinite(Number(trimmed)) ? Number(trimmed) : trimmed
}

function Token({ token, catalog, onChange, onRemove }: { token: ExpressionToken; catalog: MetricCatalog; onChange: (token: ExpressionToken) => void; onRemove: () => void }) {
  return <span className={`expr-token expr-token--${token.type}`}>{token.type === 'column' && <MetricSelect catalog={catalog} table={token.table} column={token.column} label="Expression metric" onChange={(table, column) => onChange({ type: 'column', table, column })} />}{token.type === 'value' && <input className="expr-value" value={String(token.value ?? '')} onChange={event => onChange({ type: 'value', value: numericValue(event.target.value) })} aria-label="Expression value" />}{token.type === 'op' && <select className="expr-op" value={token.op} onChange={event => onChange({ type: 'op', op: event.target.value as typeof ARITHMETIC[number] })}>{ARITHMETIC.map(operator => <option key={operator}>{operator}</option>)}</select>}<button className="expr-remove" onClick={onRemove} aria-label="Remove expression token"><X /></button></span>
}

function TokenList({ value, catalog, onChange, label }: { value: ExpressionToken[]; catalog: MetricCatalog; onChange: (tokens: ExpressionToken[]) => void; label: string }) {
  const replace = (index: number, token: ExpressionToken) => onChange(value.map((item, itemIndex) => itemIndex === index ? token : item))
  const append = (kind: string) => {
    if (kind === 'column') onChange([...value, { type: 'column', table: '', column: '' }])
    if (kind === 'value') onChange([...value, { type: 'value', value: 0 }])
    if (kind === 'op') onChange([...value, { type: 'op', op: '*' }])
  }
  return <div className="expression-side"><span className="expression-label">{label}</span><div className="expression-tokens">{value.map((token, index) => <Token key={`${index}-${token.type}`} token={token} catalog={catalog} onChange={next => replace(index, next)} onRemove={() => onChange(value.filter((_, itemIndex) => itemIndex !== index))} />)}<select className="expression-add-select" value="" onChange={event => append(event.target.value)} aria-label={`Add ${label.toLowerCase()} expression token`}><option value="">+ Add</option><option value="column">Metric</option><option value="value">Value</option><option value="op">Math</option></select></div></div>
}

function changeKind(criterion: Criterion, kind: string): Criterion {
  if (kind === 'full_expression') return { ...newExpressionCriterion(), id: criterion.id }
  if (kind === 'like') return { id: criterion.id, table: 'CompanyInfo', column: 'Company_Industry', operator: 'LIKE', value: '%', comparison_mode: 'like' }
  if (kind === 'in') return { id: criterion.id, table: 'CompanyInfo', column: 'Company_Industry', operator: 'IN', values: [''], comparison_mode: 'in' }
  return { id: criterion.id, table: 'Stock_Prices', column: 'Price', operator: 'BETWEEN', value: 0, value2: 1000, comparison_mode: 'fixed' }
}

function SimpleCriterion({ criterion, catalog, onChange }: { criterion: Criterion; catalog: MetricCatalog; onChange: (next: Criterion) => void }) {
  return <div className="simple-rule"><MetricSelect catalog={catalog} table={criterion.table ?? ''} column={criterion.column ?? ''} label="Rule metric" onChange={(table, column) => onChange({ ...criterion, table, column })} /><strong>{criterion.operator}</strong>{criterion.operator === 'IN' && <input className="input" value={(criterion.values ?? []).join(', ')} onChange={event => onChange({ ...criterion, values: event.target.value.split(',').map(numericValue) })} placeholder="Value 1, Value 2" />}{criterion.operator === 'LIKE' && <input className="input" value={String(criterion.value ?? '')} onChange={event => onChange({ ...criterion, value: event.target.value })} placeholder="%text%" />}{criterion.operator === 'BETWEEN' && <><input className="input" value={String(criterion.value ?? '')} onChange={event => onChange({ ...criterion, value: numericValue(event.target.value) })} /><span>and</span><input className="input" value={String(criterion.value2 ?? '')} onChange={event => onChange({ ...criterion, value2: numericValue(event.target.value) })} /></>}</div>
}

export function CriterionEditor({ criterion, catalog, index, onChange, onRemove }: { criterion: Criterion; catalog: MetricCatalog; index: number; onChange: (next: Criterion) => void; onRemove: () => void }) {
  const expression = criterion.comparison_mode === 'full_expression'
  const kind = expression ? 'full_expression' : criterion.operator === 'BETWEEN' ? 'between' : criterion.comparison_mode
  return <div className="criterion-editor"><div className="criterion-toolbar"><span>{index + 1}</span><select value={kind} onChange={event => onChange(changeKind(criterion, event.target.value))}><option value="full_expression">Expression</option><option value="like">Text contains</option><option value="in">One of</option><option value="between">Between</option></select><button className="icon-button" onClick={onRemove} aria-label={`Remove rule ${index + 1}`}><X /></button></div>{expression ? <div className="expression-rule"><TokenList label="Left" value={criterion.left_side ?? []} catalog={catalog} onChange={left_side => onChange({ ...criterion, left_side })} /><select className="comparison-select" value={criterion.operator} onChange={event => onChange({ ...criterion, operator: event.target.value })}>{COMPARISONS.map(operator => <option key={operator}>{operator}</option>)}</select><TokenList label="Right" value={criterion.right_side ?? []} catalog={catalog} onChange={right_side => onChange({ ...criterion, right_side })} /></div> : <SimpleCriterion criterion={criterion} catalog={catalog} onChange={onChange} />}</div>
}
