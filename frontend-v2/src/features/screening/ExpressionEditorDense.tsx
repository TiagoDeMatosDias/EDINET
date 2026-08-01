import { X } from 'lucide-react'

import { newExpressionCriterion, newRecentSplitCriterion } from './expression-model'
import { MetricSelect } from './MetricSelect'
import type { Criterion, ExpressionToken, MetricCatalog } from './types'

const ARITHMETIC = ['+', '-', '*', '/'] as const
const COMPARISONS = ['>', '>=', '<', '<=', '=', '!=', 'IN', 'IS', 'IS NOT']

function Token({ token, catalog, tagNames, valueType, onChange, onRemove }: { token: ExpressionToken; catalog: MetricCatalog; tagNames: string[]; valueType?: 'date'; onChange: (token: ExpressionToken) => void; onRemove: () => void }) {
  return <span className={`expr-token expr-token--${token.type}`}>
    {token.type === 'column' && <MetricSelect catalog={catalog} table={token.table} column={token.column} label="Expression metric" onChange={(table, column) => onChange({ type: 'column', table, column })} />}
    {token.type === 'value' && <input className="expr-value" type={valueType === 'date' ? 'date' : 'text'} inputMode={valueType === 'date' ? undefined : 'decimal'} value={String(token.value ?? '')} onChange={event => onChange({ type: 'value', value: event.target.value })} aria-label={valueType === 'date' ? 'Expression date value' : 'Expression value'} />}
    {token.type === 'tag' && <select className="expr-tag" value={token.value} onChange={event => onChange({ type: 'tag', value: event.target.value })} aria-label="Tag value"><option value="">— tag —</option>{tagNames.map(t => <option key={t} value={t}>{t}</option>)}</select>}
    {token.type === 'op' && <select className="expr-op" value={token.op} onChange={event => onChange({ type: 'op', op: event.target.value as typeof ARITHMETIC[number] })}>{ARITHMETIC.map(operator => <option key={operator}>{operator}</option>)}</select>}
    {token.type === 'paren' && <span className="expr-paren" aria-label={token.value === '(' ? 'Open parenthesis' : 'Close parenthesis'}>{token.value}</span>}
    <button className="expr-remove" type="button" onClick={onRemove} aria-label="Remove expression token"><X /></button>
  </span>
}

export function ExpressionTokenList({ value, catalog, tagNames, valueType, onChange, label }: { value: ExpressionToken[]; catalog: MetricCatalog; tagNames: string[]; valueType?: 'date'; onChange: (tokens: ExpressionToken[]) => void; label: string }) {
  const replace = (index: number, token: ExpressionToken) => onChange(value.map((item, itemIndex) => itemIndex === index ? token : item))
  const append = (kind: string) => {
    if (kind === 'column') onChange([...value, { type: 'column', table: '', column: '' }])
    if (kind === 'value' || kind === 'date') onChange([...value, { type: 'value', value: valueType === 'date' ? '' : 0 }])
    if (kind === 'tag') onChange([...value, { type: 'tag', value: tagNames[0] ?? '' }])
    if (kind === 'op') onChange([...value, { type: 'op', op: '*' }])
    if (kind === 'lparen') onChange([...value, { type: 'paren', value: '(' }])
    if (kind === 'rparen') onChange([...value, { type: 'paren', value: ')' }])
  }
  return <div className="expression-side">
    <span className="expression-label">{label}</span>
    <div className="expression-tokens">
      {value.map((token, index) => <Token key={`${index}-${token.type}`} token={token} catalog={catalog} tagNames={tagNames} valueType={valueType} onChange={next => replace(index, next)} onRemove={() => onChange(value.filter((_, itemIndex) => itemIndex !== index))} />)}
      <select className="expression-add-select" value="" onChange={event => append(event.target.value)} aria-label={`Add ${label.toLowerCase()} expression token`}>
        <option value="">+ Add</option>
        <option value="column">Metric</option>
        <option value="value">{valueType === 'date' ? 'Date' : 'Value'}</option>
        <option value="tag">Tag</option>
        <option value="op">Math</option>
        <option value="lparen">(</option>
        <option value="rparen">)</option>
      </select>
    </div>
  </div>
}

const SIMPLE_COMPARISONS = ['>', '>=', '<', '<=', '=', '!=', 'BETWEEN', 'IN', 'LIKE', 'IS', 'IS NOT']

function changeKind(criterion: Criterion, kind: string): Criterion {
  if (kind === 'recent_split') return { ...newRecentSplitCriterion(), id: criterion.id }
  if (kind === 'full_expression') return { ...newExpressionCriterion(), id: criterion.id }
  if (kind === 'like') return { id: criterion.id, table: 'CompanyInfo', column: 'Company_Industry', operator: 'LIKE', value: '%', comparison_mode: 'like' }
  if (kind === 'in') return { id: criterion.id, table: 'CompanyInfo', column: 'Company_Industry', operator: 'IN', values: [''], comparison_mode: 'in' }
  if (kind === 'between') {
    return {
      ...criterion,
      table: criterion.table || 'Stock_Prices',
      column: criterion.column || 'Price',
      operator: 'BETWEEN',
      value: criterion.value ?? 0,
      value2: criterion.value2 ?? 1000,
      comparison_mode: 'fixed',
    }
  }
  if (kind === 'fixed') {
    return {
      ...criterion,
      id: criterion.id,
      table: criterion.table || 'Stock_Prices',
      column: criterion.column || 'Price',
      operator: criterion.operator === 'BETWEEN' ? '>' : criterion.operator || '>',
      value: criterion.value ?? 0,
      comparison_mode: 'fixed',
    }
  }
  return { id: criterion.id, table: 'Stock_Prices', column: 'Price', operator: '>', value: 0, comparison_mode: 'fixed' }
}

function isDateMetric(criterion: Criterion) {
  return criterion.table === 'Stock_Splits' && ['split_date', 'announced_at', 'ex_date', 'effective_date', 'record_date'].includes(criterion.column ?? '')
}

function containsStockSplitDate(tokens: ExpressionToken[] | undefined) {
  return tokens?.some(token => token.type === 'column' && token.table === 'Stock_Splits' && ['split_date', 'announced_at', 'ex_date', 'effective_date', 'record_date'].includes(token.column)) ?? false
}

function SimpleCriterion({ criterion, catalog, onChange }: { criterion: Criterion; catalog: MetricCatalog; onChange: (next: Criterion) => void }) {
  const dateMetric = isDateMetric(criterion)
  const inputType = dateMetric ? 'date' : criterion.field_type === 'num' ? 'number' : 'text'
  const parseValue = (value: string) => inputType === 'number' && value !== '' ? Number(value) : value
  const input = (field: 'value' | 'value2', label: string) => <input className="input" type={inputType} aria-label={label} value={String(criterion[field] ?? '')} onChange={event => onChange({ ...criterion, [field]: parseValue(event.target.value) })} />
  const onMetricChange = (table: string, column: string) => onChange({ ...criterion, table, column, field_type: table === 'Stock_Splits' && column.endsWith('_date') ? 'date' : criterion.field_type === 'date' ? 'text' : criterion.field_type })
  return <div className="simple-rule"><MetricSelect catalog={catalog} table={criterion.table ?? ''} column={criterion.column ?? ''} label="Rule metric" onChange={onMetricChange} /><select className="comparison-select" value={criterion.operator} onChange={event => onChange({ ...criterion, operator: event.target.value })} aria-label="Rule comparison">{SIMPLE_COMPARISONS.map(operator => <option key={operator}>{operator}</option>)}</select>{criterion.operator === 'IN' && <input className="input" value={(criterion.values ?? []).join(', ')} onChange={event => onChange({ ...criterion, values: event.target.value.split(',') })} placeholder="Value 1, Value 2" />}{criterion.operator === 'LIKE' && <input className="input" value={String(criterion.value ?? '')} onChange={event => onChange({ ...criterion, value: event.target.value })} placeholder="%text%" />}{criterion.operator !== 'IN' && criterion.operator !== 'LIKE' && criterion.operator !== 'IS' && criterion.operator !== 'IS NOT' && criterion.operator === 'BETWEEN' && <>{input('value', dateMetric ? 'Start date' : 'Minimum value')}<span>and</span>{input('value2', dateMetric ? 'End date' : 'Maximum value')}</>}{criterion.operator !== 'IN' && criterion.operator !== 'LIKE' && criterion.operator !== 'IS' && criterion.operator !== 'IS NOT' && criterion.operator !== 'BETWEEN' && input('value', dateMetric ? 'Filter date' : 'Filter value')}</div>
}

function RecentSplitCriterion({ criterion, onChange }: { criterion: Criterion; onChange: (next: Criterion) => void }) {
  const action = criterion.split_action === 'include' ? 'include' : 'exclude'
  const status = ['confirmed', 'rejected', 'pending', 'any'].includes(criterion.split_status ?? '') ? criterion.split_status : 'confirmed'
  const dateOperator = criterion.split_date_operator === 'on_or_before' ? 'on_or_before' : 'on_or_after'
  return <div className="simple-rule recent-split-rule">
    <select className="input" aria-label="Split match action" value={action} onChange={event => onChange({ ...criterion, split_action: event.target.value })}>
      <option value="exclude">Exclude</option>
      <option value="include">Include</option>
    </select>
    <span>companies with a</span>
    <select className="input" aria-label="Split confirmation status" value={status} onChange={event => onChange({ ...criterion, split_status: event.target.value })}>
      <option value="confirmed">Confirmed</option>
      <option value="rejected">Rejected</option>
      <option value="pending">Pending</option>
      <option value="any">Any</option>
    </select>
    <span>split</span>
    <select className="input" aria-label="Split date comparison" value={dateOperator} onChange={event => onChange({ ...criterion, split_date_operator: event.target.value })}>
      <option value="on_or_after">on or after</option>
      <option value="on_or_before">on or before</option>
    </select>
    <input className="input" type="date" aria-label="Recent split cutoff date" value={String(criterion.value ?? '')} onChange={event => onChange({ ...criterion, value: event.target.value, field_type: 'date' })} />
  </div>
}

export function CriterionEditor({ criterion, catalog, tagNames, index, onChange, onRemove }: { criterion: Criterion; catalog: MetricCatalog; tagNames: string[]; index: number; onChange: (next: Criterion) => void; onRemove: () => void }) {
  const recentSplit = criterion.comparison_mode === 'recent_split'
  const expression = criterion.comparison_mode === 'full_expression'
  const leftDateExpression = expression && containsStockSplitDate(criterion.left_side)
  const rightDateExpression = expression && containsStockSplitDate(criterion.right_side)
  const kind = recentSplit
    ? 'recent_split'
    : expression
      ? 'full_expression'
      : criterion.operator === 'BETWEEN'
        ? 'between'
        : criterion.comparison_mode === 'like' || criterion.comparison_mode === 'in'
          ? criterion.comparison_mode
          : 'fixed'
  return <div className="criterion-editor"><div className="criterion-toolbar"><span>{index + 1}</span><select value={kind} aria-label="Rule type" onChange={event => onChange(changeKind(criterion, event.target.value))}><option value="full_expression">Expression</option><option value="recent_split">No recent split</option><option value="fixed">Filter</option><option value="like">Text contains</option><option value="in">One of</option><option value="between">Between</option></select><button className="icon-button" type="button" onClick={onRemove} aria-label={`Remove rule ${index + 1}`}><X /></button></div>{recentSplit ? <RecentSplitCriterion criterion={criterion} onChange={onChange} /> : expression ? <div className="expression-rule"><ExpressionTokenList label="Left" value={criterion.left_side ?? []} catalog={catalog} tagNames={tagNames} valueType={rightDateExpression ? 'date' : undefined} onChange={left_side => onChange({ ...criterion, left_side })} /><select className="comparison-select" value={criterion.operator} onChange={event => onChange({ ...criterion, operator: event.target.value })}>{COMPARISONS.map(operator => <option key={operator}>{operator}</option>)}</select><ExpressionTokenList label="Right" value={criterion.right_side ?? []} catalog={catalog} tagNames={tagNames} valueType={leftDateExpression ? 'date' : undefined} onChange={right_side => onChange({ ...criterion, right_side })} /></div> : <SimpleCriterion criterion={criterion} catalog={catalog} onChange={onChange} />}</div>
}
