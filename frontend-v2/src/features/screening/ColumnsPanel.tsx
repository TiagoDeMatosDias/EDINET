import { ArrowDown, ArrowUp, FunctionSquare, Plus, RotateCcw, Trash2, X } from 'lucide-react'
import { useEffect, useState } from 'react'

import { ExpressionTokenList } from './ExpressionEditorDense'
import { columnLabel, presetColumn, tableInfo, type MetricOption } from './metricCatalog'
import { MetricPicker } from './MetricPicker'
import { expressionProblem } from './ruleModel'
import { COLUMN_SETS, defaultOutput, mergeColumnSet } from './screenDefaults'
import type { ComputedColumn, ExpressionToken, MetricCatalog } from './types'

function formulaTokens(column: ComputedColumn): ExpressionToken[] {
  if (column.expression_tokens) return column.expression_tokens
  if (column.formula_type === 'price_ratio' && column.numerator_table && column.numerator_column && column.denominator_table && column.denominator_column) {
    return [
      { type: 'column', table: column.numerator_table, column: column.numerator_column },
      { type: 'op', op: '/' },
      { type: 'column', table: column.denominator_table, column: column.denominator_column },
    ]
  }
  return []
}

/** Derived output columns: a name, a formula over metrics, and how to show the result. */
export function DerivedColumns({ value, options, onChange }: { value: ComputedColumn[]; options: MetricOption[]; onChange: (next: ComputedColumn[]) => void }) {
  const [editing, setEditing] = useState<number | null>(null)
  const update = (index: number, patch: Partial<ComputedColumn>) => onChange(value.map((item, itemIndex) => itemIndex === index ? { ...item, ...patch } : item))
  return <ul className="derived-list">
    {value.map((column, index) => {
      const tokens = formulaTokens(column)
      const problem = expressionProblem(tokens, 'formula')
      const open = editing === index || Boolean(problem)
      return <li className="derived-column" key={`derived-${index}`}>
        <div className="derived-column__head">
          <FunctionSquare aria-hidden="true" />
          <input className="input" aria-label="Derived column name" value={column.name} onChange={event => update(index, { name: event.target.value })} />
          <select aria-label={`Format of ${column.name}`} value={column.format === 'percent' ? 'percent' : 'number'} onChange={event => update(index, { format: event.target.value as 'percent' | 'number' })} title="Percent shows fractions such as yields as percentages">
            <option value="number">Number</option>
            <option value="percent">Percent</option>
          </select>
          <button type="button" className="text-button" aria-expanded={open} onClick={() => setEditing(open ? null : index)}>{open ? 'Done' : 'Formula'}</button>
          <button className="icon-button" type="button" aria-label={`Remove derived column ${column.name}`} onClick={() => onChange(value.filter((_, itemIndex) => itemIndex !== index))}><Trash2 /></button>
        </div>
        {open && <ExpressionTokenList label="Formula" value={tokens} options={options} tagNames={[]} onChange={expression_tokens => update(index, { formula_type: 'expression', expression_tokens, formula: null })} />}
        {problem && <p className="form-error">{problem}</p>}
      </li>
    })}
  </ul>
}

function blankDerived(): ComputedColumn {
  return { name: 'Derived metric', formula_type: 'expression', format: 'number', expression_tokens: [{ type: 'column', table: 'Stock_Prices', column: 'Price' }, { type: 'op', op: '/' }, { type: 'column', table: '', column: '' }] }
}

/**
 * What the results show: plain columns (ordered, removable), derived formulas,
 * and column sets that add a theme's metrics in one step.
 */
export function ColumnsPanel({ catalog, options, columns, computed, onChange, onClose }: {
  catalog: MetricCatalog; options: MetricOption[]; columns: string[]; computed: ComputedColumn[]
  onChange: (next: { columns: string[]; computed: ComputedColumn[] }) => void; onClose: () => void
}) {
  useEffect(() => {
    const onKey = (event: KeyboardEvent) => { if (event.key === 'Escape' && !(event.target as HTMLElement).closest('.metric-picker__popover')) onClose() }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [onClose])
  const move = (index: number, delta: number) => {
    const next = [...columns]
    const target = index + delta
    if (target < 0 || target >= next.length) return
    ;[next[index], next[target]] = [next[target], next[index]]
    onChange({ columns: next, computed })
  }
  const add = (option: MetricOption) => {
    if (option.preset) {
      if (computed.some(column => column.name === option.preset!.label)) return
      onChange({ columns, computed: [...computed, presetColumn(option.preset)] })
    } else if (option.table && option.column && !columns.includes(option.key)) {
      onChange({ columns: [...columns, option.key], computed })
    }
  }
  return <div className="columns-drawer" role="dialog" aria-modal="false" aria-labelledby="columns-drawer-title">
    <header className="columns-drawer__head">
      <h2 id="columns-drawer-title">Output columns <small>{columns.length + computed.length}</small></h2>
      <button type="button" className="icon-button" aria-label="Close output columns" onClick={onClose}><X /></button>
    </header>
    <section>
      <h3>Add a column</h3>
      <MetricPicker options={options} label="Add a column" placeholder="Search metrics and formulas" onSelect={add} />
      <div className="column-sets" role="group" aria-label="Column sets">
        {COLUMN_SETS.map(set => <button key={set.id} type="button" className="column-set" title={set.hint} onClick={() => onChange(mergeColumnSet({ columns, computed }, set.id, catalog))}><Plus aria-hidden="true" />{set.label}</button>)}
        <button type="button" className="text-button" onClick={() => onChange(defaultOutput(catalog))} title="Replace the columns with the default overview"><RotateCcw aria-hidden="true" />Reset</button>
      </div>
    </section>
    <section>
      <h3>Shown <small>in order; the company name always comes first</small></h3>
      <ol className="column-list">
        {columns.map((reference, index) => {
          const [table, ...rest] = reference.split('.')
          const name = rest.join('.')
          return <li key={reference}>
            <span><strong>{columnLabel(table, name)}</strong><small>{tableInfo(table).label}</small></span>
            <button type="button" className="icon-button" aria-label={`Move ${columnLabel(table, name)} up`} disabled={index === 0} onClick={() => move(index, -1)}><ArrowUp /></button>
            <button type="button" className="icon-button" aria-label={`Move ${columnLabel(table, name)} down`} disabled={index === columns.length - 1} onClick={() => move(index, 1)}><ArrowDown /></button>
            <button type="button" className="icon-button" aria-label={`Remove ${columnLabel(table, name)}`} onClick={() => onChange({ columns: columns.filter(item => item !== reference), computed })}><X /></button>
          </li>
        })}
      </ol>
    </section>
    <section>
      <h3>Derived <small>formulas computed for each company</small></h3>
      <DerivedColumns value={computed} options={options} onChange={next => onChange({ columns, computed: next })} />
      <button type="button" className="button button--ghost button--small" onClick={() => onChange({ columns, computed: [...computed, blankDerived()] })}><Plus aria-hidden="true" />Derived column</button>
    </section>
  </div>
}
