import { AlertTriangle, ArrowRightLeft, Copy, FolderInput, FolderOutput, FunctionSquare, Hash, MoreHorizontal, Plus, Split, X } from 'lucide-react'
import { useEffect, useRef, useState } from 'react'

import { Tip } from '../../components/Tooltip'
import { RecentSplitCriterion, ExpressionTokenList } from './ExpressionEditorDense'
import { newRecentSplitCriterion } from './expression-model'
import { columnLabel, isDateMetric, metricKey, optionForTokens, parsePercentText, toPercentText, tokensFormat, type MetricOption } from './metricCatalog'
import { MetricPicker } from './MetricPicker'
import { useAnchoredPopover } from './useAnchoredPopover'
import { describeRule, newGroupId, operatorsFor, ruleColumn, ruleLeft, ruleProblem, ruleShape, withMetric, withOperator } from './ruleModel'
import type { ColumnFormats, Criterion, CriteriaMatch, ExpressionToken } from './types'

const EXPRESSION_OPERATORS = ['>', '>=', '<', '<=', '=', '!=', 'IN', 'IS', 'IS NOT']
const OPERATOR_TEXT: Record<string, string> = { '>': '>', '>=': '≥', '<': '<', '<=': '≤', '=': '=', '!=': '≠', IN: 'is one of', IS: 'is empty', 'IS NOT': 'has a value' }

/** A number input that keeps half-typed text ("0.", "-") while committing parsed values. */
function ValueInput({ value, onChange, label, percent = false, date = false, placeholder }: { value: unknown; onChange: (value: string | number) => void; label: string; percent?: boolean; date?: boolean; placeholder?: string }) {
  const shown = percent ? toPercentText(value) : String(value ?? '')
  const [draft, setDraft] = useState(shown)
  const [focused, setFocused] = useState(false)
  if (date) return <input className="input rule-value" type="date" aria-label={label} value={String(value ?? '')} onChange={event => onChange(event.target.value)} />
  return <span className={percent ? 'rule-value-wrap is-percent' : 'rule-value-wrap'}>
    <input
      className="input rule-value"
      type="text"
      inputMode="decimal"
      aria-label={label}
      placeholder={placeholder ?? (percent ? '15' : 'value')}
      value={focused ? draft : shown}
      onFocus={() => { setDraft(shown); setFocused(true) }}
      onBlur={() => setFocused(false)}
      onChange={event => {
        setDraft(event.target.value)
        if (!percent) { onChange(event.target.value); return }
        const parsed = parsePercentText(event.target.value)
        if (parsed !== null || event.target.value.trim() === '') onChange(parsed ?? '')
      }}
    />
    {percent && <span className="rule-value__unit" title="Entered as a percentage; stored as a fraction">%</span>}
  </span>
}

function TextInput({ value, onChange, label, placeholder }: { value: string; onChange: (value: string) => void; label: string; placeholder?: string }) {
  return <input className="input rule-value rule-value--text" type="text" aria-label={label} placeholder={placeholder} value={value} onChange={event => onChange(event.target.value)} />
}

function SimpleRule({ criterion, options, formats, tagNames, autoOpen, onChange }: { criterion: Criterion; options: MetricOption[]; formats: ColumnFormats; tagNames: string[]; autoOpen: boolean; onChange: (next: Criterion) => void }) {
  const left = ruleLeft(criterion)
  const column = ruleColumn(criterion)
  const chosen = optionForTokens(options, left) ?? (column?.table && column.column ? { key: metricKey(column.table, column.column), label: columnLabel(column.table, column.column), tableLabel: column.table, order: 99, search: '' } : undefined)
  const percent = tokensFormat(left, formats) === 'percent'
  const date = isDateMetric(column?.table, column?.column)
  const operator = criterion.operator ?? '>'
  const tags = column?.table === 'Company_Tags'
  const right = criterion.comparison_mode === 'full_expression' ? criterion.right_side ?? [] : []
  const rightColumn = right.length === 1 && right[0].type === 'column' ? right[0] : undefined
  const pickMetric = (option: MetricOption) => onChange(withMetric(criterion, option.preset ? option.preset.tokens : [{ type: 'column', table: option.table ?? '', column: option.column ?? '' }]))
  const setRight = (tokens: ExpressionToken[]) => onChange({ ...criterion, right_side: tokens })
  const valueEditor = () => {
    if (operator === 'IS' || operator === 'IS NOT') return null
    if (operator === 'BETWEEN') return <>
      <ValueInput label={date ? 'Start date' : 'Minimum value'} value={criterion.value} date={date} percent={percent} onChange={value => onChange({ ...criterion, value })} />
      <span className="rule-word">and</span>
      <ValueInput label={date ? 'End date' : 'Maximum value'} value={criterion.value2} date={date} percent={percent} onChange={value2 => onChange({ ...criterion, value2 })} />
    </>
    if (operator === 'IN') return <TextInput label="Values, comma separated" placeholder={tags ? 'Watchlist, Owned' : 'Banks, Insurance'} value={(criterion.values ?? []).join(', ')} onChange={text => onChange({ ...criterion, values: text.split(',').map(item => item.trim()) })} />
    if (operator === 'LIKE') return <TextInput label="Text to find" placeholder="text" value={String(criterion.value ?? '').replace(/^%|%$/g, '')} onChange={text => onChange({ ...criterion, value: /[%_]/.test(text) ? text : `%${text}%` })} />
    if (tags) return <select className="input rule-value" aria-label="Tag" value={String(criterion.comparison_mode === 'full_expression' ? (right[0] && 'value' in right[0] ? right[0].value : '') : criterion.value ?? '')} onChange={event => onChange(criterion.comparison_mode === 'full_expression' ? { ...criterion, right_side: [{ type: 'value', value: event.target.value }] } : { ...criterion, value: event.target.value })}>
      <option value="">Choose a tag</option>
      {tagNames.map(tag => <option key={tag} value={tag}>{tag}</option>)}
    </select>
    if (criterion.comparison_mode !== 'full_expression') return <ValueInput label={date ? 'Filter date' : 'Filter value'} date={date} percent={percent} value={criterion.value} onChange={value => onChange({ ...criterion, value })} />
    if (rightColumn) return <MetricPicker compact options={options.filter(option => !option.preset)} value={options.find(option => option.key === metricKey(rightColumn.table, rightColumn.column))} label="Compared metric" onSelect={option => setRight([{ type: 'column', table: option.table ?? '', column: option.column ?? '' }])} />
    const scalar = right[0] && right[0].type === 'value' ? right[0].value : ''
    return <ValueInput label={date ? 'Filter date' : 'Filter value'} date={date} percent={percent} value={scalar} onChange={value => setRight([{ type: 'value', value }])} />
  }
  const canCompareMetric = criterion.comparison_mode === 'full_expression' && !['IS', 'IS NOT', 'IN'].includes(operator) && !tags
  return <div className="simple-rule">
    <MetricPicker options={options} value={chosen} label="Rule metric" autoOpen={autoOpen} onSelect={pickMetric} />
    <select className="rule-operator" aria-label="Rule comparison" value={operator} onChange={event => onChange(withOperator(criterion, event.target.value))} title={operatorsFor(left).find(item => item.value === operator)?.hint}>
      {operatorsFor(left).map(item => <option key={item.value} value={item.value} title={item.hint}>{item.label}</option>)}
    </select>
    {valueEditor()}
    {canCompareMetric && <button type="button" className="icon-button rule-compare" onClick={() => setRight(rightColumn ? [{ type: 'value', value: '' }] : [{ type: 'column', table: '', column: '' }])} title={rightColumn ? 'Compare with a number instead' : 'Compare with another metric instead of a number'} aria-label={rightColumn ? 'Compare with a number' : 'Compare with another metric'}>{rightColumn ? <Hash /> : <ArrowRightLeft />}</button>}
  </div>
}

function ExpressionRule({ criterion, options, tagNames, onChange }: { criterion: Criterion; options: MetricOption[]; tagNames: string[]; onChange: (next: Criterion) => void }) {
  const usesTags = (criterion.left_side ?? []).some(token => token.type === 'column' && token.table === 'Company_Tags')
  const dateOn = (tokens?: ExpressionToken[]) => tokens?.some(token => token.type === 'column' && isDateMetric(token.table, token.column)) ?? false
  const operators = usesTags ? ['=', '!=', 'IN'] : EXPRESSION_OPERATORS
  const updateLeft = (left_side: ExpressionToken[]) => {
    const tags = left_side.some(token => token.type === 'column' && token.table === 'Company_Tags')
    onChange({ ...criterion, left_side, operator: tags && !['=', '!=', 'IN'].includes(criterion.operator ?? '') ? '=' : criterion.operator })
  }
  return <div className="expression-rule">
    <ExpressionTokenList label="Left" value={criterion.left_side ?? []} options={options} tagNames={tagNames} valueType={dateOn(criterion.right_side) ? 'date' : undefined} onChange={updateLeft} />
    <select className="rule-operator" aria-label="Expression comparison" value={criterion.operator} onChange={event => onChange({ ...criterion, operator: event.target.value })}>{operators.map(operator => <option key={operator} value={operator}>{OPERATOR_TEXT[operator] ?? operator}</option>)}</select>
    {criterion.operator !== 'IS' && criterion.operator !== 'IS NOT' && <ExpressionTokenList label="Right" value={criterion.right_side ?? []} options={options} tagNames={tagNames} valueType={dateOn(criterion.left_side) ? 'date' : undefined} onChange={right_side => onChange({ ...criterion, right_side })} />}
  </div>
}

interface GroupInfo { id: string; match: 'any' | 'all'; label: string }

function RuleMenu({ criterion, groups, shape, onDuplicate, onMove, onAsExpression }: { criterion: Criterion; groups: GroupInfo[]; shape: string; onDuplicate: () => void; onMove: (group: GroupInfo | null | 'new') => void; onAsExpression: () => void }) {
  const [open, setOpen] = useState(false)
  const menu = useRef<HTMLDivElement>(null)
  const trigger = useRef<HTMLButtonElement>(null)
  const items = useRef<HTMLDivElement>(null)
  useAnchoredPopover(trigger, items, open, 'right')
  useEffect(() => {
    if (!open) return
    const close = (event: MouseEvent) => { if (!menu.current?.contains(event.target as Node)) setOpen(false) }
    document.addEventListener('mousedown', close)
    return () => document.removeEventListener('mousedown', close)
  }, [open])
  const run = (action: () => void) => { action(); setOpen(false) }
  const otherGroups = groups.filter(group => group.id !== criterion.group)
  return <div ref={menu} className="rule-menu" onKeyDown={event => { if (event.key === 'Escape') setOpen(false) }}>
    <button ref={trigger} type="button" className="icon-button" aria-haspopup="menu" aria-expanded={open} aria-label="More rule actions" title="Duplicate, group, or edit as an expression" onClick={() => setOpen(!open)}><MoreHorizontal /></button>
    {open && <div ref={items} className="rule-menu__items" role="menu">
      <button type="button" role="menuitem" onClick={() => run(onDuplicate)}><Copy aria-hidden="true" />Duplicate</button>
      {shape === 'simple' && criterion.comparison_mode === 'full_expression' && <button type="button" role="menuitem" onClick={() => run(onAsExpression)} title="Turn the metric into an expression you can extend with + − × ÷ and parentheses"><FunctionSquare aria-hidden="true" />Add math to the metric</button>}
      {criterion.group && <button type="button" role="menuitem" onClick={() => run(() => onMove(null))}><FolderOutput aria-hidden="true" />Take out of group</button>}
      {otherGroups.map(group => <button key={group.id} type="button" role="menuitem" onClick={() => run(() => onMove(group))}><FolderInput aria-hidden="true" />Move into {group.label}</button>)}
      {!criterion.group && <button type="button" role="menuitem" onClick={() => run(() => onMove('new'))}><FolderInput aria-hidden="true" />Start a group with this rule</button>}
    </div>}
  </div>
}

function RuleRow({ criterion, number, options, ruleOptions, formats, tagNames, groups, autoOpen, onChange, onRemove, onDuplicate, onMove }: {
  criterion: Criterion; number: string; options: MetricOption[]; ruleOptions: MetricOption[]; formats: ColumnFormats; tagNames: string[]; groups: GroupInfo[]; autoOpen: boolean
  onChange: (next: Criterion) => void; onRemove: () => void; onDuplicate: () => void; onMove: (group: GroupInfo | null | 'new') => void
}) {
  const shape = ruleShape(criterion)
  const problem = ruleProblem(criterion)
  const enabled = criterion.enabled !== false
  const label = (table: string, column: string) => columnLabel(table, column)
  const summary = describeRule(criterion, label, tokens => tokensFormat(tokens, formats) === 'percent')
  const usesSplits = ruleLeft(criterion).some(token => token.type === 'column' && token.table === 'Stock_Splits')
  return <li className={['rule-row', enabled ? '' : 'is-off', problem ? 'has-problem' : ''].filter(Boolean).join(' ')} data-rule-id={criterion.id}>
    <label className="rule-row__toggle" title={enabled ? 'Applied. Untick to switch this rule off without deleting it' : 'Switched off. Tick to apply it again'}>
      <input type="checkbox" checked={enabled} aria-label={`Apply rule ${number}`} onChange={event => onChange({ ...criterion, enabled: event.target.checked ? undefined : false })} />
      <Tip content={summary} focusable={false}><span className="rule-row__number">{number}</span></Tip>
    </label>
    <div className="rule-row__body">
      {shape === 'split' && <RecentSplitCriterion criterion={criterion} onChange={onChange} />}
      {shape === 'simple' && <SimpleRule criterion={criterion} options={usesSplits ? options : ruleOptions} formats={formats} tagNames={tagNames} autoOpen={autoOpen} onChange={onChange} />}
      {shape === 'expression' && <ExpressionRule criterion={criterion} options={options} tagNames={tagNames} onChange={onChange} />}
    </div>
    {problem && <Tip content={problem} className="rule-row__problem"><AlertTriangle aria-label={problem} /></Tip>}
    <RuleMenu criterion={criterion} groups={groups} shape={shape} onDuplicate={onDuplicate} onMove={onMove} onAsExpression={() => onChange({ ...criterion, left_side: [...ruleLeft(criterion), { type: 'op', op: '*' }, { type: 'value', value: 1 }] })} />
    <button type="button" className="icon-button rule-row__remove" aria-label={`Remove rule ${number}`} title="Remove this rule" onClick={onRemove}><X /></button>
  </li>
}

/**
 * Every rule of a screen, combined by ``match``; rules sharing a group form one
 * term combined by the group's own any/all setting. Order and wire shapes are
 * exactly what the screening API runs.
 */
export function RuleBuilder({ criteria, match, options, ruleOptions, formats, tagNames, openRuleId, onChange, onMatchChange, onAddRule }: {
  criteria: Criterion[]; match: CriteriaMatch; options: MetricOption[]; ruleOptions: MetricOption[]; formats: ColumnFormats; tagNames: string[]; openRuleId: string | null
  onChange: (criteria: Criterion[]) => void; onMatchChange: (match: CriteriaMatch) => void; onAddRule: (group?: GroupInfo) => void
}) {
  const groupOrder: string[] = []
  for (const criterion of criteria) if (criterion.group && !groupOrder.includes(criterion.group)) groupOrder.push(criterion.group)
  const groups: GroupInfo[] = groupOrder.map((id, index) => ({ id, match: criteria.find(item => item.group === id)?.group_match === 'all' ? 'all' : 'any', label: `group ${String.fromCharCode(65 + index)}` }))
  const update = (id: string, next: Criterion) => onChange(criteria.map(item => item.id === id ? next : item))
  const remove = (id: string) => onChange(criteria.filter(item => item.id !== id))
  const duplicate = (criterion: Criterion) => {
    const index = criteria.findIndex(item => item.id === criterion.id)
    onChange([...criteria.slice(0, index + 1), { ...structuredClone(criterion), id: crypto.randomUUID() }, ...criteria.slice(index + 1)])
  }
  const move = (criterion: Criterion, target: GroupInfo | null | 'new') => {
    if (target === null) { update(criterion.id, { ...criterion, group: undefined, group_match: undefined }); return }
    const group = target === 'new' ? { id: newGroupId(), match: 'any' as const } : target
    const rest = criteria.filter(item => item.id !== criterion.id)
    const moved = { ...criterion, group: group.id, group_match: group.match }
    const lastMember = rest.map(item => item.group).lastIndexOf(group.id)
    const at = lastMember === -1 ? criteria.findIndex(item => item.id === criterion.id) : lastMember + 1
    onChange([...rest.slice(0, at), moved, ...rest.slice(at)])
  }
  const setGroupMatch = (id: string, groupMatch: 'any' | 'all') => onChange(criteria.map(item => item.group === id ? { ...item, group_match: groupMatch } : item))
  const ungroup = (id: string) => onChange(criteria.map(item => item.group === id ? { ...item, group: undefined, group_match: undefined } : item))
  const removeGroup = (id: string) => onChange(criteria.filter(item => item.group !== id))

  // Number rules in display order: each group's members follow its first member.
  const displayOrder: Criterion[] = []
  for (const criterion of criteria) {
    if (displayOrder.includes(criterion)) continue
    displayOrder.push(...(criterion.group ? criteria.filter(item => item.group === criterion.group) : [criterion]))
  }
  const numbers = new Map(displayOrder.map((criterion, index) => [criterion.id, String(index + 1)]))
  const rendered = new Set<string>()
  const rows = (items: Criterion[]) => items.map(criterion => <RuleRow key={criterion.id} criterion={criterion} number={numbers.get(criterion.id) ?? ''} options={options} ruleOptions={ruleOptions} formats={formats} tagNames={tagNames} groups={groups} autoOpen={criterion.id === openRuleId}
    onChange={next => update(criterion.id, next)} onRemove={() => remove(criterion.id)} onDuplicate={() => duplicate(criterion)} onMove={target => move(criterion, target)} />)
  const terms = criteria.flatMap(criterion => {
    if (!criterion.group) return [rows([criterion])[0]]
    if (rendered.has(criterion.group)) return []
    rendered.add(criterion.group)
    const group = groups.find(item => item.id === criterion.group)!
    const members = criteria.filter(item => item.group === group.id)
    return [<li key={group.id} className="rule-group" aria-label={`Rule ${group.label}`}>
      <header className="rule-group__head">
        <span className="rule-group__name">{group.label}</span>
        <span>matches</span>
        <select aria-label={`How ${group.label} combines`} value={group.match} onChange={event => setGroupMatch(group.id, event.target.value as 'any' | 'all')}>
          <option value="any">any</option>
          <option value="all">all</option>
        </select>
        <span>{members.length === 1 ? 'of this rule' : `of these ${members.length} rules`}</span>
        <span className="rule-builder__spacer" />
        <button type="button" className="text-button" onClick={() => onAddRule(group)}><Plus aria-hidden="true" />Rule in group</button>
        <button type="button" className="text-button" onClick={() => ungroup(group.id)} title="Keep the rules, drop the grouping">Ungroup</button>
        <button type="button" className="text-button text-button--danger" onClick={() => removeGroup(group.id)} title="Delete the group and its rules">Delete group</button>
      </header>
      <ol className="rule-list">{rows(members)}</ol>
    </li>]
  })
  const termCount = terms.length
  return <div className="rule-builder">
    <header className="rule-builder__head">
      <Tip content="All: a company must pass every rule and group. Any: passing one is enough. Groups have their own any/all setting, so screens like A and (B or C), or (A and B) or (C and D), are possible.">
        <span>Companies matching</span>
      </Tip>
      <select aria-label="How rules combine" value={match} onChange={event => onMatchChange(event.target.value as CriteriaMatch)}>
        <option value="all">all</option>
        <option value="any">any</option>
      </select>
      <span>of these {termCount === 1 ? 'rule' : `${termCount} rules${groups.length ? ' and groups' : ''}`}</span>
      <span className="rule-builder__spacer" />
      <button type="button" className="button button--secondary button--small" onClick={() => onAddRule()} title="Add a rule (N)"><Plus aria-hidden="true" />Rule <kbd>N</kbd></button>
      <button type="button" className="button button--ghost button--small" onClick={() => onAddRule({ id: newGroupId(), match: 'any', label: 'new group' })} title="Add a group of alternatives (Shift+N)"><FolderInput aria-hidden="true" />Group <kbd>Shift N</kbd></button>
      <button type="button" className="button button--ghost button--small" onClick={() => onChange([...criteria, newRecentSplitCriterion()])} title="Exclude or keep companies with recent stock splits"><Split aria-hidden="true" />Split event</button>
    </header>
    {criteria.length ? <ol className="rule-list">{terms}</ol> : <p className="rule-builder__empty">No rules yet, so every company with a filing matches. Press <kbd>N</kbd> to add a rule or <kbd>O</kbd> to open a starter screen.</p>}
  </div>
}
