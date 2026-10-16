import type { MetricDefinition } from '../../metrics'
import { columnLabel, presetForTokens } from '../screening/metricCatalog'
import type { ExpressionToken } from '../screening/types'

/**
 * A formula as a comparison metric: ``=`` and the formula with bracketed
 * columns, ``=[Stock_Prices.Price] / [ShareMetrics.Net assets per share]``.
 * The server evaluates it (``src/comparison/formulas.py``); links and saved
 * comparisons keep it as text like any other metric.
 */

const SYMBOLS: Record<string, string> = { '*': '×', '/': '÷', '+': '+', '-': '−' }

export function isFormulaMetric(metric: string) {
  return metric.startsWith('=')
}

/** The metric for ``tokens``; columns are bracketed, with ``]`` doubled. */
export function formulaMetric(tokens: ExpressionToken[]) {
  return `=${tokens.map(token => {
    if (token.type === 'column') return `[${`${token.table}.${token.column}`.replace(/]/g, ']]')}]`
    if (token.type === 'op') return token.op
    if (token.type === 'paren') return token.value
    const value = Number(token.value)
    return Number.isFinite(value) ? String(value) : String(token.value)
  }).join(' ')}`
}

/** The tokens of a formula metric, or ``null`` when it is not one. */
export function parseFormulaMetric(metric: string): ExpressionToken[] | null {
  if (!isFormulaMetric(metric)) return null
  const tokens: ExpressionToken[] = []
  const text = metric.slice(1)
  let position = 0
  while (position < text.length) {
    const char = text[position]
    if (/\s/.test(char)) { position += 1; continue }
    if (char === '[') {
      let name = ''
      let end = position + 1
      for (;;) {
        const close = text.indexOf(']', end)
        if (close < 0) return null
        name += text.slice(end, close)
        if (text[close + 1] === ']') { name += ']'; end = close + 2; continue }
        position = close + 1
        break
      }
      const dot = name.indexOf('.')
      if (dot < 1 || dot === name.length - 1) return null
      tokens.push({ type: 'column', table: name.slice(0, dot), column: name.slice(dot + 1) })
      continue
    }
    const last = tokens[tokens.length - 1]
    const expectsTerm = !last || last.type === 'op' || (last.type === 'paren' && last.value === '(')
    const number = /^-?(\d+(\.\d*)?|\.\d+)(e[+-]?\d+)?/i.exec(text.slice(position))
    if (number && (expectsTerm || number[0][0] !== '-')) {
      tokens.push({ type: 'value', value: Number(number[0]) })
      position += number[0].length
    } else if ('+-*/'.includes(char)) {
      tokens.push({ type: 'op', op: char as '+' | '-' | '*' | '/' })
      position += 1
    } else if (char === '(' || char === ')') {
      tokens.push({ type: 'paren', value: char })
      position += 1
    } else {
      return null
    }
  }
  return tokens.some(token => token.type === 'column') ? tokens : null
}

/** "Share price × Basic earnings (loss) per share": a formula in readable labels. */
export function formulaText(tokens: ExpressionToken[]) {
  return tokens.map(token => token.type === 'column' ? columnLabel(token.table, token.column) : token.type === 'op' ? SYMBOLS[token.op] : String(token.value)).join(' ')
}

/** A formula metric's definition: a preset's name and format, or the formula itself. */
export function describeFormulaMetric(metric: string): MetricDefinition | null {
  const tokens = parseFormulaMetric(metric)
  if (!tokens) return null
  const preset = presetForTokens(tokens)
  const text = formulaText(tokens)
  return {
    label: preset?.label ?? text,
    group: 'Formula',
    ...(preset?.format === 'percent' ? { format: 'percent' as const } : {}),
    description: preset ? `${preset.description} (${text})` : `Calculated for each company: ${text}.`,
  }
}
