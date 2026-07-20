import type { MetricCatalog } from './types'

export function MetricSelect({ catalog, table, column, label, onChange }: { catalog: MetricCatalog; table: string; column: string; label: string; onChange: (table: string, column: string) => void }) {
  const tables = Object.keys(catalog).sort((a, b) => a.localeCompare(b))
  const tableOptions = table && !tables.includes(table) ? [table, ...tables] : tables
  const columns = catalog[table] ?? []
  const columnOptions = column && !columns.includes(column) ? [column, ...columns] : columns
  const selectTable = (nextTable: string) => onChange(nextTable, catalog[nextTable]?.[0] ?? '')
  return <span className="metric-select"><select value={table} onChange={event => selectTable(event.target.value)} aria-label={`${label} table`}><option value="">Table…</option>{tableOptions.map(item => <option key={item}>{item}</option>)}</select><select value={column} onChange={event => onChange(table, event.target.value)} disabled={!table} aria-label={`${label} column`}><option value="">Column…</option>{columnOptions.map(item => <option key={item}>{item}</option>)}</select></span>
}
