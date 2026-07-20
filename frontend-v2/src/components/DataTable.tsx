import { flexRender, getCoreRowModel, getSortedRowModel, useReactTable, type ColumnDef, type SortingState } from '@tanstack/react-table'
import { ArrowDown, ArrowUp, ChevronsUpDown } from 'lucide-react'
import { useState } from 'react'

export function DataTable<T>({ data, columns, emptyText = 'No rows to show', dense = false }: { data: T[]; columns: ColumnDef<T>[]; emptyText?: string; dense?: boolean }) {
  const [sorting, setSorting] = useState<SortingState>([])
  const table = useReactTable({ data, columns, state: { sorting }, onSortingChange: setSorting, getCoreRowModel: getCoreRowModel(), getSortedRowModel: getSortedRowModel() })

  if (!data.length) return <div className="table-empty">{emptyText}</div>
  return <div className="table-scroll"><table className={dense ? 'data-grid data-grid--dense' : 'data-grid'}><thead>{table.getHeaderGroups().map(group => <tr key={group.id}>{group.headers.map(header => <th key={header.id}>{header.isPlaceholder ? null : <button className="table-sort" onClick={header.column.getToggleSortingHandler()} disabled={!header.column.getCanSort()}>{flexRender(header.column.columnDef.header, header.getContext())}{header.column.getCanSort() && (header.column.getIsSorted() === 'asc' ? <ArrowUp /> : header.column.getIsSorted() === 'desc' ? <ArrowDown /> : <ChevronsUpDown />)}</button>}</th>)}</tr>)}</thead><tbody>{table.getRowModel().rows.map(row => <tr key={row.id}>{row.getVisibleCells().map(cell => <td key={cell.id}>{flexRender(cell.column.columnDef.cell, cell.getContext())}</td>)}</tr>)}</tbody></table></div>
}
