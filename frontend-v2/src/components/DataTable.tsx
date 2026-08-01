import { flexRender, getCoreRowModel, getPaginationRowModel, getSortedRowModel, useReactTable, type ColumnDef, type SortingState } from '@tanstack/react-table'
import { ArrowDown, ArrowUp, ChevronLeft, ChevronRight, ChevronsUpDown } from 'lucide-react'
import { useRef, useState } from 'react'

export function DataTable<T>({ data, columns, emptyText = 'No rows to show', dense = false, pageSize }: { data: T[]; columns: ColumnDef<T>[]; emptyText?: string; dense?: boolean; pageSize?: number }) {
  const [sorting, setSorting] = useState<SortingState>([])
  const scrollRef = useRef<HTMLDivElement>(null)
  const table = useReactTable({
    data,
    columns,
    state: { sorting },
    initialState: { pagination: { pageIndex: 0, pageSize: (pageSize ?? data.length) || 1 } },
    onSortingChange: setSorting,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getPaginationRowModel: pageSize ? getPaginationRowModel() : undefined,
  })
  const changePage = (pageIndex: number) => {
    table.setPageIndex(pageIndex)
    const scrollContainer = scrollRef.current?.closest('.portfolio-table-frame')
    if (scrollContainer instanceof HTMLElement) scrollContainer.scrollTop = 0
  }

  if (!data.length) return <div className="table-empty">{emptyText}</div>
  return <div ref={scrollRef} className="table-scroll">
    <table className={dense ? 'data-grid data-grid--dense' : 'data-grid'}><thead>{table.getHeaderGroups().map(group => <tr key={group.id}>{group.headers.map(header => <th key={header.id}>{header.isPlaceholder ? null : <button className="table-sort" onClick={header.column.getToggleSortingHandler()} disabled={!header.column.getCanSort()}>{flexRender(header.column.columnDef.header, header.getContext())}{header.column.getCanSort() && (header.column.getIsSorted() === 'asc' ? <ArrowUp /> : header.column.getIsSorted() === 'desc' ? <ArrowDown /> : <ChevronsUpDown />)}</button>}</th>)}</tr>)}</thead><tbody>{table.getRowModel().rows.map(row => <tr key={row.id}>{row.getVisibleCells().map(cell => <td key={cell.id}>{flexRender(cell.column.columnDef.cell, cell.getContext())}</td>)}</tr>)}</tbody></table>
    {pageSize && table.getPageCount() > 1 && <div className="table-pagination" aria-label="Table pagination">
      <span>{table.getState().pagination.pageIndex * pageSize + 1}–{Math.min((table.getState().pagination.pageIndex + 1) * pageSize, data.length)} of {data.length.toLocaleString()}</span>
      <button type="button" aria-label="Previous page" disabled={!table.getCanPreviousPage()} onClick={() => changePage(table.getState().pagination.pageIndex - 1)}><ChevronLeft /></button>
      <strong>Page {table.getState().pagination.pageIndex + 1} of {table.getPageCount()}</strong>
      <button type="button" aria-label="Next page" disabled={!table.getCanNextPage()} onClick={() => changePage(table.getState().pagination.pageIndex + 1)}><ChevronRight /></button>
    </div>}
  </div>
}
