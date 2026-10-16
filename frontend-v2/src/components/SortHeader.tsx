import { ArrowDown, ArrowUp } from 'lucide-react'
import type { ReactNode } from 'react'

import type { TableSort } from './tableSort'

/** A column header that sorts its table when clicked; see ``useTableSort``. */
export function SortHeader<K extends string>({ sort, column, className, children }: { sort: TableSort<K>; column: K; className?: string; children: ReactNode }) {
  const active = sort.key === column
  return <th scope="col" className={className} aria-sort={active ? (sort.descending ? 'descending' : 'ascending') : 'none'}>
    <button type="button" className="rs-sort" onClick={() => sort.toggle(column)}>
      {children}
      {active && (sort.descending ? <ArrowDown aria-hidden="true" /> : <ArrowUp aria-hidden="true" />)}
    </button>
  </th>
}
