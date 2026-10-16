import { type ReactNode } from 'react'

import { filingStorageNoticeText, type FilingStorageNotice } from './filingStorage'

/** The notice at the top of the overview and Admin pages while filings.db has the old layout. */
export function FilingStorageBanner({ notice, action }: { notice: FilingStorageNotice; action?: ReactNode }) {
  return <div className="callout callout--warning filing-storage-banner" role="status">
    <strong>{filingStorageNoticeText(notice)}</strong>
    {notice.state === 'legacy' && <span> Filings can be read as usual; downloading new ones waits until the split is done.</span>}
    {action}
  </div>
}
