import { authenticatedFetch } from '../../api/client'
import { downloadBlob } from '../../api/download'
import { safeFileName } from '../analysis/downloads'

/** Download every retained archive of one company as a ZIP with a manifest. */
export async function exportCompanyFilings(companyCode: string) {
  const response = await authenticatedFetch(`/api/filings/company/${encodeURIComponent(companyCode)}/export`)
  if (!response.ok) {
    throw new Error(response.status === 404 ? 'No filing archives are available to export.' : response.status === 401 ? 'Sign in to export filing archives.' : `Export failed (${response.status})`)
  }
  downloadBlob(`${safeFileName(companyCode)}-filings.zip`, await response.blob())
}
