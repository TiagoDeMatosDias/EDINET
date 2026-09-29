import { ApiError, authenticatedFetch } from './client'

export function downloadBlob(filename: string, blob: Blob): void {
  const url = URL.createObjectURL(blob)
  const link = document.createElement('a')
  link.href = url
  link.download = filename
  link.click()
  URL.revokeObjectURL(url)
}

function filenameFromDisposition(header: string | null): string | null {
  if (!header) return null
  const encoded = /filename\*=(?:UTF-8'')?([^;]+)/i.exec(header)
  if (encoded) {
    try {
      return decodeURIComponent(encoded[1].trim().replace(/^"|"$/g, ''))
    } catch {
      // Fall through to the plain filename parameter.
    }
  }
  const plain = /filename=("?)([^";]+)\1/i.exec(header)
  return plain ? plain[2].trim() : null
}

/**
 * Download an API file through the authenticated client. A plain anchor
 * navigation cannot carry the bearer token, so API downloads must use this.
 */
export async function downloadApiFile(path: string, fallbackFilename: string): Promise<void> {
  const response = await authenticatedFetch(path)
  if (!response.ok) {
    let detail = `Download failed (${response.status})`
    if ((response.headers.get('content-type') ?? '').includes('application/json')) {
      const payload = await response.json().catch(() => null) as { detail?: unknown } | null
      if (typeof payload?.detail === 'string') detail = payload.detail
    }
    throw new ApiError(detail, response.status, null)
  }
  const blob = await response.blob()
  downloadBlob(filenameFromDisposition(response.headers.get('content-disposition')) ?? fallbackFilename, blob)
}
