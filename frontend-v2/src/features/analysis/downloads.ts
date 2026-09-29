import { downloadBlob } from '../../api/download'

export function safeFileName(value: string): string {
  const cleaned = value.replace(/[\\/:*?"<>|\s]+/g, '-').replace(/-+/g, '-').replace(/^[-.]+|[-.]+$/g, '')
  return cleaned || 'download'
}

export function downloadTextFile(filename: string, content: string, mime = 'text/plain;charset=utf-8'): void {
  downloadBlob(filename, new Blob([content], { type: mime }))
}
