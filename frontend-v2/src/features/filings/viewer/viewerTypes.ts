import type { FilingRow } from '../FilingsTable'

export interface Filing extends FilingRow {
  archive_sha256?: string | null
}

export interface Artifact { artifact_id: string; member_path: string; kind: string; size_bytes: number }

export interface FilingDetail {
  filing: Filing
  artifacts: Artifact[]
}

/** An HTML document of the filing, named by its own opening heading. */
export interface ReportFile {
  artifact_id: string
  member_path: string
  filename: string
  size_bytes: number
  label: string
  heading?: string | null
  code?: string | null
  group: 'cover' | 'business' | 'financials' | 'shareholders' | 'guarantor' | 'audit' | 'other'
}

export interface Section {
  section_id: string
  artifact_id?: string | null
  title?: string
  title_en?: string
  text: string
  text_en?: string
  ordinal: number
}

export interface QualityIssue { issue_id: string; severity: string; code: string; message: string; fact_id?: string | null }
export interface TaxonomyEntry { namespace_uri?: string; concept?: string }

export const FILE_GROUP_LABELS: Record<ReportFile['group'], string> = {
  cover: 'Cover',
  business: 'Company and business',
  financials: 'Financial information',
  shareholders: 'Shares and reference information',
  guarantor: 'Guarantor information',
  audit: "Auditor's reports",
  other: 'Other documents',
}

export const VIEWER_TABS = [
  { key: 'report', label: 'Report', hint: 'The original EDINET report, with English alongside on request' },
  { key: 'sections', label: 'Sections', hint: 'Narrative sections as text, searchable, with English translation' },
  { key: 'statements', label: 'Statements', hint: 'Statement and note tables built from the filing’s XBRL' },
  { key: 'details', label: 'Details', hint: 'Package contents, data-quality notes, and taxonomy concepts' },
] as const

export type ViewerTab = typeof VIEWER_TABS[number]['key']
