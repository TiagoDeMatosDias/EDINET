/** The JSON ``GET /api/docs`` and ``GET /api/docs/{section}`` answer (``src/api_docs``). */

export type Access = 'public' | 'member' | 'operator' | 'admin'

export interface DocField {
  name: string
  type: string
  required: boolean
  default: unknown
  description: string
  enum: unknown[] | null
}

export interface DocParameter extends DocField {
  in: 'path' | 'query' | 'header' | 'cookie'
  example: unknown
}

export interface DocModel {
  name: string
  fields: DocField[]
}

export interface DocBody {
  content_type: string
  required?: boolean
  model?: string | null
  fields: DocField[]
  models: DocModel[]
  example: unknown
  form: Record<string, string> | null
}

export interface DocResponse {
  status: number
  content_type: string | null
  type: string | null
  fields: DocField[]
  models: DocModel[]
  description: string
}

export interface DocEndpoint {
  id: string
  method: string
  path: string
  summary: string
  details: string[]
  access: Access
  access_label: string
  read_only_token: boolean
  parameters: DocParameter[]
  body: DocBody | null
  response: DocResponse
  try_it: boolean
  streams: boolean
}

export interface RelatedEndpoint extends DocEndpoint {
  section: string
  section_title: string
}

export interface DocGuide {
  title: string
  text: string
  curl: string
  python: string
  response: string
}

export interface DocGroup {
  title: string
  intro: string
  endpoints: DocEndpoint[]
}

export interface SectionSummary {
  id: string
  title: string
  screen_paths: string[]
  summary: string
  endpoint_count: number
}

export interface DocsIndex {
  auth_mode: 'accounts' | 'disabled'
  sections: SectionSummary[]
  python_setup: string
  openapi_url: string | null
  explorer_url: string | null
}

export interface DocSection extends SectionSummary {
  intro: string
  guides: DocGuide[]
  groups: DocGroup[]
  related: RelatedEndpoint[]
  auth_mode: 'accounts' | 'disabled'
  python_setup: string
}

export type Language = 'curl' | 'python' | 'javascript'
