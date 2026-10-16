import type { DocEndpoint, Language, SectionSummary } from './apiDocsTypes'

/** The values an example call uses: path and query parameters as typed, and the JSON body text. */
export interface CallValues {
  path: Record<string, string>
  query: Record<string, string>
  body: string
}

function text(value: unknown) {
  if (value === null || value === undefined) return ''
  if (Array.isArray(value)) return value.join(',')
  return typeof value === 'object' ? JSON.stringify(value) : String(value)
}

export function initialValues(endpoint: DocEndpoint): CallValues {
  const path: Record<string, string> = {}
  const query: Record<string, string> = {}
  for (const parameter of endpoint.parameters) {
    if (parameter.in === 'path') path[parameter.name] = text(parameter.example) || parameter.name.toUpperCase()
    if (parameter.in === 'query') query[parameter.name] = text(parameter.example)
  }
  const body = endpoint.body && !endpoint.body.form && endpoint.body.example !== undefined && endpoint.body.example !== null
    ? JSON.stringify(endpoint.body.example, null, 2)
    : ''
  return { path, query, body }
}

// Readable URLs: keep the separators people type (``company:E02144``, ``a,b``) but
// encode everything a shell or URL would misread, ``$`` included.
function encodePart(value: string) {
  return encodeURIComponent(value).replace(/%2C/gi, ',').replace(/%3A/gi, ':')
}

/** ``/api/filings/S100Y8NY/sections?limit=20``: the path with its parameters filled in. */
export function requestPath(endpoint: DocEndpoint, values: CallValues) {
  const path = endpoint.path.replace(/\{([^}]+)\}/g, (_, name: string) => encodePart(values.path[name] ?? name.toUpperCase()))
  const query = Object.entries(values.query).filter(([, value]) => value !== '').map(([name, value]) => `${encodePart(name)}=${encodePart(value)}`)
  return query.length ? `${path}?${query.join('&')}` : path
}

const EXTENSIONS: Array<[RegExp, string]> = [
  [/zip/, 'zip'],
  [/csv/, 'csv'],
  [/html/, 'html'],
  [/pem/, 'pem'],
  [/spreadsheet|excel/, 'xlsx'],
  [/pdf/, 'pdf'],
]

/** A file name for an endpoint that answers with a download, or ``null`` for JSON. */
export function downloadName(endpoint: DocEndpoint) {
  const type = endpoint.response.content_type ?? ''
  if (!type || type.includes('json') || endpoint.streams) return null
  const extension = EXTENSIONS.find(([pattern]) => pattern.test(type))?.[1] ?? 'bin'
  const segment = endpoint.path.split('/').filter(part => part && !part.startsWith('{')).pop() ?? 'download'
  return `${segment}.${extension}`
}

function parsedBody(values: CallValues): { ok: true; value: unknown } | { ok: false } {
  if (!values.body.trim()) return { ok: false }
  try {
    return { ok: true, value: JSON.parse(values.body) }
  } catch {
    return { ok: false }
  }
}

function hasJsonBody(endpoint: DocEndpoint, values: CallValues) {
  return Boolean(endpoint.body && !endpoint.body.form && values.body.trim())
}

function shellSingleQuoted(value: string) {
  return `'${value.replace(/'/g, `'\\''`)}'`
}

export function curlExample(endpoint: DocEndpoint, values: CallValues) {
  const form = endpoint.body?.form ?? null
  const json = hasJsonBody(endpoint, values)
  const lines = [`curl -s${endpoint.streams ? ' -N' : ''}`]
  // --json and -F imply POST; every other method is named.
  if (endpoint.method !== 'GET' && !(endpoint.method === 'POST' && (json || form))) lines[0] += ` -X ${endpoint.method}`
  lines[0] += ` "$SHADE_URL${requestPath(endpoint, values)}"`
  const options = ['-H "Authorization: Bearer $SHADE_TOKEN"']
  if (form) for (const [name, value] of Object.entries(form)) options.push(`-F "${name}=${value}"`)
  if (json) options.push(`--json ${shellSingleQuoted(values.body.trim())}`)
  const file = downloadName(endpoint)
  if (file) options.push(`-o ${file}`)
  if (options.length === 1) return `${lines[0]} ${options[0]}`
  return [lines[0], ...options].join(' \\\n  ')
}

const NUMBER = /^-?\d+(\.\d+)?$/

/** A query value typed into a field, as Python would write it for the parameter's type. */
function pythonScalar(value: string, type = 'string') {
  if (/\bboolean\b/.test(type) && (value === 'true' || value === 'false')) return value === 'true' ? 'True' : 'False'
  if (/\b(integer|number)\b/.test(type) && NUMBER.test(value)) return value
  return JSON.stringify(value)
}

/** A JSON value as a Python literal, on one line when it is short. */
export function pythonLiteral(value: unknown, indent = 0): string {
  if (value === null || value === undefined) return 'None'
  if (value === true) return 'True'
  if (value === false) return 'False'
  if (typeof value === 'number') return String(value)
  if (typeof value === 'string') return JSON.stringify(value)
  const pad = ' '.repeat(indent + 4)
  if (Array.isArray(value)) {
    const items = value.map(item => pythonLiteral(item, indent + 4))
    const inline = `[${items.join(', ')}]`
    return inline.length <= 72 && !inline.includes('\n') ? inline : `[\n${items.map(item => `${pad}${item},`).join('\n')}\n${' '.repeat(indent)}]`
  }
  const entries = Object.entries(value as Record<string, unknown>).map(([key, item]) => `${JSON.stringify(key)}: ${pythonLiteral(item, indent + 4)}`)
  const inline = `{${entries.join(', ')}}`
  return inline.length <= 72 && !inline.includes('\n') ? inline : `{\n${entries.map(item => `${pad}${item},`).join('\n')}\n${' '.repeat(indent)}}`
}

export function pythonExample(endpoint: DocEndpoint, values: CallValues) {
  const pathOnly = requestPath(endpoint, { ...values, query: {} })
  const query = Object.entries(values.query).filter(([, value]) => value !== '')
  const args = [`"${endpoint.method}"`, JSON.stringify(pathOnly)]
  const types = Object.fromEntries(endpoint.parameters.map(parameter => [parameter.name, parameter.type]))
  if (query.length) args.push(`params={${query.map(([name, value]) => `${JSON.stringify(name)}: ${pythonScalar(value, types[name])}`).join(', ')}}`)
  const form = endpoint.body?.form ?? null
  if (form) {
    const files = Object.entries(form).filter(([, value]) => value.startsWith('@'))
    const data = Object.entries(form).filter(([, value]) => !value.startsWith('@'))
    if (files.length) args.push(`files={${files.map(([name, value]) => `${JSON.stringify(name)}: open(${JSON.stringify(value.slice(1))}, "rb")`).join(', ')}}`)
    if (data.length) args.push(`data={${data.map(([name, value]) => `${JSON.stringify(name)}: ${JSON.stringify(value)}`).join(', ')}}`)
  }
  if (hasJsonBody(endpoint, values)) {
    const body = parsedBody(values)
    args.push(`json=${body.ok ? pythonLiteral(body.value) : values.body.trim()}`)
  }
  const file = downloadName(endpoint)
  if (file) {
    const call = `api.request(${args.join(', ')}`.replace(`api.request("${endpoint.method}", "`, `api.request("${endpoint.method}", BASE + "`)
    return `response = ${call})\nresponse.raise_for_status()\nopen(${JSON.stringify(file)}, "wb").write(response.content)`
  }
  if (endpoint.streams) {
    const call = `api.request(${args.join(', ')}, stream=True)`.replace(`api.request("${endpoint.method}", "`, `api.request("${endpoint.method}", BASE + "`)
    return `with ${call} as response:\n    for line in response.iter_lines(decode_unicode=True):\n        if line.startswith("data:"):\n            print(line[5:].strip())`
  }
  const call = `call(${args.join(', ')})`
  return endpoint.response.status === 204 ? call : `result = ${call}\nprint(result)`
}

function indentLines(value: string, spaces: number) {
  return value.split('\n').map((line, index) => (index ? ' '.repeat(spaces) + line : line)).join('\n')
}

export function javascriptExample(endpoint: DocEndpoint, values: CallValues) {
  const fsImports = new Set<string>()
  const lines = ['const { SHADE_URL, SHADE_TOKEN } = process.env']
  const form = endpoint.body?.form ?? null
  const json = hasJsonBody(endpoint, values)
  if (form) {
    fsImports.add('readFile')
    lines.push('const form = new FormData()')
    for (const [name, value] of Object.entries(form)) {
      lines.push(value.startsWith('@')
        ? `form.append(${JSON.stringify(name)}, new Blob([await readFile(${JSON.stringify(value.slice(1))})]), ${JSON.stringify(value.slice(1))})`
        : `form.append(${JSON.stringify(name)}, ${JSON.stringify(value)})`)
    }
  }
  const options = []
  if (endpoint.method !== 'GET') options.push(`method: '${endpoint.method}'`)
  options.push(json ? "headers: { Authorization: `Bearer ${SHADE_TOKEN}`, 'Content-Type': 'application/json' }" : 'headers: { Authorization: `Bearer ${SHADE_TOKEN}` }')
  if (json) {
    const body = parsedBody(values)
    options.push(`body: JSON.stringify(${indentLines(body.ok ? JSON.stringify(body.value, null, 2) : values.body.trim(), 2)})`)
  }
  if (form) options.push('body: form')
  lines.push(`const response = await fetch(\`\${SHADE_URL}${requestPath(endpoint, values).replace(/`/g, '\\`')}\`, {\n  ${options.join(',\n  ')},\n})`)
  lines.push('if (!response.ok) throw new Error(`${response.status}: ${await response.text()}`)')
  const file = downloadName(endpoint)
  if (file) {
    fsImports.add('writeFile')
    lines.push(`await writeFile(${JSON.stringify(file)}, Buffer.from(await response.arrayBuffer()))`)
  } else if (endpoint.streams) {
    lines.push('for await (const chunk of response.body) process.stdout.write(chunk)')
  } else if (endpoint.response.status !== 204) {
    lines.push('console.log(await response.json())')
  }
  const imports = fsImports.size ? [`import { ${[...fsImports].sort().join(', ')} } from 'node:fs/promises'`] : []
  return [...imports, ...lines].join('\n')
}

export function example(language: Language, endpoint: DocEndpoint, values: CallValues) {
  if (language === 'python') return pythonExample(endpoint, values)
  if (language === 'javascript') return javascriptExample(endpoint, values)
  return curlExample(endpoint, values)
}

/** Which docs section covers a workspace path such as ``/analyze/E02144``. */
export function sectionForPath(sections: SectionSummary[], pathname: string) {
  const path = pathname.replace(/\/+$/, '') || '/'
  for (const section of sections) {
    for (const pattern of section.screen_paths) {
      const regex = new RegExp(`^${pattern.replace(/[.*+?^${}()|[\]\\]/g, '\\$&').replace(/:[A-Za-z]+/g, '[^/]+')}$`)
      if (regex.test(path)) return section.id
    }
  }
  return null
}
