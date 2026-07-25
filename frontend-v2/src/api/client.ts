export class ApiError extends Error {
  readonly status: number
  readonly payload: unknown

  constructor(message: string, status: number, payload: unknown) {
    super(message)
    this.name = 'ApiError'
    this.status = status
    this.payload = payload
  }
}

let accessToken: string | null = null
let refreshPromise: Promise<boolean> | null = null

export function setAccessToken(token: string | null) {
  accessToken = token
}

export function getAccessToken() {
  return accessToken
}

function errorMessage(payload: unknown, fallback: string) {
  if (typeof payload === 'object' && payload && 'detail' in payload) {
    const detail = (payload as { detail?: unknown }).detail
    if (typeof detail === 'string') return detail
  }
  return fallback
}

async function fetchWithAuth(path: string, init?: RequestInit, includeBearer = true) {
  const headers = new Headers(init?.headers)
  if (init?.body && !(init.body instanceof FormData)) headers.set('Content-Type', 'application/json')
  if (includeBearer && accessToken) headers.set('Authorization', `Bearer ${accessToken}`)
  return fetch(path, { ...init, headers, credentials: 'include' })
}

async function performRefresh() {
  const response = await fetchWithAuth('/api/auth/refresh', { method: 'POST' }, false)
  if (!response.ok) {
    accessToken = null
    return false
  }
  const payload = await response.json() as { access_token?: string }
  if (!payload.access_token) {
    accessToken = null
    return false
  }
  accessToken = payload.access_token
  return true
}

function tryRefresh() {
  if (!refreshPromise) {
    refreshPromise = performRefresh().finally(() => {
      refreshPromise = null
    })
  }
  return refreshPromise
}

export async function authenticatedFetch(path: string, init?: RequestInit) {
  let response = await fetchWithAuth(path, init)
  if (response.status === 401 && path.startsWith('/api/') && !path.startsWith('/api/auth/refresh') && await tryRefresh()) {
    response = await fetchWithAuth(path, init)
  }
  return response
}

async function requestOnce<T>(path: string, init?: RequestInit): Promise<{ response: Response; payload: T }> {
  const response = await authenticatedFetch(path, init)
  const contentType = response.headers.get('content-type') ?? ''
  const payload = contentType.includes('application/json') ? await response.json() : await response.text()
  return { response, payload: payload as T }
}

export async function apiRequest<T>(path: string, init?: RequestInit): Promise<T> {
  const result = await requestOnce<T>(path, init)
  if (!result.response.ok) throw new ApiError(errorMessage(result.payload, result.response.statusText), result.response.status, result.payload)
  return result.payload
}

export function apiPost<T>(path: string, body: unknown, signal?: AbortSignal) {
  return apiRequest<T>(path, { method: 'POST', body: JSON.stringify(body), signal })
}

export function queryString(params: Record<string, string | number | boolean | null | undefined>) {
  const query = new URLSearchParams()
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null && value !== '') query.set(key, String(value))
  }
  const text = query.toString()
  return text ? `?${text}` : ''
}
