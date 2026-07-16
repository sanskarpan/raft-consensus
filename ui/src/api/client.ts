// Node list from config (user-configurable in UI)
export type NodeConfig = { id: string; httpAddr: string }

export interface ClusterInfo {
  node_id: string
  state: 'Leader' | 'Follower' | 'Candidate' | 'Learner' | 'Shutdown'
  leader: string
  term: number
  commit_idx: number
  config: { Servers: { ID: string; Address: string; Learner: boolean }[] }
}

function buildHeaders(token?: string): HeadersInit {
  const headers: Record<string, string> = { 'Content-Type': 'application/json' }
  if (token) {
    headers['Authorization'] = `Bearer ${token}`
  }
  return headers
}

export async function fetchClusterInfo(addr: string, token?: string): Promise<ClusterInfo> {
  const url = `http://${addr}/admin/cluster${token ? `?token=${encodeURIComponent(token)}` : ''}`
  const res = await fetch(url, {
    headers: buildHeaders(token),
  })
  if (!res.ok) {
    throw new Error(`HTTP ${res.status}: ${res.statusText}`)
  }
  return res.json() as Promise<ClusterInfo>
}

export async function fetchMetrics(addr: string): Promise<string> {
  const url = `http://${addr}/metrics`
  const res = await fetch(url)
  if (!res.ok) {
    throw new Error(`HTTP ${res.status}: ${res.statusText}`)
  }
  return res.text()
}

export async function triggerSnapshot(addr: string, token: string): Promise<void> {
  const url = `http://${addr}/admin/snapshot`
  const res = await fetch(url, {
    method: 'POST',
    headers: buildHeaders(token),
  })
  if (!res.ok) {
    const body = await res.text()
    throw new Error(`HTTP ${res.status}: ${body}`)
  }
}

export async function submitCommand(
  addr: string,
  op: 'set' | 'get' | 'delete',
  key: string,
  value?: string,
  token?: string,
): Promise<string> {
  const url = `http://${addr}/command`
  // Encode as simple text command: "op key [value]"
  const parts = [op, key]
  if (value !== undefined && op === 'set') {
    parts.push(value)
  }
  const body = parts.join(' ')

  const res = await fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'text/plain',
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
    body,
  })

  const json = (await res.json()) as { result?: string; error?: string }
  if (json.error) {
    throw new Error(json.error)
  }
  return json.result ?? ''
}

export async function checkHealth(addr: string): Promise<boolean> {
  try {
    const url = `http://${addr}/health`
    const res = await fetch(url, { signal: AbortSignal.timeout(3000) })
    return res.ok
  } catch {
    return false
  }
}

export function parsePrometheus(text: string): Map<string, number> {
  const result = new Map<string, number>()
  const lines = text.split('\n')
  for (const line of lines) {
    const trimmed = line.trim()
    if (!trimmed || trimmed.startsWith('#')) continue
    // Match: metric_name{optional_labels} value [timestamp]
    const match = trimmed.match(/^([a-zA-Z_:][a-zA-Z0-9_:]*(?:\{[^}]*\})?)\s+([-+]?\d*\.?\d+(?:[eE][-+]?\d+)?|NaN|[+-]?Inf)/)
    if (match) {
      const name = match[1]
      const value = parseFloat(match[2])
      result.set(name, value)
    }
  }
  return result
}

export function parsePrometheusHistogram(
  text: string,
  metricName: string,
): { buckets: Array<{ le: string; count: number }>; sum: number; count: number } {
  const buckets: Array<{ le: string; count: number }> = []
  let sum = 0
  let count = 0

  const lines = text.split('\n')
  for (const line of lines) {
    const trimmed = line.trim()
    if (!trimmed || trimmed.startsWith('#')) continue

    const bucketMatch = trimmed.match(
      new RegExp(`^${metricName}_bucket\\{.*?le="([^"]+)".*?\\}\\s+([-+]?\\d*\\.?\\d+)`),
    )
    if (bucketMatch) {
      buckets.push({ le: bucketMatch[1], count: parseFloat(bucketMatch[2]) })
      continue
    }

    const sumMatch = trimmed.match(new RegExp(`^${metricName}_sum\\s+([-+]?\\d*\\.?\\d+)`))
    if (sumMatch) {
      sum = parseFloat(sumMatch[1])
      continue
    }

    const countMatch = trimmed.match(new RegExp(`^${metricName}_count\\s+([-+]?\\d*\\.?\\d+)`))
    if (countMatch) {
      count = parseFloat(countMatch[1])
    }
  }

  return { buckets, sum, count }
}

// ---------------------------------------------------------------------------
// v1 KV API
// ---------------------------------------------------------------------------

export interface KVEntry {
  key: string
  value: string
  create_revision: number
  mod_revision: number
  version: number
}

export interface TxnCompare {
  key: string
  target: 'value' | 'version' | 'create_revision' | 'mod_revision'
  result: 'equal' | 'not_equal' | 'greater' | 'less'
  value?: string
  rev?: number
}

export interface TxnOp {
  type: 0 | 1 // 0=put, 1=delete
  key: string
  value?: string
}

export interface TxnRequest {
  compare: TxnCompare[]
  success: TxnOp[]
  failure: TxnOp[]
}

export interface TxnResponse {
  succeeded: boolean
  results: Array<{ kv?: KVEntry; error?: string }>
  revision: number
}

export interface WatchEvent {
  events: Array<{
    type: number // 0=put, 1=delete
    key: string
    kv?: KVEntry
    prev_kv?: KVEntry
    revision: number
  }>
  revision: number
}

export async function kvGet(addr: string, key: string, token?: string): Promise<KVEntry> {
  const url = `http://${addr}/v1/kv/${encodeURIComponent(key)}`
  const res = await fetch(url, { headers: buildHeaders(token) })
  if (!res.ok) {
    const body = await res.text().catch(() => res.statusText)
    throw new Error(`HTTP ${res.status}: ${body}`)
  }
  return res.json() as Promise<KVEntry>
}

export async function kvGetStale(addr: string, key: string, token?: string): Promise<KVEntry> {
  const url = `http://${addr}/v1/kv/${encodeURIComponent(key)}?consistency=stale`
  const res = await fetch(url, { headers: buildHeaders(token) })
  if (!res.ok) {
    const body = await res.text().catch(() => res.statusText)
    throw new Error(`HTTP ${res.status}: ${body}`)
  }
  return res.json() as Promise<KVEntry>
}

export async function kvPut(addr: string, key: string, value: string, token?: string): Promise<KVEntry> {
  const url = `http://${addr}/v1/kv/${encodeURIComponent(key)}`
  const res = await fetch(url, {
    method: 'PUT',
    headers: { 'Content-Type': 'text/plain', ...(token ? { Authorization: `Bearer ${token}` } : {}) },
    body: value,
  })
  if (!res.ok) {
    const body = await res.text().catch(() => res.statusText)
    throw new Error(`HTTP ${res.status}: ${body}`)
  }
  return res.json() as Promise<KVEntry>
}

export async function kvDelete(addr: string, key: string, token?: string): Promise<void> {
  const url = `http://${addr}/v1/kv/${encodeURIComponent(key)}`
  const res = await fetch(url, {
    method: 'DELETE',
    headers: buildHeaders(token),
  })
  if (!res.ok) {
    const body = await res.text().catch(() => res.statusText)
    throw new Error(`HTTP ${res.status}: ${body}`)
  }
}

export async function kvRange(addr: string, prefix: string, token?: string): Promise<KVEntry[]> {
  const url = `http://${addr}/v1/kv?prefix=${encodeURIComponent(prefix)}`
  const res = await fetch(url, { headers: buildHeaders(token) })
  if (!res.ok) {
    const body = await res.text().catch(() => res.statusText)
    throw new Error(`HTTP ${res.status}: ${body}`)
  }
  return res.json() as Promise<KVEntry[]>
}

export async function kvTxn(addr: string, req: TxnRequest, token?: string): Promise<TxnResponse> {
  const url = `http://${addr}/v1/txn`
  const res = await fetch(url, {
    method: 'POST',
    headers: buildHeaders(token),
    body: JSON.stringify(req),
  })
  if (!res.ok) {
    const body = await res.text().catch(() => res.statusText)
    throw new Error(`HTTP ${res.status}: ${body}`)
  }
  return res.json() as Promise<TxnResponse>
}

export function kvWatch(
  addr: string,
  keyOrPrefix: string,
  isPrefix: boolean,
  sinceRevision: number,
  onEvent: (we: WatchEvent) => void,
  onError: (err: string) => void,
): () => void {
  const param = isPrefix ? `prefix=${encodeURIComponent(keyOrPrefix)}` : `key=${encodeURIComponent(keyOrPrefix)}`
  const url = `http://${addr}/v1/watch?${param}${sinceRevision > 0 ? `&revision=${sinceRevision}` : ''}`

  const controller = new AbortController()

  ;(async () => {
    try {
      const res = await fetch(url, { signal: controller.signal, headers: { Accept: 'text/event-stream' } })
      if (!res.ok || !res.body) {
        onError(`HTTP ${res.status}`)
        return
      }
      const reader = res.body.getReader()
      const decoder = new TextDecoder()
      let buf = ''
      while (true) {
        const { done, value } = await reader.read()
        if (done) break
        buf += decoder.decode(value, { stream: true })
        const lines = buf.split('\n')
        buf = lines.pop() ?? ''
        let dataBuf = ''
        for (const line of lines) {
          if (line.startsWith('data: ')) {
            dataBuf += line.slice(6)
          } else if (line === '') {
            if (dataBuf) {
              try {
                const we = JSON.parse(dataBuf) as WatchEvent
                onEvent(we)
              } catch {
                // skip malformed event
              }
              dataBuf = ''
            }
          }
        }
      }
    } catch (err) {
      if ((err as Error).name !== 'AbortError') {
        onError(String(err))
      }
    }
  })()

  return () => controller.abort()
}

export async function v1Status(addr: string, token?: string): Promise<Record<string, unknown>> {
  const url = `http://${addr}/v1/status`
  const res = await fetch(url, { headers: buildHeaders(token) })
  if (!res.ok) throw new Error(`HTTP ${res.status}`)
  return res.json() as Promise<Record<string, unknown>>
}

// Calculate approximate percentile from histogram buckets
export function histogramPercentile(
  buckets: Array<{ le: string; count: number }>,
  totalCount: number,
  percentile: number,
): number {
  if (buckets.length === 0 || totalCount === 0) return 0
  const target = (percentile / 100) * totalCount
  for (let i = 0; i < buckets.length; i++) {
    if (buckets[i].count >= target) {
      if (i === 0) {
        const le = buckets[i].le === '+Inf' ? 0 : parseFloat(buckets[i].le)
        return le
      }
      const prevCount = buckets[i - 1].count
      const currCount = buckets[i].count
      const prevLe = parseFloat(buckets[i - 1].le) || 0
      const currLe = buckets[i].le === '+Inf' ? prevLe * 2 : parseFloat(buckets[i].le)
      const fraction = (target - prevCount) / (currCount - prevCount)
      return prevLe + fraction * (currLe - prevLe)
    }
  }
  // Return last finite bucket upper bound
  for (let i = buckets.length - 1; i >= 0; i--) {
    if (buckets[i].le !== '+Inf') return parseFloat(buckets[i].le)
  }
  return 0
}
