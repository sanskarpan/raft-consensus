import { useState, useEffect, useRef, useCallback } from 'react'
import {
  kvGet,
  kvGetStale,
  kvPut,
  kvDelete,
  kvRange,
  kvWatch,
  kvTxn,
  type KVEntry,
  type WatchEvent,
  type TxnCompare,
  type TxnOp,
  type TxnResponse,
} from '../api/client'

interface Props {
  nodeAddrs: string[]
  token?: string
}

type ActiveTab = 'browse' | 'edit' | 'watch' | 'txn'

function EntryRow({ entry, onDelete }: { entry: KVEntry; onDelete?: (key: string) => void }) {
  const [expanded, setExpanded] = useState(false)
  return (
    <div
      className="border border-gray-200 dark:border-gray-700 rounded-lg overflow-hidden"
    >
      <button
        onClick={() => setExpanded((e) => !e)}
        className="w-full flex items-center justify-between px-4 py-2 bg-white dark:bg-gray-800 hover:bg-gray-50 dark:hover:bg-gray-750 text-left transition-colors"
      >
        <span className="font-mono text-sm text-blue-600 dark:text-blue-400 truncate max-w-xs">
          {entry.key}
        </span>
        <div className="flex items-center gap-3 ml-2">
          <span className="font-mono text-sm text-gray-600 dark:text-gray-400 truncate max-w-xs">
            {entry.value.length > 40 ? entry.value.slice(0, 40) + '…' : entry.value}
          </span>
          <span className="text-xs text-gray-400 dark:text-gray-500">rev:{entry.mod_revision}</span>
          <span className={`text-gray-400 dark:text-gray-500 text-xs transition-transform ${expanded ? 'rotate-90' : ''}`}>
            ▶
          </span>
        </div>
      </button>
      {expanded && (
        <div className="border-t border-gray-100 dark:border-gray-700 bg-gray-50 dark:bg-gray-900 px-4 py-3 space-y-2 text-sm">
          <div className="grid grid-cols-3 gap-4 text-xs text-gray-500 dark:text-gray-400">
            <div><span className="font-medium">Create Rev:</span> {entry.create_revision}</div>
            <div><span className="font-medium">Mod Rev:</span> {entry.mod_revision}</div>
            <div><span className="font-medium">Version:</span> {entry.version}</div>
          </div>
          <div>
            <p className="text-xs font-medium text-gray-500 dark:text-gray-400 mb-1">Value:</p>
            <pre className="font-mono text-xs bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded p-2 overflow-auto max-h-32 text-gray-800 dark:text-gray-200">
              {entry.value}
            </pre>
          </div>
          {onDelete && (
            <div className="flex justify-end">
              <button
                onClick={() => onDelete(entry.key)}
                className="px-3 py-1 text-xs bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400 hover:bg-red-200 dark:hover:bg-red-900/50 rounded transition-colors"
              >
                Delete key
              </button>
            </div>
          )}
        </div>
      )}
    </div>
  )
}

function BrowseTab({ nodeAddrs, token }: Props) {
  const [prefix, setPrefix] = useState('')
  const [entries, setEntries] = useState<KVEntry[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [deleteStatus, setDeleteStatus] = useState<string | null>(null)

  const load = useCallback(async () => {
    if (nodeAddrs.length === 0) return
    setLoading(true)
    setError(null)
    setDeleteStatus(null)
    try {
      const addr = nodeAddrs[0]
      const result = await kvRange(addr, prefix, token)
      setEntries(result ?? [])
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setLoading(false)
    }
  }, [nodeAddrs, prefix, token])

  useEffect(() => {
    load()
  }, [load])

  const handleDelete = useCallback(
    async (key: string) => {
      if (!window.confirm(`Delete key "${key}"?`)) return
      try {
        await kvDelete(nodeAddrs[0], key, token)
        setDeleteStatus(`Deleted: ${key}`)
        load()
      } catch (err) {
        setDeleteStatus(`Error: ${err instanceof Error ? err.message : String(err)}`)
      }
    },
    [nodeAddrs, token, load],
  )

  return (
    <div className="space-y-4">
      <div className="flex gap-2">
        <input
          type="text"
          placeholder="Key prefix (leave empty for all)"
          value={prefix}
          onChange={(e) => setPrefix(e.target.value)}
          onKeyDown={(e) => e.key === 'Enter' && load()}
          className="flex-1 px-3 py-2 text-sm border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500"
        />
        <button
          onClick={load}
          disabled={loading}
          className="px-4 py-2 text-sm bg-blue-600 hover:bg-blue-700 disabled:bg-blue-400 text-white rounded-lg transition-colors"
        >
          {loading ? 'Loading…' : 'List'}
        </button>
      </div>

      {error && (
        <div className="text-sm text-red-600 dark:text-red-400 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg px-4 py-2">
          {error}
        </div>
      )}
      {deleteStatus && (
        <div className="text-sm text-green-700 dark:text-green-400 bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800 rounded-lg px-4 py-2">
          {deleteStatus}
        </div>
      )}

      {entries.length === 0 && !loading && !error && (
        <p className="text-sm text-gray-500 dark:text-gray-400">No keys found.</p>
      )}

      <div className="space-y-2">
        {entries.map((e) => (
          <EntryRow key={e.key} entry={e} onDelete={handleDelete} />
        ))}
      </div>

      <p className="text-xs text-gray-400 dark:text-gray-500">
        {entries.length} key{entries.length !== 1 ? 's' : ''} — stale read from {nodeAddrs[0]}
      </p>
    </div>
  )
}

function EditTab({ nodeAddrs, token }: Props) {
  const [op, setOp] = useState<'get' | 'put' | 'delete'>('get')
  const [key, setKey] = useState('')
  const [value, setValue] = useState('')
  const [stale, setStale] = useState(false)
  const [result, setResult] = useState<KVEntry | null>(null)
  const [status, setStatus] = useState<{ ok: boolean; msg: string } | null>(null)
  const [loading, setLoading] = useState(false)

  const handleSubmit = useCallback(async () => {
    if (!key.trim()) return
    setLoading(true)
    setStatus(null)
    setResult(null)
    try {
      const addr = nodeAddrs[0]
      if (op === 'get') {
        const kv = stale
          ? await kvGetStale(addr, key.trim(), token)
          : await kvGet(addr, key.trim(), token)
        setResult(kv)
        setStatus({ ok: true, msg: 'Key retrieved.' })
      } else if (op === 'put') {
        const kv = await kvPut(addr, key.trim(), value, token)
        setResult(kv)
        setStatus({ ok: true, msg: 'Key set successfully.' })
      } else if (op === 'delete') {
        await kvDelete(addr, key.trim(), token)
        setStatus({ ok: true, msg: 'Key deleted.' })
      }
    } catch (err) {
      setStatus({ ok: false, msg: err instanceof Error ? err.message : String(err) })
    } finally {
      setLoading(false)
    }
  }, [op, key, value, stale, nodeAddrs, token])

  return (
    <div className="space-y-4">
      {/* Operation picker */}
      <div className="flex gap-2">
        {(['get', 'put', 'delete'] as const).map((o) => (
          <button
            key={o}
            onClick={() => setOp(o)}
            className={`px-4 py-1.5 text-sm rounded-lg transition-colors font-medium ${
              op === o
                ? o === 'delete'
                  ? 'bg-red-600 text-white'
                  : 'bg-blue-600 text-white'
                : 'bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-gray-600'
            }`}
          >
            {o.toUpperCase()}
          </button>
        ))}
      </div>

      <div className="space-y-3">
        <div>
          <label className="block text-xs font-medium text-gray-500 dark:text-gray-400 mb-1">Key</label>
          <input
            type="text"
            value={key}
            onChange={(e) => setKey(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && op !== 'put' && handleSubmit()}
            placeholder="e.g. myapp/config"
            className="w-full px-3 py-2 text-sm border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
        </div>

        {op === 'put' && (
          <div>
            <label className="block text-xs font-medium text-gray-500 dark:text-gray-400 mb-1">Value</label>
            <textarea
              value={value}
              onChange={(e) => setValue(e.target.value)}
              rows={3}
              placeholder="Value to store"
              className="w-full px-3 py-2 text-sm border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500 resize-none font-mono"
            />
          </div>
        )}

        {op === 'get' && (
          <label className="flex items-center gap-2 text-sm text-gray-600 dark:text-gray-400 cursor-pointer">
            <input
              type="checkbox"
              checked={stale}
              onChange={(e) => setStale(e.target.checked)}
              className="rounded"
            />
            Stale read (local FSM, no Raft round-trip)
          </label>
        )}

        <button
          onClick={handleSubmit}
          disabled={loading || !key.trim()}
          className={`px-5 py-2 text-sm font-medium text-white rounded-lg transition-colors disabled:opacity-50 ${
            op === 'delete'
              ? 'bg-red-600 hover:bg-red-700'
              : 'bg-blue-600 hover:bg-blue-700'
          }`}
        >
          {loading ? 'Working…' : `Execute ${op.toUpperCase()}`}
        </button>
      </div>

      {status && (
        <div
          className={`text-sm rounded-lg px-4 py-2 border ${
            status.ok
              ? 'bg-green-50 dark:bg-green-900/20 border-green-200 dark:border-green-800 text-green-700 dark:text-green-400'
              : 'bg-red-50 dark:bg-red-900/20 border-red-200 dark:border-red-800 text-red-700 dark:text-red-400'
          }`}
        >
          {status.msg}
        </div>
      )}

      {result && (
        <div className="bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg p-4">
          <p className="text-xs font-medium text-gray-500 dark:text-gray-400 mb-2">Result</p>
          <div className="grid grid-cols-3 gap-4 text-xs text-gray-500 dark:text-gray-400 mb-3">
            <div><span className="font-medium">Create Rev:</span> {result.create_revision}</div>
            <div><span className="font-medium">Mod Rev:</span> {result.mod_revision}</div>
            <div><span className="font-medium">Version:</span> {result.version}</div>
          </div>
          <pre className="font-mono text-sm bg-gray-50 dark:bg-gray-900 border border-gray-200 dark:border-gray-700 rounded p-3 overflow-auto max-h-48 text-gray-800 dark:text-gray-200">
            {result.value}
          </pre>
        </div>
      )}
    </div>
  )
}

function WatchTab({ nodeAddrs }: Props) {
  const [keyOrPrefix, setKeyOrPrefix] = useState('')
  const [isPrefix, setIsPrefix] = useState(false)
  const [watching, setWatching] = useState(false)
  const [events, setEvents] = useState<WatchEvent[]>([])
  const [error, setError] = useState<string | null>(null)
  const stopRef = useRef<(() => void) | null>(null)
  const listRef = useRef<HTMLDivElement>(null)

  const startWatch = useCallback(() => {
    if (!keyOrPrefix.trim() && !isPrefix) return
    setEvents([])
    setError(null)
    setWatching(true)

    const addr = nodeAddrs[0]
    const stop = kvWatch(
      addr,
      keyOrPrefix.trim(),
      isPrefix,
      0,
      (we) => {
        setEvents((prev) => [...prev.slice(-199), we])
      },
      (err) => {
        setError(err)
        setWatching(false)
      },
    )
    stopRef.current = stop
  }, [keyOrPrefix, isPrefix, nodeAddrs])

  const stopWatch = useCallback(() => {
    stopRef.current?.()
    stopRef.current = null
    setWatching(false)
  }, [])

  useEffect(() => {
    return () => stopRef.current?.()
  }, [])

  // Auto-scroll
  useEffect(() => {
    if (listRef.current) {
      listRef.current.scrollTop = listRef.current.scrollHeight
    }
  }, [events])

  const eventTypeLabel = (t: number) => (t === 0 ? 'PUT' : 'DELETE')
  const eventTypeColor = (t: number) =>
    t === 0
      ? 'bg-green-100 dark:bg-green-900 text-green-800 dark:text-green-200'
      : 'bg-red-100 dark:bg-red-900 text-red-800 dark:text-red-200'

  return (
    <div className="space-y-4">
      <div className="flex gap-2 flex-wrap">
        <input
          type="text"
          value={keyOrPrefix}
          onChange={(e) => setKeyOrPrefix(e.target.value)}
          placeholder={isPrefix ? 'Prefix (e.g. myapp/)' : 'Exact key (e.g. myapp/config)'}
          disabled={watching}
          className="flex-1 min-w-48 px-3 py-2 text-sm border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500 disabled:opacity-50"
        />
        <label className="flex items-center gap-2 text-sm text-gray-600 dark:text-gray-400 cursor-pointer">
          <input
            type="checkbox"
            checked={isPrefix}
            onChange={(e) => setIsPrefix(e.target.checked)}
            disabled={watching}
            className="rounded"
          />
          Prefix match
        </label>
        {!watching ? (
          <button
            onClick={startWatch}
            disabled={!keyOrPrefix.trim() && !isPrefix}
            className="px-4 py-2 text-sm bg-green-600 hover:bg-green-700 disabled:bg-gray-400 text-white rounded-lg transition-colors"
          >
            Watch
          </button>
        ) : (
          <button
            onClick={stopWatch}
            className="px-4 py-2 text-sm bg-red-600 hover:bg-red-700 text-white rounded-lg transition-colors"
          >
            Stop
          </button>
        )}
        {events.length > 0 && (
          <button
            onClick={() => setEvents([])}
            className="px-4 py-2 text-sm bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-gray-600 rounded-lg transition-colors"
          >
            Clear
          </button>
        )}
      </div>

      {watching && (
        <div className="flex items-center gap-2 text-sm text-green-600 dark:text-green-400">
          <span className="inline-block w-2 h-2 rounded-full bg-green-500 animate-pulse" />
          Watching {isPrefix ? `prefix "${keyOrPrefix}"` : `key "${keyOrPrefix}"`} on {nodeAddrs[0]}
        </div>
      )}

      {error && (
        <div className="text-sm text-red-600 dark:text-red-400 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg px-4 py-2">
          Watch error: {error}
        </div>
      )}

      <div
        ref={listRef}
        className="h-80 overflow-y-auto bg-gray-900 rounded-lg p-3 space-y-2 font-mono text-xs"
      >
        {events.length === 0 ? (
          <p className="text-gray-500">
            {watching ? 'Waiting for events…' : 'Start a watch to see live events.'}
          </p>
        ) : (
          events.map((we, wi) =>
            we.events.map((ev, ei) => (
              <div key={`${wi}-${ei}`} className="flex items-start gap-2">
                <span
                  className={`px-1.5 py-0.5 rounded text-xs font-bold shrink-0 ${eventTypeColor(ev.type)}`}
                >
                  {eventTypeLabel(ev.type)}
                </span>
                <span className="text-blue-400 truncate">{ev.key}</span>
                {ev.kv && (
                  <span className="text-green-400 truncate">
                    = {ev.kv.value.length > 60 ? ev.kv.value.slice(0, 60) + '…' : ev.kv.value}
                  </span>
                )}
                <span className="text-gray-500 ml-auto shrink-0">rev:{ev.revision}</span>
              </div>
            )),
          )
        )}
      </div>

      <p className="text-xs text-gray-400 dark:text-gray-500">
        {events.reduce((sum, we) => sum + we.events.length, 0)} event
        {events.reduce((sum, we) => sum + we.events.length, 0) !== 1 ? 's' : ''} received
      </p>
    </div>
  )
}

// -----------------------------------------------------------------------
// Transaction builder helpers
// -----------------------------------------------------------------------

type CompareTarget = TxnCompare['target']
type CompareResult = TxnCompare['result']

interface CompareRow extends TxnCompare {
  id: number
}
interface OpRow extends TxnOp {
  id: number
}

let _rowId = 0
const nextId = () => ++_rowId

function defaultCompare(): CompareRow {
  return { id: nextId(), key: '', target: 'value', result: 'equal', value: '' }
}
function defaultOp(type: 0 | 1 = 0): OpRow {
  return { id: nextId(), type, key: '', value: '' }
}

function TxnTab({ nodeAddrs, token }: Props) {
  const [compares, setCompares] = useState<CompareRow[]>([defaultCompare()])
  const [successOps, setSuccessOps] = useState<OpRow[]>([defaultOp(0)])
  const [failureOps, setFailureOps] = useState<OpRow[]>([])
  const [loading, setLoading] = useState(false)
  const [result, setResult] = useState<TxnResponse | null>(null)
  const [error, setError] = useState<string | null>(null)

  const updateCompare = (id: number, patch: Partial<TxnCompare>) =>
    setCompares((rows) => rows.map((r) => (r.id === id ? { ...r, ...patch } : r)))
  const removeCompare = (id: number) => setCompares((rows) => rows.filter((r) => r.id !== id))

  const updateOp = (list: OpRow[], setList: (v: OpRow[]) => void, id: number, patch: Partial<TxnOp>) =>
    setList(list.map((r) => (r.id === id ? { ...r, ...patch } : r)))
  const removeOp = (list: OpRow[], setList: (v: OpRow[]) => void, id: number) =>
    setList(list.filter((r) => r.id !== id))

  const handleSubmit = useCallback(async () => {
    if (nodeAddrs.length === 0) return
    setLoading(true)
    setError(null)
    setResult(null)
    try {
      const req = {
        compare: compares.map(({ id: _id, ...c }) => c),
        success: successOps.map(({ id: _id, ...op }) => op),
        failure: failureOps.map(({ id: _id, ...op }) => op),
      }
      const res = await kvTxn(nodeAddrs[0], req, token)
      setResult(res)
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setLoading(false)
    }
  }, [compares, successOps, failureOps, nodeAddrs, token])

  const inputCls =
    'px-2 py-1.5 text-sm border border-gray-300 dark:border-gray-600 rounded bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-1 focus:ring-blue-500'
  const selectCls = inputCls

  return (
    <div className="space-y-6">
      {/* Compare conditions */}
      <section>
        <div className="flex items-center justify-between mb-2">
          <h3 className="text-sm font-semibold text-gray-700 dark:text-gray-300">
            Compare Conditions
            <span className="ml-1 text-xs font-normal text-gray-400">(all must be true for success path)</span>
          </h3>
          <button
            onClick={() => setCompares((r) => [...r, defaultCompare()])}
            className="text-xs px-2 py-1 bg-blue-50 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300 hover:bg-blue-100 dark:hover:bg-blue-900/50 rounded transition-colors"
          >
            + Add condition
          </button>
        </div>
        <div className="space-y-2">
          {compares.map((c) => (
            <div key={c.id} className="flex gap-2 items-center flex-wrap">
              <input
                type="text"
                placeholder="key"
                value={c.key}
                onChange={(e) => updateCompare(c.id, { key: e.target.value })}
                className={`${inputCls} flex-1 min-w-28`}
              />
              <select
                value={c.target}
                onChange={(e) => updateCompare(c.id, { target: e.target.value as CompareTarget })}
                className={selectCls}
              >
                <option value="value">value</option>
                <option value="version">version</option>
                <option value="create_revision">create_rev</option>
                <option value="mod_revision">mod_rev</option>
              </select>
              <select
                value={c.result}
                onChange={(e) => updateCompare(c.id, { result: e.target.value as CompareResult })}
                className={selectCls}
              >
                <option value="equal">=</option>
                <option value="not_equal">≠</option>
                <option value="greater">&gt;</option>
                <option value="less">&lt;</option>
              </select>
              {(c.target === 'value') ? (
                <input
                  type="text"
                  placeholder="value"
                  value={c.value ?? ''}
                  onChange={(e) => updateCompare(c.id, { value: e.target.value })}
                  className={`${inputCls} flex-1 min-w-24`}
                />
              ) : (
                <input
                  type="number"
                  placeholder="revision"
                  value={c.rev ?? ''}
                  onChange={(e) => updateCompare(c.id, { rev: Number(e.target.value) })}
                  className={`${inputCls} w-28`}
                />
              )}
              <button
                onClick={() => removeCompare(c.id)}
                disabled={compares.length === 1}
                className="text-gray-400 hover:text-red-500 disabled:opacity-30 text-lg leading-none"
                title="Remove"
              >
                ×
              </button>
            </div>
          ))}
        </div>
      </section>

      {/* Success ops */}
      <OpsSection
        title="Success Operations"
        subtitle="executed when all compares pass"
        ops={successOps}
        setOps={setSuccessOps}
        updateOp={(id, patch) => updateOp(successOps, setSuccessOps, id, patch)}
        removeOp={(id) => removeOp(successOps, setSuccessOps, id)}
        inputCls={inputCls}
        selectCls={selectCls}
      />

      {/* Failure ops */}
      <OpsSection
        title="Failure Operations"
        subtitle="executed when any compare fails"
        ops={failureOps}
        setOps={setFailureOps}
        updateOp={(id, patch) => updateOp(failureOps, setFailureOps, id, patch)}
        removeOp={(id) => removeOp(failureOps, setFailureOps, id)}
        inputCls={inputCls}
        selectCls={selectCls}
      />

      {/* Submit */}
      <button
        onClick={handleSubmit}
        disabled={loading}
        className="px-5 py-2 text-sm font-medium text-white bg-purple-600 hover:bg-purple-700 disabled:opacity-50 rounded-lg transition-colors"
      >
        {loading ? 'Submitting…' : 'Submit Transaction'}
      </button>

      {/* Error */}
      {error && (
        <div className="text-sm text-red-600 dark:text-red-400 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg px-4 py-2">
          {error}
        </div>
      )}

      {/* Result */}
      {result && (
        <div
          className={`rounded-lg border p-4 space-y-3 ${
            result.succeeded
              ? 'bg-green-50 dark:bg-green-900/20 border-green-200 dark:border-green-800'
              : 'bg-yellow-50 dark:bg-yellow-900/20 border-yellow-200 dark:border-yellow-800'
          }`}
        >
          <div className="flex items-center justify-between">
            <span
              className={`text-sm font-semibold ${
                result.succeeded
                  ? 'text-green-700 dark:text-green-400'
                  : 'text-yellow-700 dark:text-yellow-400'
              }`}
            >
              {result.succeeded ? '✓ Transaction SUCCEEDED' : '✗ Transaction FAILED (compare mismatch)'}
            </span>
            <span className="text-xs text-gray-500 dark:text-gray-400">revision {result.revision}</span>
          </div>
          {result.results && result.results.length > 0 && (
            <div className="space-y-1">
              <p className="text-xs font-medium text-gray-500 dark:text-gray-400">Op results:</p>
              {result.results.map((r, i) =>
                r.error ? (
                  <div key={i} className="text-xs text-red-600 dark:text-red-400">
                    [{i}] Error: {r.error}
                  </div>
                ) : r.kv ? (
                  <div key={i} className="text-xs font-mono text-gray-700 dark:text-gray-300">
                    [{i}] {r.kv.key} = {r.kv.value}{' '}
                    <span className="text-gray-400">(rev:{r.kv.mod_revision})</span>
                  </div>
                ) : (
                  <div key={i} className="text-xs text-gray-500 dark:text-gray-400">
                    [{i}] OK
                  </div>
                ),
              )}
            </div>
          )}
        </div>
      )}
    </div>
  )
}

function OpsSection({
  title,
  subtitle,
  ops,
  setOps,
  updateOp,
  removeOp,
  inputCls,
  selectCls,
}: {
  title: string
  subtitle: string
  ops: OpRow[]
  setOps: (v: OpRow[]) => void
  updateOp: (id: number, patch: Partial<TxnOp>) => void
  removeOp: (id: number) => void
  inputCls: string
  selectCls: string
}) {
  return (
    <section>
      <div className="flex items-center justify-between mb-2">
        <h3 className="text-sm font-semibold text-gray-700 dark:text-gray-300">
          {title}
          <span className="ml-1 text-xs font-normal text-gray-400">({subtitle})</span>
        </h3>
        <button
          onClick={() => setOps([...ops, defaultOp(0)])}
          className="text-xs px-2 py-1 bg-blue-50 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300 hover:bg-blue-100 dark:hover:bg-blue-900/50 rounded transition-colors"
        >
          + Add op
        </button>
      </div>
      {ops.length === 0 ? (
        <p className="text-xs text-gray-400 dark:text-gray-500 italic">No operations (noop path).</p>
      ) : (
        <div className="space-y-2">
          {ops.map((op) => (
            <div key={op.id} className="flex gap-2 items-center flex-wrap">
              <select
                value={op.type}
                onChange={(e) => updateOp(op.id, { type: Number(e.target.value) as 0 | 1 })}
                className={selectCls}
              >
                <option value={0}>PUT</option>
                <option value={1}>DELETE</option>
              </select>
              <input
                type="text"
                placeholder="key"
                value={op.key}
                onChange={(e) => updateOp(op.id, { key: e.target.value })}
                className={`${inputCls} flex-1 min-w-28`}
              />
              {op.type === 0 && (
                <input
                  type="text"
                  placeholder="value"
                  value={op.value ?? ''}
                  onChange={(e) => updateOp(op.id, { value: e.target.value })}
                  className={`${inputCls} flex-1 min-w-24`}
                />
              )}
              <button
                onClick={() => removeOp(op.id)}
                className="text-gray-400 hover:text-red-500 text-lg leading-none"
                title="Remove"
              >
                ×
              </button>
            </div>
          ))}
        </div>
      )}
    </section>
  )
}

// -----------------------------------------------------------------------

export default function KVExplorer({ nodeAddrs, token }: Props) {
  const [tab, setTab] = useState<ActiveTab>('browse')

  const tabs: { id: ActiveTab; label: string }[] = [
    { id: 'browse', label: 'Browse' },
    { id: 'edit', label: 'Get / Put / Delete' },
    { id: 'watch', label: 'Watch (SSE)' },
    { id: 'txn', label: 'Transactions' },
  ]

  return (
    <div>
      <h2 className="text-xl font-bold text-gray-900 dark:text-gray-100 mb-4">KV Store Explorer</h2>

      {nodeAddrs.length === 0 ? (
        <p className="text-gray-500 dark:text-gray-400">
          No nodes configured. Add node addresses in Settings.
        </p>
      ) : (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow">
          {/* Inner tab bar */}
          <div className="flex border-b border-gray-200 dark:border-gray-700">
            {tabs.map((t) => (
              <button
                key={t.id}
                onClick={() => setTab(t.id)}
                className={`px-5 py-3 text-sm font-medium transition-colors ${
                  tab === t.id
                    ? 'border-b-2 border-blue-600 text-blue-600 dark:text-blue-400 dark:border-blue-400'
                    : 'text-gray-600 dark:text-gray-400 hover:text-gray-900 dark:hover:text-gray-200'
                }`}
              >
                {t.label}
              </button>
            ))}
          </div>

          <div className="p-6">
            {tab === 'browse' && <BrowseTab nodeAddrs={nodeAddrs} token={token} />}
            {tab === 'edit' && <EditTab nodeAddrs={nodeAddrs} token={token} />}
            {tab === 'watch' && <WatchTab nodeAddrs={nodeAddrs} token={token} />}
            {tab === 'txn' && <TxnTab nodeAddrs={nodeAddrs} token={token} />}
          </div>
        </div>
      )}
    </div>
  )
}
