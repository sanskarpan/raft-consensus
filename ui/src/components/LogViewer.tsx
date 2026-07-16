import { useState, useEffect, useRef, useCallback } from 'react'
import { fetchClusterInfo, type ClusterInfo } from '../api/client'

interface LogEntry {
  timestamp: string
  nodeId: string
  event: string
}

interface NodeSnapshot {
  term: number
  leader: string
  commitIdx: number
  state: string
}

interface Props {
  nodeAddrs: string[]
  token?: string
}

const MAX_EVENTS = 200

function formatTime(date: Date): string {
  return date.toISOString().replace('T', ' ').substring(0, 23)
}

function diffSnapshots(
  addr: string,
  prev: NodeSnapshot | undefined,
  curr: ClusterInfo,
): LogEntry[] {
  const events: LogEntry[] = []
  const ts = formatTime(new Date())
  const nodeId = curr.node_id

  if (!prev) {
    events.push({
      timestamp: ts,
      nodeId,
      event: `Connected to ${addr} — state=${curr.state} term=${curr.term} leader=${curr.leader || 'none'} commit=${curr.commit_idx}`,
    })
    return events
  }

  if (prev.term !== curr.term) {
    events.push({
      timestamp: ts,
      nodeId,
      event: `Term changed ${prev.term} → ${curr.term}`,
    })
  }

  if (prev.state !== curr.state) {
    events.push({
      timestamp: ts,
      nodeId,
      event: `State changed ${prev.state} → ${curr.state}`,
    })
  }

  if (prev.leader !== curr.leader && curr.leader) {
    events.push({
      timestamp: ts,
      nodeId,
      event: `Leader is now ${curr.leader}`,
    })
  }

  if (prev.commitIdx !== curr.commit_idx) {
    events.push({
      timestamp: ts,
      nodeId,
      event: `commit_idx advanced ${prev.commitIdx} → ${curr.commit_idx}`,
    })
  }

  return events
}

export default function LogViewer({ nodeAddrs, token }: Props) {
  const [entries, setEntries] = useState<LogEntry[]>([])
  const [paused, setPaused] = useState(false)
  const textareaRef = useRef<HTMLTextAreaElement>(null)
  const snapshotsRef = useRef<Map<string, NodeSnapshot>>(new Map())
  const pausedRef = useRef(paused)
  pausedRef.current = paused

  const appendEntries = useCallback((newEntries: LogEntry[]) => {
    if (newEntries.length === 0) return
    setEntries((prev) => {
      const combined = [...prev, ...newEntries]
      return combined.slice(-MAX_EVENTS)
    })
  }, [])

  useEffect(() => {
    if (nodeAddrs.length === 0) return

    const poll = async () => {
      if (pausedRef.current) return

      const results = await Promise.allSettled(
        nodeAddrs.map((addr) => fetchClusterInfo(addr, token || undefined)),
      )

      const newEvents: LogEntry[] = []
      for (let i = 0; i < nodeAddrs.length; i++) {
        const addr = nodeAddrs[i]
        const result = results[i]

        if (result.status === 'rejected') {
          const prev = snapshotsRef.current.get(addr)
          if (prev) {
            // Node went down
            newEvents.push({
              timestamp: formatTime(new Date()),
              nodeId: addr,
              event: `Node unreachable: ${result.reason instanceof Error ? result.reason.message : 'connection failed'}`,
            })
            snapshotsRef.current.delete(addr)
          }
          continue
        }

        const curr = result.value
        const prev = snapshotsRef.current.get(addr)
        const events = diffSnapshots(addr, prev, curr)
        newEvents.push(...events)

        snapshotsRef.current.set(addr, {
          term: curr.term,
          leader: curr.leader,
          commitIdx: curr.commit_idx,
          state: curr.state,
        })
      }

      appendEntries(newEvents)
    }

    poll()
    const id = setInterval(poll, 1000)
    return () => clearInterval(id)
  }, [nodeAddrs, token, appendEntries])

  // Auto-scroll to bottom
  useEffect(() => {
    if (!paused && textareaRef.current) {
      textareaRef.current.scrollTop = textareaRef.current.scrollHeight
    }
  }, [entries, paused])

  const logText = entries
    .map((e) => `[${e.timestamp}] [${e.nodeId}] ${e.event}`)
    .join('\n')

  const handleClear = () => {
    setEntries([])
    snapshotsRef.current.clear()
  }

  return (
    <div>
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-xl font-bold text-gray-900 dark:text-gray-100">Cluster Event Log</h2>
        <div className="flex gap-2">
          <button
            onClick={() => setPaused((p) => !p)}
            className={`px-3 py-1.5 text-sm rounded-lg transition-colors ${
              paused
                ? 'bg-yellow-100 dark:bg-yellow-900 text-yellow-800 dark:text-yellow-200 border border-yellow-300 dark:border-yellow-700'
                : 'bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-gray-600'
            }`}
          >
            {paused ? 'Resume' : 'Pause'}
          </button>
          <button
            onClick={handleClear}
            className="px-3 py-1.5 text-sm rounded-lg bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-gray-600 transition-colors"
          >
            Clear
          </button>
        </div>
      </div>

      {nodeAddrs.length === 0 ? (
        <p className="text-gray-500 dark:text-gray-400">
          No nodes configured. Add node addresses in Settings.
        </p>
      ) : (
        <div className="bg-gray-900 rounded-lg shadow overflow-hidden">
          <div className="flex items-center justify-between px-3 py-2 bg-gray-800 border-b border-gray-700">
            <span className="text-xs font-mono text-gray-400">
              {entries.length} events (last {MAX_EVENTS} kept)
            </span>
            {paused && (
              <span className="text-xs text-yellow-400 font-semibold">PAUSED</span>
            )}
          </div>
          <textarea
            ref={textareaRef}
            readOnly
            value={logText}
            placeholder="Waiting for cluster events..."
            className="w-full h-96 bg-gray-900 text-green-400 font-mono text-xs p-3 resize-none focus:outline-none"
            style={{ fontFamily: "'Courier New', Courier, monospace" }}
          />
        </div>
      )}
    </div>
  )
}
