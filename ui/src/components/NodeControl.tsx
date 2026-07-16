import { useState, useEffect, useCallback } from 'react'
import { checkHealth, triggerSnapshot } from '../api/client'
import { useCluster, useLeaderInfo } from '../hooks/useCluster'

interface Props {
  nodeAddrs: string[]
  token?: string
}

type HealthStatus = 'unknown' | 'healthy' | 'unhealthy' | 'checking'

export default function NodeControl({ nodeAddrs, token }: Props) {
  const nodes = useCluster(nodeAddrs, token)
  const leaderInfo = useLeaderInfo(nodes)
  const [healthStatus, setHealthStatus] = useState<Record<string, HealthStatus>>({})
  const [snapshotStatus, setSnapshotStatus] = useState<Record<string, string>>({})
  const [snapshotLoading, setSnapshotLoading] = useState<Record<string, boolean>>({})

  // Initial health check
  useEffect(() => {
    const initial: Record<string, HealthStatus> = {}
    for (const addr of nodeAddrs) {
      initial[addr] = 'unknown'
    }
    setHealthStatus(initial)
  }, [nodeAddrs])

  const handleCheckHealth = useCallback(async (addr: string) => {
    setHealthStatus((prev) => ({ ...prev, [addr]: 'checking' }))
    const ok = await checkHealth(addr)
    setHealthStatus((prev) => ({ ...prev, [addr]: ok ? 'healthy' : 'unhealthy' }))
  }, [])

  const handleForceSnapshot = useCallback(
    async (addr: string) => {
      if (!token) {
        setSnapshotStatus((prev) => ({ ...prev, [addr]: 'No auth token' }))
        return
      }
      setSnapshotLoading((prev) => ({ ...prev, [addr]: true }))
      setSnapshotStatus((prev) => ({ ...prev, [addr]: '' }))
      try {
        await triggerSnapshot(addr, token)
        setSnapshotStatus((prev) => ({ ...prev, [addr]: 'Snapshot triggered' }))
      } catch (err) {
        setSnapshotStatus((prev) => ({
          ...prev,
          [addr]: err instanceof Error ? err.message : 'Failed',
        }))
      } finally {
        setSnapshotLoading((prev) => ({ ...prev, [addr]: false }))
      }
    },
    [token],
  )

  function healthBadge(status: HealthStatus) {
    switch (status) {
      case 'healthy':
        return (
          <span className="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-green-100 dark:bg-green-900 text-green-800 dark:text-green-200">
            Healthy
          </span>
        )
      case 'unhealthy':
        return (
          <span className="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-red-100 dark:bg-red-900 text-red-800 dark:text-red-200">
            Unhealthy
          </span>
        )
      case 'checking':
        return (
          <span className="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-blue-100 dark:bg-blue-900 text-blue-800 dark:text-blue-200">
            Checking...
          </span>
        )
      default:
        return (
          <span className="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400">
            Unknown
          </span>
        )
    }
  }

  return (
    <div>
      <h2 className="text-xl font-bold text-gray-900 dark:text-gray-100 mb-4">Node Control</h2>

      {leaderInfo && (
        <div className="bg-yellow-50 dark:bg-yellow-900/20 border border-yellow-200 dark:border-yellow-800 rounded-lg p-4 mb-6">
          <div className="flex items-center gap-2">
            <span className="text-yellow-500 text-xl">&#9812;</span>
            <div>
              <p className="text-sm font-semibold text-yellow-800 dark:text-yellow-200">
                Current Leader: {leaderInfo.leader || '(none)'}
              </p>
              <p className="text-xs text-yellow-600 dark:text-yellow-400">
                Leader election is automatic in Raft. When the leader becomes unavailable, a new
                election begins. Term: {leaderInfo.term}
              </p>
            </div>
          </div>
        </div>
      )}

      {nodeAddrs.length === 0 ? (
        <p className="text-gray-500 dark:text-gray-400">
          No nodes configured. Add node addresses in Settings.
        </p>
      ) : (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow overflow-hidden">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-750">
                <th className="text-left px-4 py-3 text-xs uppercase text-gray-500 dark:text-gray-400">
                  Node
                </th>
                <th className="text-left px-4 py-3 text-xs uppercase text-gray-500 dark:text-gray-400">
                  Liveness
                </th>
                <th className="text-left px-4 py-3 text-xs uppercase text-gray-500 dark:text-gray-400">
                  State
                </th>
                <th className="text-right px-4 py-3 text-xs uppercase text-gray-500 dark:text-gray-400">
                  Actions
                </th>
              </tr>
            </thead>
            <tbody>
              {nodeAddrs.map((addr) => {
                const node = nodes[addr]
                const nodeId = node?.info?.node_id ?? addr
                const state = node?.info?.state
                const isDown = !!node?.error

                return (
                  <tr
                    key={addr}
                    className="border-b border-gray-100 dark:border-gray-700 last:border-0 hover:bg-gray-50 dark:hover:bg-gray-750"
                  >
                    <td className="px-4 py-3">
                      <p className="font-mono font-medium text-gray-900 dark:text-gray-100">
                        {nodeId}
                      </p>
                      <p className="text-xs text-gray-500 dark:text-gray-400 font-mono">{addr}</p>
                    </td>
                    <td className="px-4 py-3">
                      <div className="flex flex-col gap-1">
                        {isDown ? (
                          <span className="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-red-100 dark:bg-red-900 text-red-800 dark:text-red-200">
                            Down
                          </span>
                        ) : (
                          <span className="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-green-100 dark:bg-green-900 text-green-800 dark:text-green-200">
                            Reachable
                          </span>
                        )}
                        {healthStatus[addr] && healthBadge(healthStatus[addr])}
                      </div>
                    </td>
                    <td className="px-4 py-3">
                      <span className="text-sm text-gray-700 dark:text-gray-300">
                        {state ?? '—'}
                      </span>
                    </td>
                    <td className="px-4 py-3">
                      <div className="flex items-center justify-end gap-2 flex-wrap">
                        <button
                          onClick={() => handleCheckHealth(addr)}
                          disabled={healthStatus[addr] === 'checking'}
                          className="px-3 py-1 text-xs bg-gray-100 dark:bg-gray-700 hover:bg-gray-200 dark:hover:bg-gray-600 text-gray-700 dark:text-gray-300 rounded transition-colors disabled:opacity-50"
                        >
                          Check Health
                        </button>
                        <div className="flex flex-col items-end gap-0.5">
                          <button
                            onClick={() => handleForceSnapshot(addr)}
                            disabled={!token || snapshotLoading[addr]}
                            className="px-3 py-1 text-xs bg-blue-600 hover:bg-blue-700 disabled:bg-gray-400 dark:disabled:bg-gray-600 text-white rounded transition-colors disabled:cursor-not-allowed"
                          >
                            {snapshotLoading[addr] ? 'Snapshotting...' : 'Force Snapshot'}
                          </button>
                          {snapshotStatus[addr] && (
                            <span className="text-xs text-gray-500 dark:text-gray-400">
                              {snapshotStatus[addr]}
                            </span>
                          )}
                        </div>
                      </div>
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
