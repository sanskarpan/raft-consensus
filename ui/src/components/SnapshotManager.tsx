import { useState } from 'react'
import { triggerSnapshot } from '../api/client'

interface Props {
  nodeAddrs: string[]
  token?: string
}

interface SnapshotRecord {
  nodeAddr: string
  timestamp: Date
  success: boolean
  message: string
}

export default function SnapshotManager({ nodeAddrs, token }: Props) {
  const [triggering, setTriggering] = useState<Record<string, boolean>>({})
  const [history, setHistory] = useState<SnapshotRecord[]>([])

  const handleTrigger = async (addr: string) => {
    if (!token) return

    setTriggering((prev) => ({ ...prev, [addr]: true }))
    try {
      await triggerSnapshot(addr, token)
      setHistory((prev) => [
        {
          nodeAddr: addr,
          timestamp: new Date(),
          success: true,
          message: 'Snapshot triggered successfully',
        },
        ...prev.slice(0, 19),
      ])
    } catch (err) {
      setHistory((prev) => [
        {
          nodeAddr: addr,
          timestamp: new Date(),
          success: false,
          message: err instanceof Error ? err.message : 'Unknown error',
        },
        ...prev.slice(0, 19),
      ])
    } finally {
      setTriggering((prev) => ({ ...prev, [addr]: false }))
    }
  }

  return (
    <div>
      <h2 className="text-xl font-bold text-gray-900 dark:text-gray-100 mb-4">
        Snapshot Manager
      </h2>

      {!token && (
        <div className="bg-yellow-50 dark:bg-yellow-900/20 border border-yellow-300 dark:border-yellow-700 rounded-lg p-4 mb-6">
          <p className="text-sm text-yellow-800 dark:text-yellow-200">
            Admin token required for snapshot operations. Set your token in the Settings tab.
          </p>
        </div>
      )}

      <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6 mb-6">
        <h3 className="text-base font-semibold text-gray-900 dark:text-gray-100 mb-4">
          Trigger Snapshot
        </h3>

        {nodeAddrs.length === 0 ? (
          <p className="text-gray-500 dark:text-gray-400">
            No nodes configured. Add node addresses in Settings.
          </p>
        ) : (
          <div className="space-y-3">
            {nodeAddrs.map((addr) => (
              <div key={addr} className="flex items-center justify-between py-2 border-b border-gray-100 dark:border-gray-700 last:border-0">
                <div>
                  <p className="font-mono text-sm text-gray-900 dark:text-gray-100">{addr}</p>
                </div>
                <button
                  onClick={() => handleTrigger(addr)}
                  disabled={!token || triggering[addr]}
                  className="px-4 py-2 text-sm bg-blue-600 hover:bg-blue-700 disabled:bg-gray-400 dark:disabled:bg-gray-600 text-white rounded-lg transition-colors disabled:cursor-not-allowed"
                >
                  {triggering[addr] ? 'Triggering...' : 'Take Snapshot'}
                </button>
              </div>
            ))}
          </div>
        )}
      </div>

      {history.length > 0 && (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
          <h3 className="text-base font-semibold text-gray-900 dark:text-gray-100 mb-4">
            Snapshot History
          </h3>
          <div className="space-y-2">
            {history.map((record, idx) => (
              <div
                key={idx}
                className={`flex items-start justify-between p-3 rounded-lg text-sm ${
                  record.success
                    ? 'bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800'
                    : 'bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800'
                }`}
              >
                <div>
                  <span
                    className={`font-medium ${record.success ? 'text-green-800 dark:text-green-200' : 'text-red-800 dark:text-red-200'}`}
                  >
                    {record.success ? 'Success' : 'Failed'}
                  </span>
                  <span className="ml-2 font-mono text-gray-600 dark:text-gray-400">
                    {record.nodeAddr}
                  </span>
                  {!record.success && (
                    <p className="mt-1 text-xs text-red-600 dark:text-red-400">{record.message}</p>
                  )}
                </div>
                <span className="text-xs text-gray-500 dark:text-gray-400 whitespace-nowrap ml-4">
                  {record.timestamp.toLocaleTimeString()}
                </span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}
