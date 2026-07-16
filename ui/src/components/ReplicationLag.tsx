import { useCluster, useLeaderInfo } from '../hooks/useCluster'

interface Props {
  nodeAddrs: string[]
  token?: string
}

function lagColor(lag: number): string {
  if (lag <= 5) return 'bg-green-500'
  if (lag <= 50) return 'bg-yellow-500'
  return 'bg-red-500'
}

function lagTextColor(lag: number): string {
  if (lag <= 5) return 'text-green-600 dark:text-green-400'
  if (lag <= 50) return 'text-yellow-600 dark:text-yellow-400'
  return 'text-red-600 dark:text-red-400'
}

export default function ReplicationLag({ nodeAddrs, token }: Props) {
  const nodes = useCluster(nodeAddrs, token)
  const leaderInfo = useLeaderInfo(nodes)
  const leaderCommit = leaderInfo?.commit_idx ?? 0

  return (
    <div>
      <h2 className="text-xl font-bold text-gray-900 dark:text-gray-100 mb-4">
        Replication Lag
      </h2>

      {nodeAddrs.length === 0 ? (
        <p className="text-gray-500 dark:text-gray-400">
          No nodes configured. Add node addresses in Settings.
        </p>
      ) : (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6 space-y-5">
          <div className="flex items-center justify-between text-xs text-gray-500 dark:text-gray-400 mb-1">
            <span>0</span>
            <span>Leader commit: {leaderCommit}</span>
          </div>

          {nodeAddrs.map((addr) => {
            const node = nodes[addr]
            const commitIdx = node?.info?.commit_idx ?? 0
            const nodeId = node?.info?.node_id ?? addr
            const isDown = !!node?.error
            const lag = isDown ? leaderCommit : Math.max(0, leaderCommit - commitIdx)
            const fillPercent = leaderCommit > 0 ? (commitIdx / leaderCommit) * 100 : 0

            return (
              <div key={addr} className="space-y-1">
                <div className="flex items-center justify-between text-sm">
                  <span className="font-mono font-medium text-gray-800 dark:text-gray-200">
                    {nodeId}
                  </span>
                  <span className={`font-mono text-sm font-semibold ${isDown ? 'text-red-500' : lagTextColor(lag)}`}>
                    {isDown ? 'DOWN' : `lag: ${lag}`}
                  </span>
                </div>

                <div className="h-5 bg-gray-200 dark:bg-gray-700 rounded-full overflow-hidden relative">
                  {!isDown && (
                    <div
                      className={`h-full rounded-full transition-all duration-500 ${lagColor(lag)}`}
                      style={{ width: `${Math.min(100, fillPercent)}%` }}
                    />
                  )}
                  {isDown && (
                    <div className="h-full w-full bg-red-400 dark:bg-red-700 rounded-full opacity-50" />
                  )}
                </div>

                <div className="flex justify-between text-xs text-gray-500 dark:text-gray-400">
                  <span>commit: {isDown ? '?' : commitIdx}</span>
                  <span className="font-mono text-gray-400 dark:text-gray-500">{addr}</span>
                </div>
              </div>
            )
          })}

          <div className="flex gap-4 text-xs pt-2 border-t border-gray-200 dark:border-gray-700">
            <span className="flex items-center gap-1">
              <span className="inline-block w-3 h-3 rounded bg-green-500" /> Within 5 entries
            </span>
            <span className="flex items-center gap-1">
              <span className="inline-block w-3 h-3 rounded bg-yellow-500" /> Within 50 entries
            </span>
            <span className="flex items-center gap-1">
              <span className="inline-block w-3 h-3 rounded bg-red-500" /> &gt;50 entries behind
            </span>
          </div>
        </div>
      )}
    </div>
  )
}
