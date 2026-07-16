import { useCluster, type NodeState } from '../hooks/useCluster'

interface Props {
  nodeAddrs: string[]
  token?: string
}

function stateBorderClass(state: string | undefined, error: string | null): string {
  if (error) return 'border-red-500 dark:border-red-400'
  switch (state) {
    case 'Leader':
      return 'border-yellow-400 dark:border-yellow-300'
    case 'Follower':
      return 'border-blue-500 dark:border-blue-400'
    case 'Candidate':
      return 'border-purple-500 dark:border-purple-400'
    case 'Learner':
      return 'border-gray-400 dark:border-gray-500'
    default:
      return 'border-red-500 dark:border-red-400'
  }
}

function StateBadge({ state, error }: { state: string | undefined; error: string | null }) {
  if (error) {
    return (
      <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-bold bg-red-100 dark:bg-red-900 text-red-700 dark:text-red-300">
        DOWN
      </span>
    )
  }
  if (!state) return null
  const colors: Record<string, string> = {
    Leader: 'bg-yellow-100 dark:bg-yellow-900 text-yellow-800 dark:text-yellow-200',
    Follower: 'bg-blue-100 dark:bg-blue-900 text-blue-800 dark:text-blue-200',
    Candidate: 'bg-purple-100 dark:bg-purple-900 text-purple-800 dark:text-purple-200',
    Learner: 'bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300',
    Shutdown: 'bg-red-100 dark:bg-red-900 text-red-700 dark:text-red-300',
  }
  return (
    <span
      className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-bold ${colors[state] ?? colors.Shutdown}`}
    >
      {state}
    </span>
  )
}

function NodeCard({ node }: { node: NodeState }) {
  const { addr, info, error, loading } = node
  const state = info?.state
  const border = stateBorderClass(state, error)

  return (
    <div
      className={`bg-white dark:bg-gray-800 rounded-lg shadow border-2 ${border} p-4 transition-all`}
    >
      <div className="flex items-start justify-between mb-3">
        <div className="flex items-center gap-2">
          {state === 'Leader' && (
            <span title="Leader" className="text-yellow-400 text-xl">
              &#9812;
            </span>
          )}
          <div>
            <p className="font-mono text-sm font-semibold text-gray-900 dark:text-gray-100">
              {info?.node_id ?? addr}
            </p>
            <p className="text-xs text-gray-500 dark:text-gray-400 font-mono">{addr}</p>
          </div>
        </div>
        <div className="flex flex-col items-end gap-1">
          <StateBadge state={state} error={error} />
          {info?.config.Servers.find((s) => s.ID === info.node_id)?.Learner && (
            <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-bold bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400">
              LEARNER
            </span>
          )}
        </div>
      </div>

      {loading && !info && (
        <p className="text-sm text-gray-400 dark:text-gray-500">Connecting...</p>
      )}

      {error && (
        <p className="text-xs text-red-500 dark:text-red-400 truncate" title={error}>
          {error}
        </p>
      )}

      {info && (
        <dl className="grid grid-cols-2 gap-x-4 gap-y-1 text-sm">
          <dt className="text-gray-500 dark:text-gray-400">Term</dt>
          <dd className="font-mono font-medium text-gray-900 dark:text-gray-100">{info.term}</dd>

          <dt className="text-gray-500 dark:text-gray-400">Commit</dt>
          <dd className="font-mono font-medium text-gray-900 dark:text-gray-100">
            {info.commit_idx}
          </dd>

          <dt className="text-gray-500 dark:text-gray-400">Leader</dt>
          <dd className="font-mono font-medium text-gray-900 dark:text-gray-100 truncate">
            {info.leader || '—'}
          </dd>
        </dl>
      )}
    </div>
  )
}

export default function ClusterTopology({ nodeAddrs, token }: Props) {
  const nodes = useCluster(nodeAddrs, token)

  return (
    <div>
      <h2 className="text-xl font-bold text-gray-900 dark:text-gray-100 mb-4">
        Cluster Topology
      </h2>
      {nodeAddrs.length === 0 ? (
        <p className="text-gray-500 dark:text-gray-400">
          No nodes configured. Add node addresses in Settings.
        </p>
      ) : (
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
          {nodeAddrs.map((addr) => (
            <NodeCard key={addr} node={nodes[addr] ?? { addr, info: null, error: 'Unknown', loading: false }} />
          ))}
        </div>
      )}
    </div>
  )
}
