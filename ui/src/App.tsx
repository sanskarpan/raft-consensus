import { useState, useEffect } from 'react'
import ClusterTopology from './components/ClusterTopology'
import ReplicationLag from './components/ReplicationLag'
import MetricsDashboard from './components/MetricsDashboard'
import LogViewer from './components/LogViewer'
import SnapshotManager from './components/SnapshotManager'
import NodeControl from './components/NodeControl'
import AuthLogin from './components/AuthLogin'
import KVExplorer from './components/KVExplorer'
import { useAuth } from './hooks/useAuth'

const TABS = ['Cluster', 'KV Store', 'Metrics', 'Logs', 'Snapshots', 'Settings'] as const
type Tab = typeof TABS[number]

// Default HTTP addresses matching config-node{1,2,3}.yaml (http_addr :8002/:8004/:8006)
const DEFAULT_NODES = ['localhost:8002', 'localhost:8004', 'localhost:8006']
const NODES_KEY = 'raft_node_addrs'

function loadNodes(): string[] {
  try {
    const raw = localStorage.getItem(NODES_KEY)
    if (raw) return JSON.parse(raw)
  } catch {}
  return DEFAULT_NODES
}

export default function App() {
  const [tab, setTab] = useState<Tab>('Cluster')
  const [dark, setDark] = useState(() => localStorage.getItem('raft_dark') === 'true')
  const [nodes, setNodes] = useState<string[]>(loadNodes)
  const [nodeInput, setNodeInput] = useState(() => loadNodes().join('\n'))
  const [selectedMetricsNode, setSelectedMetricsNode] = useState<string>(() => loadNodes()[0] ?? '')
  const { token } = useAuth()

  useEffect(() => {
    document.documentElement.classList.toggle('dark', dark)
    localStorage.setItem('raft_dark', String(dark))
  }, [dark])

  // Keep selectedMetricsNode valid when nodes list changes
  useEffect(() => {
    if (nodes.length > 0 && !nodes.includes(selectedMetricsNode)) {
      setSelectedMetricsNode(nodes[0])
    }
  }, [nodes, selectedMetricsNode])

  function saveNodes() {
    const list = nodeInput.split('\n').map(s => s.trim()).filter(Boolean)
    setNodes(list)
    localStorage.setItem(NODES_KEY, JSON.stringify(list))
  }

  return (
    <div className="min-h-screen bg-gray-50 dark:bg-gray-900 text-gray-900 dark:text-gray-100 transition-colors">
      {/* Header */}
      <header className="bg-gray-900 dark:bg-gray-800 text-white shadow-lg">
        <div className="max-w-7xl mx-auto px-4 py-3 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <span className="text-2xl">⚙️</span>
            <h1 className="text-xl font-bold tracking-tight">Raft Admin</h1>
          </div>
          <div className="flex items-center gap-4">
            {token && (
              <span className="text-xs bg-green-600 text-white px-2 py-1 rounded-full">
                Authenticated
              </span>
            )}
            <button
              onClick={() => setDark(d => !d)}
              className="text-sm px-3 py-1 rounded bg-gray-700 hover:bg-gray-600 transition-colors"
              title="Toggle dark mode"
            >
              {dark ? '☀️ Light' : '🌙 Dark'}
            </button>
          </div>
        </div>

        {/* Tab nav */}
        <nav className="max-w-7xl mx-auto px-4 flex gap-1 pb-0">
          {TABS.map(t => (
            <button
              key={t}
              onClick={() => setTab(t)}
              className={`px-4 py-2 text-sm font-medium rounded-t transition-colors ${
                tab === t
                  ? 'bg-white dark:bg-gray-900 text-gray-900 dark:text-white'
                  : 'text-gray-300 hover:text-white hover:bg-gray-700'
              }`}
            >
              {t}
            </button>
          ))}
        </nav>
      </header>

      {/* Content */}
      <main className="max-w-7xl mx-auto px-4 py-6">
        {tab === 'Cluster' && (
          <div className="space-y-6">
            <ClusterTopology nodeAddrs={nodes} token={token ?? undefined} />
            <ReplicationLag nodeAddrs={nodes} token={token ?? undefined} />
          </div>
        )}

        {tab === 'KV Store' && (
          <KVExplorer nodeAddrs={nodes} token={token ?? undefined} />
        )}

        {tab === 'Metrics' && (
          <div className="space-y-4">
            {nodes.length > 1 && (
              <div className="flex items-center gap-3">
                <label className="text-sm font-medium text-gray-700 dark:text-gray-300">
                  Node:
                </label>
                <select
                  value={selectedMetricsNode}
                  onChange={e => setSelectedMetricsNode(e.target.value)}
                  className="text-sm border border-gray-300 dark:border-gray-600 rounded px-2 py-1 bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500"
                >
                  {nodes.map(addr => (
                    <option key={addr} value={addr}>{addr}</option>
                  ))}
                </select>
              </div>
            )}
            {nodes.length === 0 ? (
              <p className="text-gray-500 dark:text-gray-400">
                No nodes configured. Add node addresses in Settings.
              </p>
            ) : (
              <MetricsDashboard nodeAddr={selectedMetricsNode || nodes[0]} />
            )}
          </div>
        )}

        {tab === 'Logs' && (
          <LogViewer nodeAddrs={nodes} token={token ?? undefined} />
        )}

        {tab === 'Snapshots' && (
          <div className="space-y-6">
            <SnapshotManager nodeAddrs={nodes} token={token ?? undefined} />
            <NodeControl nodeAddrs={nodes} token={token ?? undefined} />
          </div>
        )}

        {tab === 'Settings' && (
          <div className="space-y-6">
            <AuthLogin />
            <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
              <h2 className="text-lg font-semibold mb-4">Node Addresses</h2>
              <p className="text-sm text-gray-500 dark:text-gray-400 mb-3">
                One HTTP address per line (e.g. <code>localhost:8081</code>)
              </p>
              <textarea
                className="w-full h-32 text-sm font-mono border rounded p-2 bg-gray-50 dark:bg-gray-700 dark:border-gray-600 resize-none focus:outline-none focus:ring-2 focus:ring-blue-500"
                value={nodeInput}
                onChange={e => setNodeInput(e.target.value)}
              />
              <button
                onClick={saveNodes}
                className="mt-3 px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white text-sm rounded transition-colors"
              >
                Save
              </button>
              <div className="mt-3 text-xs text-gray-500 dark:text-gray-400">
                Active nodes: {nodes.join(', ')}
              </div>
            </div>
          </div>
        )}
      </main>
    </div>
  )
}
