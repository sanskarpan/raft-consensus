import { useState, useEffect } from 'react'
import ErrorBoundary from './components/ErrorBoundary'
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

// Default HTTP addresses matching config-node{1,2,3}.yaml (http_addr :8012/:8014/:8016)
const DEFAULT_NODES = ['localhost:8012', 'localhost:8014', 'localhost:8016']
const NODES_KEY = 'raft_node_addrs'

function loadNodes(): string[] {
  try {
    const raw = localStorage.getItem(NODES_KEY)
    if (raw) return JSON.parse(raw)
  } catch {
    // ignore malformed stored value
  }
  return DEFAULT_NODES
}

export default function App() {
  const [tab, setTab] = useState<Tab>('Cluster')
  const [dark, setDark] = useState(() => {
    const stored = localStorage.getItem('raft-theme')
    if (stored === 'dark') return true
    if (stored === 'light') return false
    return window.matchMedia('(prefers-color-scheme: dark)').matches
  })
  const [nodes, setNodes] = useState<string[]>(loadNodes)
  const [nodeInput, setNodeInput] = useState(() => loadNodes().join('\n'))
  const [selectedMetricsNode, setSelectedMetricsNode] = useState<string>(() => loadNodes()[0] ?? '')
  const { token } = useAuth()

  useEffect(() => {
    document.documentElement.classList.toggle('dark', dark)
    localStorage.setItem('raft-theme', dark ? 'dark' : 'light')
  }, [dark])

  // Derived during render (no setState-in-effect): fall back to the first node
  // whenever the current selection is not in the list.
  const effectiveMetricsNode = nodes.includes(selectedMetricsNode)
    ? selectedMetricsNode
    : (nodes[0] ?? '')

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
            <span className="text-2xl" aria-hidden="true">⚙️</span>
            <h1 className="text-xl font-bold tracking-tight hidden sm:block">Raft Admin</h1>
          </div>
          <div className="flex items-center gap-4">
            {token && (
              <span className="text-xs bg-green-600 text-white px-2 py-1 rounded-full">
                Authenticated
              </span>
            )}
            <button
              onClick={() => setDark(d => !d)}
              className="p-2 rounded bg-gray-700 hover:bg-gray-600 transition-colors"
              aria-label="Toggle dark mode"
              title="Toggle dark mode"
            >
              {dark ? (
                /* Sun icon — shown in dark mode to switch to light */
                <svg xmlns="http://www.w3.org/2000/svg" className="w-5 h-5 text-yellow-300" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2} aria-hidden="true">
                  <path strokeLinecap="round" strokeLinejoin="round" d="M12 3v2m0 14v2M4.22 4.22l1.42 1.42m12.72 12.72 1.42 1.42M3 12H1m22 0h-2M4.22 19.78l1.42-1.42M18.36 5.64l1.42-1.42M12 8a4 4 0 1 0 0 8 4 4 0 0 0 0-8z" />
                </svg>
              ) : (
                /* Moon icon — shown in light mode to switch to dark */
                <svg xmlns="http://www.w3.org/2000/svg" className="w-5 h-5 text-gray-300" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2} aria-hidden="true">
                  <path strokeLinecap="round" strokeLinejoin="round" d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z" />
                </svg>
              )}
            </button>
          </div>
        </div>

        {/* Tab nav — horizontally scrollable on mobile */}
        <nav className="max-w-7xl mx-auto px-4 flex gap-1 overflow-x-auto scrollbar-none pb-px" role="tablist" aria-label="Dashboard sections">
          {TABS.map(t => (
            <button
              key={t}
              role="tab"
              aria-selected={tab === t}
              aria-controls={`panel-${t}`}
              onClick={() => setTab(t)}
              onKeyDown={e => { if (e.key === 'ArrowLeft' || e.key === 'ArrowRight') { e.preventDefault(); const idx = TABS.indexOf(t); const next = e.key === 'ArrowLeft' ? (idx - 1 + TABS.length) % TABS.length : (idx + 1) % TABS.length; setTab(TABS[next]); } }}
              className={`px-4 py-2 text-sm font-medium rounded-t transition-colors whitespace-nowrap shrink-0 ${
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
        <ErrorBoundary>
        {tab === 'Cluster' && (
          <div id="panel-Cluster" role="tabpanel" className="space-y-6">
            <ClusterTopology nodeAddrs={nodes} token={token ?? undefined} />
            <ReplicationLag nodeAddrs={nodes} token={token ?? undefined} />
          </div>
        )}

        {tab === 'KV Store' && (
          <div id="panel-KV Store" role="tabpanel">
            <KVExplorer nodeAddrs={nodes} token={token ?? undefined} />
          </div>
        )}

        {tab === 'Metrics' && (
          <div className="space-y-4">
            {nodes.length > 1 && (
              <div className="flex flex-wrap items-center gap-3">
                <label className="text-sm font-medium text-gray-700 dark:text-gray-300">
                  Node:
                </label>
                <select
                  value={effectiveMetricsNode}
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
              <MetricsDashboard nodeAddr={effectiveMetricsNode} />
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
        </ErrorBoundary>
      </main>
    </div>
  )
}
