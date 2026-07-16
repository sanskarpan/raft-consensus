import { useMetrics } from '../hooks/useMetrics'
import { parsePrometheusHistogram, histogramPercentile } from '../api/client'

interface Props {
  nodeAddr: string
}

interface MetricCardProps {
  label: string
  value: string | number
  unit?: string
  description?: string
}

function MetricCard({ label, value, unit, description }: MetricCardProps) {
  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-4">
      <p className="text-xs text-gray-500 dark:text-gray-400 uppercase tracking-wide mb-1">
        {label}
      </p>
      <p className="text-2xl font-bold font-mono text-gray-900 dark:text-gray-100">
        {value}
        {unit && <span className="text-sm font-normal text-gray-500 ml-1">{unit}</span>}
      </p>
      {description && (
        <p className="text-xs text-gray-400 dark:text-gray-500 mt-1">{description}</p>
      )}
    </div>
  )
}

interface HistogramCardProps {
  label: string
  buckets: Array<{ le: string; count: number }>
  totalCount: number
}

function HistogramCard({ label, buckets, totalCount }: HistogramCardProps) {
  const p50 = histogramPercentile(buckets, totalCount, 50)
  const p99 = histogramPercentile(buckets, totalCount, 99)

  const finiteBuckets = buckets.filter((b) => b.le !== '+Inf')
  const maxCount = Math.max(...finiteBuckets.map((b) => b.count), 1)

  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-4 col-span-1 sm:col-span-2">
      <p className="text-xs text-gray-500 dark:text-gray-400 uppercase tracking-wide mb-3">
        {label}
      </p>

      <div className="flex gap-6 mb-4">
        <div>
          <p className="text-xs text-gray-500 dark:text-gray-400">p50</p>
          <p className="text-lg font-bold font-mono text-gray-900 dark:text-gray-100">
            {p50 > 0 ? `${(p50 * 1000).toFixed(2)}ms` : '—'}
          </p>
        </div>
        <div>
          <p className="text-xs text-gray-500 dark:text-gray-400">p99</p>
          <p className="text-lg font-bold font-mono text-gray-900 dark:text-gray-100">
            {p99 > 0 ? `${(p99 * 1000).toFixed(2)}ms` : '—'}
          </p>
        </div>
        <div>
          <p className="text-xs text-gray-500 dark:text-gray-400">Total</p>
          <p className="text-lg font-bold font-mono text-gray-900 dark:text-gray-100">
            {totalCount}
          </p>
        </div>
      </div>

      {finiteBuckets.length > 0 && (
        <div className="space-y-1">
          {finiteBuckets.slice(-8).map((bucket, i) => {
            const prevCount = i > 0 ? finiteBuckets[finiteBuckets.length - 8 + i - 1].count : 0
            const actualCount = Math.max(0, bucket.count - prevCount)
            const barWidth = maxCount > 0 ? (actualCount / maxCount) * 100 : 0
            return (
              <div key={bucket.le} className="flex items-center gap-2 text-xs">
                <span className="w-16 text-right font-mono text-gray-500 dark:text-gray-400">
                  {parseFloat(bucket.le) < 0.001
                    ? `${(parseFloat(bucket.le) * 1000000).toFixed(0)}µs`
                    : parseFloat(bucket.le) < 1
                      ? `${(parseFloat(bucket.le) * 1000).toFixed(1)}ms`
                      : `${parseFloat(bucket.le).toFixed(1)}s`}
                </span>
                <div className="flex-1 h-3 bg-gray-100 dark:bg-gray-700 rounded overflow-hidden">
                  <div
                    className="h-full bg-blue-500 dark:bg-blue-400 rounded transition-all"
                    style={{ width: `${barWidth}%` }}
                  />
                </div>
                <span className="w-8 text-right font-mono text-gray-500 dark:text-gray-400">
                  {actualCount}
                </span>
              </div>
            )
          })}
        </div>
      )}
    </div>
  )
}

export default function MetricsDashboard({ nodeAddr }: Props) {
  const { rawText, metrics, error, loading } = useMetrics(nodeAddr)

  const raftTerm = metrics.get('raft_term') ?? null
  const commitIndex = metrics.get('raft_commit_index') ?? null
  const appliedIndex = metrics.get('raft_applied_index') ?? null
  const electionsTotal = metrics.get('raft_elections_total') ?? null
  const snapshotSize = metrics.get('raft_snapshot_size_bytes') ?? null

  const aeHist = parsePrometheusHistogram(rawText, 'raft_append_entries_duration_seconds')
  const rvHist = parsePrometheusHistogram(rawText, 'raft_request_vote_duration_seconds')

  return (
    <div>
      <h2 className="text-xl font-bold text-gray-900 dark:text-gray-100 mb-4">
        Metrics Dashboard
      </h2>

      {loading && (
        <p className="text-gray-500 dark:text-gray-400">Loading metrics from {nodeAddr}...</p>
      )}

      {error && (
        <div className="bg-red-50 dark:bg-red-900/20 border border-red-300 dark:border-red-700 rounded-lg p-4 mb-4">
          <p className="text-sm text-red-700 dark:text-red-400">
            Error fetching metrics from {nodeAddr}: {error}
          </p>
        </div>
      )}

      {!loading && !error && metrics.size === 0 && (
        <p className="text-gray-500 dark:text-gray-400">
          No metrics available from {nodeAddr}.
        </p>
      )}

      {metrics.size > 0 && (
        <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-4">
          {raftTerm !== null && (
            <MetricCard label="Raft Term" value={raftTerm} description="Current consensus term" />
          )}
          {commitIndex !== null && (
            <MetricCard
              label="Commit Index"
              value={commitIndex}
              description="Highest committed log index"
            />
          )}
          {appliedIndex !== null && (
            <MetricCard
              label="Applied Index"
              value={appliedIndex}
              description="Highest applied log index"
            />
          )}
          {electionsTotal !== null && (
            <MetricCard
              label="Elections"
              value={electionsTotal}
              description="Total elections started"
            />
          )}
          {snapshotSize !== null && (
            <MetricCard
              label="Snapshot Size"
              value={
                snapshotSize > 1024 * 1024
                  ? `${(snapshotSize / (1024 * 1024)).toFixed(1)}`
                  : snapshotSize > 1024
                    ? `${(snapshotSize / 1024).toFixed(1)}`
                    : snapshotSize.toString()
              }
              unit={
                snapshotSize > 1024 * 1024
                  ? 'MB'
                  : snapshotSize > 1024
                    ? 'KB'
                    : 'B'
              }
              description="Latest snapshot size"
            />
          )}

          {aeHist.buckets.length > 0 && (
            <HistogramCard
              label="AppendEntries Duration"
              buckets={aeHist.buckets}
              totalCount={aeHist.count}
            />
          )}

          {rvHist.buckets.length > 0 && (
            <HistogramCard
              label="RequestVote Duration"
              buckets={rvHist.buckets}
              totalCount={rvHist.count}
            />
          )}
        </div>
      )}
    </div>
  )
}
