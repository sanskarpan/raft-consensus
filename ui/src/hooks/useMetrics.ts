import { useState, useEffect, useCallback } from 'react'
import { fetchMetrics, parsePrometheus } from '../api/client'

export function useMetrics(addr: string, intervalMs = 5000) {
  const [rawText, setRawText] = useState<string>('')
  const [metrics, setMetrics] = useState<Map<string, number>>(new Map())
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)

  const fetch = useCallback(async () => {
    try {
      const text = await fetchMetrics(addr)
      setRawText(text)
      setMetrics(parsePrometheus(text))
      setError(null)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Unknown error')
    } finally {
      setLoading(false)
    }
  }, [addr])

  useEffect(() => {
    // Poll-on-mount + interval. `loading` starts true (useState) and clears in
    // fetch's finally, so no flicker on poll ticks. This is the documented
    // data-fetching-in-an-effect pattern (the rule flags any effect that calls a
    // setState-containing callback); same convention as useCluster.ts / KVExplorer.
    // eslint-disable-next-line react-hooks/set-state-in-effect
    fetch()
    const id = setInterval(fetch, intervalMs)
    return () => clearInterval(id)
  }, [fetch, intervalMs])

  return { rawText, metrics, error, loading }
}
