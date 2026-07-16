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
    setLoading(true)
    fetch()
    const id = setInterval(fetch, intervalMs)
    return () => clearInterval(id)
  }, [fetch, intervalMs])

  return { rawText, metrics, error, loading }
}
