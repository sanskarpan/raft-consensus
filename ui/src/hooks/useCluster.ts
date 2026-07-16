import { useState, useEffect, useCallback } from 'react'
import { fetchClusterInfo, type ClusterInfo } from '../api/client'

export interface NodeState {
  addr: string
  info: ClusterInfo | null
  error: string | null
  loading: boolean
}

export function useCluster(nodeAddrs: string[], token?: string, intervalMs = 2000) {
  const [nodes, setNodes] = useState<Record<string, NodeState>>(() => {
    const initial: Record<string, NodeState> = {}
    for (const addr of nodeAddrs) {
      initial[addr] = { addr, info: null, error: null, loading: true }
    }
    return initial
  })

  const fetchAll = useCallback(async () => {
    const updates = await Promise.allSettled(
      nodeAddrs.map(async (addr) => {
        try {
          const info = await fetchClusterInfo(addr, token || undefined)
          return { addr, info, error: null, loading: false }
        } catch (err) {
          return {
            addr,
            info: null,
            error: err instanceof Error ? err.message : 'Unknown error',
            loading: false,
          }
        }
      }),
    )

    setNodes((prev) => {
      const next = { ...prev }
      for (const result of updates) {
        if (result.status === 'fulfilled') {
          next[result.value.addr] = result.value
        }
      }
      // Remove nodes that are no longer in the list
      for (const addr of Object.keys(next)) {
        if (!nodeAddrs.includes(addr)) {
          delete next[addr]
        }
      }
      return next
    })
  }, [nodeAddrs, token])

  useEffect(() => {
    // Initialize states for new nodes
    setNodes((prev) => {
      const next: Record<string, NodeState> = {}
      for (const addr of nodeAddrs) {
        next[addr] = prev[addr] ?? { addr, info: null, error: null, loading: true }
      }
      return next
    })

    fetchAll()
    const id = setInterval(fetchAll, intervalMs)
    return () => clearInterval(id)
  }, [fetchAll, intervalMs, nodeAddrs])

  return nodes
}

export function useLeaderInfo(nodes: Record<string, NodeState>): ClusterInfo | null {
  for (const node of Object.values(nodes)) {
    if (node.info?.state === 'Leader') return node.info
  }
  // Return any available info to find the leader
  for (const node of Object.values(nodes)) {
    if (node.info) return node.info
  }
  return null
}
