import { useState, useCallback } from 'react'

const TOKEN_KEY = 'raft_admin_token'

export function useAuth() {
  const [token, setTokenState] = useState<string>(() => {
    return localStorage.getItem(TOKEN_KEY) ?? ''
  })

  const setToken = useCallback((newToken: string) => {
    const trimmed = newToken.trim()
    if (trimmed) {
      localStorage.setItem(TOKEN_KEY, trimmed)
    } else {
      localStorage.removeItem(TOKEN_KEY)
    }
    setTokenState(trimmed)
  }, [])

  const clearToken = useCallback(() => {
    localStorage.removeItem(TOKEN_KEY)
    setTokenState('')
  }, [])

  return { token, setToken, clearToken }
}
