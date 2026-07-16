import { useState } from 'react'
import { useAuth } from '../hooks/useAuth'

export default function AuthLogin() {
  const { token, setToken, clearToken } = useAuth()
  const [inputValue, setInputValue] = useState('')

  const handleSave = () => {
    setToken(inputValue)
    setInputValue('')
  }

  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6 max-w-md">
      <h2 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-4">
        Authentication
      </h2>

      {token ? (
        <div className="space-y-4">
          <div className="flex items-center gap-2">
            <span className="inline-flex items-center px-3 py-1 rounded-full text-sm font-medium bg-green-100 dark:bg-green-900 text-green-800 dark:text-green-200">
              <span className="mr-1.5">&#10003;</span> Authenticated
            </span>
          </div>
          <p className="text-sm text-gray-600 dark:text-gray-400">
            Token: <span className="font-mono">{'*'.repeat(Math.min(token.length, 8))}...</span>
          </p>
          <button
            onClick={clearToken}
            className="px-4 py-2 text-sm bg-red-600 hover:bg-red-700 text-white rounded-lg transition-colors"
          >
            Clear Token
          </button>
        </div>
      ) : (
        <div className="space-y-4">
          <div className="flex items-center gap-2">
            <span className="inline-flex items-center px-3 py-1 rounded-full text-sm font-medium bg-yellow-100 dark:bg-yellow-900 text-yellow-800 dark:text-yellow-200">
              No token set
            </span>
          </div>
          <p className="text-sm text-gray-600 dark:text-gray-400">
            An admin token is required for write operations (snapshots, etc.).
          </p>
          <div className="flex gap-2">
            <input
              type="password"
              placeholder="Admin Token"
              value={inputValue}
              onChange={(e) => setInputValue(e.target.value)}
              onKeyDown={(e) => e.key === 'Enter' && handleSave()}
              className="flex-1 px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg text-sm bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
            <button
              onClick={handleSave}
              disabled={!inputValue.trim()}
              className="px-4 py-2 text-sm bg-blue-600 hover:bg-blue-700 disabled:bg-blue-400 text-white rounded-lg transition-colors"
            >
              Save
            </button>
          </div>
        </div>
      )}
    </div>
  )
}
