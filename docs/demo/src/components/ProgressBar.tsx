import React from 'react'
import { InstantiationProgress } from '@duckdb/duckdb-wasm'

export const ProgressBar = ({ progress }: { progress: InstantiationProgress }) => {
  const getProgressPercentage = () => {
    const blocks = Math.max(
      Math.min(Math.floor((progress.bytesLoaded / progress.bytesTotal) * 10.0), 10.0),
      0.0,
    )
    const percentage = Math.round((blocks / 10.0) * 100)

    return percentage
  }

  const percentage = getProgressPercentage()
  const stage = `${(progress.bytesLoaded / 1024 / 1024).toFixed(2)} mb loaded`

  return (
    <div className="bg-blue-50 border border-blue-200 rounded-md p-3 mb-4">
      <div className="flex items-center justify-between mb-2">
        <span className="text-sm font-medium text-blue-800">Initialization Progress</span>
        <span className="text-sm text-blue-600">{percentage}%</span>
      </div>
      <div className="w-full bg-blue-200 rounded-full h-2">
        <div
          className="bg-blue-600 h-2 rounded-full transition-all duration-300 ease-out"
          style={{ width: `${percentage}%` }}
        />
      </div>
      <div className="mt-2 text-xs text-blue-700">{stage}</div>
    </div>
  )
}
