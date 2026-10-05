import React, { useState } from 'react'
import {
  DuckDbCatalogSnapshot,
  DuckDbInitialistionStatus,
  duckdbMachine,
  LoadedTableEntry,
  MachineConfig,
  QueryDbParams,
  TableDefinition,
} from '@jr200-labs/xstate-duckdb'
import { useActor, useSelector } from '@xstate/react'
import { InstantiationProgress, LogLevel } from '@duckdb/duckdb-wasm'
import { DisplayOutputResult } from './types'
import { ProgressBar } from './ProgressBar'
import { getButtonClasses, getTypeStyles } from './styles'
import payloadContent from '/payload.b64ipc_zlib.txt?raw'
import configContent from '/config_yaml.txt?raw'
import yaml from 'js-yaml'
import { formatValue } from '../formatValue'
import { MachineDiagram } from './MachineDiagram'

export const MachineExample = () => {
  const [state, send, actor] = useActor(duckdbMachine)
  const dbCatalogRef = useSelector(actor, (state) => state.children.dbCatalog)
  const dbCatalogState = useSelector(
    dbCatalogRef,
    (state) => state as DuckDbCatalogSnapshot | undefined,
  )
  const activeSubscriptions: Map<string, object> =
    dbCatalogState?.context?.subscriptions ?? new Map()

  const [query, setQuery] = useState('SELECT * FROM duckdb_databases();')
  const [outputs, setOutputs] = useState<DisplayOutputResult[]>([])
  const [config, setConfig] = useState(configContent)

  // New state for catalog panel
  const [tableName, setTableName] = useState('test_table')
  const [tableType, setTableType] = useState<'b64ipc' | 'json'>('b64ipc')
  const [tablePayload, setTablePayload] = useState(payloadContent)

  // New state for initialization progress
  const [initProgress, setInitProgress] = useState<InstantiationProgress | null>(null)

  const addOutput = (type: DisplayOutputResult['type'], data?: any) => {
    const newOutput: DisplayOutputResult = {
      type,
      data,
      timestamp: new Date(),
    }
    setOutputs((prev) => [newOutput, ...prev].slice(0, 100))
  }

  const clearOutput = () => {
    setOutputs([])
  }

  const handleConfigure = () => {
    try {
      const yamlConfig = yaml.load(config) as MachineConfig
      const level = yamlConfig.dbLogLevel
      if (typeof level === 'string') {
        yamlConfig.dbLogLevel = LogLevel[level as keyof typeof LogLevel]
      }
      send({
        type: 'CONFIGURE',
        config: yamlConfig as MachineConfig,
      })
      addOutput('configure', 'Configuration applied successfully')
    } catch (error) {
      console.error(error)
      addOutput('error', `Configuration error: ${error}`)
    }
  }

  const handleConnect = () => {
    const dbProgressHandler = (progress: InstantiationProgress) => {
      setInitProgress(progress)
    }

    send({
      type: 'CONNECT',
      dbProgressHandler: dbProgressHandler,
      statusHandler: (status: DuckDbInitialistionStatus) => {
        addOutput('connect', `Connect command sent: ${status}`)
      },
    })
    addOutput('connect', 'Connect command sent')
  }

  const handleDisconnect = () => {
    send({ type: 'DISCONNECT' })
    addOutput('disconnect', 'Disconnect command sent')
  }

  const handleReset = () => {
    send({ type: 'RESET' })
    addOutput('reset', 'Reset command sent')
  }

  const handleQueryAutoCommit = () => {
    addOutput('query.execute', `Query sent: ${query}`)
    const queryParams: QueryDbParams = {
      sql: query,
      callback: (data) => {
        addOutput('query.execute', data)
      },
      description: 'execute',
      resultOptions: { type: 'array' },
    }
    send({
      type: 'QUERY.EXECUTE',
      queryParams,
    })
  }

  const handleSubscribe = () => {
    addOutput('catalog.subscribe', `Subscribed to ${tableName}`)
    send({
      type: 'CATALOG.SUBSCRIBE',
      subscription: {
        tableSpecName: tableName,
        onSubscribe: (id: string, tableSpecName: string) => {
          addOutput('catalog.subscribe', `Subscribed to ${tableSpecName}, id=${id}`)
        },
        onChange: (tableInstanceName: string, tableVersionId: number) => {
          const received = { tableInstanceName, tableVersionId }
          addOutput('catalog.subscribe', formatValue(received))
        },
      },
    })
  }

  const handleUnsubscribe = () => {
    send({ type: 'CATALOG.UNSUBSCRIBE', id: tableName })
    addOutput('catalog.unsubscribe', `Unsubscribed from ${tableName}`)
  }

  const handleTransactionBegin = () => {
    send({ type: 'TRANSACTION.BEGIN' })
    addOutput('transaction.begin', 'Transaction begin command sent')
  }

  const handleTransactionExecute = () => {
    addOutput('transaction.execute', `Query sent: ${query}`)
    const queryParams: QueryDbParams = {
      sql: query,
      callback: (data) => {
        addOutput('transaction.execute', data)
      },
      description: 'transaction.execute',
      resultOptions: { type: 'array' },
    }
    send({
      type: 'TRANSACTION.EXECUTE',
      queryParams,
    })
  }

  const handleTransactionCommit = () => {
    send({ type: 'TRANSACTION.COMMIT' })
    addOutput('transaction.commit', 'Transaction commit command sent')
  }

  const handleTransactionRollback = () => {
    send({ type: 'TRANSACTION.ROLLBACK' })
    addOutput('transaction.rollback', 'Transaction rollback command sent')
  }

  // New catalog handlers
  const handleListTables = () => {
    send({
      type: 'CATALOG.LIST_TABLES',
      callback: (tables: LoadedTableEntry[]) => {
        addOutput('catalog.list_tables', formatValue(tables))
      },
    })
  }

  const handleLoadTable = () => {
    try {
      let payload
      if (tableType === 'json') {
        payload = JSON.parse(tablePayload)
      } else {
        payload = tablePayload // For arrow, this would be base64 encoded data
      }

      send({
        type: 'CATALOG.LOAD_TABLE',
        data: {
          tableSpecName: tableName,
          tablePayload: payload,
          payloadType: tableType,
          payloadCompression: tableType === 'json' ? 'none' : 'zlib',
          callback: (tableInstanceName: string, error?: string) => {
            addOutput('catalog.load_table', { tableInstanceName, error })
          },
        },
      })
    } catch (error) {
      console.error(error)
      addOutput('error', `Load table error: ${error}`)
    }
  }

  const handleDropTable = () => {
    send({ type: 'CATALOG.DROP_TABLE', tableName })
    addOutput('catalog.drop_table', `Drop table command sent for: ${tableName}`)
  }

  const handleShowConfiguration = () => {
    send({
      type: 'CATALOG.LIST_DEFINITIONS',
      callback: (config: TableDefinition[]) => {
        addOutput('catalog.list_definitions', formatValue(config))
      },
    })
  }

  return (
    <div className="space-y-4 bg-gray-100 p-4">
      <MachineDiagram snapshot={state} />
      <div className="grid grid-cols-1 xl:grid-cols-[1fr_2fr] gap-4">
        {/* Left Panel - Output */}
        <div className="min-w-0 bg-white shadow-lg border-r border-gray-200 p-4 flex flex-col">
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-lg font-semibold">Output</h2>
            <div className="flex gap-2">
              <button onClick={clearOutput} className={getButtonClasses('clear')}>
                Clear
              </button>
            </div>
          </div>
          <div className="bg-gray-50 border border-gray-300 rounded-md p-3 flex-1 overflow-y-auto">
            {outputs.length === 0 ? (
              <div className="text-gray-500 text-center py-8">No output yet...</div>
            ) : (
              <div className="space-y-3">
                {outputs.map((output, index) => (
                  <div
                    key={index}
                    className="bg-white rounded-lg border border-gray-200 p-3 shadow-sm"
                  >
                    <div className="flex items-center justify-between mb-2">
                      <span
                        className={`${getTypeStyles(output.type).base} text-white px-2 py-1 rounded text-xs font-medium uppercase`}
                      >
                        {output.type}
                      </span>
                      <span className="text-gray-500 text-xs">
                        {output.timestamp.toLocaleTimeString()}
                      </span>
                    </div>
                    <div className="font-mono text-sm text-gray-800 break-words">
                      {typeof output.data === 'string' ? output.data : formatValue(output.data)}
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>

        {/* Middle Panel - Controls and Configuration */}
        <div className="flex-1 flex flex-col p-4">
          {/* 2x2 Grid Layout */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4 h-full">
            {/* Left Column - Configuration and Query stacked */}
            <div className="flex flex-col gap-4">
              {/* Configuration Panel */}
              <div className="bg-white rounded-lg shadow-md p-4">
                <h2 className="text-lg font-semibold mb-2">Configuration</h2>
                <div className="grid grid-cols-1 gap-4">
                  <div>
                    <h3 className="text-sm font-medium text-gray-700 mb-2">DuckDB Configuration</h3>
                    <textarea
                      aria-label="DuckDB configuration"
                      value={config}
                      onChange={(e) => setConfig(e.target.value)}
                      className="w-full h-48 p-2 border border-gray-300 rounded-md font-mono text-xs"
                      placeholder="Enter configuration YAML..."
                    />
                  </div>
                </div>
              </div>

              {/* Query Panel */}
              <div className="bg-white rounded-lg shadow-md p-4 flex flex-col flex-1">
                <h2 className="text-lg font-semibold mb-2">Query</h2>
                <textarea
                  aria-label="SQL query"
                  value={query}
                  onChange={(e) => setQuery(e.target.value)}
                  className="w-full flex-1 p-2 border border-gray-300 rounded-md font-mono text-sm resize-none"
                  placeholder="Enter your SQL query..."
                />

                <div className="mb-3 mt-4" />
                <div className="flex flex-wrap gap-2">
                  <button
                    disabled={
                      !state.can({
                        type: 'CONFIGURE',
                        config: {
                          dbLogLevel: LogLevel.DEBUG,
                          dbInitParams: {},
                          tableDefinitions: [],
                        },
                      })
                    }
                    onClick={handleConfigure}
                    className={getButtonClasses(
                      'configure',
                      !state.can({
                        type: 'CONFIGURE',
                        config: {
                          dbLogLevel: LogLevel.DEBUG,
                          dbInitParams: {},
                          tableDefinitions: [],
                        },
                      }),
                    )}
                  >
                    Configure
                  </button>
                  <button
                    disabled={
                      !state.can({ type: 'CONNECT', dbProgressHandler: null, statusHandler: null })
                    }
                    onClick={handleConnect}
                    className={getButtonClasses(
                      'connect',
                      !state.can({ type: 'CONNECT', dbProgressHandler: null, statusHandler: null }),
                    )}
                  >
                    Connect
                  </button>
                  <button
                    disabled={!state.can({ type: 'DISCONNECT' })}
                    onClick={handleDisconnect}
                    className={getButtonClasses('disconnect', !state.can({ type: 'DISCONNECT' }))}
                  >
                    Disconnect
                  </button>
                  <button
                    disabled={!state.can({ type: 'RESET' })}
                    onClick={handleReset}
                    className={getButtonClasses('reset', !state.can({ type: 'RESET' }))}
                  >
                    Reset
                  </button>
                  <button
                    disabled={
                      !state.can({
                        type: 'QUERY.EXECUTE',
                        queryParams: {
                          sql: query,
                          callback: () => {},
                          description: 'QUERY.EXECUTE',
                          resultOptions: { type: 'array' },
                        },
                      })
                    }
                    onClick={() => handleQueryAutoCommit()}
                    className={getButtonClasses(
                      'query.execute',
                      !state.can({
                        type: 'QUERY.EXECUTE',
                        queryParams: {
                          sql: query,
                          callback: () => {},
                          description: 'QUERY.EXECUTE',
                          resultOptions: { type: 'array' },
                        },
                      }),
                    )}
                  >
                    Query (auto-commit)
                  </button>
                  <button
                    disabled={!state.can({ type: 'TRANSACTION.BEGIN' })}
                    onClick={handleTransactionBegin}
                    className={getButtonClasses(
                      'transaction.begin',
                      !state.can({ type: 'TRANSACTION.BEGIN' }),
                    )}
                  >
                    Begin Transaction
                  </button>
                  <button
                    disabled={
                      !state.can({
                        type: 'TRANSACTION.EXECUTE',
                        queryParams: {
                          sql: query,
                          callback: () => {},
                          description: 'TRANSACTION.EXECUTE',
                          resultOptions: { type: 'array' },
                        },
                      })
                    }
                    onClick={handleTransactionExecute}
                    className={getButtonClasses(
                      'transaction.execute',
                      !state.can({
                        type: 'TRANSACTION.EXECUTE',
                        queryParams: {
                          sql: query,
                          callback: () => {},
                          description: 'TRANSACTION.EXECUTE',
                          resultOptions: { type: 'array' },
                        },
                      }),
                    )}
                  >
                    Execute
                  </button>
                  <button
                    disabled={!state.can({ type: 'TRANSACTION.COMMIT' })}
                    onClick={handleTransactionCommit}
                    className={getButtonClasses(
                      'transaction.commit',
                      !state.can({ type: 'TRANSACTION.COMMIT' }),
                    )}
                  >
                    Commit
                  </button>
                  <button
                    disabled={!state.can({ type: 'TRANSACTION.ROLLBACK' })}
                    onClick={handleTransactionRollback}
                    className={getButtonClasses(
                      'transaction.rollback',
                      !state.can({ type: 'TRANSACTION.ROLLBACK' }),
                    )}
                  >
                    Rollback
                  </button>
                </div>
              </div>
            </div>

            {/* Right Column - Catalog spanning both rows */}
            <div className="bg-white rounded-lg shadow-md p-4 flex flex-col row-span-2">
              <h2 className="text-lg font-semibold mb-2">Catalog</h2>

              {/* Active Subscriptions Section */}
              <div className="mb-4">
                <h3 className="text-md font-medium text-gray-700 mb-2">Active Subscriptions</h3>
                {activeSubscriptions && activeSubscriptions.size > 0 ? (
                  <div className="space-y-2">
                    {Array.from(activeSubscriptions.keys()).map((id) => (
                      <div
                        key={id}
                        className="flex items-center justify-between bg-gray-50 rounded-md p-2"
                      >
                        <span className="text-sm text-gray-700">{id}</span>
                        <button
                          onClick={() => {
                            send({
                              type: 'CATALOG.UNSUBSCRIBE',
                              id,
                            })
                            addOutput('catalog.unsubscribe', `Unsubscribed from ${id}`)
                          }}
                          className="text-red-500 hover:text-red-700 text-sm font-medium px-2 py-1 rounded hover:bg-red-50 transition-colors"
                        >
                          ✕
                        </button>
                      </div>
                    ))}
                  </div>
                ) : (
                  <div className="text-sm text-gray-500 italic">No active subscriptions</div>
                )}
              </div>

              <div className="grid grid-cols-2 gap-4 mb-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Table Name</label>
                  <input
                    type="text"
                    value={tableName}
                    onChange={(e) => setTableName(e.target.value)}
                    className="w-full p-2 border border-gray-300 rounded-md text-sm"
                    placeholder="Enter table name..."
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Table Type</label>
                  <select
                    value={tableType}
                    onChange={(e) => setTableType(e.target.value as 'b64ipc' | 'json')}
                    className="w-full p-2 border border-gray-300 rounded-md text-sm"
                  >
                    <option value="b64ipc">IPC (Base64)</option>
                    <option value="json" disabled>
                      JSON (not implemented)
                    </option>
                  </select>
                </div>
              </div>
              <div className="flex-1 flex flex-col min-h-0">
                <label className="block text-sm font-medium text-gray-700 mb-1">Payload</label>
                <textarea
                  aria-label="Table payload"
                  value={tablePayload}
                  onChange={(e) => setTablePayload(e.target.value)}
                  className="w-full flex-1 p-2 border border-gray-300 rounded-md text-sm resize-none"
                  placeholder={
                    tableType === 'json' ? 'Enter JSON payload...' : 'Enter base64 Arrow data...'
                  }
                />
              </div>

              <div className="flex flex-wrap gap-2 mt-4">
                <button
                  disabled={!state.can({ type: 'CATALOG.LIST_DEFINITIONS', callback: () => {} })}
                  onClick={handleShowConfiguration}
                  className={getButtonClasses(
                    'catalog.list_definitions',
                    !state.can({ type: 'CATALOG.LIST_DEFINITIONS', callback: () => {} }),
                  )}
                >
                  List Definitions
                </button>

                <button
                  disabled={!state.can({ type: 'CATALOG.LIST_TABLES', callback: () => {} })}
                  onClick={handleListTables}
                  className={getButtonClasses(
                    'catalog.list_tables',
                    !state.can({ type: 'CATALOG.LIST_TABLES', callback: () => {} }),
                  )}
                >
                  List Tables
                </button>
                <button
                  disabled={
                    !state.can({
                      type: 'CATALOG.LOAD_TABLE',
                      data: {
                        tableSpecName: '',
                        tablePayload: '',
                        payloadType: 'b64ipc',
                        payloadCompression: 'none',
                      },
                    })
                  }
                  onClick={handleLoadTable}
                  className={getButtonClasses(
                    'catalog.load_table',
                    !state.can({
                      type: 'CATALOG.LOAD_TABLE',
                      data: {
                        tableSpecName: '',
                        tablePayload: '',
                        payloadType: 'b64ipc',
                        payloadCompression: 'none',
                      },
                    }),
                  )}
                >
                  Load Table
                </button>
                <button
                  disabled
                  title="Table removal is not implemented by the catalog yet"
                  onClick={handleDropTable}
                  className={getButtonClasses('catalog.drop_table', true)}
                >
                  Drop Table
                </button>
                <button
                  disabled={
                    !state.can({
                      type: 'CATALOG.SUBSCRIBE',
                      subscription: {
                        tableSpecName: '',
                        onSubscribe: () => {},
                        onChange: () => {},
                      },
                    })
                  }
                  onClick={handleSubscribe}
                  className={getButtonClasses(
                    'catalog.subscribe',
                    !state.can({
                      type: 'CATALOG.SUBSCRIBE',
                      subscription: {
                        tableSpecName: '',
                        onSubscribe: () => {},
                        onChange: () => {},
                      },
                    }),
                  )}
                >
                  Subscribe
                </button>
                <button
                  disabled={!state.can({ type: 'CATALOG.UNSUBSCRIBE', id: tableName })}
                  onClick={handleUnsubscribe}
                  className={getButtonClasses(
                    'catalog.unsubscribe',
                    !state.can({ type: 'CATALOG.UNSUBSCRIBE', id: tableName }),
                  )}
                >
                  Unsubscribe
                </button>
              </div>
            </div>
          </div>
        </div>

        {/* Right Sidepanel - Machine State */}
        <div className="xl:col-span-2 min-w-0 bg-white shadow-lg border-l border-gray-200 p-4 flex flex-col h-full">
          <h2 className="text-lg font-semibold mb-4 flex-shrink-0">Machine State</h2>

          {/* Progress Bar - Show only during initialization */}
          {state.matches('initializing') && initProgress && <ProgressBar progress={initProgress} />}

          <div className="space-y-4 flex-1 flex flex-col min-h-0">
            <div className="flex-shrink-0">
              <h4 className="font-medium text-gray-700 mb-1 flex-shrink-0">Root / Catalog</h4>
              <div className="bg-blue-50 border border-blue-200 rounded-md p-3">
                <code className="text-sm text-blue-800">
                  {formatValue(state.value)} / {formatValue(dbCatalogState?.value)}
                </code>
              </div>
            </div>

            <div className="flex-1 flex flex-col min-h-0">
              <h4 className="font-medium text-gray-700 mb-2 flex-shrink-0">Root State</h4>
              <div className="bg-gray-50 border border-gray-200 rounded-md p-3 flex-1 overflow-y-auto min-h-0">
                <pre className="text-xs text-gray-700">
                  {formatValue(state.context, { maxDepth: 2 })}
                </pre>
              </div>
            </div>

            <div className="flex-1 flex flex-col min-h-0">
              <h4 className="font-medium text-gray-700 mb-2 flex-shrink-0">Catalog State</h4>
              <div className="bg-gray-50 p-3 rounded-lg flex-1 overflow-y-auto min-h-0">
                <pre className="text-xs text-gray-700 whitespace-pre-wrap">
                  {formatValue(dbCatalogState?.context, { maxDepth: 3 })}
                </pre>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
