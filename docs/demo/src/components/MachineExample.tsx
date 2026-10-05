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

  const [panel, setPanel] = useState<'query' | 'catalog' | 'configuration'>('query')
  const [inspector, setInspector] = useState<'output' | 'state' | null>(null)
  const [queryResult, setQueryResult] = useState<unknown>(null)
  const [latestMessage, setLatestMessage] = useState('Configure → Connect → Run a query')
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
    setLatestMessage(
      type === 'error'
        ? String(data)
        : `${type} · ${typeof data === 'string' ? data : 'Result received'}`,
    )
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
        setQueryResult(data)
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
        setQueryResult(data)
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

  const handleShowConfiguration = () => {
    send({
      type: 'CATALOG.LIST_DEFINITIONS',
      callback: (config: TableDefinition[]) => {
        addOutput('catalog.list_definitions', formatValue(config))
      },
    })
  }

  const connected = state.matches('connected')
  const withinTransaction = state.matches({ transaction: 'within_transaction' })
  const canConfigure = state.matches('idle')
  const canConnect = state.matches('configured')
  const toggleInspector = (next: 'output' | 'state') =>
    setInspector((current) => (current === next ? null : next))

  return (
    <div className="demo-app">
      <header className="demo-toolbar">
        <div className="connection-controls" aria-label="Database connection">
          <button disabled={!canConfigure} onClick={handleConfigure}>
            Configure
          </button>
          <button className="primary" disabled={!canConnect} onClick={handleConnect}>
            Connect
          </button>
          <button disabled={!connected} onClick={handleDisconnect}>
            Disconnect
          </button>
          <button disabled={!state.can({ type: 'RESET' })} onClick={handleReset}>
            Reset
          </button>
        </div>
        <div className="inspector-controls">
          <button
            aria-expanded={inspector === 'output'}
            aria-controls="output-inspector"
            onClick={() => toggleInspector('output')}
          >
            Output <span className="count">{outputs.length}</span>
          </button>
          <button
            aria-expanded={inspector === 'state'}
            aria-controls="state-inspector"
            onClick={() => toggleInspector('state')}
          >
            State details
          </button>
        </div>
      </header>
      <MachineDiagram snapshot={state} />
      <div className={`demo-workspace ${inspector ? 'with-inspector' : ''}`}>
        <main className="demo-editor">
          <nav className="editor-tabs" aria-label="Demo workspace">
            {(['query', 'catalog', 'configuration'] as const).map((tab) => (
              <button key={tab} aria-pressed={panel === tab} onClick={() => setPanel(tab)}>
                {tab === 'query'
                  ? 'SQL query'
                  : tab === 'catalog'
                    ? 'Table catalog'
                    : 'Configuration'}
              </button>
            ))}
          </nav>
          {panel === 'query' && (
            <section className="editor-panel query-panel" aria-label="SQL query workspace">
              <textarea
                aria-label="SQL query"
                spellCheck={false}
                value={query}
                onChange={(e) => setQuery(e.target.value)}
              />
              <div className="editor-actions">
                <button
                  className="primary"
                  disabled={!connected && !withinTransaction}
                  onClick={withinTransaction ? handleTransactionExecute : handleQueryAutoCommit}
                >
                  {withinTransaction ? 'Execute in transaction' : 'Run query'}
                </button>
                <span className="action-divider" />
                <button disabled={!connected} onClick={handleTransactionBegin}>
                  Begin transaction
                </button>
                <button disabled={!withinTransaction} onClick={handleTransactionCommit}>
                  Commit
                </button>
                <button disabled={!withinTransaction} onClick={handleTransactionRollback}>
                  Rollback
                </button>
              </div>
              <div className="query-result" aria-label="Query result">
                <h3>Result</h3>
                {queryResult === null ? (
                  <p className="muted">Connect, then run SQL. Results appear here.</p>
                ) : (
                  <pre>{formatValue(queryResult)}</pre>
                )}
              </div>
            </section>
          )}
          {panel === 'configuration' && (
            <section className="editor-panel" aria-label="Configuration workspace">
              <p className="muted">
                The sample configuration is ready to use. Edit before clicking Configure.
              </p>
              <textarea
                className="fill-editor"
                aria-label="DuckDB configuration"
                spellCheck={false}
                value={config}
                onChange={(e) => setConfig(e.target.value)}
              />
              <p className="muted">
                To change configuration after connecting, disconnect and reset first.
              </p>
            </section>
          )}
          {panel === 'catalog' && (
            <section className="editor-panel" aria-label="Table catalog workspace">
              <div className="catalog-fields">
                <label>
                  Table name
                  <input value={tableName} onChange={(e) => setTableName(e.target.value)} />
                </label>
                <label>
                  Encoding
                  <select
                    value={tableType}
                    onChange={(e) => setTableType(e.target.value as 'b64ipc' | 'json')}
                  >
                    <option value="b64ipc">Arrow IPC · base64 + zlib</option>
                    <option value="json" disabled>
                      JSON (not implemented)
                    </option>
                  </select>
                </label>
              </div>
              <textarea
                className="fill-editor"
                aria-label="Table payload"
                spellCheck={false}
                value={tablePayload}
                onChange={(e) => setTablePayload(e.target.value)}
              />
              <div className="editor-actions">
                <button className="primary" disabled={!connected} onClick={handleLoadTable}>
                  Load table
                </button>
                <button
                  disabled={!connected}
                  onClick={() => {
                    handleListTables()
                    setInspector('output')
                  }}
                >
                  List tables
                </button>
                <button
                  onClick={() => {
                    handleShowConfiguration()
                    setInspector('output')
                  }}
                >
                  Definitions
                </button>
                <button
                  onClick={activeSubscriptions.has(tableName) ? handleUnsubscribe : handleSubscribe}
                >
                  {activeSubscriptions.has(tableName) ? 'Unsubscribe' : 'Subscribe'}
                </button>
              </div>
              <p className="muted">
                {activeSubscriptions.size} active subscription
                {activeSubscriptions.size === 1 ? '' : 's'}. Load results and notifications appear
                in Output.
              </p>
            </section>
          )}
        </main>
        {inspector && (
          <aside
            className="demo-inspector"
            id={`${inspector}-inspector`}
            aria-label={inspector === 'output' ? 'Event output' : 'Machine state details'}
          >
            <div className="inspector-heading">
              <h2>{inspector === 'output' ? 'Event output' : 'Machine state'}</h2>
              <button aria-label="Close inspector" onClick={() => setInspector(null)}>
                ✕
              </button>
            </div>
            {inspector === 'output' ? (
              <>
                <div className="inspector-subheading">
                  <span className="muted">Latest first · last 100 events</span>
                  <button onClick={clearOutput}>Clear</button>
                </div>
                <div className="inspector-scroll">
                  {outputs.length === 0 ? (
                    <p className="muted">No events yet.</p>
                  ) : (
                    outputs.map((output, index) => (
                      <details
                        className="event-entry"
                        key={`${output.timestamp.getTime()}-${index}`}
                        open={index === 0}
                      >
                        <summary>
                          <span>{output.type}</span>
                          <time>{output.timestamp.toLocaleTimeString()}</time>
                        </summary>
                        <pre>
                          {typeof output.data === 'string' ? output.data : formatValue(output.data)}
                        </pre>
                      </details>
                    ))
                  )}
                </div>
              </>
            ) : (
              <div className="inspector-scroll">
                <p className="muted">
                  Root: {JSON.stringify(state.value)}
                  <br />
                  Catalog: {JSON.stringify(dbCatalogState?.value)}
                </p>
                <details open>
                  <summary>Root context</summary>
                  <pre>{formatValue(state.context, { maxDepth: 2 })}</pre>
                </details>
                <details>
                  <summary>Catalog context &amp; load metrics</summary>
                  <pre>{formatValue(dbCatalogState?.context, { maxDepth: 3 })}</pre>
                </details>
              </div>
            )}
          </aside>
        )}
      </div>
      <footer className="demo-status" role="status">
        <span className="status-dot" />
        {latestMessage}
      </footer>
      {state.matches('initializing') && initProgress && (
        <div className="connection-progress">
          <ProgressBar progress={initProgress} />
        </div>
      )}
    </div>
  )
}
