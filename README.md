# @jr200-labs/xstate-duckdb

A state machine for managing DuckDB operations in web applications. This library provides a type-safe interface for database initialization, query execution, transaction management, and table catalog operations.

## Features

- **State Management**: Full XState integration for predictable database state management
- **DuckDB Integration**: Built on top of `@duckdb/duckdb-wasm` for browser-based analytics
- **Transaction Support**: Complete transaction lifecycle management (begin, execute, commit, rollback)
- **Table Catalog**: Dynamic table loading, versioning, and management
- **Type Safety**: Full TypeScript support with comprehensive type definitions
- **Multiple Data Formats**: Support for Arrow IPC and JSON data formats
- **Compression**: Built-in support for data compression (zlib)
- **Real-time Updates**: Subscription-based table change notifications

## Installation

```bash
npm install @jr200-labs/xstate-duckdb
# or
yarn add @jr200-labs/xstate-duckdb
# or
pnpm add @jr200-labs/xstate-duckdb
```

## API Reference

### Machine States

The `duckdbMachine` has the following states:

- **`idle`**: Initial state, waiting for configuration
- **`configured`**: Database configured, ready to connect
- **`initializing`**: Database initialization in progress
- **`connected`**: Database connected and ready for operations
- **`disconnected`**: Database disconnected
- **`error`**: Error state

### Events

#### Configuration Events

- `CONFIGURE`: Configure database parameters and catalog
- `RESET`: Reset to initial state

#### Connection Events

- `CONNECT`: Initialize and connect to database
- `DISCONNECT`: Disconnect from database

#### Query Events

- `QUERY.EXECUTE`: Execute a one-shot query with auto-commit

#### Transaction Events

- `TRANSACTION.BEGIN`: Start a new transaction
- `TRANSACTION.EXECUTE`: Execute a query within a transaction
- `TRANSACTION.COMMIT`: Commit the current transaction
- `TRANSACTION.ROLLBACK`: Rollback the current transaction

#### Catalog Events

- `CATALOG.SUBSCRIBE`: Subscribe to table changes with a subscription object
- `CATALOG.UNSUBSCRIBE`: Unsubscribe from table changes using subscription ID
- `CATALOG.LIST_TABLES`: List all loaded tables
- `CATALOG.LOAD_TABLE`: Load data into a table
- `CATALOG.DROP_TABLE`: Drop a table
- `CATALOG.LIST_DEFINITIONS`: Get catalog configuration

## OpenTelemetry

This library emits OpenTelemetry spans for DuckDB lifecycle, query, transaction,
and catalog operations. DuckDB runs in-process (WASM) so there is no cross-
process context propagation — spans attach to the ambient OTel context so they
nest correctly under any caller-provided parent span.

### Emitted spans

| Span name                   | Emitted by                        | Attributes                                                             |
| --------------------------- | --------------------------------- | ---------------------------------------------------------------------- |
| `xstate.duckdb.init`        | `initDuckDb`                      | `duckdb.version`                                                       |
| `xstate.duckdb.close`       | `closeDuckDb`                     | —                                                                      |
| `xstate.duckdb.query`       | `duckdbRunQuery` / `queryDuckDb`  | `query.description`, `result.type`, `result.row_count`                 |
| `xstate.duckdb.tx.begin`    | `beginTransaction`                | —                                                                      |
| `xstate.duckdb.tx.commit`   | `commitTransaction`               | —                                                                      |
| `xstate.duckdb.tx.rollback` | `rollbackTransaction`             | —                                                                      |
| `xstate.duckdb.load_table`  | `loadTableIntoDuckDb`             | `table.spec`, `payload.type`, `payload.compression`, `table.instance`  |
| `xstate.duckdb.prune`       | `pruneTableVersions`              | `pruned.instances`, `kept.versions`                                    |

All error paths record exceptions on the active span, set span status to
`ERROR`, and emit an `xstate.duckdb.error` event with a truncated stack.

### Enabling tracing

`@opentelemetry/api` is a peer dependency — the consumer controls the installed
version and registers the SDK. If no provider is registered all telemetry calls
become no-ops. Minimal setup:

```ts
import { trace, propagation, context } from '@opentelemetry/api'
import { AsyncLocalStorageContextManager } from '@opentelemetry/context-async-hooks'
import { W3CTraceContextPropagator } from '@opentelemetry/core'
import { BasicTracerProvider, SimpleSpanProcessor } from '@opentelemetry/sdk-trace-base'

const provider = new BasicTracerProvider({
  spanProcessors: [
    /* your exporter */
  ],
})
trace.setGlobalTracerProvider(provider)
propagation.setGlobalPropagator(new W3CTraceContextPropagator())

const ctxMgr = new AsyncLocalStorageContextManager()
ctxMgr.enable()
context.setGlobalContextManager(ctxMgr)
```

## Examples

### Basic Usage

```typescript
import { duckdbMachine } from '@jr200-labs/xstate-duckdb'
import { useActor } from '@xstate/react'

function DatabaseComponent() {
  const [state, send] = useActor(duckdbMachine)

  const initializeDB = () => {
    send({
      type: 'CONFIGURE',
      dbInitParams: {
        logLevel: LogLevel.INFO,
        config: {},
      },
      catalogConfig: {},
    })
    send({ type: 'CONNECT' })
  }

  const runQuery = () => {
    send({
      type: 'QUERY.EXECUTE',
      queryParams: {
        sql: 'SELECT 1 as test',
        callback: (result) => console.log(result),
        description: 'test_query',
        resultType: 'json',
      },
    })
  }

  return (
    <div>
      <button onClick={initializeDB}>Initialize DB</button>
      <button onClick={runQuery}>Run Query</button>
    </div>
  )
}
```

### Transaction Management

```typescript
const handleTransaction = () => {
  // Begin transaction
  send({ type: 'TRANSACTION.BEGIN' })

  // Execute queries within transaction
  send({
    type: 'TRANSACTION.EXECUTE',
    queryParams: {
      sql: 'INSERT INTO users (name) VALUES ("John")',
      callback: (result) => console.log('Insert result:', result),
      description: 'insert_user',
      resultType: 'json',
    },
  })

  // Commit or rollback
  send({ type: 'TRANSACTION.COMMIT' })
  // or send({ type: 'TRANSACTION.ROLLBACK' })
}
```

### Table Management

```typescript
const handleTableOperations = () => {
  // Load a table with Arrow data
  send({
    type: 'CATALOG.LOAD_TABLE',
    tableName: 'my_table',
    tablePayload: arrowDataBase64,
    payloadType: 'b64ipc',
    payloadCompression: 'zlib',
    callback: (tableInstanceName, error) => {
      if (error) console.error('Load error:', error)
      else console.log('Table loaded:', tableInstanceName)
    },
  })

  // List all tables
  send({
    type: 'CATALOG.LIST_TABLES',
    callback: (tables) => console.log('Tables:', tables),
  })

  // Subscribe to table changes with enhanced subscription object
  send({
    type: 'CATALOG.SUBSCRIBE',
    subscription: {
      tableSpecName: 'my_table',
      onSubscribe: (id: string, tableSpecName: string) => {
        console.log(`Subscribed to ${tableSpecName} with ID: ${id}`)
      },
      onChange: (tableInstanceName: string, tableVersionId: number) => {
        console.log(`Table updated: ${tableInstanceName}, version: ${tableVersionId}`)
      },
    },
  })

  // Unsubscribe using the subscription ID
  send({
    type: 'CATALOG.UNSUBSCRIBE',
    id: 'subscription_id_here',
  })
}
```

### Subscription Management

The subscription system provides real-time notifications when tables are updated:

```typescript
// Create a subscription with custom callbacks
const subscription = {
  tableSpecName: 'users',
  onSubscribe: (id: string, tableSpecName: string) => {
    console.log(`Successfully subscribed to ${tableSpecName} with ID: ${id}`)
    // Store the subscription ID for later unsubscription
    setSubscriptionId(id)
  },
  onChange: (tableInstanceName: string, tableVersionId: number) => {
    console.log(`Table ${tableSpecName} updated to version ${tableVersionId}`)
    // Handle table updates - e.g., refresh UI, fetch new data
    refreshTableData(tableInstanceName)
  },
}

send({
  type: 'CATALOG.SUBSCRIBE',
  subscription,
})

// Later, unsubscribe using the stored ID
send({
  type: 'CATALOG.UNSUBSCRIBE',
  id: subscriptionId,
})
```

## Development

### Prerequisites

- Node.js 18+
- pnpm (recommended)

### Setup

```bash
# Install dependencies
pnpm install

# Build the project
pnpm build

# Run tests
pnpm test

# Start development mode
pnpm dev
```

### Running Examples

The project includes a React example in `examples/react-test/`:

```bash
cd examples/react-test
pnpm install
pnpm dev
```

This will start a development server with a comprehensive UI for testing all database operations.

## Contributing

Contributions welcome!

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

MIT License - see [LICENSE](LICENSE) file for details.
