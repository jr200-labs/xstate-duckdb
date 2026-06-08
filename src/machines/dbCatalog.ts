import { assign, setup } from 'xstate'
import { TableDefinition, LoadedTableEntry, CatalogSubscription } from '../lib/types'
import { loadTableIntoDuckDb, pruneTableVersions } from '../actors/dbCatalog'
import { AsyncDuckDB } from '@duckdb/duckdb-wasm'
import {
  createEmptyDuckDbLoadMetrics,
  DuckDbLoadMetricRecord,
  DuckDbLoadMetrics,
  recordDuckDbLoadMetric,
  resetDuckDbLoadMetrics,
} from '../loadMetrics'

export interface TableLoadEvent {
  tableSpecName: string
  tablePayload: any
  payloadType: 'json' | 'b64ipc'
  payloadCompression: 'none' | 'zlib'
  callback?: (tableInstanceName: string, error?: string) => void
}

export interface TableLoadEventInternal extends TableLoadEvent {
  duckDbHandle: AsyncDuckDB | null
}

type LoadedTableWithMetrics = LoadedTableEntry & {
  loadMetrics?: DuckDbLoadMetricRecord
}

export interface Context {
  tableDefinitions: TableDefinition[]
  loadedVersions: Array<LoadedTableEntry>
  loadMetrics: DuckDbLoadMetrics
  subscriptions: Map<string, CatalogSubscription>
  nextTableId: number
  pendingTableLoads: TableLoadEventInternal[]
  currentTableLoad: TableLoadEventInternal | null
  error: string | null
}

type ExternalEvents =
  // these events are used to reset the catalog
  | { type: 'CATALOG.RESET' }
  | { type: 'CATALOG.METRICS.RESET_LOADS'; tableSpecName?: string }
  | { type: 'CATALOG.CONFIGURE'; tableDefinitions: TableDefinition[] }
  | { type: 'CATALOG.CONNECT' }
  | { type: 'CATALOG.DISCONNECT' }

  // these events are used to load data and delete tables
  | { type: 'CATALOG.LIST_TABLES'; callback: (tables: LoadedTableEntry[]) => void }
  | {
      type: 'CATALOG.LOAD_TABLE'
      data: TableLoadEvent
    }
  | { type: 'CATALOG.DROP_TABLE'; tableName: string }
  | { type: 'CATALOG.LIST_DEFINITIONS'; callback: (config: TableDefinition[]) => void }

  // these events are used to subscribe to table changes
  | {
      type: 'CATALOG.SUBSCRIBE'
      subscription: CatalogSubscription
    }
  | {
      type: 'CATALOG.UNSUBSCRIBE'
      id: string
    }
  | { type: 'CATALOG.FORCE_NOTIFY'; id: string }

export type Events = ExternalEvents

export const dbCatalogLogic = setup({
  types: {
    context: {} as Context,
    events: {} as Events,
  },
  actors: {
    loadTableIntoDuckDb: loadTableIntoDuckDb,
    pruneTableVersions: pruneTableVersions,
  },
  guards: {
    hasPendingTableLoads: ({ context }) => {
      return context.pendingTableLoads.length > 0
    },
  },
}).createMachine({
  initial: 'idle',

  context: {
    tableDefinitions: [],
    loadedVersions: [],
    loadMetrics: createEmptyDuckDbLoadMetrics(),
    subscriptions: new Map<string, CatalogSubscription>(),
    nextTableId: 1,
    pendingTableLoads: [],
    currentTableLoad: null,
    error: null,
  },

  on: {
    'CATALOG.SUBSCRIBE': {
      actions: assign(({ context, event }) => {
        const { subscription } = event
        const subscriptionKey = subscription.subscriptionUid ?? subscription.tableSpecName
        const newSubscriptions = new Map(context.subscriptions)
        newSubscriptions.set(subscriptionKey, {
          ...subscription,
          subscriptionUid: subscriptionKey,
        })
        return {
          subscriptions: newSubscriptions,
        }
      }),
    },

    'CATALOG.UNSUBSCRIBE': {
      actions: assign(({ context, event }) => {
        const newSubscriptions = new Map(context.subscriptions)
        newSubscriptions.delete(event.id)
        return { subscriptions: newSubscriptions }
      }),
    },

    'CATALOG.LOAD_TABLE': {
      actions: assign(({ context, event }) => {
        const eventWithDbHandle = {
          ...event.data,
          duckDbHandle: (event as any).duckDbHandle,
        }
        const newPendingTableLoads = [...context.pendingTableLoads, eventWithDbHandle]
        return {
          pendingTableLoads: newPendingTableLoads,
        }
      }),
    },

    'CATALOG.LIST_DEFINITIONS': {
      actions: ({ context, event }) => {
        event.callback(context.tableDefinitions)
      },
    },

    'CATALOG.METRICS.RESET_LOADS': {
      actions: assign({
        loadMetrics: ({ context, event }) =>
          resetDuckDbLoadMetrics(context.loadMetrics, event.tableSpecName),
      }),
    },
  },

  states: {
    idle: {
      on: {
        'CATALOG.CONFIGURE': {
          target: 'configured',
          actions: [
            assign({
              tableDefinitions: ({ event }: any) => event.tableDefinitions,
            }),
          ],
        },
      },
    },

    configured: {
      on: {
        'CATALOG.RESET': {
          target: 'idle',
          actions: assign(() => ({
            config: [],
            loadedVersions: [],
            loadMetrics: createEmptyDuckDbLoadMetrics(),
            subscriptions: new Map<string, CatalogSubscription>(),
            pendingTableLoads: [],
            currentTableLoad: null,
            // shoudl we reset this?? nextTableId: 1,
          })),
        },
        'CATALOG.CONNECT': {
          target: 'connected',
        },
      },
    },
    connected: {
      always: {
        target: 'loading_table',
        guard: 'hasPendingTableLoads',
      },
      on: {
        'CATALOG.DISCONNECT': {
          target: 'configured',
        },
        'CATALOG.LIST_TABLES': {
          actions: ({ context, event }) => {
            event.callback(context.loadedVersions)
          },
        },
        'CATALOG.DROP_TABLE': {
          //   actions: fromPromise(async ({ context, event }) => {
          //     const { tableName } = event
          //     const versions = context.loadedVersions[tableName] ?? []
          //     const def = context.config[tableName]
          //     if (!def) return
          //     const conn = await def.loader({}).then(t => t.db.connect()) // hacky but works
          //     for (const id of versions) {
          //       await conn.query(`DROP TABLE IF EXISTS ${tableName}_${id}`)
          //     }
          //     context.loadedVersions[tableName] = []
          //   }),
        },

        'CATALOG.FORCE_NOTIFY': {
          actions: ({ context, event }) => {
            const foundSub = context.subscriptions.get(event.id)
            if (foundSub) {
              // Find the most recent version of this table
              const latestTable = context.loadedVersions
                .filter((entry) => entry.tableSpecName === foundSub.tableSpecName)
                .sort((a, b) => b.tableVersionId - a.tableVersionId)[0]

              if (latestTable) {
                foundSub.onChange(
                  latestTable.tableInstanceName,
                  latestTable.tableVersionId,
                  latestTable.tableIsVersioned,
                )
              }
            }
          },
        },
      },
    },
    loading_table: {
      entry: assign(({ context }) => {
        // Read the first item off the queue
        const [firstItem, ...remainingItems] = context.pendingTableLoads
        return {
          pendingTableLoads: remainingItems,
          currentTableLoad: firstItem,
        }
      }),
      invoke: {
        src: 'loadTableIntoDuckDb',
        input: ({ context }: any) => {
          return {
            ...context.currentTableLoad,
            nextTableId: context.nextTableId,
            tableDefinitions: context.tableDefinitions,
          }
        },
        onDone: {
          target: 'pruning_versions',
          actions: assign(({ event, context }) => {
            const loadedTable = event.output as LoadedTableWithMetrics
            const newLoadedVersions = [loadedTable, ...context.loadedVersions]
            const nextLoadMetrics = loadedTable.loadMetrics
              ? recordDuckDbLoadMetric(context.loadMetrics, loadedTable.loadMetrics)
              : context.loadMetrics

            // Notify subscribers about the new table
            for (const [_, subscription] of context.subscriptions.entries()) {
              if (subscription.tableSpecName === loadedTable.tableSpecName) {
                subscription.onChange(
                  loadedTable.tableInstanceName,
                  loadedTable.tableVersionId,
                  loadedTable.tableIsVersioned,
                )
              }
            }

            return {
              nextTableId: context.nextTableId + 1,
              loadedVersions: newLoadedVersions,
              loadMetrics: nextLoadMetrics,
            }
          }),
        },
        onError: {
          target: 'error',
          actions: assign({
            nextTableId: ({ context }: any) => context.nextTableId + 1,
            error: ({ event }: any) => event.error.message,
            currentTableLoad: null,
          }),
        },
      },
    },
    pruning_versions: {
      invoke: {
        src: 'pruneTableVersions',
        input: ({ event, context }: any) => {
          return {
            ...event,
            currentLoadedVersions: context.loadedVersions,
            tableDefinitions: context.tableDefinitions,
            duckDbHandle: context.currentTableLoad?.duckDbHandle,
          }
        },
        onDone: {
          target: 'connected',
          actions: assign(({ event }) => ({
            loadedVersions: event.output.loadedVersions,
            currentTableLoad: null,
          })),
        },
        onError: {
          target: 'error',
          actions: assign({
            nextTableId: ({ context }: any) => context.nextTableId + 1,
            error: ({ event }: any) => event,
            currentTableLoad: null,
          }),
        },
      },
    },
    error: {
      on: {
        'CATALOG.RESET': {
          target: 'idle',
        },
      },
    },
  },
})
