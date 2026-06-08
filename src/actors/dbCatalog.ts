import { fromPromise } from 'xstate/actors'
import { JSONObject, TableDefinition, LoadedTableEntry } from '../lib/types'
import { AsyncDuckDBConnection } from '@duckdb/duckdb-wasm'
import pako from 'pako'
import { withSpan } from '../telemetry'
import { byteLength, DuckDbLoadMetricRecord } from '../loadMetrics'

export interface LoadTableInput {
  nextTableId: number
  payloadType: 'json' | 'b64ipc'
  payloadCompression: 'none' | 'zlib'
  tableDefinitions: TableDefinition[]
  callback: (tableInstanceName: string, error?: string) => void
}

type LoadTableResult = {
  error?: null | string
  loadMetrics: DuckDbLoadMetricRecord
}

export const loadTableIntoDuckDb = fromPromise(async ({ input }: any) => {
  return withSpan(
    'xstate.duckdb.load_table',
    'xstate.duckdb.error',
    {
      'table.spec': input.tableSpecName,
      'payload.type': input.payloadType,
      'payload.compression': input.payloadCompression,
    },
    async (span) => {
      try {
        const { nextTableId, payloadType, tableDefinitions, payloadCompression } = input
        const tableDefinition = findTableDefinition(input.tableSpecName, tableDefinitions)
        if (!tableDefinition) {
          input.callback?.({ error: `Table definition for table ${input.tableSpecName} not found` })
          return
        }

        const tableNameInstance = makeTableNameInstance(tableDefinition, nextTableId)
        span.setAttribute('table.instance', tableNameInstance)
        const catalogEntry: LoadedTableEntry = {
          tableIsVersioned: tableDefinition.isVersioned,
          tableVersionId: nextTableId,
          tableSpecName: tableDefinition.name,
          tableInstanceName: tableNameInstance,
          loadedEpoch: Date.now(),
        }
        let result: LoadTableResult

        const dbConnection = await input.duckDbHandle.connect()
        if (payloadType === 'json') {
          result = await loadTableFromJson(
            tableDefinition.isVersioned,
            tableDefinition.name,
            tableDefinition.schema,
            tableNameInstance,
            input.tablePayload,
            dbConnection,
            payloadCompression,
          )
        } else if (payloadType === 'b64ipc') {
          result = await loadTableFromB64ipc(
            tableDefinition.isVersioned,
            tableDefinition.name,
            tableDefinition.schema,
            tableNameInstance,
            input.tablePayload,
            dbConnection,
            payloadCompression,
          )
        } else {
          result = {
            error: `Unknown payload type: ${payloadType}`,
            loadMetrics: {
              tableSpecName: tableDefinition.name,
              encodedBytes: 0,
              decodedBytes: 0,
              loadedBytes: 0,
            },
          }
        }

        span.setAttribute('payload.encoded_bytes', result.loadMetrics.encodedBytes)
        span.setAttribute('payload.decoded_bytes', result.loadMetrics.decodedBytes)
        span.setAttribute('payload.loaded_bytes', result.loadMetrics.loadedBytes)
        input.callback?.(tableNameInstance, result.error)
        return {
          ...catalogEntry,
          loadMetrics: result.loadMetrics,
        }
      } catch (error: any) {
        input.callback?.({ error: error.message })
        return { error: error.message }
      }
    },
  )
})

function findTableDefinition(tableSpecName: string, definitions: TableDefinition[]) {
  return definitions.find((def) => def.name === tableSpecName)
}

function makeTableNameInstance(definition: TableDefinition, nextTableId: number): string {
  if (definition.isVersioned) {
    return `${definition.name}_${nextTableId}`
  }
  return definition.name
}
async function loadTableFromJson(
  tableIsVersioned: boolean,
  tableSpecName: string,
  tableSchema: string,
  tableName: string,
  jsonPayload: JSONObject,
  _dbConnection: AsyncDuckDBConnection,
  _compression: 'none' | 'zlib',
): Promise<LoadTableResult> {
  console.log('loadTableFromJson', tableName, jsonPayload, tableIsVersioned)
  const bytes = byteLength(jsonPayload)
  return {
    loadMetrics: {
      tableSpecName,
      encodedBytes: bytes,
      decodedBytes: bytes,
      loadedBytes: bytes,
    },
  }
}

async function loadTableFromB64ipc(
  tableIsVersioned: boolean,
  tableSpecName: string,
  tableSchema: string,
  tableName: string,
  base64ipc: string,
  connection: AsyncDuckDBConnection,
  compression: 'none' | 'zlib',
): Promise<LoadTableResult> {
  // const msgSizeMb = base64ipc.length / 1024 / 1024

  const binaryString = window.atob(base64ipc)
  const byteArray = new Uint8Array(binaryString.length)
  for (let i = 0; i < binaryString.length; i++) {
    byteArray[i] = binaryString.charCodeAt(i)
  }

  // Decompress with pako if zlib compression is enabled
  const finalByteArray = compression === 'zlib' ? pako.inflate(byteArray) : byteArray
  const loadMetrics = {
    tableSpecName,
    encodedBytes: byteLength(base64ipc),
    decodedBytes: byteArray.byteLength,
    loadedBytes: finalByteArray.byteLength,
  }

  try {
    if (!tableIsVersioned) {
      await connection.query(`DROP TABLE IF EXISTS ${tableName};`)
    }

    await connection.insertArrowFromIPCStream(finalByteArray, {
      name: tableName,
      schema: tableSchema,
      create: true,
    })
  } catch (error: any) {
    console.error('Error loading table from b64ipc', error)
    return { error, loadMetrics }
  }

  return { error: null, loadMetrics }
}

export const pruneTableVersions = fromPromise(async ({ input }: any) => {
  return withSpan('xstate.duckdb.prune', 'xstate.duckdb.error', {}, async (span) => {
    const currentLoadedVersions: LoadedTableEntry[] = input.currentLoadedVersions
    const definitions: TableDefinition[] = input.tableDefinitions
    const dbConnection = await input.duckDbHandle.connect()
    await dbConnection.query(`BEGIN TRANSACTION;`)

    try {
      let prunedLoadedVersions: LoadedTableEntry[] = []
      let prunedInstances = 0
      for (const definition of definitions) {
        const { isVersioned, name, maxVersions } = definition
        const loadedTables = currentLoadedVersions
          .filter((loadedTbl) => loadedTbl.tableSpecName === name)
          .sort((a, b) => b.tableVersionId - a.tableVersionId)

        const versionsToKeep = loadedTables.slice(0, maxVersions)
        if (isVersioned) {
          const tableInstancesToPrune = loadedTables
            .slice(maxVersions)
            .map((tbl) => tbl.tableInstanceName)
          prunedInstances += tableInstancesToPrune.length
          await dropTables(tableInstancesToPrune, dbConnection)
        }
        prunedLoadedVersions = [...prunedLoadedVersions, ...versionsToKeep]
      }

      await dbConnection.query(`COMMIT;`)
      span.setAttribute('pruned.instances', prunedInstances)
      span.setAttribute('kept.versions', prunedLoadedVersions.length)

      return { loadedVersions: prunedLoadedVersions }
    } catch (error: any) {
      console.error('Error pruning table versions', error)
      await dbConnection.query(`ROLLBACK;`)
      return { error: error.message }
    }
  })
})

export const dropTables = async (tableInstances: string[], connection: AsyncDuckDBConnection) => {
  for (const tableInstance of tableInstances) {
    await connection.query(`DROP TABLE IF EXISTS ${tableInstance};`)
  }
}
