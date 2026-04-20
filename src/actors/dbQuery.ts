import { AsyncDuckDB, AsyncDuckDBConnection } from '@duckdb/duckdb-wasm'
import { fromPromise } from 'xstate'
import { JSONObject } from '../lib/types'
import { arrowToJSON } from 'duckdb-wasm-kit'
import { Table } from 'apache-arrow'
import {
  arrayToObjectMap,
  arrayToObjectMultiMap,
  arrayToSimpleMap,
  arrayToFirstValue,
  arrayToFirstRowMap,
} from '../lib/utils'

export interface ResultOptions {
  key?: string
  value?: string
  type: 'dictionary' | 'multimap' | 'singlevaluemap' | 'array' | 'arrow' | 'firstvalue' | 'firstrow'
}

export interface QueryDbParams {
  description: string
  sql: string | ((context: any) => string)
  resultOptions: ResultOptions
  callback?: (result: any) => void
}

export const queryDuckDb = fromPromise(
  async ({
    input,
  }: {
    input: QueryDbParams & { connection: Promise<AsyncDuckDBConnection> | AsyncDuckDBConnection }
  }) => {
    return duckdbRunQuery({
      ...input,
      connection: input.connection instanceof Promise ? await input.connection : input.connection,
    })
  },
)

type DuckDbQueryResult =
  | Record<string, JSONObject>[]
  | Table<any>
  | Map<string, any>
  | Map<string, any[]>
  | null

export async function duckdbRunQuery(
  input: QueryDbParams & { connection: AsyncDuckDBConnection },
): Promise<DuckDbQueryResult | void> {
  const sql = input.sql as string
  const raw =
    input.resultOptions?.type === 'arrow'
      ? await duckDbExecuteToArrow(input.description, sql, input.connection)
      : await duckDbExecuteToJson(input.description, sql, input.connection)

  const result = formatResult(raw, input.resultOptions)

  if (input.callback) {
    input.callback(result)
  } else {
    return result
  }
}

function formatResult(
  result: Record<string, JSONObject>[] | Table<any> | undefined,
  resultOptions?: ResultOptions,
): Record<string, JSONObject>[] | Table<any> | Map<string, any> | Map<string, any[]> | any | null {
  if (!resultOptions) {
    return result
  }

  let transformed:
    | Record<string, JSONObject>[]
    | Table<any>
    | Map<string, any>
    | Map<string, any[]>
    | any
    | null

  if (resultOptions.type === 'singlevaluemap') {
    if (!Array.isArray(result)) {
      throw new Error('Result must be an array for singlevaluemap transformation')
    }
    transformed = arrayToSimpleMap(result, resultOptions.key!, resultOptions.value!)
  } else if (resultOptions.type === 'multimap') {
    if (!Array.isArray(result)) {
      throw new Error('Result must be an array for multimap transformation')
    }
    transformed = arrayToObjectMultiMap(result, resultOptions.key!)
  } else if (resultOptions.type === 'dictionary') {
    if (!Array.isArray(result)) {
      throw new Error('Result must be an array for dictionary transformation')
    }
    transformed = arrayToObjectMap(result, resultOptions.key!)
  } else if (resultOptions.type === 'array' || resultOptions.type === 'arrow') {
    transformed = result
  } else if (resultOptions.type === 'firstvalue') {
    if (!Array.isArray(result)) {
      throw new Error('Result must be an array for firstvalue transformation')
    }
    transformed = arrayToFirstValue(result, resultOptions.key!)
  } else if (resultOptions.type === 'firstrow') {
    if (!Array.isArray(result)) {
      throw new Error('Result must be an array for firstrow transformation')
    }
    transformed = arrayToFirstRowMap(result)
  } else {
    throw new Error(`Unsupported result type: ${resultOptions.type}`)
  }

  return transformed
}

async function duckDbExecuteToArrow(
  description: string,
  sqlText: string,
  connection: AsyncDuckDBConnection,
  debug: boolean = false,
): Promise<Table<any> | undefined> {
  if (!connection) return undefined

  try {
    if (debug) {
      //   console.log(`[${nowUtc().toString()}] -- query[${description}]: ${sqlText}`)
    }

    const result = await connection.query(sqlText)
    return result as unknown as Table<any>
  } catch (error) {
    console.error(`duckDbError[${description}]`, error)
    return undefined
  }
}

async function duckDbExecuteToJson(
  description: string,
  sqlText: string,
  connection: AsyncDuckDBConnection,
): Promise<Record<string, JSONObject>[]> {
  const response = await duckDbExecuteToArrow(description, sqlText, connection)
  if (!response) {
    return []
  }
  const jsonResponse = arrowToJSON(response)
  return jsonResponse
}

export const beginTransaction = fromPromise(
  async ({ input }: { input: AsyncDuckDB }): Promise<AsyncDuckDBConnection> => {
    const connection = await input.connect()
    await connection.query('BEGIN TRANSACTION;')
    return connection
  },
)

export const commitTransaction = fromPromise(
  async ({ input }: { input: AsyncDuckDBConnection }): Promise<void> => {
    await input.query('COMMIT;')
  },
)

export const rollbackTransaction = fromPromise(
  async ({ input }: { input: AsyncDuckDBConnection }): Promise<void> => {
    await input.query('ROLLBACK;')
  },
)
