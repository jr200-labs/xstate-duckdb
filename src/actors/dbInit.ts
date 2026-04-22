import { fromPromise } from 'xstate'
import { AsyncDuckDB, ConsoleLogger, getJsDelivrBundles, selectBundle } from '@duckdb/duckdb-wasm'
import { InitDuckDbParams } from '../lib/types'
import { withSpan } from '../telemetry'

export const initDuckDb = fromPromise(async ({ input }: { input: InitDuckDbParams }) => {
  return withSpan('xstate.duckdb.init', 'xstate.duckdb.error', {}, async (span) => {
    const bundles = getJsDelivrBundles()
    const bundle = await selectBundle(bundles)

    const workerUrl = URL.createObjectURL(
      new Blob([`importScripts("${bundle.mainWorker!}");`], {
        type: 'text/javascript',
      }),
    )

    const worker = new Worker(workerUrl)
    const db = new AsyncDuckDB(new ConsoleLogger(input.dbLogLevel), worker)

    input.statusHandler?.('initializing')
    await db.instantiate(
      bundle.mainModule,
      bundle.pthreadWorker,
      input.dbProgressHandler ?? undefined,
    )
    URL.revokeObjectURL(workerUrl)

    if (input.dbInitParams) {
      console.debug('initDuckDb with config', input.dbInitParams)
      await db.open(input.dbInitParams)
    }

    const version = await db.getVersion()
    span.setAttribute('duckdb.version', version)

    input.statusHandler?.('ready')

    return {
      db,
      version,
    }
  })
})

export const closeDuckDb = fromPromise(async ({ input }: { input: { db: AsyncDuckDB | null } }) => {
  return withSpan('xstate.duckdb.close', 'xstate.duckdb.error', {}, async () => {
    if (input.db) {
      await input.db.terminate()
    }

    return {
      db: null,
    }
  })
})
