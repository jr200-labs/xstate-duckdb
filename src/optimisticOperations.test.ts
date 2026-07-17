import { createRequire } from 'node:module'
import { describe, expect, it } from 'vitest'
import { createDuckDB, NODE_RUNTIME, VoidLogger } from '@duckdb/duckdb-wasm/blocking'
import {
  createOptimisticOperationAckSql,
  createOptimisticOperationBeginSql,
  createOptimisticOperationReconcileSql,
  createOptimisticOperationsTableSql,
  createOptimisticOverlayViewSql,
} from './optimisticOperations'

const fields = [
  {
    column: 'classification',
    fieldPath: 'classification',
    metadataJsonPath: '$.classification.change_set_id',
  },
  {
    column: 'amount',
    fieldPath: 'amount',
    metadataJsonPath: '$.amount.change_set_id',
  },
]

describe('optimistic DuckDB projection', () => {
  it('overlays typed local values and removes them after authoritative reconciliation', async () => {
    const require = createRequire(import.meta.url)
    const db = await createDuckDB(
      {
        mvp: {
          mainModule: require.resolve('@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm'),
          mainWorker: '',
        },
      },
      new VoidLogger(),
      NODE_RUNTIME,
    )
    const connection = db.connect()
    try {
      connection.query(`CREATE TABLE source (
        entity_id VARCHAR, classification VARCHAR, amount DOUBLE, override_metadata_json VARCHAR
      )`)
      connection.query(`INSERT INTO source VALUES ('trade-1', 'Expected', 10, '{}')`)
      connection.query(createOptimisticOperationsTableSql())
      connection.query(
        createOptimisticOperationBeginSql({
          changeSetId: 'change-1',
          entityId: 'trade-1',
          fields: [
            { fieldPath: 'classification', value: 'Available' },
            { fieldPath: 'amount', value: 12.5 },
          ],
        }),
      )
      connection.query(
        createOptimisticOverlayViewSql({
          entityColumn: 'entity_id',
          fields,
          sourceTable: 'source',
          viewName: 'effective',
        }),
      )

      expect(
        connection.query(`SELECT classification, amount FROM effective`).toArray()[0]?.toJSON(),
      ).toEqual({
        amount: 12.5,
        classification: 'Available',
      })

      connection.query(
        createOptimisticOperationAckSql({
          changeSetId: 'change-1',
          committedAt: '2026-07-17T00:00:00Z',
          operations: [
            { fieldPath: 'classification', operationId: 'op-1' },
            { fieldPath: 'amount', operationId: 'op-2' },
          ],
        }),
      )
      expect(
        connection.query(`SELECT DISTINCT state FROM optimistic_operations`).toArray()[0]?.toJSON(),
      ).toEqual({
        state: 'committed',
      })

      connection.query(`UPDATE source SET classification = 'Available', amount = 12.5,
        override_metadata_json = '{"classification":{"change_set_id":"change-1"},"amount":{"change_set_id":"change-1"}}'`)
      connection.query(
        createOptimisticOperationReconcileSql({
          entityColumn: 'entity_id',
          fields,
          metadataColumn: 'override_metadata_json',
          sourceTable: 'source',
        }),
      )
      expect(
        connection
          .query(`SELECT count(*) AS count FROM optimistic_operations`)
          .toArray()[0]
          ?.toJSON(),
      ).toEqual({
        count: 0n,
      })
    } finally {
      connection.close()
    }
  })
})
