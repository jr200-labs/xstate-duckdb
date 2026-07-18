import { createRequire } from 'node:module'
import { describe, expect, it } from 'vitest'
import { createDuckDB, NODE_RUNTIME, VoidLogger } from '@duckdb/duckdb-wasm/blocking'
import {
  createOptimisticOperationAckSql,
  createOptimisticOperationBeginSql,
  createOptimisticOperationReconcileSql,
  createOptimisticOperationsTableSql,
  createOptimisticOverlayViewSql,
  rebuildOptimisticOverlayView,
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
  {
    column: 'service_date',
    fieldPath: 'service_date',
    metadataJsonPath: '$.service_date.change_set_id',
  },
  {
    column: 'observed_at',
    fieldPath: 'observed_at',
    metadataJsonPath: '$.observed_at.change_set_id',
  },
  {
    column: 'enabled',
    fieldPath: 'enabled',
    metadataJsonPath: '$.enabled.change_set_id',
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
        entity_id VARCHAR, classification VARCHAR, amount DOUBLE, service_date DATE,
        observed_at TIMESTAMP, enabled BOOLEAN, override_metadata_json VARCHAR
      )`)
      connection.query(
        `INSERT INTO source VALUES ('trade-1', 'Expected', 10, DATE '2026-07-01', TIMESTAMP '2026-07-01 01:02:03', false, '{}')`,
      )
      connection.query(createOptimisticOperationsTableSql())
      connection.query(
        createOptimisticOperationBeginSql({
          changeSetId: 'change-1',
          entityId: 'trade-1',
          fields: [
            { fieldPath: 'classification', value: 'Available' },
            { fieldPath: 'amount', value: 12.5 },
            { fieldPath: 'service_date', value: '2026-07-17' },
            { fieldPath: 'observed_at', value: '2026-07-17T12:34:56' },
            { fieldPath: 'enabled', value: true },
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
      expect(
        connection
          .query(
            `SELECT typeof(service_date) AS date_type, cast(service_date AS VARCHAR) AS service_date,
            typeof(observed_at) AS timestamp_type, cast(observed_at AS VARCHAR) AS observed_at,
            typeof(enabled) AS boolean_type, enabled FROM effective`,
          )
          .toArray()[0]
          ?.toJSON(),
      ).toEqual({
        boolean_type: 'BOOLEAN',
        date_type: 'DATE',
        enabled: true,
        observed_at: '2026-07-17 12:34:56',
        service_date: '2026-07-17',
        timestamp_type: 'TIMESTAMP',
      })

      connection.query(
        createOptimisticOperationAckSql({
          changeSetId: 'change-1',
          committedAt: '2026-07-17T00:00:00Z',
          operations: [
            { fieldPath: 'classification', operationId: 'op-1' },
            { fieldPath: 'amount', operationId: 'op-2' },
            { fieldPath: 'service_date', operationId: 'op-3' },
            { fieldPath: 'observed_at', operationId: 'op-4' },
            { fieldPath: 'enabled', operationId: 'op-5' },
          ],
        }),
      )
      expect(
        connection.query(`SELECT DISTINCT state FROM optimistic_operations`).toArray()[0]?.toJSON(),
      ).toEqual({
        state: 'committed',
      })

      connection.query(`UPDATE source SET classification = 'Available', amount = 12.5,
        service_date = DATE '2026-07-17', observed_at = TIMESTAMP '2026-07-17 12:34:56', enabled = true,
        override_metadata_json = '{"classification":{"change_set_id":"change-1"},"amount":{"change_set_id":"change-1"},"service_date":{"change_set_id":"change-1"},"observed_at":{"change_set_id":"change-1"},"enabled":{"change_set_id":"change-1"}}'`)
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

  it('skips mappings for optional columns absent from an Arrow snapshot', async () => {
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
      connection.query(`CREATE TABLE sparse_source (entity_id VARCHAR, amount DOUBLE)`)
      connection.query(`INSERT INTO sparse_source VALUES ('trade-1', 10)`)
      connection.query(createOptimisticOperationsTableSql())
      connection.query(
        createOptimisticOperationBeginSql({
          changeSetId: 'change-1',
          entityId: 'trade-1',
          fields: [{ fieldPath: 'amount', value: 12.5 }],
        }),
      )

      await rebuildOptimisticOverlayView(connection, {
        entityColumn: 'entity_id',
        fields,
        sourceTable: 'sparse_source',
        viewName: 'sparse_effective',
      })

      expect(connection.query(`SELECT amount FROM sparse_effective`).toArray()[0]?.toJSON()).toEqual({
        amount: 12.5,
      })
    } finally {
      connection.close()
    }
  })
})
