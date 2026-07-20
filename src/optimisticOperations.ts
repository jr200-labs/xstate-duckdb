import { context as otelContext, SpanStatusCode } from '@opentelemetry/api'
import { SeverityNumber } from '@opentelemetry/api-logs'
import { getLogger, getMeter, getTracer } from './telemetry'

export type OptimisticOperationAction =
  'ack' | 'begin' | 'overlay' | 'reconcile' | 'reject' | 'unknown'

export interface OptimisticOperationField {
  fieldPath: string
  value: unknown
}

export interface OptimisticFieldMapping {
  column: string
  fieldPath: string
  metadataJsonPath?: string
  valueJsonPath?: string
}

const DEFAULT_TABLE = 'optimistic_operations'

type DuckDbQueryConnection = {
  query(sql: string): unknown | Promise<unknown>
}

export function createOptimisticOperationsTableSql(tableName = DEFAULT_TABLE): string {
  return `CREATE TABLE IF NOT EXISTS ${identifier(tableName)} (
  change_set_id VARCHAR NOT NULL,
  entity_id VARCHAR NOT NULL,
  field_path VARCHAR NOT NULL,
  value_json JSON NOT NULL,
  state VARCHAR NOT NULL CHECK (state IN ('committing', 'committed', 'unknown')),
  operation_id VARCHAR,
  committed_at TIMESTAMP,
  created_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
  PRIMARY KEY (change_set_id, field_path)
);`
}

export function createOptimisticOperationBeginSql(args: {
  changeSetId: string
  entityId: string
  fields: OptimisticOperationField[]
  tableName?: string
}): string {
  if (!args.fields.length) throw new Error('optimistic operation requires fields')
  const rows = args.fields.map(
    ({ fieldPath, value }) =>
      `(${string(args.changeSetId)}, ${string(args.entityId)}, ${string(fieldPath)}, JSON ${string(json(value))}, 'committing')`,
  )
  return `INSERT OR REPLACE INTO ${identifier(args.tableName ?? DEFAULT_TABLE)}
  (change_set_id, entity_id, field_path, value_json, state)
VALUES ${rows.join(',\n')};`
}

export function createOptimisticOperationAckSql(args: {
  changeSetId: string
  committedAt: string
  operations: Array<{ fieldPath: string; operationId: string }>
  tableName?: string
}): string {
  const operationId = args.operations.length
    ? `${args.operations.reduce(
        (sql, operation) =>
          `${sql} WHEN ${string(operation.fieldPath)} THEN ${string(operation.operationId)}`,
        'CASE field_path',
      )} ELSE operation_id END`
    : 'operation_id'
  return `UPDATE ${identifier(args.tableName ?? DEFAULT_TABLE)}
SET state = 'committed',
    committed_at = ${timestamp(args.committedAt)},
    operation_id = ${operationId}
WHERE change_set_id = ${string(args.changeSetId)};`
}

export function createOptimisticOperationUnknownSql(
  changeSetId: string,
  tableName = DEFAULT_TABLE,
): string {
  return `UPDATE ${identifier(tableName)} SET state = 'unknown' WHERE change_set_id = ${string(changeSetId)};`
}

export function createOptimisticOperationRejectSql(
  changeSetId: string,
  tableName = DEFAULT_TABLE,
): string {
  return `DELETE FROM ${identifier(tableName)} WHERE change_set_id = ${string(changeSetId)};`
}

export async function hasOptimisticChangeSet(
  connection: DuckDbQueryConnection,
  args: { changeSetId: string; tableName?: string },
): Promise<boolean> {
  const result = (await connection.query(
    `SELECT count(*) > 0 AS matched FROM ${identifier(args.tableName ?? DEFAULT_TABLE)} WHERE change_set_id = ${string(args.changeSetId)};`,
  )) as { toArray(): Array<{ matched?: unknown }> }
  return result.toArray()[0]?.matched === true
}

export function createOptimisticOverlayViewSql(args: {
  entityColumn: string
  fields: OptimisticFieldMapping[]
  operationsTable?: string
  sourceTable: string
  viewName: string
}): string {
  validateMappings(args.fields)
  const operations = identifier(args.operationsTable ?? DEFAULT_TABLE)
  const source = identifier(args.sourceTable)
  const entityColumn = identifier(args.entityColumn)
  if (!args.fields.length) {
    return `CREATE OR REPLACE VIEW ${identifier(args.viewName)} AS SELECT * FROM ${source};`
  }
  const values = args.fields
    .map(
      (field, index) =>
        `    max(CASE WHEN field_path = ${string(field.fieldPath)} THEN cast(value_json AS VARCHAR) END) AS _value_${index},\n    count(*) FILTER (WHERE field_path = ${string(field.fieldPath)}) > 0 AS _present_${index}`,
    )
    .join(',\n')
  const replacements = args.fields
    .map((field, index) => {
      const column = identifier(field.column)
      return `  CASE WHEN coalesce(local._present_${index}, false)
    THEN cast_to_type(json_extract_string(local._value_${index}, ${string(field.valueJsonPath ?? '$')}), source.${column})
    ELSE source.${column}
  END AS ${column}`
    })
    .join(',\n')
  return `CREATE OR REPLACE VIEW ${identifier(args.viewName)} AS
WITH ranked_local AS (
  SELECT *, row_number() OVER (PARTITION BY entity_id, field_path ORDER BY created_at DESC, change_set_id DESC) AS rank
  FROM ${operations}
  WHERE state IN ('committing', 'committed', 'unknown')
), local_values AS (
  SELECT
    entity_id,
${values}
  FROM ranked_local
  WHERE rank = 1
  GROUP BY entity_id
)
SELECT source.* REPLACE (
${replacements}
)
FROM ${source} source
LEFT JOIN local_values local ON local.entity_id = cast(source.${entityColumn} AS VARCHAR);`
}

/**
 * Rebuild an optimistic overlay using only columns present in the current
 * source table. Arrow snapshots may legitimately omit optional fields.
 */
export async function rebuildOptimisticOverlayView(
  connection: DuckDbQueryConnection,
  args: {
    entityColumn: string
    fields: OptimisticFieldMapping[]
    operationsTable?: string
    sourceTable: string
    viewName: string
  },
): Promise<void> {
  const source = identifier(args.sourceTable)
  const rows = (await connection.query(`DESCRIBE ${source}`)) as {
    toArray(): Array<{ column_name?: unknown }>
  }
  const columns = new Set(rows.toArray().map((row) => String(row.column_name)))
  const fields = args.fields.filter((field) => columns.has(field.column))
  await executeOptimisticOperation(
    connection,
    'overlay',
    createOptimisticOverlayViewSql({ ...args, fields }),
  )
}

export function createOptimisticOperationReconcileSql(args: {
  entityColumn: string
  fields: OptimisticFieldMapping[]
  metadataColumn: string
  operationsTable?: string
  sourceTable: string
}): string {
  validateMappings(args.fields)
  const cases = args.fields
    .map((field) => {
      if (!field.metadataJsonPath) throw new Error(`${field.fieldPath} requires metadataJsonPath`)
      return `    WHEN ${string(field.fieldPath)} THEN json_extract_string(source.${identifier(args.metadataColumn)}, ${string(field.metadataJsonPath)})`
    })
    .join('\n')
  return `DELETE FROM ${identifier(args.operationsTable ?? DEFAULT_TABLE)} local
USING ${identifier(args.sourceTable)} source
WHERE cast(source.${identifier(args.entityColumn)} AS VARCHAR) = local.entity_id
  AND CASE local.field_path
${cases}
  END = local.change_set_id;`
}

export function executeOptimisticOperation(
  connection: DuckDbQueryConnection,
  action: OptimisticOperationAction,
  sql: string,
): Promise<void> {
  return getTracer().startActiveSpan(
    'xstate.duckdb.optimistic_operation',
    { attributes: { 'operation.action': action } },
    async (span) => {
      const started = performance.now()
      let outcome = 'accepted'
      try {
        await connection.query(sql)
      } catch (error) {
        outcome = 'rejected'
        span.setStatus({ code: SpanStatusCode.ERROR, message: 'optimistic operation failed' })
        span.setAttribute('error.type', error instanceof Error ? error.name : 'unknown')
        span.addEvent('xstate.duckdb.error')
        throw error
      } finally {
        const duration = performance.now() - started
        const attributes = { 'operation.action': action, 'operation.outcome': outcome }
        span.setAttribute('operation.outcome', outcome)
        getMeter().createCounter('xstate.duckdb.optimistic.operation.count').add(1, attributes)
        getMeter()
          .createHistogram('xstate.duckdb.optimistic.operation.duration', { unit: 'ms' })
          .record(duration, attributes)
        getLogger().emit({
          eventName: 'xstate.duckdb.optimistic_operation',
          severityNumber: outcome === 'accepted' ? SeverityNumber.INFO : SeverityNumber.ERROR,
          severityText: outcome === 'accepted' ? 'INFO' : 'ERROR',
          body: 'Optimistic DuckDB operation completed',
          attributes: { ...attributes, 'duration.ms': duration },
          context: otelContext.active(),
        })
        span.end()
      }
    },
  )
}

function validateMappings(fields: OptimisticFieldMapping[]): void {
  if (new Set(fields.map((field) => field.fieldPath)).size !== fields.length) {
    throw new Error('optimistic field paths must be unique')
  }
  if (new Set(fields.map((field) => field.column)).size !== fields.length) {
    throw new Error('optimistic target columns must be unique')
  }
}

function identifier(value: string): string {
  return value
    .split('.')
    .map((part) => {
      if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(part))
        throw new Error(`invalid DuckDB identifier: ${value}`)
      return `"${part}"`
    })
    .join('.')
}

function string(value: string): string {
  return `'${value.replaceAll("'", "''")}'`
}

function timestamp(value: string): string {
  const parsed = new Date(value)
  if (Number.isNaN(parsed.valueOf())) throw new Error(`invalid timestamp: ${value}`)
  return `TIMESTAMP '${parsed.toISOString().replace('T', ' ').replace('Z', '')}'`
}

function json(value: unknown): string {
  const serialized = JSON.stringify(value)
  if (serialized === undefined) throw new Error('optimistic value must be JSON serializable')
  return serialized
}
