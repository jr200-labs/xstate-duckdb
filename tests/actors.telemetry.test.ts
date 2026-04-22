import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { SpanStatusCode } from '@opentelemetry/api'
import { createActor } from 'xstate'
import { setupInMemoryTracer, type OtelTestHarness } from './otel-setup'
import {
  beginTransaction,
  commitTransaction,
  rollbackTransaction,
  queryDuckDb,
  duckdbRunQuery,
} from '../src/actors/dbQuery'
import { closeDuckDb } from '../src/actors/dbInit'

function createMockConnection() {
  return {
    query: vi.fn().mockResolvedValue(undefined),
    close: vi.fn().mockResolvedValue(undefined),
  } as any
}

function createMockDb() {
  const connection = createMockConnection()
  return {
    db: {
      connect: vi.fn().mockResolvedValue(connection),
      terminate: vi.fn().mockResolvedValue(undefined),
    } as any,
    connection,
  }
}

async function runActor<T>(actor: ReturnType<typeof createActor>): Promise<T> {
  return new Promise((resolve, reject) => {
    actor.subscribe({
      next: (snap) => {
        if (snap.status === 'done') resolve(snap.output as T)
      },
      error: (err) => reject(err),
    })
    actor.start()
  })
}

describe('actor telemetry', () => {
  let harness: OtelTestHarness

  beforeEach(() => {
    harness = setupInMemoryTracer()
    vi.spyOn(console, 'error').mockImplementation(() => {})
  })

  afterEach(() => {
    harness.teardown()
    vi.restoreAllMocks()
  })

  it('beginTransaction emits xstate.duckdb.tx.begin span', async () => {
    const { db, connection } = createMockDb()
    const actor = createActor(beginTransaction, { input: db })
    await runActor(actor)

    expect(connection.query).toHaveBeenCalledWith('BEGIN TRANSACTION;')
    const spans = harness.exporter.getFinishedSpans()
    expect(spans.find((s) => s.name === 'xstate.duckdb.tx.begin')).toBeDefined()
  })

  it('commitTransaction emits xstate.duckdb.tx.commit span', async () => {
    const connection = createMockConnection()
    const actor = createActor(commitTransaction, { input: connection })
    await runActor(actor)

    expect(connection.query).toHaveBeenCalledWith('COMMIT;')
    const spans = harness.exporter.getFinishedSpans()
    expect(spans.find((s) => s.name === 'xstate.duckdb.tx.commit')).toBeDefined()
  })

  it('rollbackTransaction emits xstate.duckdb.tx.rollback span', async () => {
    const connection = createMockConnection()
    const actor = createActor(rollbackTransaction, { input: connection })
    await runActor(actor)

    expect(connection.query).toHaveBeenCalledWith('ROLLBACK;')
    const spans = harness.exporter.getFinishedSpans()
    expect(spans.find((s) => s.name === 'xstate.duckdb.tx.rollback')).toBeDefined()
  })

  it('closeDuckDb emits xstate.duckdb.close span', async () => {
    const { db } = createMockDb()
    const actor = createActor(closeDuckDb, { input: { db } })
    await runActor(actor)

    expect(db.terminate).toHaveBeenCalled()
    const spans = harness.exporter.getFinishedSpans()
    expect(spans.find((s) => s.name === 'xstate.duckdb.close')).toBeDefined()
  })

  it('closeDuckDb is a no-op but still emits span when db is null', async () => {
    const actor = createActor(closeDuckDb, { input: { db: null } })
    await runActor(actor)

    const spans = harness.exporter.getFinishedSpans()
    expect(spans.find((s) => s.name === 'xstate.duckdb.close')).toBeDefined()
  })

  it('duckdbRunQuery emits xstate.duckdb.query span with description attr', async () => {
    const connection = {
      query: vi.fn().mockResolvedValue({ numRows: 0 } as any),
    } as any

    await duckdbRunQuery({
      description: 'my-query',
      sql: 'SELECT 1',
      resultOptions: { type: 'arrow' },
      connection,
      callback: vi.fn(),
    })

    const spans = harness.exporter.getFinishedSpans()
    const qs = spans.find((s) => s.name === 'xstate.duckdb.query')
    expect(qs).toBeDefined()
    expect(qs!.attributes['query.description']).toBe('my-query')
    expect(qs!.attributes['result.type']).toBe('arrow')
  })

  it('beginTransaction records error and sets ERROR status on rejection', async () => {
    const connection = {
      query: vi.fn().mockRejectedValue(new Error('tx failed')),
    } as any
    const db = {
      connect: vi.fn().mockResolvedValue(connection),
    } as any

    const actor = createActor(beginTransaction, { input: db })
    await expect(runActor(actor)).rejects.toThrow('tx failed')

    const spans = harness.exporter.getFinishedSpans()
    const txSpan = spans.find((s) => s.name === 'xstate.duckdb.tx.begin')!
    expect(txSpan.status.code).toBe(SpanStatusCode.ERROR)
    expect(txSpan.events.some((e) => e.name === 'xstate.duckdb.error')).toBe(true)
  })
})
