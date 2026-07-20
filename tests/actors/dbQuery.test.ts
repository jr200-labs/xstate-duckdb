import { describe, it, expect, vi, beforeEach } from 'vitest'
import { Binary, Table, vectorFromArray } from 'apache-arrow'
import { createActor } from 'xstate'
import {
  duckdbRunQuery,
  queryDuckDb,
  beginTransaction,
  commitTransaction,
  rollbackTransaction,
  type QueryDbParams,
} from '../../src/actors/dbQuery'

// Suppress expected console.error output in error-handling tests
vi.spyOn(console, 'error').mockImplementation(() => {})

function arrowRows(rows: any[]) {
  return { numRows: rows.length, toArray: () => rows }
}

function createMockConnection(queryResult: any = arrowRows([])) {
  return {
    query: vi.fn().mockResolvedValue(queryResult),
  } as any
}

describe('duckdbRunQuery', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('returns array result type as-is', async () => {
    const rows = [{ id: 1 }, { id: 2 }]
    const conn = createMockConnection(arrowRows(rows))

    const result = await duckdbRunQuery({
      description: 'test',
      sql: 'SELECT 1',
      resultOptions: { type: 'array' },
      connection: conn,
    })

    expect(result).toEqual(rows)
    expect((result as any[])[0]).toBe(rows[0])
    expect(conn.query).toHaveBeenCalledWith('SELECT 1')
  })

  it('keeps non-Arrow result rows backed by Arrow vectors', async () => {
    const payload = Uint8Array.of(1, 2, 3)
    const table = new Table({
      id: vectorFromArray(['a']),
      payload: vectorFromArray([payload], new Binary()),
    })
    const conn = createMockConnection(table)

    const result = (await duckdbRunQuery({
      description: 'typed rows',
      sql: 'SELECT * FROM t',
      resultOptions: { type: 'array' },
      connection: conn,
    })) as any[]

    expect(result[0]?.[Symbol.toStringTag]).toBe('Row')
    expect(result[0]?.payload).toEqual(payload)
  })

  it('returns arrow result type directly from query', async () => {
    const arrowTable = { numRows: 0, schema: 'mock-arrow' }
    const conn = createMockConnection(arrowTable)

    const result = await duckdbRunQuery({
      description: 'arrow-test',
      sql: 'SELECT * FROM t',
      resultOptions: { type: 'arrow' },
      connection: conn,
    })

    expect(result).toBe(arrowTable)
  })

  it('returns dictionary result type', async () => {
    const rows = [
      { id: 'a', name: 'Alice' },
      { id: 'b', name: 'Bob' },
    ]
    const conn = createMockConnection(arrowRows(rows))

    const result = await duckdbRunQuery({
      description: 'dict-test',
      sql: 'SELECT *',
      resultOptions: { type: 'dictionary', key: 'id' },
      connection: conn,
    })

    expect(result).toBeInstanceOf(Map)
    expect((result as Map<string, any>).get('a')).toEqual({ id: 'a', name: 'Alice' })
  })

  it('returns singlevaluemap result type', async () => {
    const rows = [
      { k: 'x', v: 10 },
      { k: 'y', v: 20 },
    ]
    const conn = createMockConnection(arrowRows(rows))

    const result = await duckdbRunQuery({
      description: 'svm-test',
      sql: 'SELECT *',
      resultOptions: { type: 'singlevaluemap', key: 'k', value: 'v' },
      connection: conn,
    })

    expect(result).toBeInstanceOf(Map)
    expect((result as Map<string, any>).get('x')).toBe(10)
  })

  it('returns multimap result type', async () => {
    const rows = [
      { cat: 'a', val: 1 },
      { cat: 'a', val: 2 },
      { cat: 'b', val: 3 },
    ]
    const conn = createMockConnection(arrowRows(rows))

    const result = await duckdbRunQuery({
      description: 'mm-test',
      sql: 'SELECT *',
      resultOptions: { type: 'multimap', key: 'cat' },
      connection: conn,
    })

    expect(result).toBeInstanceOf(Map)
    expect((result as Map<string, any[]>).get('a')).toHaveLength(2)
  })

  it('returns firstvalue result type', async () => {
    const rows = [{ count: 42 }]
    const conn = createMockConnection(arrowRows(rows))

    const result = await duckdbRunQuery({
      description: 'fv-test',
      sql: 'SELECT count(*)',
      resultOptions: { type: 'firstvalue', key: 'count' },
      connection: conn,
    })

    expect(result).toBe(42)
  })

  it('returns firstrow result type', async () => {
    const rows = [
      { a: 1, b: 2 },
      { a: 3, b: 4 },
    ]
    const conn = createMockConnection(arrowRows(rows))

    const result = await duckdbRunQuery({
      description: 'fr-test',
      sql: 'SELECT *',
      resultOptions: { type: 'firstrow' },
      connection: conn,
    })

    expect(result).toEqual({ a: 1, b: 2 })
  })

  it('invokes callback instead of returning when callback is provided', async () => {
    const rows = [{ id: 1 }]
    const conn = createMockConnection(arrowRows(rows))
    const callback = vi.fn()

    const result = await duckdbRunQuery({
      description: 'cb-test',
      sql: 'SELECT 1',
      resultOptions: { type: 'array' },
      callback,
      connection: conn,
    })

    expect(result).toBeUndefined()
    expect(callback).toHaveBeenCalledWith(rows)
  })

  it('returns empty array when connection.query returns undefined', async () => {
    const conn = createMockConnection(undefined)
    // Non-Arrow result types fall back to an empty row array.

    const result = await duckdbRunQuery({
      description: 'empty-test',
      sql: 'SELECT 1',
      resultOptions: { type: 'array' },
      connection: conn,
    })

    expect(result).toEqual([])
  })

  it('returns undefined when arrow query errors', async () => {
    const conn = {
      query: vi.fn().mockRejectedValue(new Error('fail')),
    } as any

    const result = await duckdbRunQuery({
      description: 'no-conn',
      sql: 'SELECT 1',
      resultOptions: { type: 'arrow' },
      connection: conn,
    })

    // duckDbExecuteToArrow catches errors and returns undefined, which passes through formatResult
    expect(result).toBeUndefined()
  })

  it('throws for unsupported result type', async () => {
    const conn = createMockConnection(arrowRows([]))

    await expect(
      duckdbRunQuery({
        description: 'bad-type',
        sql: 'SELECT 1',
        resultOptions: { type: 'unknown' as any },
        connection: conn,
      }),
    ).rejects.toThrow('Unsupported result type: unknown')
  })

  it('returns empty map when query returns no rows for dictionary type', async () => {
    const conn = createMockConnection(arrowRows([]))

    const result = await duckdbRunQuery({
      description: 'empty-dict',
      sql: 'SELECT 1',
      resultOptions: { type: 'dictionary', key: 'id' },
      connection: conn,
    })

    expect(result).toBeInstanceOf(Map)
    expect((result as Map<string, any>).size).toBe(0)
  })

  it('handles query errors gracefully in arrow path', async () => {
    const conn = {
      query: vi.fn().mockRejectedValue(new Error('SQL error')),
    } as any

    // duckDbExecuteToArrow catches errors and returns undefined
    const result = await duckdbRunQuery({
      description: 'err-test',
      sql: 'BAD SQL',
      resultOptions: { type: 'array' },
      connection: conn,
    })

    expect(result).toEqual([])
  })
})

describe('queryDuckDb actor', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('resolves a Promise connection before running query', async () => {
    const rows = [{ id: 1 }]
    const mockConn = createMockConnection(arrowRows(rows))

    const actor = createActor(queryDuckDb, {
      input: {
        description: 'promise-conn',
        sql: 'SELECT 1',
        resultOptions: { type: 'array' },
        connection: Promise.resolve(mockConn),
      },
    })

    const result = await new Promise<any>((resolve, reject) => {
      actor.subscribe({
        complete: () => resolve(actor.getSnapshot().output),
        error: reject,
      })
      actor.start()
    })

    expect(result).toEqual(rows)
  })

  it('accepts a non-Promise connection directly', async () => {
    const rows = [{ id: 2 }]
    const mockConn = createMockConnection(arrowRows(rows))

    const actor = createActor(queryDuckDb, {
      input: {
        description: 'direct-conn',
        sql: 'SELECT 2',
        resultOptions: { type: 'array' },
        connection: mockConn,
      },
    })

    const result = await new Promise<any>((resolve, reject) => {
      actor.subscribe({
        complete: () => resolve(actor.getSnapshot().output),
        error: reject,
      })
      actor.start()
    })

    expect(result).toEqual(rows)
  })
})

describe('beginTransaction actor', () => {
  it('connects and begins a transaction', async () => {
    const mockConn = { query: vi.fn().mockResolvedValue(undefined) }
    const mockDb = { connect: vi.fn().mockResolvedValue(mockConn) }

    const actor = createActor(beginTransaction, { input: mockDb as any })

    const result = await new Promise<any>((resolve, reject) => {
      actor.subscribe({
        complete: () => resolve(actor.getSnapshot().output),
        error: reject,
      })
      actor.start()
    })

    expect(mockDb.connect).toHaveBeenCalled()
    expect(mockConn.query).toHaveBeenCalledWith('BEGIN TRANSACTION;')
    expect(result).toBe(mockConn)
  })
})

describe('commitTransaction actor', () => {
  it('sends COMMIT query', async () => {
    const mockConn = { query: vi.fn().mockResolvedValue(undefined) }

    const actor = createActor(commitTransaction, { input: mockConn as any })

    await new Promise<void>((resolve, reject) => {
      actor.subscribe({
        complete: () => resolve(),
        error: reject,
      })
      actor.start()
    })

    expect(mockConn.query).toHaveBeenCalledWith('COMMIT;')
  })
})

describe('rollbackTransaction actor', () => {
  it('sends ROLLBACK query', async () => {
    const mockConn = { query: vi.fn().mockResolvedValue(undefined) }

    const actor = createActor(rollbackTransaction, { input: mockConn as any })

    await new Promise<void>((resolve, reject) => {
      actor.subscribe({
        complete: () => resolve(),
        error: reject,
      })
      actor.start()
    })

    expect(mockConn.query).toHaveBeenCalledWith('ROLLBACK;')
  })
})
