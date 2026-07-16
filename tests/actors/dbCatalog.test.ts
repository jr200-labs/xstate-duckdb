import { describe, it, expect, vi, beforeEach } from 'vitest'
import { createActor } from 'xstate'

// Suppress expected console.log/error output in tests
vi.spyOn(console, 'log').mockImplementation(() => {})
vi.spyOn(console, 'error').mockImplementation(() => {})

const mockQuery = vi.fn().mockResolvedValue(undefined)
const mockInsertArrowFromIPCStream = vi.fn().mockResolvedValue(undefined)
const mockConnect = vi.fn().mockResolvedValue({
  query: mockQuery,
  insertArrowFromIPCStream: mockInsertArrowFromIPCStream,
})

vi.mock('pako', () => ({
  inflate: vi.fn((data: Uint8Array) => data),
}))

// Mock window.atob for b64ipc decoding
vi.stubGlobal('window', {
  atob: (str: string) => Buffer.from(str, 'base64').toString('binary'),
})

import { loadTableIntoDuckDb, pruneTableVersions, dropTables } from '../../src/actors/dbCatalog'
import type { TableDefinition } from '../../src/lib/types'

const tableDefs: TableDefinition[] = [
  { schema: 'main', name: 'users', isVersioned: true, maxVersions: 2 },
  { schema: 'main', name: 'config', isVersioned: false, maxVersions: 1 },
]

describe('loadTableIntoDuckDb', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('loads a versioned table from json payload', async () => {
    const callback = vi.fn()

    const actor = createActor(loadTableIntoDuckDb, {
      input: {
        nextTableId: 5,
        payloadType: 'json',
        payloadCompression: 'none',
        tableDefinitions: tableDefs,
        tableSpecName: 'users',
        tablePayload: { data: [1, 2, 3] },
        duckDbHandle: { connect: mockConnect },
        callback,
      },
    })

    const result = await new Promise<any>((resolve, reject) => {
      actor.subscribe({
        complete: () => resolve(actor.getSnapshot().output),
        error: reject,
      })
      actor.start()
    })

    expect(result.tableSpecName).toBe('users')
    expect(result.tableInstanceName).toBe('users_5')
    expect(result.tableIsVersioned).toBe(true)
    expect(result.tableVersionId).toBe(5)
    expect(result.loadMetrics).toEqual({
      tableSpecName: 'users',
      encodedBytes: 16,
      decodedBytes: 16,
      loadedBytes: 16,
    })
    expect(callback).toHaveBeenCalledWith('users_5', undefined)
  })

  it('loads a non-versioned table (name without id suffix)', async () => {
    const callback = vi.fn()

    const actor = createActor(loadTableIntoDuckDb, {
      input: {
        nextTableId: 3,
        payloadType: 'json',
        payloadCompression: 'none',
        tableDefinitions: tableDefs,
        tableSpecName: 'config',
        tablePayload: {},
        duckDbHandle: { connect: mockConnect },
        callback,
      },
    })

    const result = await new Promise<any>((resolve, reject) => {
      actor.subscribe({
        complete: () => resolve(actor.getSnapshot().output),
        error: reject,
      })
      actor.start()
    })

    expect(result.tableInstanceName).toBe('config')
    expect(result.tableIsVersioned).toBe(false)
  })

  it('calls callback with error when table definition not found', async () => {
    const callback = vi.fn()

    const actor = createActor(loadTableIntoDuckDb, {
      input: {
        nextTableId: 1,
        payloadType: 'json',
        payloadCompression: 'none',
        tableDefinitions: tableDefs,
        tableSpecName: 'nonexistent',
        tablePayload: {},
        duckDbHandle: { connect: mockConnect },
        callback,
      },
    })

    await new Promise<void>((resolve, reject) => {
      actor.subscribe({
        complete: () => resolve(),
        error: reject,
      })
      actor.start()
    })

    expect(callback).toHaveBeenCalledWith({
      error: 'Table definition for table nonexistent not found',
    })
  })

  it('loads table from b64ipc payload', async () => {
    const callback = vi.fn()
    // Create a simple base64 string
    const b64Payload = Buffer.from('test-data').toString('base64')

    const actor = createActor(loadTableIntoDuckDb, {
      input: {
        nextTableId: 1,
        payloadType: 'b64ipc',
        payloadCompression: 'none',
        tableDefinitions: tableDefs,
        tableSpecName: 'users',
        tablePayload: b64Payload,
        duckDbHandle: { connect: mockConnect },
        callback,
      },
    })

    const result = await new Promise<any>((resolve, reject) => {
      actor.subscribe({
        complete: () => resolve(actor.getSnapshot().output),
        error: reject,
      })
      actor.start()
    })

    expect(result.tableSpecName).toBe('users')
    expect(result.loadMetrics).toEqual({
      tableSpecName: 'users',
      encodedBytes: 12,
      decodedBytes: 9,
      loadedBytes: 9,
    })
    expect(mockInsertArrowFromIPCStream).toHaveBeenCalled()
    expect(callback).toHaveBeenCalledWith('users_1', null)
  })

  it('loads non-versioned table from b64ipc and drops existing first', async () => {
    const callback = vi.fn()
    const b64Payload = Buffer.from('test-data').toString('base64')

    const actor = createActor(loadTableIntoDuckDb, {
      input: {
        nextTableId: 1,
        payloadType: 'b64ipc',
        payloadCompression: 'none',
        tableDefinitions: tableDefs,
        tableSpecName: 'config',
        tablePayload: b64Payload,
        duckDbHandle: { connect: mockConnect },
        callback,
      },
    })

    await new Promise<void>((resolve, reject) => {
      actor.subscribe({
        complete: () => resolve(),
        error: reject,
      })
      actor.start()
    })

    // Non-versioned tables drop existing table first
    expect(mockQuery).toHaveBeenCalledWith('DROP TABLE IF EXISTS config;')
  })

  it('loads b64ipc with zlib compression', async () => {
    const callback = vi.fn()
    const b64Payload = Buffer.from('compressed-data').toString('base64')

    const actor = createActor(loadTableIntoDuckDb, {
      input: {
        nextTableId: 1,
        payloadType: 'b64ipc',
        payloadCompression: 'zlib',
        tableDefinitions: tableDefs,
        tableSpecName: 'users',
        tablePayload: b64Payload,
        duckDbHandle: { connect: mockConnect },
        callback,
      },
    })

    await new Promise<void>((resolve, reject) => {
      actor.subscribe({
        complete: () => resolve(),
        error: reject,
      })
      actor.start()
    })

    const pako = await import('pako')
    expect(pako.inflate).toHaveBeenCalled()
  })

  it('handles unknown payload type', async () => {
    const callback = vi.fn()

    const actor = createActor(loadTableIntoDuckDb, {
      input: {
        nextTableId: 1,
        payloadType: 'csv',
        payloadCompression: 'none',
        tableDefinitions: tableDefs,
        tableSpecName: 'users',
        tablePayload: 'data',
        duckDbHandle: { connect: mockConnect },
        callback,
      },
    })

    await new Promise<void>((resolve, reject) => {
      actor.subscribe({
        complete: () => resolve(),
        error: reject,
      })
      actor.start()
    })

    expect(callback).toHaveBeenCalledWith('users_1', 'Unknown payload type: csv')
  })

  it('handles errors during loading', async () => {
    const callback = vi.fn()
    const failConnect = vi.fn().mockRejectedValue(new Error('connection failed'))

    const actor = createActor(loadTableIntoDuckDb, {
      input: {
        nextTableId: 1,
        payloadType: 'json',
        payloadCompression: 'none',
        tableDefinitions: tableDefs,
        tableSpecName: 'users',
        tablePayload: {},
        duckDbHandle: { connect: failConnect },
        callback,
      },
    })

    const result = await new Promise<any>((resolve, reject) => {
      actor.subscribe({
        complete: () => resolve(actor.getSnapshot().output),
        error: reject,
      })
      actor.start()
    })

    expect(result.error).toBe('connection failed')
    expect(callback).toHaveBeenCalledWith({ error: 'connection failed' })
  })

  it('handles b64ipc insertion error', async () => {
    const callback = vi.fn()
    const b64Payload = Buffer.from('bad-data').toString('base64')
    mockInsertArrowFromIPCStream.mockRejectedValueOnce(new Error('ipc parse error'))

    const actor = createActor(loadTableIntoDuckDb, {
      input: {
        nextTableId: 1,
        payloadType: 'b64ipc',
        payloadCompression: 'none',
        tableDefinitions: tableDefs,
        tableSpecName: 'users',
        tablePayload: b64Payload,
        duckDbHandle: { connect: mockConnect },
        callback,
      },
    })

    const result = await new Promise<any>((resolve, reject) => {
      actor.subscribe({
        complete: () => resolve(actor.getSnapshot().output),
        error: reject,
      })
      actor.start()
    })

    // The error is caught inside loadTableFromB64ipc and returned as { result: 'error', error }
    // Then callback is called with tableNameInstance and result.error
    expect(callback).toHaveBeenCalled()
  })
})

describe('pruneTableVersions', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('keeps only maxVersions entries per definition', async () => {
    const loadedVersions = [
      {
        tableSpecName: 'users',
        tableVersionId: 3,
        tableInstanceName: 'users_3',
        tableIsVersioned: true,
        loadedEpoch: 3,
      },
      {
        tableSpecName: 'users',
        tableVersionId: 2,
        tableInstanceName: 'users_2',
        tableIsVersioned: true,
        loadedEpoch: 2,
      },
      {
        tableSpecName: 'users',
        tableVersionId: 1,
        tableInstanceName: 'users_1',
        tableIsVersioned: true,
        loadedEpoch: 1,
      },
      {
        tableSpecName: 'config',
        tableVersionId: 1,
        tableInstanceName: 'config',
        tableIsVersioned: false,
        loadedEpoch: 1,
      },
    ]

    const actor = createActor(pruneTableVersions, {
      input: {
        currentLoadedVersions: loadedVersions,
        tableDefinitions: tableDefs,
        duckDbHandle: { connect: mockConnect },
      },
    })

    const result = await new Promise<any>((resolve, reject) => {
      actor.subscribe({
        complete: () => resolve(actor.getSnapshot().output),
        error: reject,
      })
      actor.start()
    })

    // users has maxVersions=2, so only 2 kept
    const usersVersions = result.loadedVersions.filter((v: any) => v.tableSpecName === 'users')
    expect(usersVersions).toHaveLength(2)
    expect(usersVersions[0].tableVersionId).toBe(3)
    expect(usersVersions[1].tableVersionId).toBe(2)

    // config has maxVersions=1, keeps 1
    const configVersions = result.loadedVersions.filter((v: any) => v.tableSpecName === 'config')
    expect(configVersions).toHaveLength(1)

    // Should have dropped users_1
    expect(mockQuery).toHaveBeenCalledWith('DROP TABLE IF EXISTS users_1;')
    // Should have committed
    expect(mockQuery).toHaveBeenCalledWith('COMMIT;')
  })

  it('handles errors during pruning with rollback', async () => {
    mockQuery
      .mockResolvedValueOnce(undefined) // BEGIN
      .mockRejectedValueOnce(new Error('drop failed')) // DROP fails

    const loadedVersions = [
      {
        tableSpecName: 'users',
        tableVersionId: 3,
        tableInstanceName: 'users_3',
        tableIsVersioned: true,
        loadedEpoch: 3,
      },
      {
        tableSpecName: 'users',
        tableVersionId: 2,
        tableInstanceName: 'users_2',
        tableIsVersioned: true,
        loadedEpoch: 2,
      },
      {
        tableSpecName: 'users',
        tableVersionId: 1,
        tableInstanceName: 'users_1',
        tableIsVersioned: true,
        loadedEpoch: 1,
      },
    ]

    const actor = createActor(pruneTableVersions, {
      input: {
        currentLoadedVersions: loadedVersions,
        tableDefinitions: [tableDefs[0]], // only users
        duckDbHandle: { connect: mockConnect },
      },
    })

    const result = await new Promise<any>((resolve, reject) => {
      actor.subscribe({
        complete: () => resolve(actor.getSnapshot().output),
        error: reject,
      })
      actor.start()
    })

    expect(result.error).toBe('drop failed')
  })
})

describe('dropTables', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('drops each table by name', async () => {
    const conn = { query: mockQuery } as any
    await dropTables(['t1', 't2', 't3'], conn)

    expect(mockQuery).toHaveBeenCalledTimes(3)
    expect(mockQuery).toHaveBeenCalledWith('DROP TABLE IF EXISTS t1;')
    expect(mockQuery).toHaveBeenCalledWith('DROP TABLE IF EXISTS t2;')
    expect(mockQuery).toHaveBeenCalledWith('DROP TABLE IF EXISTS t3;')
  })

  it('handles empty array', async () => {
    const conn = { query: mockQuery } as any
    await dropTables([], conn)
    expect(mockQuery).not.toHaveBeenCalled()
  })
})
