import { describe, it, expect, vi } from 'vitest'
import { createActor, fromPromise } from 'xstate'
import { dbCatalogLogic } from '../../src/machines/dbCatalog'
import type { LoadedTableEntry, CatalogSubscription } from '../../src/lib/types'

const mockLoadedEntry: LoadedTableEntry = {
  tableIsVersioned: true,
  tableVersionId: 1,
  tableSpecName: 'test',
  tableInstanceName: 'test_1',
  loadedEpoch: Date.now(),
}

const mockLoadedEntryWithMetrics = {
  ...mockLoadedEntry,
  loadMetrics: {
    tableSpecName: 'test',
    encodedBytes: 12,
    decodedBytes: 9,
    loadedBytes: 120,
  },
}

function createTestMachine(
  loadResult: LoadedTableEntry = mockLoadedEntry,
  pruneResult?: { loadedVersions: LoadedTableEntry[] },
) {
  return dbCatalogLogic.provide({
    actors: {
      loadTableIntoDuckDb: fromPromise(async () => loadResult),
      pruneTableVersions: fromPromise(async () => pruneResult ?? { loadedVersions: [loadResult] }),
    },
  })
}

function waitForState(actor: any, targetState: string, timeout = 3000): Promise<void> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error(`Timed out waiting for state: ${targetState}`)),
      timeout,
    )
    const check = () => {
      const snap = actor.getSnapshot()
      const stateValue = typeof snap.value === 'string' ? snap.value : JSON.stringify(snap.value)
      if (stateValue.includes(targetState)) {
        clearTimeout(timer)
        resolve()
      }
    }
    check()
    actor.subscribe(() => check())
  })
}

function waitForCondition(actor: any, condition: () => boolean, timeout = 3000): Promise<void> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('Timed out waiting for condition')), timeout)
    const check = () => {
      if (condition()) {
        clearTimeout(timer)
        resolve()
      }
    }
    check()
    actor.subscribe(() => check())
  })
}

describe('actual dbCatalogLogic with mocked actors', () => {
  it('starts in idle state', () => {
    const machine = createTestMachine(mockLoadedEntryWithMetrics)
    const actor = createActor(machine)
    actor.start()
    expect(actor.getSnapshot().value).toBe('idle')
    actor.stop()
  })

  it('idle -> configured -> connected', () => {
    const machine = createTestMachine(mockLoadedEntryWithMetrics)
    const actor = createActor(machine)
    actor.start()

    actor.send({
      type: 'CATALOG.CONFIGURE',
      tableDefinitions: [{ schema: 'main', name: 'test', isVersioned: true, maxVersions: 3 }],
    })
    expect(actor.getSnapshot().value).toBe('configured')

    actor.send({ type: 'CATALOG.CONNECT' })
    expect(actor.getSnapshot().value).toBe('connected')
    actor.stop()
  })

  it('processes CATALOG.LOAD_TABLE through loading_table -> pruning_versions -> connected', async () => {
    const machine = createTestMachine(mockLoadedEntryWithMetrics)
    const actor = createActor(machine)
    actor.start()

    actor.send({
      type: 'CATALOG.CONFIGURE',
      tableDefinitions: [{ schema: 'main', name: 'test', isVersioned: true, maxVersions: 3 }],
    })
    actor.send({ type: 'CATALOG.CONNECT' })

    actor.send({
      type: 'CATALOG.LOAD_TABLE',
      data: {
        tableSpecName: 'test',
        tablePayload: {},
        payloadType: 'json',
        payloadCompression: 'none',
      },
      duckDbHandle: {},
    } as any)

    await waitForCondition(actor, () => actor.getSnapshot().context.nextTableId === 2)

    expect(actor.getSnapshot().context.loadedVersions).toHaveLength(1)
    expect(actor.getSnapshot().context.loadMetrics).toEqual({
      encodedBytes: 12,
      decodedBytes: 9,
      loadedBytes: 120,
      byTable: {
        test: {
          encodedBytes: 12,
          decodedBytes: 9,
          loadedBytes: 120,
        },
      },
    })
    expect(actor.getSnapshot().context.nextTableId).toBe(2)

    actor.send({ type: 'CATALOG.METRICS.RESET_LOADS', tableSpecName: 'test' })
    expect(actor.getSnapshot().context.loadMetrics).toEqual({
      encodedBytes: 0,
      decodedBytes: 0,
      loadedBytes: 0,
      byTable: {},
    })
    actor.stop()
  })

  it('handles CATALOG.LIST_TABLES callback', async () => {
    const machine = createTestMachine()
    const actor = createActor(machine)
    actor.start()

    actor.send({ type: 'CATALOG.CONFIGURE', tableDefinitions: [] })
    actor.send({ type: 'CATALOG.CONNECT' })

    const callback = vi.fn()
    actor.send({ type: 'CATALOG.LIST_TABLES', callback })
    expect(callback).toHaveBeenCalledWith([])
    actor.stop()
  })

  it('handles CATALOG.LIST_DEFINITIONS callback', () => {
    const defs = [{ schema: 'main', name: 'users', isVersioned: false, maxVersions: 1 }]
    const machine = createTestMachine()
    const actor = createActor(machine)
    actor.start()

    actor.send({ type: 'CATALOG.CONFIGURE', tableDefinitions: defs })

    const callback = vi.fn()
    actor.send({ type: 'CATALOG.LIST_DEFINITIONS', callback })
    expect(callback).toHaveBeenCalledWith(defs)
    actor.stop()
  })

  it('handles CATALOG.SUBSCRIBE and CATALOG.UNSUBSCRIBE', () => {
    const machine = createTestMachine()
    const actor = createActor(machine)
    actor.start()

    const sub: CatalogSubscription = {
      tableSpecName: 'test',
      subscriptionUid: 'sub1',
      onSubscribe: vi.fn(),
      onChange: vi.fn(),
    }

    actor.send({ type: 'CATALOG.SUBSCRIBE', subscription: sub })
    expect(actor.getSnapshot().context.subscriptions.size).toBe(1)

    actor.send({ type: 'CATALOG.UNSUBSCRIBE', id: 'sub1' })
    expect(actor.getSnapshot().context.subscriptions.size).toBe(0)
    actor.stop()
  })

  it('CATALOG.FORCE_NOTIFY calls onChange for matching subscription', async () => {
    const onChange = vi.fn()
    const machine = createTestMachine()
    const actor = createActor(machine)
    actor.start()

    actor.send({
      type: 'CATALOG.CONFIGURE',
      tableDefinitions: [{ schema: 'main', name: 'test', isVersioned: true, maxVersions: 3 }],
    })
    actor.send({
      type: 'CATALOG.SUBSCRIBE',
      subscription: {
        tableSpecName: 'test',
        subscriptionUid: 'sub1',
        onSubscribe: vi.fn(),
        onChange,
      },
    })
    actor.send({ type: 'CATALOG.CONNECT' })

    // Load a table to populate loadedVersions
    actor.send({
      type: 'CATALOG.LOAD_TABLE',
      data: {
        tableSpecName: 'test',
        tablePayload: {},
        payloadType: 'json',
        payloadCompression: 'none',
      },
      duckDbHandle: {},
    } as any)

    await waitForState(actor, 'connected')
    onChange.mockClear()

    actor.send({ type: 'CATALOG.FORCE_NOTIFY', id: 'sub1' })
    expect(onChange).toHaveBeenCalledWith('test_1', 1, true)
    actor.stop()
  })

  it('CATALOG.FORCE_NOTIFY with no matching subscription does nothing', () => {
    const machine = createTestMachine()
    const actor = createActor(machine)
    actor.start()

    actor.send({ type: 'CATALOG.CONFIGURE', tableDefinitions: [] })
    actor.send({ type: 'CATALOG.CONNECT' })

    // Should not throw
    actor.send({ type: 'CATALOG.FORCE_NOTIFY', id: 'nonexistent' })
    actor.stop()
  })

  it('CATALOG.RESET from configured returns to idle', () => {
    const machine = createTestMachine()
    const actor = createActor(machine)
    actor.start()

    actor.send({
      type: 'CATALOG.CONFIGURE',
      tableDefinitions: [{ schema: 'main', name: 'x', isVersioned: false, maxVersions: 1 }],
    })
    actor.send({ type: 'CATALOG.RESET' })

    expect(actor.getSnapshot().value).toBe('idle')
    expect(actor.getSnapshot().context.loadedVersions).toEqual([])
    actor.stop()
  })

  it('CATALOG.DISCONNECT from connected returns to configured', () => {
    const machine = createTestMachine()
    const actor = createActor(machine)
    actor.start()

    actor.send({ type: 'CATALOG.CONFIGURE', tableDefinitions: [] })
    actor.send({ type: 'CATALOG.CONNECT' })
    actor.send({ type: 'CATALOG.DISCONNECT' })

    expect(actor.getSnapshot().value).toBe('configured')
    actor.stop()
  })

  it('notifies subscribers on table load', async () => {
    const onChange = vi.fn()
    const machine = createTestMachine()
    const actor = createActor(machine)
    actor.start()

    actor.send({
      type: 'CATALOG.CONFIGURE',
      tableDefinitions: [{ schema: 'main', name: 'test', isVersioned: true, maxVersions: 3 }],
    })
    actor.send({
      type: 'CATALOG.SUBSCRIBE',
      subscription: {
        tableSpecName: 'test',
        subscriptionUid: 'sub1',
        onSubscribe: vi.fn(),
        onChange,
      },
    })
    actor.send({ type: 'CATALOG.CONNECT' })

    actor.send({
      type: 'CATALOG.LOAD_TABLE',
      data: {
        tableSpecName: 'test',
        tablePayload: {},
        payloadType: 'json',
        payloadCompression: 'none',
      },
      duckDbHandle: {},
    } as any)

    await waitForState(actor, 'connected')
    expect(onChange).toHaveBeenCalledWith('test_1', 1, true)
    actor.stop()
  })

  it('handles load error -> error state -> CATALOG.RESET', async () => {
    const failMachine = dbCatalogLogic.provide({
      actors: {
        loadTableIntoDuckDb: fromPromise(async () => {
          throw new Error('load failed')
        }),
        pruneTableVersions: fromPromise(async () => ({ loadedVersions: [] })),
      },
    })

    const actor = createActor(failMachine)
    actor.start()

    actor.send({
      type: 'CATALOG.CONFIGURE',
      tableDefinitions: [{ schema: 'main', name: 'test', isVersioned: true, maxVersions: 3 }],
    })
    actor.send({ type: 'CATALOG.CONNECT' })
    actor.send({
      type: 'CATALOG.LOAD_TABLE',
      data: {
        tableSpecName: 'test',
        tablePayload: {},
        payloadType: 'json',
        payloadCompression: 'none',
      },
      duckDbHandle: {},
    } as any)

    await waitForState(actor, 'error')
    expect(actor.getSnapshot().value).toBe('error')

    actor.send({ type: 'CATALOG.RESET' })
    expect(actor.getSnapshot().value).toBe('idle')
    actor.stop()
  })
})
