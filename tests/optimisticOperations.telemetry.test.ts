import { describe, expect, it, vi } from 'vitest'

const telemetry = vi.hoisted(() => ({
  add: vi.fn(),
  emit: vi.fn(),
  end: vi.fn(),
  record: vi.fn(),
  setAttribute: vi.fn(),
  setStatus: vi.fn(),
}))

vi.mock('../src/telemetry', () => ({
  getLogger: () => ({ emit: telemetry.emit }),
  getMeter: () => ({
    createCounter: () => ({ add: telemetry.add }),
    createHistogram: () => ({ record: telemetry.record }),
  }),
  getTracer: () => ({
    startActiveSpan: (_name: string, _options: unknown, callback: (span: unknown) => unknown) =>
      callback({
        addEvent: vi.fn(),
        end: telemetry.end,
        setAttribute: telemetry.setAttribute,
        setStatus: telemetry.setStatus,
      }),
  }),
}))

import { executeOptimisticOperation } from '../src/optimisticOperations'

describe('optimistic operation telemetry', () => {
  it('emits low-cardinality telemetry after a local operation', async () => {
    const query = vi.fn().mockResolvedValue(undefined)

    await executeOptimisticOperation({ query } as never, 'begin', 'sensitive sql')

    expect(telemetry.setAttribute).toHaveBeenCalledWith('operation.outcome', 'accepted')
    expect(telemetry.add).toHaveBeenCalledWith(1, {
      'operation.action': 'begin',
      'operation.outcome': 'accepted',
    })
    expect(telemetry.record).toHaveBeenCalledOnce()
    expect(telemetry.emit).toHaveBeenCalledOnce()
    expect(JSON.stringify(telemetry.emit.mock.calls)).not.toContain('sensitive sql')
    expect(telemetry.end).toHaveBeenCalledOnce()
  })
})
