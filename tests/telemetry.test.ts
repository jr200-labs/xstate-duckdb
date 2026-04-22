import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { SpanStatusCode } from '@opentelemetry/api'
import { setupInMemoryTracer, type OtelTestHarness } from './otel-setup'
import { withSpan, recordError } from '../src/telemetry'

describe('telemetry helpers', () => {
  let harness: OtelTestHarness

  beforeEach(() => {
    harness = setupInMemoryTracer()
  })

  afterEach(() => {
    harness.teardown()
  })

  it('withSpan emits a span on sync success', () => {
    const result = withSpan('test.sync', 'test.error', { foo: 'bar' }, () => 42)
    expect(result).toBe(42)

    const spans = harness.exporter.getFinishedSpans()
    expect(spans).toHaveLength(1)
    expect(spans[0].name).toBe('test.sync')
    expect(spans[0].attributes.foo).toBe('bar')
    expect(spans[0].status.code).not.toBe(SpanStatusCode.ERROR)
  })

  it('withSpan emits a span on async success', async () => {
    const result = await withSpan('test.async', 'test.error', {}, async () => 'done')
    expect(result).toBe('done')

    const spans = harness.exporter.getFinishedSpans()
    expect(spans[0].name).toBe('test.async')
    expect(spans[0].status.code).not.toBe(SpanStatusCode.ERROR)
  })

  it('withSpan records error on sync throw and re-throws', () => {
    expect(() =>
      withSpan('test.throw', 'test.error', {}, () => {
        throw new Error('boom')
      }),
    ).toThrow('boom')

    const spans = harness.exporter.getFinishedSpans()
    expect(spans[0].status.code).toBe(SpanStatusCode.ERROR)
    expect(spans[0].events.some((e) => e.name === 'test.error')).toBe(true)
  })

  it('withSpan records error on async rejection and re-throws', async () => {
    await expect(
      withSpan('test.reject', 'test.error', {}, async () => {
        throw new Error('nope')
      }),
    ).rejects.toThrow('nope')

    const spans = harness.exporter.getFinishedSpans()
    expect(spans[0].status.code).toBe(SpanStatusCode.ERROR)
  })

  it('withSpan drops undefined attribute values', () => {
    withSpan('test.attrs', 'test.error', { a: 'x', b: undefined, c: 0 }, () => null)

    const spans = harness.exporter.getFinishedSpans()
    expect(spans[0].attributes.a).toBe('x')
    expect(spans[0].attributes.c).toBe(0)
    expect('b' in spans[0].attributes).toBe(false)
  })

  it('recordError truncates stacks longer than 1KB', () => {
    withSpan('test.bigstack', 'test.error', {}, (span) => {
      const err = new Error('big')
      err.stack = 'x'.repeat(2000)
      recordError(span, 'test.error', err)
    })

    const spans = harness.exporter.getFinishedSpans()
    const event = spans[0].events.find((e) => e.name === 'test.error')!
    expect((event.attributes?.stack as string).length).toBeLessThan(1100)
    expect((event.attributes?.stack as string).endsWith('...(truncated)')).toBe(true)
  })
})
