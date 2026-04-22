// OpenTelemetry helpers scoped to this package.
//
// `@opentelemetry/api` is declared as a peer dependency so the consumer
// controls the installed version. If the consumer never registers a
// TracerProvider, every call here becomes a no-op.
//
// Unlike `@jr200-labs/xstate-nats`, this package has no cross-process
// boundary to propagate through — DuckDB-WASM runs in the same JS VM — so we
// only emit spans and do not inject/extract W3C trace context headers.

import type { Span, Tracer } from '@opentelemetry/api'
import { context as otelContext, SpanStatusCode, trace } from '@opentelemetry/api'
import pkg from '../package.json' with { type: 'json' }

const TRACER_NAME = '@jr200-labs/xstate-duckdb'

// Do not cache — `trace.getTracer` already returns a lightweight ProxyTracer,
// and caching across global-provider swaps (e.g. test teardown / re-register)
// would pin the delegate to a retired provider and silently lose spans.
export function getTracer(): Tracer {
  return trace.getTracer(TRACER_NAME, pkg.version)
}

// Truncate the stack trace before attaching it to a span event. Some backends
// (and the wire format itself) perform poorly with arbitrarily long strings;
// the first ~1KB is almost always enough to identify the site.
const MAX_STACK_LEN = 1024

function truncateStack(err: unknown): string | undefined {
  if (!(err instanceof Error) || !err.stack) return undefined
  return err.stack.length > MAX_STACK_LEN
    ? err.stack.slice(0, MAX_STACK_LEN) + '...(truncated)'
    : err.stack
}

/**
 * Record an error on a span using the OTel canonical pattern:
 * recordException + ERROR status + a named event with the truncated stack.
 */
export function recordError(span: Span, errorEventName: string, err: unknown): void {
  const message = err instanceof Error ? err.message : String(err)
  if (err instanceof Error) {
    span.recordException(err)
  }
  span.setStatus({ code: SpanStatusCode.ERROR, message })
  const stack = truncateStack(err)
  span.addEvent(errorEventName, stack ? { stack } : undefined)
}

/**
 * Run `fn` inside an active span. On synchronous throw or async rejection the
 * error is recorded, the span status is set to ERROR, and the error is
 * re-thrown (callers keep their existing error-handling semantics).
 */
export function withSpan<T>(
  name: string,
  errorEventName: string,
  attributes: Record<string, string | number | boolean | undefined>,
  fn: (span: Span) => T | Promise<T>,
): T | Promise<T> {
  const tracer = getTracer()
  const sanitized = sanitizeAttributes(attributes)
  return tracer.startActiveSpan(name, { attributes: sanitized }, otelContext.active(), (span) => {
    try {
      const result = fn(span)
      if (result && typeof (result as Promise<T>).then === 'function') {
        return (result as Promise<T>).then(
          (value) => {
            span.end()
            return value
          },
          (err: unknown) => {
            recordError(span, errorEventName, err)
            span.end()
            throw err
          },
        ) as unknown as T
      }
      span.end()
      return result
    } catch (err) {
      recordError(span, errorEventName, err)
      span.end()
      throw err
    }
  })
}

// OTel attributes cannot be `undefined`; strip those so call sites can pass
// optional values without a pre-filter.
function sanitizeAttributes(
  attrs: Record<string, string | number | boolean | undefined>,
): Record<string, string | number | boolean> {
  const out: Record<string, string | number | boolean> = {}
  for (const [k, v] of Object.entries(attrs)) {
    if (v !== undefined) out[k] = v
  }
  return out
}
