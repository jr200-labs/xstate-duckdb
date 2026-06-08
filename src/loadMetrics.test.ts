import { describe, expect, it } from 'vitest'
import {
  byteLength,
  createEmptyDuckDbLoadMetrics,
  recordDuckDbLoadMetric,
  resetDuckDbLoadMetrics,
} from './loadMetrics'

describe('DuckDB load metrics', () => {
  it('counts UTF-8 bytes for strings and JSON payloads', () => {
    expect(byteLength('hello')).toBe(5)
    expect(byteLength({ value: 'hello' })).toBe(17)
  })

  it('records totals and per-table load metrics', () => {
    const metrics = recordDuckDbLoadMetric(createEmptyDuckDbLoadMetrics(), {
      tableSpecName: 'cargos',
      encodedBytes: 12,
      decodedBytes: 9,
      loadedBytes: 120,
    })

    expect(metrics.encodedBytes).toBe(12)
    expect(metrics.decodedBytes).toBe(9)
    expect(metrics.loadedBytes).toBe(120)
    expect(metrics.byTable.cargos).toEqual({
      encodedBytes: 12,
      decodedBytes: 9,
      loadedBytes: 120,
    })
  })

  it('resets all metrics or one table', () => {
    const metrics = recordDuckDbLoadMetric(
      recordDuckDbLoadMetric(createEmptyDuckDbLoadMetrics(), {
        tableSpecName: 'cargos',
        encodedBytes: 12,
        decodedBytes: 9,
        loadedBytes: 120,
      }),
      {
        tableSpecName: 'vectors',
        encodedBytes: 8,
        decodedBytes: 6,
        loadedBytes: 60,
      },
    )

    expect(resetDuckDbLoadMetrics(metrics, 'cargos')).toEqual({
      encodedBytes: 8,
      decodedBytes: 6,
      loadedBytes: 60,
      byTable: {
        vectors: {
          encodedBytes: 8,
          decodedBytes: 6,
          loadedBytes: 60,
        },
      },
    })
    expect(resetDuckDbLoadMetrics(metrics)).toEqual(createEmptyDuckDbLoadMetrics())
  })
})
