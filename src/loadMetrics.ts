export interface DuckDbLoadMetricBucket {
  encodedBytes: number
  decodedBytes: number
  loadedBytes: number
}

export interface DuckDbLoadMetrics extends DuckDbLoadMetricBucket {
  byTable: Partial<Record<string, DuckDbLoadMetricBucket>>
}

export interface DuckDbLoadMetricRecord extends DuckDbLoadMetricBucket {
  tableSpecName: string
}

export function createEmptyDuckDbLoadMetrics(): DuckDbLoadMetrics {
  return {
    encodedBytes: 0,
    decodedBytes: 0,
    loadedBytes: 0,
    byTable: {},
  }
}

export function recordDuckDbLoadMetric(
  metrics: DuckDbLoadMetrics,
  record: DuckDbLoadMetricRecord,
): DuckDbLoadMetrics {
  const encodedBytes = normaliseByteCount(record.encodedBytes)
  const decodedBytes = normaliseByteCount(record.decodedBytes)
  const loadedBytes = normaliseByteCount(record.loadedBytes)
  if (encodedBytes === 0 && decodedBytes === 0 && loadedBytes === 0) return metrics

  const currentTable = metrics.byTable[record.tableSpecName] ?? {
    encodedBytes: 0,
    decodedBytes: 0,
    loadedBytes: 0,
  }

  return {
    encodedBytes: metrics.encodedBytes + encodedBytes,
    decodedBytes: metrics.decodedBytes + decodedBytes,
    loadedBytes: metrics.loadedBytes + loadedBytes,
    byTable: {
      ...metrics.byTable,
      [record.tableSpecName]: {
        encodedBytes: currentTable.encodedBytes + encodedBytes,
        decodedBytes: currentTable.decodedBytes + decodedBytes,
        loadedBytes: currentTable.loadedBytes + loadedBytes,
      },
    },
  }
}

export function resetDuckDbLoadMetrics(
  metrics: DuckDbLoadMetrics,
  tableSpecName?: string,
): DuckDbLoadMetrics {
  if (!tableSpecName) return createEmptyDuckDbLoadMetrics()

  const tableMetrics = metrics.byTable[tableSpecName]
  if (!tableMetrics) return metrics

  const byTable = { ...metrics.byTable }
  delete byTable[tableSpecName]

  return {
    encodedBytes: metrics.encodedBytes - tableMetrics.encodedBytes,
    decodedBytes: metrics.decodedBytes - tableMetrics.decodedBytes,
    loadedBytes: metrics.loadedBytes - tableMetrics.loadedBytes,
    byTable,
  }
}

const textEncoder = new TextEncoder()

export function byteLength(value: unknown): number {
  if (value == null) return 0
  if (value instanceof Uint8Array) return value.byteLength
  if (value instanceof ArrayBuffer) return value.byteLength
  if (ArrayBuffer.isView(value)) return value.byteLength
  if (typeof value === 'string') return textEncoder.encode(value).byteLength

  try {
    return textEncoder.encode(JSON.stringify(value)).byteLength
  } catch {
    return 0
  }
}

function normaliseByteCount(value: number | undefined): number {
  if (!Number.isFinite(value) || value === undefined || value <= 0) return 0
  return Math.floor(value)
}
