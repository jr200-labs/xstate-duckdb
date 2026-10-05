// Browser-safe display for Arrow rows, bigint values, maps, and actor context.
export function formatValue(value: unknown, options: { maxDepth?: number } = {}): string {
  const seen = new WeakSet<object>()
  const visit = (item: unknown, depth: number): unknown => {
    if (typeof item === 'bigint') return item.toString()
    if (typeof item === 'function') return '[Function]'
    if (!item || typeof item !== 'object') return item
    if (seen.has(item)) return '[Circular]'
    if (depth > (options.maxDepth ?? 5)) return '[Object]'
    seen.add(item)
    if (item instanceof Map)
      return Object.fromEntries(
        [...item].map(([key, entry]) => [String(key), visit(entry, depth + 1)]),
      )
    if (Array.isArray(item)) return item.map((entry) => visit(entry, depth + 1))
    return Object.fromEntries(
      Object.entries(item).map(([key, entry]) => [key, visit(entry, depth + 1)]),
    )
  }
  return JSON.stringify(visit(value, 0), null, 2) ?? String(value)
}
