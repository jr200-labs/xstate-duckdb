import { duckdbMachine } from '@jr200-labs/xstate-duckdb'
import type { SnapshotFrom } from 'xstate'

const positions: Record<string, [number, number]> = {
  idle: [110, 55],
  configured: [380, 55],
  initializing: [650, 55],
  connected: [920, 55],
  error: [110, 230],
  disconnected: [380, 230],
  transaction: [920, 230],
}
const nodes = Object.values(duckdbMachine.states)
const transitions = nodes.flatMap((node) =>
  [...node.transitions.values()].flat().flatMap((transition) =>
    (transition.target ?? []).map((target) => ({
      source: node.key,
      target: target.path[0],
      label: transition.eventType.startsWith('xstate.done.') ? 'done' : transition.eventType,
    })),
  ),
)

export function MachineDiagram({ snapshot }: { snapshot: SnapshotFrom<typeof duckdbMachine> }) {
  return (
    <section className="bg-white rounded-lg shadow p-4" aria-label="Live state diagram">
      <h2 className="text-lg font-semibold">Live state machine</h2>
      <p className="text-sm text-gray-600">
        Root lifecycle overview · blue marks the active state. Use the controls below to send
        events.
      </p>
      <div className="overflow-x-auto">
        <svg
          viewBox="0 0 1080 300"
          className="w-full min-w-[720px]"
          role="img"
          aria-label={`DuckDB state: ${JSON.stringify(snapshot.value)}`}
        >
          <defs>
            <marker
              id="arrow"
              viewBox="0 0 10 10"
              refX="9"
              refY="5"
              markerWidth="6"
              markerHeight="6"
              orient="auto-start-reverse"
            >
              <path d="M 0 0 L 10 5 L 0 10 z" fill="#64748b" />
            </marker>
          </defs>
          {transitions.map(({ source, target, label }, index) => {
            const from = positions[source],
              to = positions[target]
            if (!from || !to) return null
            const horizontal = from[1] === to[1]
            const direction = horizontal ? Math.sign(to[0] - from[0]) : Math.sign(to[1] - from[1])
            const vertical = from[0] === to[0]
            const lane = vertical ? direction * 40 : 0
            const x1 = from[0] + (horizontal ? direction * 88 : lane)
            const y1 = from[1] + (horizontal ? 0 : direction * 24)
            const x2 = to[0] + (horizontal ? -direction * 88 : lane)
            const y2 = to[1] - (horizontal ? 0 : direction * 24)
            const bend = horizontal && direction < 0 ? 45 : 0
            return (
              <g key={index}>
                <path
                  d={`M${x1},${y1} Q${(x1 + x2) / 2},${(y1 + y2) / 2 + bend} ${x2},${y2}`}
                  stroke="#64748b"
                  fill="none"
                  markerEnd="url(#arrow)"
                />
                <text
                  x={(x1 + x2) / 2}
                  y={(y1 + y2) / 2 + bend / 2 - 8 + (vertical ? direction * 18 : 0)}
                  textAnchor="middle"
                  fontSize="10"
                  fill="#475569"
                  paintOrder="stroke"
                  stroke="white"
                  strokeWidth="4"
                >
                  {label}
                </text>
              </g>
            )
          })}
          {nodes.map((node) => {
            const [x, y] = positions[node.key]
            const active = snapshot.matches(node.key as Parameters<typeof snapshot.matches>[0])
            return (
              <g key={node.key}>
                <rect
                  x={x - 88}
                  y={y - 24}
                  width="176"
                  height="48"
                  rx="12"
                  fill={active ? '#dbeafe' : '#f8fafc'}
                  stroke={active ? '#2563eb' : '#94a3b8'}
                  strokeWidth={active ? 3 : 1}
                />
                <text x={x} y={y + 5} textAnchor="middle" fontSize="15" fill="#0f172a">
                  {active ? '● ' : ''}
                  {node.key}
                </text>
              </g>
            )
          })}
        </svg>
      </div>
      <p className="font-mono text-sm" role="status">
        Active: {JSON.stringify(snapshot.value)}
      </p>
      <p className="text-xs text-gray-500 mt-2">
        Transaction substates appear in the active value. Query and catalog events can run without
        changing the root state.
      </p>
    </section>
  )
}
