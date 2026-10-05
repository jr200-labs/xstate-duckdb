import { duckdbMachine } from '@jr200-labs/xstate-duckdb'
import type { SnapshotFrom } from 'xstate'

const positions: Record<string, [number, number]> = {
  idle: [110, 40],
  configured: [370, 40],
  initializing: [630, 40],
  connected: [890, 40],
  error: [110, 160],
  disconnected: [370, 160],
  transaction: [890, 160],
}
// Each directed transition has its own ports and lane, including reverse transitions.
const routes: Record<string, { path: string; label: [number, number] }> = {
  'idle:configured': { path: 'M190,30 H290', label: [240, 21] },
  'configured:idle': { path: 'M290,50 C260,88 220,88 190,50', label: [240, 83] },
  'configured:initializing': { path: 'M450,40 H550', label: [500, 30] },
  'initializing:connected': { path: 'M710,40 H810', label: [760, 30] },
  'connected:disconnected': { path: 'M810,50 H785 V116 H370 V140', label: [600, 108] },
  'disconnected:configured': { path: 'M410,140 V60', label: [435, 95] },
  'connected:transaction': { path: 'M940,60 V140', label: [1007, 98] },
  'transaction:connected': { path: 'M850,140 V60', label: [875, 108] },
  'error:configured': { path: 'M110,140 V103 H330 V60', label: [230, 120] },
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
    <section className="machine-diagram" aria-label="Live state diagram">
      <div className="diagram-heading">
        <h2>
          State machine <span className="muted">· active state in blue</span>
        </h2>
        <span className="state-badge">{JSON.stringify(snapshot.value)}</span>
      </div>
      <div className="diagram-scroll">
        <svg
          viewBox="0 0 1080 195"
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
              orient="auto"
            >
              <path d="M 0 0 L 10 5 L 0 10 z" fill="#718299" />
            </marker>
          </defs>
          {transitions.map(({ source, target, label }, index) => {
            const route = routes[`${source}:${target}`]
            if (!route) return null
            return (
              <g key={index}>
                <path
                  d={route.path}
                  stroke="#718299"
                  strokeWidth="1.4"
                  fill="none"
                  markerEnd="url(#arrow)"
                />
                <text
                  x={route.label[0]}
                  y={route.label[1]}
                  textAnchor="middle"
                  fontSize="13"
                  fill="#52657c"
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
                  x={x - 80}
                  y={y - 20}
                  width="160"
                  height="40"
                  rx="8"
                  fill={active ? '#e9f1ff' : '#f8fafc'}
                  stroke={active ? '#2563eb' : '#c0ccda'}
                  strokeWidth={active ? 2 : 1}
                />
                <text x={x} y={y + 5} textAnchor="middle" fontSize="17" fill="#172b43">
                  {active ? '● ' : ''}
                  {node.key}
                </text>
              </g>
            )
          })}
        </svg>
      </div>
    </section>
  )
}
