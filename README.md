# @jr200-labs/xstate-duckdb

A state machine for managing DuckDB operations in web applications. This library provides a type-safe interface for database initialization, query execution, transaction management, and table catalog operations.

## Features

- **State Management**: Full XState integration for predictable database state management
- **DuckDB Integration**: Built on top of `@duckdb/duckdb-wasm` for browser-based analytics
- **Transaction Support**: Complete transaction lifecycle management (begin, execute, commit, rollback)
- **Table Catalog**: Dynamic table loading, versioning, and management
- **Type Safety**: Full TypeScript support with comprehensive type definitions
- **Multiple Data Formats**: Support for Arrow IPC and JSON data formats
- **Compression**: Built-in support for data compression (zlib)
- **Real-time Updates**: Subscription-based table change notifications
- **Optimistic Projections**: Typed local operations over authoritative DuckDB views with acknowledgement and reconciliation

## Installation

```bash
npm install @jr200-labs/xstate-duckdb
# or
yarn add @jr200-labs/xstate-duckdb
# or
pnpm add @jr200-labs/xstate-duckdb
```

### Peer dependencies

`@duckdb/duckdb-wasm`, `apache-arrow`, `@opentelemetry/api`, and `@opentelemetry/api-logs` are declared as peer dependencies and must be installed directly by the consumer. This guarantees a single resolved version across the dependency tree -- preventing the class of bug where a transitive copy of DuckDB-wasm diverges from the `.wasm` assets the consumer actually ships.

```bash
pnpm add @duckdb/duckdb-wasm apache-arrow @opentelemetry/api @opentelemetry/api-logs
```

Supported ranges:

| Peer                      | Range                 |
| ------------------------- | --------------------- |
| `@duckdb/duckdb-wasm`     | `>=1.33.1-dev42.0 <2` |
| `apache-arrow`            | `>=21 <22`            |
| `@opentelemetry/api`      | `^1.9.1`              |
| `@opentelemetry/api-logs` | `^0.221.0`            |

## Documentation and live demo

The Quarto documentation lives in [`docs/`](docs/index.qmd), with guides for
[getting started](docs/getting-started.qmd), [queries and transactions](docs/queries.qmd),
[the catalog](docs/catalog.qmd), [observability](docs/observability.qmd), and
[optimistic projections](docs/optimistic.qmd).

The interactive React example now lives in `docs/demo/`. It runs DuckDB in the
visitor's browser and displays a live XState lifecycle diagram alongside query,
transaction, and catalog controls.

```bash
pnpm install --frozen-lockfile
pnpm demo:dev       # library build + Vite demo
pnpm docs:preview   # Quarto docs with embedded production demo
pnpm docs:build     # static site in docs/_site
pnpm test
```

Documentation builds require Node.js 22+ and Quarto. Quarto's pre-render hook
builds the library and demo automatically. Generated files are not committed.

### Publishing

`.github/workflows/bespoke_docs.yaml` validates the site on pull requests and
uses GAT's reusable Quarto publisher on master. The caller includes library and
lockfile changes as well as docs, so the demo is rebuilt when its code changes.
GAT publishes the rendered site to `gh-pages`. Configure GitHub Pages to serve
that branch's root directory. The demo uses relative asset paths so the site
works beneath the project's GitHub Pages path.

## Contributing

Contributions welcome!

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

MIT License - see [LICENSE](LICENSE) file for details.
