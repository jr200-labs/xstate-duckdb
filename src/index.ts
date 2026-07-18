import { SnapshotFrom } from 'xstate'
import { dbCatalogLogic } from './machines/dbCatalog'

export {
  duckdbMachine,
  type Context as DuckDbContext,
  type Events as DuckDbEvent,
} from './machines/root'
export {
  type Context as DuckDbCatalogContext,
  type Events as DuckDbCatalogEvent,
} from './machines/dbCatalog'
export type {
  DuckDbInitialistionStatus,
  TableDefinition,
  LoadedTableEntry,
  MachineConfig,
  InitDuckDbParams,
} from './lib/types'

export { duckdbRunQuery, type QueryDbParams, type ResultOptions } from './actors/dbQuery'
export {
  byteLength as duckDbLoadMetricByteLength,
  createEmptyDuckDbLoadMetrics,
  recordDuckDbLoadMetric,
  resetDuckDbLoadMetrics,
  type DuckDbLoadMetricBucket,
  type DuckDbLoadMetricRecord,
  type DuckDbLoadMetrics,
} from './loadMetrics'

export type DuckDbCatalogSnapshot = SnapshotFrom<typeof dbCatalogLogic>

export {
  createOptimisticOperationAckSql,
  createOptimisticOperationBeginSql,
  createOptimisticOperationReconcileSql,
  createOptimisticOperationRejectSql,
  createOptimisticOperationsTableSql,
  createOptimisticOperationUnknownSql,
  createOptimisticOverlayViewSql,
  hasOptimisticChangeSet,
  rebuildOptimisticOverlayView,
  executeOptimisticOperation,
  type OptimisticFieldMapping,
  type OptimisticOperationAction,
  type OptimisticOperationField,
} from './optimisticOperations'
