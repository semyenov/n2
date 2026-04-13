/**
 * Infrastructure layer composition.
 *
 * Two infrastructure layers are exported:
 *
 *   InfrastructureLayer        — dev / server.ts
 *     WorkflowEngine.layerMemory (in-process, lost on restart)
 *
 *   ClusterInfrastructureLayer — cluster.ts
 *     ClusterWorkflowEngine.layer (backed by Sharding + MessageStorage)
 *     Workflow executions survive process restarts because they are stored
 *     in SqlMessageStorage alongside shard messages.
 *
 * Both layers are exported as named constants so callers import the same
 * object reference. Effect deduplicates layers by identity — the same
 * reference is built once regardless of how many places provide it.
 */
import * as Layer from "effect/Layer"
import * as EventLogApi from "@effect/experimental/EventLog"
import { Identity } from "@effect/experimental/EventLog"
import * as SqlEventJournal from "@effect/sql/SqlEventJournal"
import { ClusterWorkflowEngine } from "@effect/cluster"
import { WorkflowEngine } from "@effect/workflow"
import { OrderFulfillmentHandlers } from "./workflows.js"
import { OrderProjectionLayer } from "./projector.js"
import { OrderEventLogSchema } from "./events.js"

// ---------------------------------------------------------------------------
// Shared base services
//
// Named constants ensure Effect deduplicates at runtime: the same reference
// used in multiple Layer.provide / mergeAll calls is built exactly once.
// ---------------------------------------------------------------------------

const identityLayer   = Layer.succeed(Identity, Identity.makeRandom())
const sqlJournalLayer = SqlEventJournal.layer()  // requires SqlClient, provides EventJournal

// ---------------------------------------------------------------------------
// Workflow layers
// ---------------------------------------------------------------------------

// Memory-based workflow engine — for dev/server mode.
// Workflows do not survive process restarts.
export const WorkflowLayer = Layer.provideMerge(
  OrderFulfillmentHandlers,
  WorkflowEngine.layerMemory
)

// Cluster-backed workflow engine — for production cluster mode.
// ClusterWorkflowEngine.layer uses Sharding + MessageStorage so workflow
// execution state is stored durably in the same PostgreSQL-backed message
// queue as shard messages. Requires Sharding | MessageStorage in context.
export const ClusterWorkflowLayer = Layer.provideMerge(
  OrderFulfillmentHandlers,
  ClusterWorkflowEngine.layer
)

// ---------------------------------------------------------------------------
// EventLog layer
//
// EventLog.layer(schema) is the service that watches SqlEventJournal and
// dispatches incoming events to the handler services registered by
// OrderProjectionLayer (via EventLog.group). Without this, the projection
// handlers are defined but never called.
//
// EventLog.layer requires:
//   EventGroup.ToService<Groups> — provided by OrderProjectionLayer
//   EventJournal                 — provided by sqlJournalLayer
//   Identity                     — provided by identityLayer
//
// All three are satisfied inline so EventLogLayer has R = SqlClient only.
// ---------------------------------------------------------------------------

const EventLogLayer = EventLogApi.layer(OrderEventLogSchema).pipe(
  Layer.provide(OrderProjectionLayer),
  Layer.provide(Layer.merge(sqlJournalLayer, identityLayer))
)

// ---------------------------------------------------------------------------
// Infrastructure layers
// ---------------------------------------------------------------------------

// Dev mode: memory-based workflow engine, SQL event journal, EventLog, projections.
// Requires SqlClient — callers must provide a PgClient layer.
export const InfrastructureLayer = Layer.mergeAll(
  sqlJournalLayer,  // R: SqlClient — provides EventJournal
  identityLayer,    // R: never    — provides Identity
  WorkflowLayer,    // R: never    — provides WorkflowEngine
  EventLogLayer     // R: SqlClient — provides EventLog
)

// Cluster mode: cluster-backed workflow engine, SQL event journal, EventLog, projections.
// Requires: SqlClient | Sharding | MessageStorage
export const ClusterInfrastructureLayer = Layer.mergeAll(
  sqlJournalLayer,
  identityLayer,
  ClusterWorkflowLayer,
  EventLogLayer
)
