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
 */
import * as Layer from "effect/Layer"
import * as EventLogApi from "@effect/experimental/EventLog"
import { Identity } from "@effect/experimental/EventLog"
import * as SqlEventJournal from "@effect/sql/SqlEventJournal"
import { ClusterWorkflowEngine } from "@effect/cluster"
import {
  ProfileProviderClickhouseBootstrapLayer,
} from "./clickhouse-schema.js"
import { ProfileProviderClickhouseLayer } from "./clickhouse.js"
import {
  ProfileProviderOutboxPgLive,
  ProfileProviderOutboxWorkerLive
} from "./outbox.js"
import { WorkflowEngine } from "@effect/workflow"
import { ProfileProviderProjectionStoreClickhouseLive } from "./projection-store-clickhouse.js"
import { ProfileProviderProjectionLayer } from "./projector.js"
import { ProfileProviderEventLogSchema } from "./events.js"
import { ProfileProviderSnapshotsLive } from "./snapshots.js"
import {
  ProfileProviderEventPublishHandlers,
  ProfileProviderEventPublisherLive
} from "./workflows.js"

const identityLayer = Layer.succeed(Identity, Identity.makeRandom())
const sqlJournalLayer = SqlEventJournal.layer()
const clickhouseLayer = ProfileProviderClickhouseLayer
const clickhouseReadyLayer = Layer.merge(
  clickhouseLayer,
  Layer.provide(ProfileProviderClickhouseBootstrapLayer, clickhouseLayer)
)
const projectionStoreLayer = Layer.provide(ProfileProviderProjectionStoreClickhouseLive, clickhouseReadyLayer)

export const WorkflowLayer = Layer.provideMerge(
  ProfileProviderEventPublishHandlers,
  Layer.merge(WorkflowEngine.layerMemory, ProfileProviderEventPublisherLive)
)

export const ClusterWorkflowLayer = Layer.provideMerge(
  ProfileProviderEventPublishHandlers,
  Layer.merge(ClusterWorkflowEngine.layer, ProfileProviderEventPublisherLive)
)

const projectionLayer = Layer.provide(
  ProfileProviderProjectionLayer,
  Layer.merge(projectionStoreLayer, ProfileProviderOutboxPgLive)
)

const EventLogLayer = EventLogApi.layer(ProfileProviderEventLogSchema).pipe(
  Layer.provide(projectionLayer),
  Layer.provide(Layer.merge(sqlJournalLayer, identityLayer))
)

export const InfrastructureLayer = Layer.mergeAll(
  sqlJournalLayer,
  identityLayer,
  WorkflowLayer,
  ProfileProviderOutboxPgLive,
  Layer.provide(ProfileProviderOutboxWorkerLive, ProfileProviderOutboxPgLive),
  EventLogLayer,
  ProfileProviderSnapshotsLive
)

export const ClusterInfrastructureLayer = Layer.mergeAll(
  sqlJournalLayer,
  identityLayer,
  ClusterWorkflowLayer,
  ProfileProviderOutboxPgLive,
  Layer.provide(ProfileProviderOutboxWorkerLive, ProfileProviderOutboxPgLive),
  EventLogLayer,
  ProfileProviderSnapshotsLive
)
