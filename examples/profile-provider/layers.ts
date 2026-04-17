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
import { WorkflowEngine } from "@effect/workflow"
import { ProfileProviderProjectionLayer } from "./projector.js"
import { ProfileProviderEventLogSchema } from "./events.js"
import { ProfileProviderSnapshotsLive } from "./snapshots.js"
import {
  ProfileProviderEventPublishHandlers,
  ProfileProviderEventPublisherLive
} from "./workflows.js"

const identityLayer   = Layer.succeed(Identity, Identity.makeRandom())
const sqlJournalLayer = SqlEventJournal.layer()

export const WorkflowLayer = Layer.provideMerge(
  ProfileProviderEventPublishHandlers,
  Layer.merge(WorkflowEngine.layerMemory, ProfileProviderEventPublisherLive)
)

export const ClusterWorkflowLayer = Layer.provideMerge(
  ProfileProviderEventPublishHandlers,
  Layer.merge(ClusterWorkflowEngine.layer, ProfileProviderEventPublisherLive)
)

const EventLogLayer = EventLogApi.layer(ProfileProviderEventLogSchema).pipe(
  Layer.provide(ProfileProviderProjectionLayer),
  Layer.provide(Layer.merge(sqlJournalLayer, identityLayer))
)

export const InfrastructureLayer = Layer.mergeAll(
  sqlJournalLayer,
  identityLayer,
  WorkflowLayer,
  EventLogLayer,
  ProfileProviderSnapshotsLive
)

export const ClusterInfrastructureLayer = Layer.mergeAll(
  sqlJournalLayer,
  identityLayer,
  ClusterWorkflowLayer,
  EventLogLayer,
  ProfileProviderSnapshotsLive
)
