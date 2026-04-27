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
import { makeServiceInfrastructureLayers } from "@semyenov/n2/runtime"
import * as EventLogApi from "@effect/experimental/EventLog"
import {
  ProfileProviderClickhouseBootstrapLayer,
} from "./clickhouse-schema.js"
import { ProfileProviderClickhouseLayer } from "./clickhouse.js"
import {
  ProfileProviderOutboxPgLive,
  ProfileProviderOutboxWorkerLive
} from "./outbox.js"
import { ProfileProviderProjectionStoreClickhouseLive } from "./projection-store-clickhouse.js"
import { ProfileProviderProjectionLayer } from "./projector.js"
import { ProfileProviderEventLogSchema } from "./events.js"
import { ProfileProviderSnapshotsLive } from "./snapshots.js"
import {
  ProfileProviderEventPublishHandlers,
  ProfileProviderEventPublisherLive
} from "./workflows.js"

const infrastructure = makeServiceInfrastructureLayers({
  eventLogLayer: EventLogApi.layer(ProfileProviderEventLogSchema),
  projectionLayer: ProfileProviderProjectionLayer,
  projectionStoreLayer: ProfileProviderProjectionStoreClickhouseLive,
  clickhouseLayer: ProfileProviderClickhouseLayer,
  clickhouseBootstrapLayer: ProfileProviderClickhouseBootstrapLayer,
  outboxLive: ProfileProviderOutboxPgLive,
  outboxWorkerLive: ProfileProviderOutboxWorkerLive,
  snapshotsLive: ProfileProviderSnapshotsLive,
  publishHandlers: ProfileProviderEventPublishHandlers,
  publisherLive: ProfileProviderEventPublisherLive
})

export const WorkflowLayer = infrastructure.WorkflowLayer
export const ClusterWorkflowLayer = infrastructure.ClusterWorkflowLayer
export const InfrastructureLayer = infrastructure.InfrastructureLayer
export const ClusterInfrastructureLayer = infrastructure.ClusterInfrastructureLayer
