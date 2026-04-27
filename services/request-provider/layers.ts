import { makeServiceInfrastructureLayers } from "@semyenov/n2/runtime"
import * as EventLogApi from "@effect/experimental/EventLog"
import {
  RequestProviderClickhouseBootstrapLayer,
} from "./clickhouse-schema.js"
import { RequestProviderClickhouseLayer } from "./clickhouse.js"
import {
  RequestProviderOutboxPgLive,
  RequestProviderOutboxWorkerLive
} from "./outbox.js"
import { RequestProviderProjectionStoreClickhouseLive } from "./projection-store-clickhouse.js"
import { RequestProviderProjectionLayer } from "./projector.js"
import { RequestProviderEventLogSchema } from "./events.js"
import { RequestProviderSnapshotsLive } from "./snapshots.js"
import {
  RequestProviderEventPublishHandlers,
  RequestProviderEventPublisherLive
} from "./workflows.js"

const infrastructure = makeServiceInfrastructureLayers({
  eventLogLayer: EventLogApi.layer(RequestProviderEventLogSchema),
  projectionLayer: RequestProviderProjectionLayer,
  projectionStoreLayer: RequestProviderProjectionStoreClickhouseLive,
  clickhouseLayer: RequestProviderClickhouseLayer,
  clickhouseBootstrapLayer: RequestProviderClickhouseBootstrapLayer,
  outboxLive: RequestProviderOutboxPgLive,
  outboxWorkerLive: RequestProviderOutboxWorkerLive,
  snapshotsLive: RequestProviderSnapshotsLive,
  publishHandlers: RequestProviderEventPublishHandlers,
  publisherLive: RequestProviderEventPublisherLive
})

export const WorkflowLayer = infrastructure.WorkflowLayer
export const ClusterWorkflowLayer = infrastructure.ClusterWorkflowLayer
export const InfrastructureLayer = infrastructure.InfrastructureLayer
export const ClusterInfrastructureLayer = infrastructure.ClusterInfrastructureLayer
