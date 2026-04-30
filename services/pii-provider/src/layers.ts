import { makeServiceInfrastructureLayers } from "@semyenov/n2/runtime"
import * as EventLogApi from "@effect/experimental/EventLog"
import * as Layer from "effect/Layer"
import {
  PIIProviderClickhouseBootstrapLayer,
} from "./clickhouse-schema.js"
import { PIIProviderClickhouseLayer } from "./clickhouse.js"
import {
  PIIProviderOutboxPgLive,
  PIIProviderOutboxWorkerLive
} from "./outbox.js"
import { PIIProviderProjectionStoreClickhouseLive } from "./projection-store-clickhouse.js"
import { PIIProviderProjectionLayer } from "./projector.js"
import { PIIProviderEventJournalTables } from "./event-journal.js"
import { PIIProviderEventLogSchema } from "./events.js"
import { PIIProviderSnapshotsLive } from "./snapshots.js"
import {
  PIIProviderEventPublishHandlers,
  PIIProviderEventPublisherLive
} from "./workflows.js"
import { PIICryptoLive } from "./crypto.js"

const infrastructure = makeServiceInfrastructureLayers({
  eventJournal: PIIProviderEventJournalTables,
  eventLogLayer: EventLogApi.layer(PIIProviderEventLogSchema),
  projectionLayer: PIIProviderProjectionLayer,
  projectionStoreLayer: PIIProviderProjectionStoreClickhouseLive,
  clickhouseLayer: PIIProviderClickhouseLayer,
  clickhouseBootstrapLayer: PIIProviderClickhouseBootstrapLayer,
  outboxLive: PIIProviderOutboxPgLive,
  outboxWorkerLive: PIIProviderOutboxWorkerLive,
  snapshotsLive: PIIProviderSnapshotsLive,
  publishHandlers: PIIProviderEventPublishHandlers,
  publisherLive: PIIProviderEventPublisherLive
})

export const WorkflowLayer = infrastructure.WorkflowLayer
export const ClusterWorkflowLayer = infrastructure.ClusterWorkflowLayer
export const InfrastructureLayer = Layer.merge(infrastructure.InfrastructureLayer, PIICryptoLive)
export const ClusterInfrastructureLayer = Layer.merge(infrastructure.ClusterInfrastructureLayer, PIICryptoLive)
