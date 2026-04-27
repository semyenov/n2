import * as Layer from "effect/Layer"
import * as EventLogApi from "@effect/experimental/EventLog"
import { Identity } from "@effect/experimental/EventLog"
import * as SqlEventJournal from "@effect/sql/SqlEventJournal"
import { ClusterWorkflowEngine } from "@effect/cluster"
import { WorkflowEngine } from "@effect/workflow"
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

const identityLayer = Layer.succeed(Identity, Identity.makeRandom())
const sqlJournalLayer = SqlEventJournal.layer()
const clickhouseLayer = RequestProviderClickhouseLayer
const clickhouseReadyLayer = Layer.merge(
  clickhouseLayer,
  Layer.provide(RequestProviderClickhouseBootstrapLayer, clickhouseLayer)
)
const projectionStoreLayer = Layer.provide(RequestProviderProjectionStoreClickhouseLive, clickhouseReadyLayer)

export const WorkflowLayer = Layer.provideMerge(
  RequestProviderEventPublishHandlers,
  Layer.merge(WorkflowEngine.layerMemory, RequestProviderEventPublisherLive)
)

export const ClusterWorkflowLayer = Layer.provideMerge(
  RequestProviderEventPublishHandlers,
  Layer.merge(ClusterWorkflowEngine.layer, RequestProviderEventPublisherLive)
)

const projectionLayer = Layer.provide(
  RequestProviderProjectionLayer,
  Layer.merge(projectionStoreLayer, RequestProviderOutboxPgLive)
)

const EventLogLayer = EventLogApi.layer(RequestProviderEventLogSchema).pipe(
  Layer.provide(projectionLayer),
  Layer.provide(Layer.merge(sqlJournalLayer, identityLayer))
)

export const InfrastructureLayer = Layer.mergeAll(
  sqlJournalLayer,
  identityLayer,
  WorkflowLayer,
  RequestProviderOutboxPgLive,
  Layer.provide(RequestProviderOutboxWorkerLive, RequestProviderOutboxPgLive),
  EventLogLayer,
  RequestProviderSnapshotsLive
)

export const ClusterInfrastructureLayer = Layer.mergeAll(
  sqlJournalLayer,
  identityLayer,
  ClusterWorkflowLayer,
  RequestProviderOutboxPgLive,
  Layer.provide(RequestProviderOutboxWorkerLive, RequestProviderOutboxPgLive),
  EventLogLayer,
  RequestProviderSnapshotsLive
)
