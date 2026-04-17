/**
 * Infrastructure layer composition.
 *
 * InfrastructureLayer (dev + cluster) composes:
 *   sqlJournalLayer       — R: SqlClient — provides EventJournal (backed by PostgreSQL)
 *   identityLayer         — R: never     — provides Identity (random UUID at startup)
 *   EventLogLayer         — R: SqlClient — provides EventLog (dispatch watcher)
 *   ProfileProviderSnapshotsLive — R: SqlClient — provides ProfileProviderSnapshots
 *
 * Named constants (sqlJournalLayer, identityLayer) ensure Effect deduplicates
 * shared layers: the same constant referenced in both InfrastructureLayer and
 * EventLogLayer is built exactly once at runtime.
 *
 * There are no durable workflows in the profile-provider service, so no
 * WorkflowEngine is needed. ClusterInfrastructureLayer is therefore identical
 * to InfrastructureLayer — both use SqlEventJournal with no WorkflowEngine dependency.
 */
import * as Layer from "effect/Layer"
import * as EventLogApi from "@effect/experimental/EventLog"
import { Identity } from "@effect/experimental/EventLog"
import * as SqlEventJournal from "@effect/sql/SqlEventJournal"
import { ProfileProviderProjectionLayer } from "./projector.js"
import { ProfileProviderEventLogSchema } from "./events.js"
import {
  OutboxWorkerConfigLive,
  ProfileProviderOutboxPublisherLive,
  ProfileProviderOutboxStoreLive,
  ProfileProviderOutboxWorkerLayer
} from "./outbox.js"
import { ProfileProviderSnapshotsLive } from "./snapshots.js"

const identityLayer   = Layer.succeed(Identity, Identity.makeRandom())
const sqlJournalLayer = SqlEventJournal.layer()
const outboxCoreLayer = Layer.mergeAll(
  ProfileProviderOutboxStoreLive,
  OutboxWorkerConfigLive,
  ProfileProviderOutboxPublisherLive
)

const EventLogLayer = EventLogApi.layer(ProfileProviderEventLogSchema).pipe(
  Layer.provide(ProfileProviderProjectionLayer),
  Layer.provide(Layer.mergeAll(sqlJournalLayer, identityLayer, ProfileProviderOutboxStoreLive))
)

const OutboxWorkerLayer = ProfileProviderOutboxWorkerLayer.pipe(
  Layer.provide(outboxCoreLayer)
)

export const InfrastructureLayer = Layer.mergeAll(
  sqlJournalLayer,
  identityLayer,
  EventLogLayer,
  outboxCoreLayer,
  OutboxWorkerLayer,
  ProfileProviderSnapshotsLive
)

export const ClusterInfrastructureLayer = InfrastructureLayer
