/**
 * Composition root: wires all layers together for the Order service.
 *
 * Uses @effect/experimental for:
 * - EventJournal-backed EventLog (replaces hand-rolled InMemoryEventLog)
 * - Persistence.layerMemory for snapshots and checkpoints
 */
import * as Layer from "effect/Layer"
import { layerMemory as EventLogMemory } from "../../framework/runtime/EventJournalEventLog.js"
import * as InMemoryKafkaPublisher from "../../framework/testing/InMemoryKafkaPublisher.js"
import * as InMemoryKafkaConsumer from "../../framework/testing/InMemoryKafkaConsumer.js"
import * as InMemoryDeadLetter from "../../framework/testing/InMemoryDeadLetter.js"
import { layerMemory as SnapshotStoreMemory } from "../../framework/runtime/SnapshotStore.js"
import { layerMemory as CheckpointStoreMemory } from "../../framework/projection/CheckpointStore.js"
import { N2ClockLive } from "../../framework/runtime/Clock.js"
import { IdGeneratorUuid } from "../../framework/runtime/IdGenerator.js"

/**
 * Infrastructure layer (in-memory for dev).
 *
 * EventLog: backed by @effect/experimental EventJournal.layerMemory
 * Snapshots: backed by @effect/experimental Persistence.layerMemory
 * Checkpoints: backed by @effect/experimental Persistence.layerMemory
 * Kafka: in-memory test doubles
 */
export const InfrastructureLayer = Layer.mergeAll(
  EventLogMemory,
  SnapshotStoreMemory,
  CheckpointStoreMemory,
  InMemoryKafkaPublisher.layerSimple,
  InMemoryKafkaConsumer.layer,
  InMemoryDeadLetter.layer,
  N2ClockLive,
  IdGeneratorUuid
)
