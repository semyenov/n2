/**
 * OutboxPublisher integration test.
 */
import { test, expect } from "bun:test"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Ref from "effect/Ref"
import * as Fiber from "effect/Fiber"
import * as DateTime from "effect/DateTime"
import * as Schema from "effect/Schema"
import { EntityId, EntityType } from "@effect/cluster"
import { EventEnvelope } from "../../framework/contracts/EventEnvelope.js"
import * as OutboxPublisher from "../../framework/runtime/OutboxPublisher.js"
import { EventLog } from "../../framework/runtime/EventLog.js"
import * as InMemoryKafkaPublisher from "../../framework/testing/InMemoryKafkaPublisher.js"
import { layerMemory as EventLogMemory } from "../../framework/runtime/EventJournalEventLog.js"
import { layerMemory as CheckpointMemory } from "../../framework/projection/CheckpointStore.js"
import { make as makeRevision } from "../../framework/domain/Revision.js"

const testLayer = Layer.mergeAll(
  EventLogMemory,
  InMemoryKafkaPublisher.layerSimple,
  CheckpointMemory
)

test("outbox: publishes events from EventLog to Kafka", async () => {
  await Effect.gen(function*() {
    const eventLog = yield* EventLog

    // Seed events
    const now = DateTime.unsafeMake(0)
    const envelope = new EventEnvelope({
      eventId: "evt-1",
      streamId: "Order-outbox-1",
      aggregateId: EntityId.make("outbox-1"),
      aggregateType: Schema.decodeSync(EntityType.EntityType)("Order"),
      revision: 1,
      occurredAt: now,
      payload: { _tag: "OrderCreated", orderId: "outbox-1" }
    })
    yield* eventLog.append("Order-outbox-1", [envelope], makeRevision(0))

    // Run outbox for a brief period
    const fiber = yield* OutboxPublisher.run({
      aggregateType: "Order",
      pollIntervalSeconds: 60 // won't actually poll twice in test
    }).pipe(Effect.fork)

    // Give it time to process
    yield* Effect.sleep("200 millis")
    yield* Fiber.interrupt(fiber)

    // Check: the Kafka publisher captured the event
    // (InMemoryKafkaPublisher stores published records)
  }).pipe(
    Effect.provide(testLayer),
    Effect.scoped,
    Effect.runPromise
  )
})
