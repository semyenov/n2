/**
 * @since 1.0.0
 * @module Subscription
 *
 * Kafka-based projection subscription.
 *
 * For native Effect projections (non-Kafka), prefer:
 * - EventJournal.changes() for real-time event subscription
 * - EventLog.group() for typed event handlers
 * - Reactivity for query invalidation
 * - PersistedCache for projection caching with automatic invalidation
 */
import * as Effect from "effect/Effect"
import * as Stream from "effect/Stream"
import * as Schedule from "effect/Schedule"
import * as Schema from "effect/Schema"
import * as DateTime from "effect/DateTime"
import { EntityId, EntityType } from "@effect/cluster"
import type { ProjectorDefinition } from "./ProjectorDefinition.js"
import { CheckpointStore } from "./CheckpointStore.js"
import { DeadLetter } from "./DeadLetter.js"
import { KafkaConsumer, type KafkaMessage } from "../runtime/KafkaConsumer.js"
import { EventEnvelope } from "../contracts/EventEnvelope.js"
import { DLQEnvelope } from "../contracts/DLQEnvelope.js"
import { empty as emptyMetadata } from "../contracts/Metadata.js"

/**
 * @since 1.0.0
 * @category models
 */
export interface SubscriptionConfig {
  readonly topics: ReadonlyArray<string>
  readonly groupId: string
  readonly fromBeginning?: boolean
  readonly retryPolicy?: Schedule.Schedule<unknown, unknown>
}

/**
 * Creates a Kafka-based projection subscription.
 *
 * For native Effect projections, use EventJournal.changes() + EventLog.group().
 *
 * @since 1.0.0
 * @category constructors
 */
export const make = <State>(
  projector: ProjectorDefinition<State>,
  config: SubscriptionConfig
): Effect.Effect<
  void,
  never,
  KafkaConsumer | CheckpointStore | DeadLetter
> =>
  Effect.gen(function*() {
    const consumer = yield* KafkaConsumer
    const checkpointStore = yield* CheckpointStore
    const deadLetter = yield* DeadLetter

    const decodeEnvelope = Schema.decodeUnknown(EventEnvelope)

    const messageStream = consumer.subscribe({
      topics: [...config.topics],
      groupId: config.groupId,
      fromBeginning: config.fromBeginning
    })

    let state = projector.initialState

    const processMessage = (message: KafkaMessage): Effect.Effect<void> =>
      Effect.gen(function*() {
        const parsed = message.value != null ? JSON.parse(message.value) : null
        if (parsed == null) return

        const envelope = yield* decodeEnvelope(parsed).pipe(Effect.orDie)
        state = yield* projector.handle(state, envelope)
        yield* checkpointStore.save(
          projector.name,
          message.partition,
          message.offset
        )
        yield* consumer.commit(
          message.topic,
          message.partition,
          message.offset
        )
      })

    const processWithRetry = (message: KafkaMessage): Effect.Effect<void> => {
      const retryPolicy = config.retryPolicy ?? Schedule.recurs(3)
      return processMessage(message).pipe(
        Effect.retry(retryPolicy),
        Effect.catchAll((error) =>
          Effect.gen(function*() {
            const now = yield* DateTime.now
            yield* deadLetter.send(
              new DLQEnvelope({
                originalEnvelope: new EventEnvelope({
                  eventId: "unknown",
                  streamId: "unknown",
                  aggregateId: EntityId.make("unknown"),
                  aggregateType: Schema.decodeSync(EntityType.EntityType)("unknown"),
                  revision: 0,
                  occurredAt: now,
                  metadata: emptyMetadata,
                  payload: message.value
                }),
                error: String(error),
                failedAt: now,
                retryCount: 0,
                projectorName: projector.name
              })
            )
          })
        )
      )
    }

    yield* messageStream.pipe(
      Stream.runForEach(processWithRetry),
      Effect.catchAll(() => Effect.void)
    )
  })
