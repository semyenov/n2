/**
 * @since 1.0.0
 * @module OutboxPublisher
 *
 * Outbox pattern: polls new events from EventLog and publishes to Kafka.
 * Guarantees at-least-once delivery. Run as a Singleton in the cluster
 * for exactly-once leader election.
 *
 * Usage:
 * ```ts
 * import { Singleton } from "@effect/cluster"
 *
 * const OutboxSingleton = Singleton.make(
 *   "outbox-publisher",
 *   OutboxPublisher.run({ aggregateType: "Order", pollInterval: "1 second" })
 * )
 * ```
 */
import * as Effect from "effect/Effect"
import * as Stream from "effect/Stream"
import * as Duration from "effect/Duration"
import * as Schedule from "effect/Schedule"
import * as Ref from "effect/Ref"
import { EventLog } from "./EventLog.js"
import { KafkaPublisher, KafkaProducerRecord } from "./KafkaPublisher.js"
import { makeAggregateTopicName } from "../contracts/TopicKey.js"
import { CheckpointStore } from "../projection/CheckpointStore.js"

/**
 * @since 1.0.0
 * @category models
 */
export interface OutboxConfig {
  readonly aggregateType: string
  readonly pollIntervalSeconds?: number
  readonly batchSize?: number
}

/**
 * Runs the outbox publisher loop.
 *
 * 1. Reads events from EventLog.readAll starting from last checkpoint
 * 2. Publishes each to Kafka
 * 3. Saves checkpoint
 * 4. Repeats on schedule
 *
 * @since 1.0.0
 * @category constructors
 */
export const run = (config: OutboxConfig): Effect.Effect<
  void,
  never,
  EventLog | KafkaPublisher | CheckpointStore
> =>
  Effect.gen(function*() {
    const eventLog = yield* EventLog
    const publisher = yield* KafkaPublisher
    const checkpointStore = yield* CheckpointStore
    const topicName = makeAggregateTopicName(config.aggregateType)
    const batchSize = config.batchSize ?? 100

    const lastPosition = yield* Ref.make<bigint>(BigInt(0))

    // Load initial checkpoint
    const savedCheckpoint = yield* checkpointStore.load("outbox", 0)
    if (savedCheckpoint._tag === "Some") {
      yield* Ref.set(lastPosition, BigInt(savedCheckpoint.value))
    }

    const pollOnce: Effect.Effect<void> = Effect.gen(function*() {
      const pos = yield* Ref.get(lastPosition)
      const events = yield* eventLog
        .readAll(pos)
        .pipe(Stream.take(batchSize), Stream.runCollect, Effect.map((c) => [...c]))

      if (events.length === 0) return

      const records = events.map(
        (env) =>
          new KafkaProducerRecord({
            topic: topicName,
            key: env.aggregateId,
            value: JSON.stringify(env)
          })
      )

      yield* publisher.publishBatch(records)

      const newPos = pos + BigInt(events.length)
      yield* Ref.set(lastPosition, newPos)
      yield* checkpointStore.save("outbox", 0, String(newPos))
    })

    yield* pollOnce.pipe(
      Effect.repeat(Schedule.spaced(Duration.seconds(config.pollIntervalSeconds ?? 1))),
      Effect.catchAll(() => Effect.void)
    )
  })
