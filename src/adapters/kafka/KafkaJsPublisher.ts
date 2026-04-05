/**
 * @since 1.0.0
 * @module KafkaJsPublisher
 *
 * KafkaPublisher implementation using kafkajs.
 *
 * Requires the `kafkajs` package to be installed separately:
 * `bun add kafkajs`
 */
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Context from "effect/Context"
import {
  KafkaPublisher,
  type KafkaPublisherService,
  type KafkaProducerRecord
} from "../../framework/runtime/KafkaPublisher.js"

/**
 * @since 1.0.0
 * @category models
 */
export interface KafkaJsConfig {
  readonly brokers: ReadonlyArray<string>
  readonly clientId?: string
}

/**
 * @since 1.0.0
 * @category tags
 */
export class KafkaJsPublisherConfig extends Context.Tag(
  "n2/adapters/KafkaJsPublisherConfig"
)<KafkaJsPublisherConfig, KafkaJsConfig>() {}

/**
 * Creates a KafkaPublisher layer backed by kafkajs.
 *
 * @since 1.0.0
 * @category layers
 */
export const layer: Layer.Layer<KafkaPublisher, never, KafkaJsPublisherConfig> =
  Layer.effect(
    KafkaPublisher,
    Effect.gen(function*() {
      const config = yield* KafkaJsPublisherConfig

      const { Kafka } = yield* Effect.tryPromise({
        try: () => import("kafkajs"),
        catch: () => new Error("kafkajs not installed. Run: bun add kafkajs")
      }).pipe(Effect.orDie)

      const kafka = new Kafka({
        clientId: config.clientId ?? "n2-publisher",
        brokers: [...config.brokers]
      })
      const producer = kafka.producer()
      yield* Effect.tryPromise({
        try: () => producer.connect(),
        catch: (e) => new Error(`Kafka connect failed: ${e}`)
      }).pipe(Effect.orDie)

      const publish = (record: KafkaProducerRecord): Effect.Effect<void> =>
        Effect.tryPromise({
          try: () =>
            producer.send({
              topic: record.topic,
              messages: [{ key: record.key, value: record.value, headers: record.headers }]
            }),
          catch: (e) => new Error(`Kafka publish failed: ${e}`)
        }).pipe(Effect.orDie, Effect.asVoid)

      const publishBatch = (
        records: ReadonlyArray<KafkaProducerRecord>
      ): Effect.Effect<void> =>
        Effect.tryPromise({
          try: () =>
            producer.sendBatch({
              topicMessages: records.map((r) => ({
                topic: r.topic,
                messages: [{ key: r.key, value: r.value, headers: r.headers }]
              }))
            }),
          catch: (e) => new Error(`Kafka batch publish failed: ${e}`)
        }).pipe(Effect.orDie, Effect.asVoid)

      return { publish, publishBatch } satisfies KafkaPublisherService
    })
  )
