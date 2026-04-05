/**
 * @since 1.0.0
 * @module KafkaJsConsumer
 *
 * KafkaConsumer implementation using kafkajs.
 *
 * Requires the `kafkajs` package to be installed separately:
 * `bun add kafkajs`
 */
import * as Effect from "effect/Effect"
import * as Stream from "effect/Stream"
import * as Layer from "effect/Layer"
import * as Context from "effect/Context"
import {
  KafkaConsumer,
  KafkaMessage,
  KafkaConsumerError,
  type KafkaConsumerService,
  type SubscribeConfig
} from "../../framework/runtime/KafkaConsumer.js"

/**
 * @since 1.0.0
 * @category models
 */
export interface KafkaJsConsumerConfig {
  readonly brokers: ReadonlyArray<string>
  readonly clientId?: string
}

/**
 * @since 1.0.0
 * @category tags
 */
export class KafkaJsConsumerConfigTag extends Context.Tag(
  "n2/adapters/KafkaJsConsumerConfig"
)<KafkaJsConsumerConfigTag, KafkaJsConsumerConfig>() {}

/**
 * Creates a KafkaConsumer layer backed by kafkajs.
 *
 * @since 1.0.0
 * @category layers
 */
export const layer: Layer.Layer<KafkaConsumer, never, KafkaJsConsumerConfigTag> =
  Layer.effect(
    KafkaConsumer,
    Effect.gen(function*() {
      const config = yield* KafkaJsConsumerConfigTag

      const { Kafka } = yield* Effect.tryPromise({
        try: () => import("kafkajs"),
        catch: () => new Error("kafkajs not installed. Run: bun add kafkajs")
      }).pipe(Effect.orDie)

      const kafka = new Kafka({
        clientId: config.clientId ?? "n2-consumer",
        brokers: [...config.brokers]
      })

      const subscribe = (
        subConfig: SubscribeConfig
      ): Stream.Stream<KafkaMessage, KafkaConsumerError> =>
        Stream.async<KafkaMessage, KafkaConsumerError>((emit) => {
          const consumer = kafka.consumer({ groupId: subConfig.groupId })

          const run = async () => {
            await consumer.connect()
            await consumer.subscribe({
              topics: [...subConfig.topics],
              fromBeginning: subConfig.fromBeginning ?? false
            })

            await consumer.run({
              eachMessage: async ({ topic, partition, message }: { topic: string; partition: number; message: Record<string, unknown> }) => {
                emit.single(
                  new KafkaMessage({
                    topic,
                    partition,
                    offset: String(message["offset"] ?? ""),
                    key: message["key"] != null ? String(message["key"]) : null,
                    value: message["value"] != null ? String(message["value"]) : null,
                    headers: Object.fromEntries(
                      Object.entries(message["headers"] ?? {}).map(
                        ([k, v]: [string, unknown]) => [k, String(v ?? "")]
                      )
                    ),
                    timestamp: String(message["timestamp"] ?? "")
                  })
                )
              }
            })
          }

          run().catch((err) =>
            emit.fail(new KafkaConsumerError({ reason: String(err) }))
          )
        })

      const commit = (
        _topic: string,
        _partition: number,
        _offset: string
      ): Effect.Effect<void> => Effect.void

      const seek = (
        _topic: string,
        _partition: number,
        _offset: string
      ): Effect.Effect<void> => Effect.void

      return { subscribe, commit, seek } satisfies KafkaConsumerService
    })
  )
