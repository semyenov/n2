/**
 * @since 1.0.0
 * @module InMemoryKafkaConsumer
 *
 * Queue-backed in-memory Kafka consumer for testing.
 */
import * as Effect from "effect/Effect"
import * as Queue from "effect/Queue"
import * as Stream from "effect/Stream"
import * as Layer from "effect/Layer"
import {
  KafkaConsumer,
  type KafkaConsumerService,
  type KafkaMessage,
  type KafkaConsumerError,
  type SubscribeConfig
} from "../runtime/KafkaConsumer.js"

/**
 * @since 1.0.0
 * @category tags
 */
export class InMemoryKafkaConsumerAccess extends Effect.Tag(
  "n2/testing/InMemoryKafkaConsumerAccess"
)<InMemoryKafkaConsumerAccess, {
  readonly push: (message: KafkaMessage) => Effect.Effect<void>
  readonly pushBatch: (messages: ReadonlyArray<KafkaMessage>) => Effect.Effect<void>
}>() {}

/**
 * @since 1.0.0
 * @category constructors
 */
export const make: Effect.Effect<
  KafkaConsumerService & {
    readonly push: (message: KafkaMessage) => Effect.Effect<void>
    readonly pushBatch: (messages: ReadonlyArray<KafkaMessage>) => Effect.Effect<void>
  }
> = Effect.gen(function*() {
  const queue = yield* Queue.unbounded<KafkaMessage>()

  const subscribe = (
    _config: SubscribeConfig
  ): Stream.Stream<KafkaMessage, KafkaConsumerError> =>
    Stream.fromQueue(queue)

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

  const push = (message: KafkaMessage): Effect.Effect<void> =>
    Queue.offer(queue, message).pipe(Effect.asVoid)

  const pushBatch = (
    messages: ReadonlyArray<KafkaMessage>
  ): Effect.Effect<void> =>
    Queue.offerAll(queue, messages).pipe(Effect.asVoid)

  return { subscribe, commit, seek, push, pushBatch }
})

/**
 * @since 1.0.0
 * @category layers
 */
export const layer: Layer.Layer<KafkaConsumer> = Layer.effect(
  KafkaConsumer,
  make.pipe(
    Effect.map((svc) => ({
      subscribe: svc.subscribe,
      commit: svc.commit,
      seek: svc.seek
    }))
  )
)
