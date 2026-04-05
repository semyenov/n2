/**
 * @since 1.0.0
 * @module InMemoryKafkaPublisher
 *
 * Ref-backed in-memory Kafka publisher for testing.
 * Captures all published messages for assertions.
 */
import * as Effect from "effect/Effect"
import * as Ref from "effect/Ref"
import * as Layer from "effect/Layer"
import {
  KafkaPublisher,
  type KafkaPublisherService,
  type KafkaProducerRecord
} from "../runtime/KafkaPublisher.js"

/**
 * @since 1.0.0
 * @category tags
 */
export class InMemoryKafkaPublisherAccess extends Effect.Tag(
  "n2/testing/InMemoryKafkaPublisherAccess"
)<InMemoryKafkaPublisherAccess, {
  readonly published: Effect.Effect<ReadonlyArray<KafkaProducerRecord>>
  readonly clear: Effect.Effect<void>
}>() {}

/**
 * @since 1.0.0
 * @category constructors
 */
export const make: Effect.Effect<KafkaPublisherService & {
  readonly published: Effect.Effect<ReadonlyArray<KafkaProducerRecord>>
  readonly clear: Effect.Effect<void>
}> = Effect.gen(function*() {
  const messages = yield* Ref.make<ReadonlyArray<KafkaProducerRecord>>([])

  const publish = (record: KafkaProducerRecord): Effect.Effect<void> =>
    Ref.update(messages, (msgs) => [...msgs, record])

  const publishBatch = (
    records: ReadonlyArray<KafkaProducerRecord>
  ): Effect.Effect<void> =>
    Ref.update(messages, (msgs) => [...msgs, ...records])

  const published: Effect.Effect<ReadonlyArray<KafkaProducerRecord>> =
    Ref.get(messages)

  const clear: Effect.Effect<void> = Ref.set(messages, [])

  return { publish, publishBatch, published, clear }
})

/**
 * @since 1.0.0
 * @category layers
 */
export const layer: Layer.Layer<KafkaPublisher | InMemoryKafkaPublisherAccess> =
  Layer.effect(
    KafkaPublisher,
    make.pipe(
      Effect.map((svc) => ({
        publish: svc.publish,
        publishBatch: svc.publishBatch
      }))
    )
  ).pipe(
    Layer.merge(
      Layer.effect(
        InMemoryKafkaPublisherAccess,
        make.pipe(
          Effect.map((svc) => ({
            published: svc.published,
            clear: svc.clear
          }))
        )
      )
    )
  )

/**
 * Simpler layer that only provides KafkaPublisher.
 *
 * @since 1.0.0
 * @category layers
 */
export const layerSimple: Layer.Layer<KafkaPublisher> = Layer.effect(
  KafkaPublisher,
  make.pipe(
    Effect.map((svc) => ({
      publish: svc.publish,
      publishBatch: svc.publishBatch
    }))
  )
)
