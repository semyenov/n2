/**
 * @since 1.0.0
 * @module KafkaPublisher
 *
 * Kafka producer abstraction. Framework core does not depend on a concrete
 * Kafka client -- implementations live in adapters/.
 */
import * as Context from "effect/Context"
import type * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"

/**
 * @since 1.0.0
 * @category schemas
 */
export class KafkaProducerRecord extends Schema.Class<KafkaProducerRecord>("n2/KafkaProducerRecord")({
  topic: Schema.String,
  key: Schema.String,
  value: Schema.String,
  headers: Schema.optional(Schema.Record({ key: Schema.String, value: Schema.String }))
}) {}

/**
 * @since 1.0.0
 * @category models
 */
export interface KafkaPublisherService {
  readonly publish: (
    record: KafkaProducerRecord
  ) => Effect.Effect<void>

  readonly publishBatch: (
    records: ReadonlyArray<KafkaProducerRecord>
  ) => Effect.Effect<void>
}

/**
 * @since 1.0.0
 * @category tags
 */
export class KafkaPublisher extends Context.Tag("n2/KafkaPublisher")<
  KafkaPublisher,
  KafkaPublisherService
>() {}
