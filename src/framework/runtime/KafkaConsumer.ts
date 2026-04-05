/**
 * @since 1.0.0
 * @module KafkaConsumer
 *
 * Kafka consumer abstraction. Framework core does not depend on a concrete
 * Kafka client -- implementations live in adapters/.
 */
import * as Context from "effect/Context"
import type * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import type * as Stream from "effect/Stream"

/**
 * @since 1.0.0
 * @category schemas
 */
export class KafkaMessage extends Schema.Class<KafkaMessage>("n2/KafkaMessage")({
  topic: Schema.String,
  partition: Schema.Number,
  offset: Schema.String,
  key: Schema.NullOr(Schema.String),
  value: Schema.NullOr(Schema.String),
  headers: Schema.Record({ key: Schema.String, value: Schema.String }),
  timestamp: Schema.String
}) {}

/**
 * @since 1.0.0
 * @category errors
 */
export class KafkaConsumerError extends Schema.TaggedError<KafkaConsumerError>()(
  "KafkaConsumerError",
  {
    reason: Schema.String
  }
) {}

/**
 * @since 1.0.0
 * @category models
 */
export interface SubscribeConfig {
  readonly topics: ReadonlyArray<string>
  readonly groupId: string
  readonly fromBeginning?: boolean
}

/**
 * @since 1.0.0
 * @category models
 */
export interface KafkaConsumerService {
  readonly subscribe: (
    config: SubscribeConfig
  ) => Stream.Stream<KafkaMessage, KafkaConsumerError>

  readonly commit: (
    topic: string,
    partition: number,
    offset: string
  ) => Effect.Effect<void>

  readonly seek: (
    topic: string,
    partition: number,
    offset: string
  ) => Effect.Effect<void>
}

/**
 * @since 1.0.0
 * @category tags
 */
export class KafkaConsumer extends Context.Tag("n2/KafkaConsumer")<
  KafkaConsumer,
  KafkaConsumerService
>() {}
