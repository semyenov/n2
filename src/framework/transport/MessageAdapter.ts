/**
 * @since 1.0.0
 * @module MessageAdapter
 *
 * Receives commands from Kafka as an async command bus pattern.
 */
import * as Effect from "effect/Effect"
import * as Stream from "effect/Stream"
import * as Schema from "effect/Schema"
import { KafkaConsumer, type KafkaMessage } from "../runtime/KafkaConsumer.js"

/**
 * @since 1.0.0
 * @category models
 */
export interface MessageHandlerMap {
  readonly [tag: string]: (payload: unknown) => Effect.Effect<unknown>
}

/**
 * @since 1.0.0
 * @category models
 */
export interface MessageConsumerConfig {
  readonly topic: string
  readonly groupId: string
  readonly handlers: MessageHandlerMap
  readonly fromBeginning?: boolean
}

/**
 * @since 1.0.0
 * @category schemas
 */
export const CommandMessageSchema = Schema.Struct({
  _tag: Schema.String,
  payload: Schema.Unknown
})

/**
 * Creates a long-running Kafka message consumer that dispatches
 * incoming messages to the appropriate command handler.
 *
 * @since 1.0.0
 * @category constructors
 */
export const make = (
  config: MessageConsumerConfig
): Effect.Effect<void, never, KafkaConsumer> =>
  Effect.gen(function*() {
    const consumer = yield* KafkaConsumer
    const decode = Schema.decodeUnknownSync(CommandMessageSchema)

    const messageStream = consumer.subscribe({
      topics: [config.topic],
      groupId: config.groupId,
      fromBeginning: config.fromBeginning
    })

    return yield* messageStream.pipe(
      Stream.runForEach((message: KafkaMessage) =>
        Effect.gen(function*() {
          if (message.value == null) return
          const parsed = decode(JSON.parse(message.value))
          const handler = config.handlers[parsed._tag]
          if (handler != null) {
            yield* handler(parsed.payload)
          }
          yield* consumer.commit(
            message.topic,
            message.partition,
            message.offset
          )
        }).pipe(Effect.catchAll(() => Effect.void))
      ),
      Effect.catchAll(() => Effect.never)
    )
  })
