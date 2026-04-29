/**
 * @since 1.0.0
 *
 * Generic durable publish workflow with exponential backoff retry.
 *
 * Generates the complete publish-with-retry pipeline from a message schema
 * and publisher service tag: workflow definition, start helper, retry logic,
 * and handler layer.
 *
 * @example
 * ```ts
 * const publishWorkflow = makePublishWorkflow({
 *   name: "MyEventPublish",
 *   messageSchema: MyEventMessage,
 *   publisherTag: MyEventPublisher,
 *   idOf: (m) => m.id
 * })
 *
 * export const startPublish = publishWorkflow.start
 * export const publishHandlers = publishWorkflow.handlers
 * ```
 */
import * as Cause from "effect/Cause"
import type * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import { Activity, DurableClock, Workflow } from "@effect/workflow"
import { computeRetryDelaySeconds } from "./Outbox.js"

export class EventPublishError extends Schema.TaggedError<EventPublishError>()(
  "EventPublishError",
  { message: Schema.String }
) { }

interface PublisherService<Message> {
  readonly publish: (message: Message) => Effect.Effect<void, unknown>
}

/**
 * Creates a complete durable publish workflow with exponential backoff retry.
 *
 * @param config.name - Workflow name (used for logging and workflow registration)
 * @param config.messageSchema - Schema for the message type (used by Workflow.make)
 * @param config.publisherTag - Context.Tag for the publisher service
 * @param config.idOf - Extract unique ID from message (defaults to `m.id`)
 */
export const makePublishWorkflow = <Message extends { readonly id: string }, PublisherI = unknown>(config: {
  readonly name: string
  readonly messageSchema: Workflow.AnyStructSchema & Schema.Schema<Message>
  readonly publisherTag: Context.Tag<PublisherI, PublisherService<Message>>
  readonly idOf?: (message: Message) => string
  /** Maximum publish attempts before giving up. Default: unlimited. */
  readonly maxAttempts?: number
}) => {
  const idOf = config.idOf ?? ((m: Message) => m.id)

  const publishActivity = (message: Message, attempt: number) =>
    Activity.make({
      name: `${config.name}/${attempt}`,
      error: EventPublishError,
      execute: Effect.gen(function* () {
        const publisher = yield* config.publisherTag
        return yield* publisher.publish(message).pipe(
          Effect.catchAllCause((cause) =>
            Effect.fail(new EventPublishError({ message: Cause.pretty(cause) }))
          )
        )
      })
    })

  const retryPublish = (
    message: Message,
    executionId: string,
    attempt: number
  ): Effect.Effect<void, EventPublishError, never> =>
    publishActivity(message, attempt).pipe(
      Effect.catchTag("EventPublishError", (error) => {
        if (config.maxAttempts !== undefined && attempt >= config.maxAttempts) {
          return Effect.logError(`[${config.name}] permanently failed after ${attempt} attempts`).pipe(
            Effect.annotateLogs({
              messageId: idOf(message),
              executionId,
              attempt,
              error: error.message
            }),
            Effect.zipRight(Effect.fail(error))
          )
        }
        const delaySeconds = computeRetryDelaySeconds(attempt)
        return Effect.logWarning(`[${config.name}] retry scheduled`).pipe(
          Effect.annotateLogs({
            messageId: idOf(message),
            executionId,
            attempt,
            delaySeconds,
            error: error.message
          }),
          Effect.zipRight(DurableClock.sleep({
            name: `retry-${attempt}`,
            duration: `${delaySeconds} seconds`,
            inMemoryThreshold: "0 millis"
          })),
          Effect.zipRight(retryPublish(message, executionId, attempt + 1))
        )
      })
    ) as Effect.Effect<void, EventPublishError, never>

  const workflow = Workflow.make({
    name: config.name,
    payload: config.messageSchema,
    success: Schema.Void,
    idempotencyKey: idOf
  })

  const start = (message: Message) =>
    workflow.execute(message, { discard: true })

  const handlers = workflow.toLayer(
    (payload, executionId) =>
      retryPublish(payload, executionId, 1).pipe(
        Effect.zipRight(
          Effect.logInfo(`[${config.name}] completed`).pipe(
            Effect.annotateLogs({
              messageId: idOf(payload),
              executionId
            })
          )
        ),
        Effect.catchTag("EventPublishError", () => Effect.void)
      )
  )

  return { workflow, start, handlers } as const
}
