import * as Cause from "effect/Cause"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import { Activity, DurableClock, Workflow, WorkflowEngine } from "@effect/workflow"
import { computeRetryDelaySeconds } from "../../src/framework/helpers/Outbox.js"

const Headers = Schema.Record({ key: Schema.String, value: Schema.Unknown })
const TOPIC = "profile-provider.events"

export class ProfileProviderEventMessage extends Schema.Class<ProfileProviderEventMessage>("ProfileProviderEventMessage")({
  id: Schema.String,
  topic: Schema.String,
  partitionKey: Schema.String,
  eventType: Schema.String,
  profileId: Schema.UUID,
  revision: Schema.Number.pipe(Schema.int()),
  occurredAt: Schema.String,
  payload: Schema.Unknown,
  headers: Headers
}) {}

export class ProfileProviderEventPublishError extends Schema.TaggedError<ProfileProviderEventPublishError>()(
  "ProfileProviderEventPublishError",
  { message: Schema.String }
) {}

export class ProfileProviderEventPublisher extends Context.Tag("ProfileProviderEventPublisher")<
  ProfileProviderEventPublisher,
  {
    readonly publish: (message: ProfileProviderEventMessage) => Effect.Effect<void, unknown>
  }
>() {}

export { computeRetryDelaySeconds }

export const makeProfileProviderEventMessage = (options: {
  readonly profileId: string
  readonly revision: number
  readonly eventType: string
  readonly occurredAt: string
  readonly payload: unknown
  readonly headers?: Record<string, unknown>
}) =>
  new ProfileProviderEventMessage({
    id: `${options.profileId}:${options.revision}:${options.eventType}`,
    topic: TOPIC,
    partitionKey: options.profileId,
    eventType: options.eventType,
    profileId: options.profileId,
    revision: options.revision,
    occurredAt: options.occurredAt,
    payload: options.payload,
    headers: {
      eventType: options.eventType,
      profileId: options.profileId,
      revision: options.revision,
      ...(options.headers ?? {})
    }
  })

const publishProfileEvent = (
  message: ProfileProviderEventMessage,
  attempt: number
) =>
  Activity.make({
    name: `PublishProfileEvent/${attempt}`,
    error: ProfileProviderEventPublishError,
    execute: Effect.gen(function* () {
      const publisher = yield* ProfileProviderEventPublisher
      return yield* publisher.publish(message).pipe(
        Effect.catchAllCause((cause) =>
          Effect.fail(new ProfileProviderEventPublishError({ message: Cause.pretty(cause) }))
        )
      )
    })
  })

const retryPublish = (
  message: ProfileProviderEventMessage,
  executionId: string,
  attempt: number
): Effect.Effect<void, never, any> =>
  publishProfileEvent(message, attempt).pipe(
    Effect.catchTag("ProfileProviderEventPublishError", (error) => {
      const delaySeconds = computeRetryDelaySeconds(attempt)
      return Effect.logWarning("[profile-provider] publish workflow retry scheduled").pipe(
        Effect.annotateLogs({
          eventMessageId: message.id,
          executionId,
          attempt,
          delaySeconds,
          error: error.message
        }),
        Effect.zipRight(DurableClock.sleep({
          name: `publish-retry-${attempt}`,
          duration: `${delaySeconds} seconds`,
          inMemoryThreshold: "0 millis"
        })),
        Effect.zipRight(retryPublish(message, executionId, attempt + 1))
      )
    })
  )

export const ProfileEventPublishWorkflow = Workflow.make({
  name: "ProfileEventPublish",
  payload: ProfileProviderEventMessage,
  success: Schema.Void,
  idempotencyKey: (payload) => payload.id
})

export const startProfileEventPublish = (message: ProfileProviderEventMessage) =>
  Effect.gen(function* () {
    const engine = yield* Effect.orDie(Effect.serviceOptional(WorkflowEngine.WorkflowEngine))
    return yield* engine.execute(ProfileEventPublishWorkflow, {
      executionId: message.id,
      payload: message,
      discard: true
    })
  })

export const ProfileProviderEventPublishHandlers = ProfileEventPublishWorkflow.toLayer(
  (payload, executionId) =>
    retryPublish(payload, executionId, 1).pipe(
      Effect.zipRight(
        Effect.logInfo("[profile-provider] publish workflow completed").pipe(
          Effect.annotateLogs({
            eventMessageId: payload.id,
            executionId,
            topic: payload.topic,
            partitionKey: payload.partitionKey
          })
        )
      )
    )
)

export const ProfileProviderEventPublisherLive = Layer.succeed(
  ProfileProviderEventPublisher,
  {
    publish: (message) =>
      Effect.logInfo("[profile-provider] event published").pipe(
        Effect.annotateLogs({
          eventMessageId: message.id,
          topic: message.topic,
          partitionKey: message.partitionKey,
          payloadSize: JSON.stringify(message.payload).length
        })
      )
  }
)
