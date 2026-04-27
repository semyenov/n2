import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import { computeRetryDelaySeconds, makePublishWorkflow } from "@semyenov/n2/helpers"

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

const publishWorkflow = makePublishWorkflow({
  name: "ProfileEventPublish",
  messageSchema: ProfileProviderEventMessage,
  publisherTag: ProfileProviderEventPublisher,
  idOf: (m) => m.id
})

export const ProfileEventPublishWorkflow = publishWorkflow.workflow
export const startProfileEventPublish = publishWorkflow.start
export const ProfileProviderEventPublishHandlers = publishWorkflow.handlers

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
