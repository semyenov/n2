import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import {
  computeRetryDelaySeconds,
  makeConsoleEventPublisherLayer,
  makeEventMessage,
  makeEventMessageFields,
  makePublishWorkflow
} from "@semyenov/n2/helpers"

const TOPIC = "profile-provider.events"

export class ProfileProviderEventMessage extends Schema.Class<ProfileProviderEventMessage>("ProfileProviderEventMessage")({
  ...makeEventMessageFields("profileId")
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
  new ProfileProviderEventMessage(makeEventMessage({
    topic: TOPIC,
    entityIdKey: "profileId",
    entityId: options.profileId,
    revision: options.revision,
    eventType: options.eventType,
    occurredAt: options.occurredAt,
    payload: options.payload,
    headers: options.headers
  }))

const publishWorkflow = makePublishWorkflow({
  name: "ProfileEventPublish",
  messageSchema: ProfileProviderEventMessage,
  publisherTag: ProfileProviderEventPublisher,
  idOf: (m) => m.id
})

export const ProfileEventPublishWorkflow = publishWorkflow.workflow
export const startProfileEventPublish = publishWorkflow.start
export const ProfileProviderEventPublishHandlers = publishWorkflow.handlers

export const ProfileProviderEventPublisherLive = makeConsoleEventPublisherLayer(
  ProfileProviderEventPublisher,
  "[profile-provider] event published"
)
