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

const TOPIC = "request-provider.events"

export class RequestProviderEventMessage extends Schema.Class<RequestProviderEventMessage>("RequestProviderEventMessage")({
  ...makeEventMessageFields("requestId")
}) {}

export class RequestProviderEventPublisher extends Context.Tag("RequestProviderEventPublisher")<
  RequestProviderEventPublisher,
  {
    readonly publish: (message: RequestProviderEventMessage) => Effect.Effect<void, unknown>
  }
>() {}

export { computeRetryDelaySeconds }

export const makeRequestProviderEventMessage = (options: {
  readonly requestId: string
  readonly revision: number
  readonly eventType: string
  readonly occurredAt: string
  readonly payload: unknown
  readonly headers?: Record<string, unknown>
}) =>
  new RequestProviderEventMessage(makeEventMessage({
    topic: TOPIC,
    entityIdKey: "requestId",
    entityId: options.requestId,
    revision: options.revision,
    eventType: options.eventType,
    occurredAt: options.occurredAt,
    payload: options.payload,
    headers: options.headers
  }))

const publishWorkflow = makePublishWorkflow({
  name: "RequestEventPublish",
  messageSchema: RequestProviderEventMessage,
  publisherTag: RequestProviderEventPublisher,
  idOf: (message) => message.id
})

export const RequestEventPublishWorkflow = publishWorkflow.workflow
export const startRequestEventPublish = publishWorkflow.start
export const RequestProviderEventPublishHandlers = publishWorkflow.handlers

export const RequestProviderEventPublisherLive = makeConsoleEventPublisherLayer(
  RequestProviderEventPublisher,
  "[request-provider] event published"
)
