import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import { computeRetryDelaySeconds, makePublishWorkflow } from "@semyenov/n2/helpers"

const Headers = Schema.Record({ key: Schema.String, value: Schema.Unknown })
const TOPIC = "request-provider.events"

export class RequestProviderEventMessage extends Schema.Class<RequestProviderEventMessage>("RequestProviderEventMessage")({
  id: Schema.String,
  topic: Schema.String,
  partitionKey: Schema.String,
  eventType: Schema.String,
  requestId: Schema.UUID,
  revision: Schema.Number.pipe(Schema.int()),
  occurredAt: Schema.String,
  payload: Schema.Unknown,
  headers: Headers
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
  new RequestProviderEventMessage({
    id: `${options.requestId}:${options.revision}:${options.eventType}`,
    topic: TOPIC,
    partitionKey: options.requestId,
    eventType: options.eventType,
    requestId: options.requestId,
    revision: options.revision,
    occurredAt: options.occurredAt,
    payload: options.payload,
    headers: {
      eventType: options.eventType,
      requestId: options.requestId,
      revision: options.revision,
      ...(options.headers ?? {})
    }
  })

const publishWorkflow = makePublishWorkflow({
  name: "RequestEventPublish",
  messageSchema: RequestProviderEventMessage,
  publisherTag: RequestProviderEventPublisher,
  idOf: (message) => message.id
})

export const RequestEventPublishWorkflow = publishWorkflow.workflow
export const startRequestEventPublish = publishWorkflow.start
export const RequestProviderEventPublishHandlers = publishWorkflow.handlers

export const RequestProviderEventPublisherLive = Layer.succeed(
  RequestProviderEventPublisher,
  {
    publish: (message) =>
      Effect.logInfo("[request-provider] event published").pipe(
        Effect.annotateLogs({
          eventMessageId: message.id,
          topic: message.topic,
          partitionKey: message.partitionKey,
          payloadSize: JSON.stringify(message.payload).length
        })
      )
  }
)
