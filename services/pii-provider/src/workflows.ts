import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import {
  computeRetryDelaySeconds,
  makeConsoleEventPublisherLayer,
  makeEventMessageFactory,
  makeEventMessageFields,
  makePublishWorkflow
} from "@semyenov/n2/helpers"

const TOPIC = "pii-provider.events"

export class PIIProviderEventMessage extends Schema.Class<PIIProviderEventMessage>("PIIProviderEventMessage")({
  ...makeEventMessageFields("recordId")
}) {}

export class PIIProviderEventPublisher extends Context.Tag("PIIProviderEventPublisher")<
  PIIProviderEventPublisher,
  {
    readonly publish: (message: PIIProviderEventMessage) => Effect.Effect<void, unknown>
  }
>() {}

export { computeRetryDelaySeconds }

export const makePIIProviderEventMessage = makeEventMessageFactory({
  topic: TOPIC,
  entityIdKey: "recordId",
  schema: PIIProviderEventMessage
})

const publishWorkflow = makePublishWorkflow({
  name: "PIIEventPublish",
  messageSchema: PIIProviderEventMessage,
  publisherTag: PIIProviderEventPublisher,
  idOf: (message) => message.id
})

export const PIIEventPublishWorkflow = publishWorkflow.workflow
export const startPIIEventPublish = publishWorkflow.start
export const PIIProviderEventPublishHandlers = publishWorkflow.handlers

export const PIIProviderEventPublisherLive = makeConsoleEventPublisherLayer(
  PIIProviderEventPublisher,
  "[pii-provider] event published"
)
