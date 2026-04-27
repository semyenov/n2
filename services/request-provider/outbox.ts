import * as Context from "effect/Context"
import { makeOutboxJsonService, type OutboxService } from "@semyenov/n2/helpers"
import {
  RequestProviderEventMessage,
  startRequestEventPublish
} from "./workflows.js"

const outbox = makeOutboxJsonService({
  table: "request_provider_event_outbox",
  schema: RequestProviderEventMessage,
  idOf: (message: RequestProviderEventMessage) => message.id,
  metrics: { prefix: "request_provider.outbox" }
})

export type OutboxEntry = {
  readonly id: string
  readonly message: RequestProviderEventMessage
  readonly retryCount: number
}

export class RequestProviderOutbox extends Context.Tag("RequestProviderOutbox")<
  RequestProviderOutbox,
  OutboxService<RequestProviderEventMessage>
>() {}

export const RequestProviderOutboxPgLive = outbox.makeLive(RequestProviderOutbox)

export const drainRequestProviderOutboxOnce = outbox.makeDrainOnce({
  outbox: RequestProviderOutbox,
  publish: (message) => startRequestEventPublish(message),
  batchSize: 50,
  maxRetries: 5
})

export const RequestProviderOutboxWorkerLive = outbox.makeWorkerLive({
  outbox: RequestProviderOutbox,
  publish: (message) => startRequestEventPublish(message),
  batchSize: 50,
  idleDelay: "1 second",
  maxRetries: 5
})
