import * as Context from "effect/Context"
import { makeStandardOutboxWiring, type OutboxService } from "@semyenov/n2/helpers"
import {
  RequestProviderEventMessage,
  startRequestEventPublish
} from "./workflows.js"

export class RequestProviderOutbox extends Context.Tag("RequestProviderOutbox")<
  RequestProviderOutbox,
  OutboxService<RequestProviderEventMessage>
>() {}

const outbox = makeStandardOutboxWiring({
  tag: RequestProviderOutbox,
  table: "request_provider_event_outbox",
  schema: RequestProviderEventMessage,
  publish: (message) => startRequestEventPublish(message),
  metricsPrefix: "request_provider.outbox"
})

export const RequestProviderOutboxPgLive = outbox.live
export const drainRequestProviderOutboxOnce = outbox.drainOnce
export const RequestProviderOutboxWorkerLive = outbox.workerLive
