import * as Context from "effect/Context"
import { makeOutboxJsonService, type OutboxService } from "n2/helpers"
import {
  ProfileProviderEventMessage,
  startProfileEventPublish
} from "./workflows.js"

const outbox = makeOutboxJsonService({
  table: "profile_provider_event_outbox",
  schema: ProfileProviderEventMessage,
  idOf: (m: ProfileProviderEventMessage) => m.id,
  metrics: { prefix: "profile_provider.outbox" }
})

export type OutboxEntry = {
  readonly id: string
  readonly message: ProfileProviderEventMessage
  readonly retryCount: number
}

export class ProfileProviderOutbox extends Context.Tag("ProfileProviderOutbox")<
  ProfileProviderOutbox,
  OutboxService<ProfileProviderEventMessage>
>() { }

export const ProfileProviderOutboxPgLive = outbox.makeLive(ProfileProviderOutbox)

export const drainProfileProviderOutboxOnce = outbox.makeDrainOnce({
  outbox: ProfileProviderOutbox,
  publish: (message) => startProfileEventPublish(message),
  batchSize: 50,
  maxRetries: 5
})

export const ProfileProviderOutboxWorkerLive = outbox.makeWorkerLive({
  outbox: ProfileProviderOutbox,
  publish: (message) => startProfileEventPublish(message),
  batchSize: 50,
  idleDelay: "1 second",
  maxRetries: 5
})
