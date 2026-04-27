import * as Context from "effect/Context"
import * as Schema from "effect/Schema"
import { makeOutboxService, type OutboxService } from "../../src/framework/helpers/Outbox.js"
import {
  ProfileProviderEventMessage,
  startProfileEventPublish
} from "./workflows.js"

const encodeMessage = Schema.encodeSync(ProfileProviderEventMessage)
const decodeMessage = Schema.decodeUnknownSync(ProfileProviderEventMessage)

const outbox = makeOutboxService({
  table: "profile_provider_event_outbox",
  idOf: (m: ProfileProviderEventMessage) => m.id,
  serialize: (m) => JSON.stringify(encodeMessage(m)),
  deserialize: (json) => decodeMessage(JSON.parse(json))
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
  batchSize: 50
})

export const ProfileProviderOutboxWorkerLive = outbox.makeWorkerLive({
  outbox: ProfileProviderOutbox,
  publish: (message) => startProfileEventPublish(message),
  batchSize: 50,
  idleDelay: "1 second"
})
