import * as Context from "effect/Context"
import { makeStandardOutboxWiring, type OutboxService } from "@semyenov/n2/helpers"
import {
  ProfileProviderEventMessage,
  startProfileEventPublish
} from "./workflows.js"

export class ProfileProviderOutbox extends Context.Tag("ProfileProviderOutbox")<
  ProfileProviderOutbox,
  OutboxService<ProfileProviderEventMessage>
>() { }

const outbox = makeStandardOutboxWiring({
  tag: ProfileProviderOutbox,
  table: "profile_provider_event_outbox",
  schema: ProfileProviderEventMessage,
  publish: (message) => startProfileEventPublish(message),
  metricsPrefix: "profile_provider.outbox"
})

export const ProfileProviderOutboxPgLive = outbox.live
export const drainProfileProviderOutboxOnce = outbox.drainOnce
export const ProfileProviderOutboxWorkerLive = outbox.workerLive
