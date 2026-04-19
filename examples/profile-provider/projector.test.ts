import { test, expect } from "bun:test"
import * as DateTime from "effect/DateTime"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as ExpEventJournal from "@effect/experimental/EventJournal"
import { Identity } from "@effect/experimental/EventLog"
import * as EventLogApi from "@effect/experimental/EventLog"
import { ProfileDocument, type ProfileEvent } from "./contracts.js"
import { ProfileProviderEventLogSchema } from "./events.js"
import { ProfileProviderProjectionLayer } from "./projector.js"
import { ProfileProviderProjectionStore, type ProjectionHandlers } from "./projection-store.js"
import { ProfileProviderOutbox } from "./outbox.js"
import type { ProfileProviderEventMessage } from "./workflows.js"

const decodeProfile = Schema.decodeUnknownSync(ProfileDocument)

const makeProfile = (profileId: string) =>
  decodeProfile({
    uuid: profileId,
    created_at: "2026-01-01T00:00:00.000Z",
    user_data: {
      personal_info: {
        first_name: "Ada",
        last_name: "Lovelace",
        relevant_position: "Platform Engineer"
      },
      salary_expectations: {
        currency: "USD",
        amount_from: 1000
      },
      skills: [{ name: "TypeScript", level: "advanced" }],
      education: [{ degree: "Bachelor", field_of_study: "Computer Science", institution: "Analytical Engine Institute" }]
    },
    user_meta_data: { version: 1 }
  })

test("projector writes through the projection store and outbox without requiring workflow services", async () => {
  const profileId = "00000000-0000-4000-8000-000000000111"
  const projected: Array<{ tag: ProfileEvent["_tag"] }> = []
  const outboxed: Array<{ messageId: string }> = []
  const journalLayer = ExpEventJournal.layerMemory
  const identityLayer = Layer.succeed(Identity, Identity.makeRandom())

  const noOpHandlers: ProjectionHandlers = {
    onProfileCreated: (event) => Effect.sync(() => { projected.push({ tag: event._tag }) }),
    onMergedDataProfile: (event) => Effect.sync(() => { projected.push({ tag: event._tag }) }),
    onSnapshotCreatedProfile: (event) => Effect.sync(() => { projected.push({ tag: event._tag }) }),
    onMetaDataCreated: (event) => Effect.sync(() => { projected.push({ tag: event._tag }) }),
    onPersonalDataExtracted: (event) => Effect.sync(() => { projected.push({ tag: event._tag }) }),
    onProfileBranchForked: (event) => Effect.sync(() => { projected.push({ tag: event._tag }) }),
    onSnapshotPublishedProfile: (event) => Effect.sync(() => { projected.push({ tag: event._tag }) })
  }

  const noOpOutbox = {
    enqueue: (message: ProfileProviderEventMessage) =>
      Effect.sync(() => { outboxed.push({ messageId: message.id }) }),
    claimPending: () => Effect.succeed([] as const),
    markDispatched: () => Effect.void,
    markFailed: () => Effect.void
  }

  const layer = Layer.mergeAll(
    journalLayer,
    identityLayer,
    Layer.succeed(ProfileProviderProjectionStore, noOpHandlers),
    Layer.succeed(ProfileProviderOutbox, noOpOutbox),
    EventLogApi.layer(ProfileProviderEventLogSchema).pipe(
      Layer.provide(ProfileProviderProjectionLayer),
      Layer.provide(Layer.merge(journalLayer, identityLayer))
    )
  )

  await Effect.runPromise(
    Effect.gen(function* () {
      const publish = yield* EventLogApi.makeClient(ProfileProviderEventLogSchema)
      yield* publish("ProfileCreated", {
        profileId,
        ownerAgentId: "agent-1",
        branchId: "main",
        schemaVersion: "1.0.0",
        maskedProfileJson: makeProfile(profileId),
        occurredAt: DateTime.unsafeMake("2026-01-01T00:00:00.000Z"),
        actorId: "agent-1",
        summary: "init",
        revision: 1
      })
    }).pipe(
      Effect.scoped,
      Effect.provide(layer)
    ) as Effect.Effect<void, never, never>
  )

  expect(projected).toEqual([{ tag: "ProfileCreated" }])
  expect(outboxed).toEqual([{ messageId: `${profileId}:1:ProfileCreated` }])
})
