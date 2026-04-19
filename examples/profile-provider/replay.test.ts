import { test, expect } from "bun:test"
import * as DateTime from "effect/DateTime"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as ExpEventJournal from "@effect/experimental/EventJournal"
import { EventLog as EL } from "@effect/experimental"
import * as EventLogApi from "@effect/experimental/EventLog"
import { Identity } from "@effect/experimental/EventLog"
import { ProfileDocument } from "./contracts.js"
import { ProfileProviderEventGroup, ProfileProviderEventLogSchema } from "./events.js"
import { collectReplayEvents, parseReplayOptions } from "./replay.js"

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

const NoOpProjection = EL.group(
  ProfileProviderEventGroup,
  (handlers) =>
    handlers
      .handle("ProfileCreated", () => Effect.void)
      .handle("MergedDataProfile", () => Effect.void)
      .handle("SnapshotCreatedProfile", () => Effect.void)
      .handle("MetaDataCreated", () => Effect.void)
      .handle("PersonalDataExtracted", () => Effect.void)
      .handle("ProfileBranchForked", () => Effect.void)
      .handle("SnapshotPublishedProfile", () => Effect.void)
)

const makeEventLogTestLayer = () => {
  const journalLayer = ExpEventJournal.layerMemory
  const identityLayer = Layer.succeed(Identity, Identity.makeRandom())

  return Layer.mergeAll(
    journalLayer,
    identityLayer,
    EventLogApi.layer(ProfileProviderEventLogSchema).pipe(
      Layer.provide(NoOpProjection),
      Layer.provide(Layer.merge(journalLayer, identityLayer))
    )
  )
}

test("parseReplayOptions parses profile and revision filters", () => {
  expect(parseReplayOptions([
    "--profile-id", "profile-1",
    "--min-revision", "2",
    "--max-revision", "8",
    "--dry-run",
    "--no-reset"
  ], true)).toEqual({
    profileId: "profile-1",
    minRevision: 2,
    maxRevision: 8,
    dryRun: true,
    reset: false
  })
})

test("parseReplayOptions rejects invalid revision ranges", () => {
  expect(() => parseReplayOptions([
    "--min-revision", "10",
    "--max-revision", "2"
  ], true)).toThrow("cannot be greater")
})

test("collectReplayEvents filters journal entries by profile and revision range", async () => {
  const layer = makeEventLogTestLayer()
  const firstProfileId = "00000000-0000-4000-8000-000000000101"
  const secondProfileId = "00000000-0000-4000-8000-000000000202"

  const events = await Effect.runPromise(
    Effect.gen(function* () {
      const publish = yield* EventLogApi.makeClient(ProfileProviderEventLogSchema)
      yield* publish("ProfileCreated", {
        profileId: firstProfileId,
        ownerAgentId: "agent-1",
        branchId: "main",
        schemaVersion: "1.0.0",
        maskedProfileJson: makeProfile(firstProfileId),
        createdAt: DateTime.unsafeMake("2026-01-01T00:00:00.000Z"),
        createdBy: "agent-1",
        summary: "first",
        revision: 1
      })
      yield* publish("ProfileCreated", {
        profileId: secondProfileId,
        ownerAgentId: "agent-2",
        branchId: "main",
        schemaVersion: "1.0.0",
        maskedProfileJson: makeProfile(secondProfileId),
        createdAt: DateTime.unsafeMake("2026-01-01T00:00:01.000Z"),
        createdBy: "agent-2",
        summary: "second",
        revision: 3
      })
      yield* publish("MergedDataProfile", {
        profileId: firstProfileId,
        branchId: "main",
        schemaVersion: "1.1.0",
        maskedProfileJson: makeProfile(firstProfileId),
        mergedAt: DateTime.unsafeMake("2026-01-01T00:00:02.000Z"),
        mergedBy: "agent-1",
        summary: "merge",
        sourceCount: 2,
        revision: 5
      })

      const journal = yield* ExpEventJournal.EventJournal
      const entries = yield* journal.entries
      return yield* collectReplayEvents(entries, {
        profileId: firstProfileId,
        minRevision: 2,
        maxRevision: 5,
        reset: false,
        dryRun: true
      })
    }).pipe(
      Effect.scoped,
      Effect.provide(layer),
      Effect.orDie
    ) as unknown as Effect.Effect<ReadonlyArray<{ readonly _tag: string; readonly revision: number; readonly profileId: string }>, never, never>
  )

  expect(events.map((event) => ({
    tag: event._tag,
    profileId: event.profileId,
    revision: event.revision
  }))).toEqual([
    {
      tag: "MergedDataProfile",
      profileId: firstProfileId,
      revision: 5
    }
  ])
})
