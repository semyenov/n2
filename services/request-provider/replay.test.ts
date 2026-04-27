import { test, expect } from "bun:test"
import * as DateTime from "effect/DateTime"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as ExpEventJournal from "@effect/experimental/EventJournal"
import { EventLog as EL } from "@effect/experimental"
import * as EventLogApi from "@effect/experimental/EventLog"
import { Identity } from "@effect/experimental/EventLog"
import { RequestDocument } from "./contracts.js"
import { RequestProviderEventGroup, RequestProviderEventLogSchema } from "./events.js"
import { collectReplayEvents, parseReplayOptions } from "./replay.js"

const decodeRequest = Schema.decodeUnknownSync(RequestDocument)

const makeRequest = (requestId: string) =>
  decodeRequest({
    id: 1,
    uuid: requestId,
    created_at: "2026-01-01T00:00:00.000Z",
    vacancy_data: {
      relevant_position: "Senior TypeScript Engineer",
      company: "Acme",
      country: "Germany",
      location: "Berlin"
    },
    vacancy_meta_data: { version: 1 }
  })

type CollectedReplayEvent = {
  readonly _tag: string
  readonly revision: number
  readonly requestId: string
}

const runProvided = <A, E, R>(effect: Effect.Effect<A, E, R>): Promise<A> =>
  Effect.runPromise(effect as unknown as Effect.Effect<A, E, never>)

const NoOpProjection = EL.group(
  RequestProviderEventGroup,
  (handlers) =>
    handlers
      .handle("RequestCreated", () => Effect.void)
      .handle("RequestUpdated", () => Effect.void)
      .handle("RequestMetaDataCreated", () => Effect.void)
      .handle("RequestSnapshotCreated", () => Effect.void)
)

const makeEventLogTestLayer = () => {
  const journalLayer = ExpEventJournal.layerMemory
  const identityLayer = Layer.succeed(Identity, Identity.makeRandom())

  return Layer.mergeAll(
    journalLayer,
    identityLayer,
    EventLogApi.layer(RequestProviderEventLogSchema).pipe(
      Layer.provide(NoOpProjection),
      Layer.provide(Layer.merge(journalLayer, identityLayer))
    )
  )
}

test("parseReplayOptions parses request and revision filters", () => {
  expect(parseReplayOptions([
    "--entity-id", "request-1",
    "--min-revision", "2",
    "--max-revision", "8",
    "--dry-run",
    "--no-reset"
  ], true)).toEqual({
    entityId: "request-1",
    minRevision: 2,
    maxRevision: 8,
    dryRun: true,
    reset: false
  })
})

test("collectReplayEvents filters journal entries by request and revision range", async () => {
  const layer = makeEventLogTestLayer()
  const firstRequestId = "00000000-0000-4000-8000-000000000101"
  const secondRequestId = "00000000-0000-4000-8000-000000000202"

  const events = await runProvided<ReadonlyArray<CollectedReplayEvent>, unknown, unknown>(
    Effect.gen(function* () {
      const publish = yield* EventLogApi.makeClient(RequestProviderEventLogSchema)
      yield* publish("RequestCreated", {
        requestId: firstRequestId,
        clientId: "client-1",
        schemaVersion: "1.0.0",
        requestJson: makeRequest(firstRequestId),
        occurredAt: DateTime.unsafeMake("2026-01-01T00:00:00.000Z"),
        actorId: "client-1",
        summary: "first",
        sourceCount: 1,
        revision: 1
      })
      yield* publish("RequestCreated", {
        requestId: secondRequestId,
        clientId: "client-2",
        schemaVersion: "1.0.0",
        requestJson: makeRequest(secondRequestId),
        occurredAt: DateTime.unsafeMake("2026-01-01T00:00:01.000Z"),
        actorId: "client-2",
        summary: "second",
        sourceCount: 1,
        revision: 3
      })
      yield* publish("RequestUpdated", {
        requestId: firstRequestId,
        schemaVersion: "1.0.1",
        requestJson: makeRequest(firstRequestId),
        occurredAt: DateTime.unsafeMake("2026-01-01T00:00:02.000Z"),
        actorId: "client-1",
        summary: "updated",
        sourceCount: 0,
        revision: 4
      })

      const journal = yield* ExpEventJournal.EventJournal
      const entries = yield* journal.entries
      return yield* collectReplayEvents(
        entries,
        {
          entityId: firstRequestId,
          minRevision: 2,
          maxRevision: 5,
          dryRun: false,
          reset: false
        }
      )
    }).pipe(
      Effect.scoped,
      Effect.provide(layer)
    )
  )

  expect(events.map((event) => ({
    _tag: event._tag,
    requestId: event.requestId,
    revision: event.revision
  }))).toEqual([
    { _tag: "RequestUpdated", requestId: firstRequestId, revision: 4 }
  ])
})
