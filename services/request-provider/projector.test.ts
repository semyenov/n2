import { test, expect } from "bun:test"
import * as DateTime from "effect/DateTime"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as ExpEventJournal from "@effect/experimental/EventJournal"
import { Identity } from "@effect/experimental/EventLog"
import * as EventLogApi from "@effect/experimental/EventLog"
import { RequestDocument, type RequestProviderEvent } from "./contracts.js"
import { RequestProviderEventLogSchema } from "./events.js"
import { RequestProviderOutbox } from "./outbox.js"
import { RequestProviderProjectionLayer } from "./projector.js"
import { makeDispatch, RequestProviderProjectionStore, type ProjectionHandlers } from "./projection-store.js"
import type { RequestProviderEventMessage } from "./workflows.js"

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

test("projector writes through the projection store and outbox", async () => {
  const requestId = "00000000-0000-4000-8000-000000000111"
  const projected: Array<{ tag: RequestProviderEvent["_tag"] }> = []
  const outboxed: Array<{ messageId: string }> = []
  const journalLayer = ExpEventJournal.layerMemory
  const identityLayer = Layer.succeed(Identity, Identity.makeRandom())

  const perEventHandlers: Omit<ProjectionHandlers, "dispatch"> = {
    onRequestCreated: (event) => Effect.sync(() => { projected.push({ tag: event._tag }) }),
    onRequestUpdated: (event) => Effect.sync(() => { projected.push({ tag: event._tag }) }),
    onRequestMetaDataCreated: (event) => Effect.sync(() => { projected.push({ tag: event._tag }) }),
    onRequestSnapshotCreated: (event) => Effect.sync(() => { projected.push({ tag: event._tag }) })
  }
  const noOpHandlers: ProjectionHandlers = { ...perEventHandlers, dispatch: makeDispatch(perEventHandlers) }

  const noOpOutbox = {
    enqueue: (message: RequestProviderEventMessage) =>
      Effect.sync(() => { outboxed.push({ messageId: message.id }) }),
    claimPending: () => Effect.succeed([] as const),
    markDispatched: () => Effect.void,
    markFailed: () => Effect.void,
    markDeadLetter: () => Effect.void
  }

  const layer = Layer.mergeAll(
    journalLayer,
    identityLayer,
    Layer.succeed(RequestProviderProjectionStore, noOpHandlers),
    Layer.succeed(RequestProviderOutbox, noOpOutbox),
    EventLogApi.layer(RequestProviderEventLogSchema).pipe(
      Layer.provide(RequestProviderProjectionLayer),
      Layer.provide(Layer.merge(journalLayer, identityLayer))
    )
  )

  await Effect.runPromise(
    Effect.gen(function* () {
      const publish = yield* EventLogApi.makeClient(RequestProviderEventLogSchema)
      yield* publish("RequestCreated", {
        requestId,
        clientId: "client-1",
        schemaVersion: "1.0.0",
        requestJson: makeRequest(requestId),
        occurredAt: DateTime.unsafeMake("2026-01-01T00:00:00.000Z"),
        actorId: "client-1",
        summary: "init",
        sourceCount: 1,
        revision: 1
      })
    }).pipe(
      Effect.scoped,
      Effect.provide(layer)
    ) as Effect.Effect<void, never, never>
  )

  expect(projected).toEqual([{ tag: "RequestCreated" }])
  expect(outboxed).toEqual([{ messageId: `${requestId}:1:RequestCreated` }])
})
