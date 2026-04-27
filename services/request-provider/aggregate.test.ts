import { test, expect } from "bun:test"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import { EventLog as EL } from "@effect/experimental"
import { RpcTest } from "@effect/rpc"
import { makeTestAggregate } from "@semyenov/n2/helpers"
import { handle, initialRequestState } from "./aggregate.js"
import {
  CreateRequest,
  CreateRequestSnapshot,
  GetRequest,
  GetRequestHistory,
  RequestDocument,
  RequestError,
  RequestNotFound,
  RequestProviderRpcs,
  RequestState,
  SourceAsset,
  UpdateRequest
} from "./contracts.js"
import { RequestProviderEventGroup, RequestProviderEventLogSchema } from "./events.js"
import { RequestProviderHandlersRaw } from "./entity.js"
import { RequestProviderSnapshots } from "./snapshots.js"

const makeRequestId = (seed: number) =>
  `00000000-0000-4000-8000-${seed.toString().padStart(12, "0")}`

const decodeRequest = Schema.decodeUnknownSync(RequestDocument)

const makeRequest = (requestId: string, relevantPosition = "Senior TypeScript Engineer") =>
  decodeRequest({
    id: 1,
    uuid: requestId,
    created_at: "2026-01-01T00:00:00.000Z",
    updated_at: "2026-01-02T00:00:00.000Z",
    vacancy_data: {
      relevant_position: relevantPosition,
      company: "Acme",
      country: "Germany",
      location: "Berlin",
      job_format: "remote",
      technology_stack: ["TypeScript", "Effect"],
      requirements: {
        experience_years: 5,
        hard_skills: [{ name: "TypeScript", level: "advanced" }]
      },
      salary_offer: {
        currency: "EUR",
        amount_from: 7000
      }
    },
    vacancy_meta_data: {
      source_platform: "manual",
      version: 1
    }
  })

const sampleSource = new SourceAsset({
  sourceId: "src-1",
  kind: "file",
  uri: "s3://bucket/vacancy.pdf",
  mediaType: "application/pdf",
  storageKey: "vacancy.pdf",
  summary: "Initial vacancy"
})

const NoOpProjection = EL.group(
  RequestProviderEventGroup,
  (handlers) =>
    handlers
      .handle("RequestCreated", () => Effect.void)
      .handle("RequestUpdated", () => Effect.void)
      .handle("RequestMetaDataCreated", () => Effect.void)
      .handle("RequestSnapshotCreated", () => Effect.void)
)

const { makeTestLayers, runWith } = makeTestAggregate({
  eventLogSchema: RequestProviderEventLogSchema,
  noOpProjection: NoOpProjection,
  handlersLayer: RequestProviderHandlersRaw,
  snapshotsTag: RequestProviderSnapshots
})

const run = <A, E>(effect: Effect.Effect<A, E, never>): Promise<A> =>
  Effect.runPromise(effect)

const expectEvent = <
  Event extends { readonly _tag: string },
  Tag extends Event["_tag"]
>(
  events: ReadonlyArray<Event>,
  tag: Tag
): Extract<Event, { readonly _tag: Tag }> => {
  const event = events.find((candidate): candidate is Extract<Event, { readonly _tag: Tag }> =>
    candidate._tag === tag
  )
  expect(event).toBeDefined()
  return event!
}

test("CreateRequest initializes draft state and metadata revision", async () => {
  const requestId = makeRequestId(1)
  const { state, events } = await run(handle(initialRequestState, new CreateRequest({
    requestId,
    clientId: "client-1",
    schemaVersion: "1.0.0",
    requestJson: makeRequest(requestId),
    metadataJson: "{\"quality\":0.7}",
    actorId: "client-1",
    summary: "created from vacancy",
    sources: [sampleSource]
  })))

  expect(state.status).toBe("draft")
  expect(state.requestId).toBe(requestId)
  expect(state.revision).toBe(2)
  expect(events.map((event) => event._tag)).toEqual(["RequestCreated", "RequestMetaDataCreated"])
})

test("CreateRequest rejects mismatched request document uuid", async () => {
  const requestId = makeRequestId(2)
  const result = await Effect.runPromise(Effect.either(handle(initialRequestState, new CreateRequest({
    requestId,
    clientId: "client-1",
    schemaVersion: "1.0.0",
    requestJson: makeRequest(makeRequestId(22)),
    metadataJson: "{}",
    actorId: "client-1",
    summary: "bad uuid",
    sources: []
  }))))

  expect(result._tag).toBe("Left")
  if (result._tag === "Left") {
    expect(result.left).toBeInstanceOf(RequestError)
  }
})

test("CreateRequest on existing request fails", async () => {
  const requestId = makeRequestId(20)
  const created = await run(handle(initialRequestState, new CreateRequest({
    requestId,
    clientId: "client-1",
    schemaVersion: "1.0.0",
    requestJson: makeRequest(requestId),
    metadataJson: "{}",
    actorId: "client-1",
    summary: "created",
    sources: []
  })))

  const result = await Effect.runPromise(Effect.either(handle(created.state, new CreateRequest({
    requestId,
    clientId: "client-1",
    schemaVersion: "1.0.0",
    requestJson: makeRequest(requestId),
    metadataJson: "{}",
    actorId: "client-1",
    summary: "duplicate",
    sources: []
  }))))

  expect(result._tag).toBe("Left")
  if (result._tag === "Left") {
    expect(result.left).toBeInstanceOf(RequestError)
  }
})

test("UpdateRequest on empty request fails", async () => {
  const requestId = makeRequestId(21)
  const result = await Effect.runPromise(Effect.either(handle(initialRequestState, new UpdateRequest({
    requestId,
    schemaVersion: "1.0.1",
    requestJson: makeRequest(requestId),
    metadataJson: "{}",
    actorId: "client-1",
    summary: "missing",
    sources: []
  }))))

  expect(result._tag).toBe("Left")
  if (result._tag === "Left") {
    expect(result.left).toBeInstanceOf(RequestError)
  }
})

test("UpdateRequest does not create branches and increments revisions", async () => {
  const requestId = makeRequestId(3)
  const created = await run(handle(initialRequestState, new CreateRequest({
    requestId,
    clientId: "client-1",
    schemaVersion: "1.0.0",
    requestJson: makeRequest(requestId),
    metadataJson: "{}",
    actorId: "client-1",
    summary: "created",
    sources: []
  })))

  const updated = await run(handle(created.state, new UpdateRequest({
    requestId,
    schemaVersion: "1.0.1",
    requestJson: makeRequest(requestId, "Principal Effect Engineer"),
    metadataJson: "{\"quality\":0.9}",
    actorId: "client-1",
    summary: "improved",
    sources: [sampleSource]
  })))

  expect(updated.state.status).toBe("draft")
  expect(updated.state.revision).toBe(4)
  expect(updated.state.revisions.map((entry) => entry.eventType)).toEqual([
    "RequestCreated",
    "RequestMetaDataCreated",
    "RequestUpdated",
    "RequestMetaDataCreated"
  ])
})

test("CreateRequestSnapshot stores summarized draft", async () => {
  const requestId = makeRequestId(4)
  const created = await run(handle(initialRequestState, new CreateRequest({
    requestId,
    clientId: "client-1",
    schemaVersion: "1.0.0",
    requestJson: makeRequest(requestId),
    metadataJson: "{}",
    actorId: "client-1",
    summary: "created",
    sources: []
  })))

  const snapshotted = await run(handle(created.state, new CreateRequestSnapshot({
    requestId,
    snapshotId: "snapshot-1",
    snapshotType: "LLM",
    schemaVersion: "1.0.1",
    requestJson: makeRequest(requestId, "Staff Platform Engineer"),
    metadataJson: "{\"summary\":\"ready\"}",
    actorId: "client-1",
    summary: "saved draft"
  })))

  expect(snapshotted.state.status).toBe("snapshotted")
  expect(snapshotted.state.latestSnapshotId).toBe("snapshot-1")
  expect(snapshotted.state.snapshots).toHaveLength(1)
  expect(snapshotted.events.map((event) => event._tag)).toEqual([
    "RequestSnapshotCreated",
    "RequestMetaDataCreated"
  ])
})

test("CreateRequestSnapshot with duplicate snapshotId fails", async () => {
  const requestId = makeRequestId(40)
  const created = await run(handle(initialRequestState, new CreateRequest({
    requestId,
    clientId: "client-1",
    schemaVersion: "1.0.0",
    requestJson: makeRequest(requestId),
    metadataJson: "{}",
    actorId: "client-1",
    summary: "created",
    sources: []
  })))
  const snapshotted = await run(handle(created.state, new CreateRequestSnapshot({
    requestId,
    snapshotId: "snapshot-1",
    snapshotType: "LLM",
    schemaVersion: "1.0.1",
    requestJson: makeRequest(requestId),
    metadataJson: "{}",
    actorId: "client-1",
    summary: "saved"
  })))

  const result = await Effect.runPromise(Effect.either(handle(snapshotted.state, new CreateRequestSnapshot({
    requestId,
    snapshotId: "snapshot-1",
    snapshotType: "LLM",
    schemaVersion: "1.0.1",
    requestJson: makeRequest(requestId),
    metadataJson: "{}",
    actorId: "client-1",
    summary: "duplicate"
  }))))

  expect(result._tag).toBe("Left")
  if (result._tag === "Left") {
    expect(result.left).toBeInstanceOf(RequestError)
  }
})

test("RequestMetaDataCreated with scope snapshot updates snapshot metadata", async () => {
  const requestId = makeRequestId(41)
  const created = await run(handle(initialRequestState, new CreateRequest({
    requestId,
    clientId: "client-1",
    schemaVersion: "1.0.0",
    requestJson: makeRequest(requestId),
    metadataJson: "{}",
    actorId: "client-1",
    summary: "created",
    sources: []
  })))
  const snapshotted = await run(handle(created.state, new CreateRequestSnapshot({
    requestId,
    snapshotId: "snapshot-meta",
    snapshotType: "LLM",
    schemaVersion: "1.0.1",
    requestJson: makeRequest(requestId),
    metadataJson: "{\"old\":true}",
    actorId: "client-1",
    summary: "saved"
  })))

  const metadata = expectEvent(snapshotted.events, "RequestMetaDataCreated")
  const evolved = snapshotted.state
  expect(metadata.scope).toBe("snapshot")
  expect(metadata.scopeId).toBe("snapshot-meta")
  expect(evolved.snapshots[0]?.metadataJson).toBe("{\"old\":true}")
  expect(evolved.snapshots[0]?.schemaVersion).toBe("1.0.1")
})

test("full lifecycle revisions are tracked correctly", async () => {
  const requestId = makeRequestId(42)
  const created = await run(handle(initialRequestState, new CreateRequest({
    requestId,
    clientId: "client-1",
    schemaVersion: "1.0.0",
    requestJson: makeRequest(requestId),
    metadataJson: "{}",
    actorId: "client-1",
    summary: "created",
    sources: []
  })))
  const updated = await run(handle(created.state, new UpdateRequest({
    requestId,
    schemaVersion: "1.0.1",
    requestJson: makeRequest(requestId, "Principal Effect Engineer"),
    metadataJson: "{}",
    actorId: "client-1",
    summary: "updated",
    sources: []
  })))
  const snapshotted = await run(handle(updated.state, new CreateRequestSnapshot({
    requestId,
    snapshotId: "snapshot-lifecycle",
    snapshotType: "LLM",
    schemaVersion: "1.0.2",
    requestJson: makeRequest(requestId, "Staff Platform Engineer"),
    metadataJson: "{}",
    actorId: "client-1",
    summary: "snapshot",
  })))

  expect(snapshotted.state.revision).toBe(6)
  expect(snapshotted.state.revisions.map((entry) => entry.revision)).toEqual([1, 2, 3, 4, 5, 6])
  expect(snapshotted.state.revisions.map((entry) => entry.eventType)).toEqual([
    "RequestCreated",
    "RequestMetaDataCreated",
    "RequestUpdated",
    "RequestMetaDataCreated",
    "RequestSnapshotCreated",
    "RequestMetaDataCreated"
  ])
})

test("handlers: create, get request, history, and source assets", async () => {
  const requestId = makeRequestId(5)
  const { handlersLayer } = makeTestLayers()

  await runWith(handlersLayer, Effect.gen(function* () {
    const client = yield* RpcTest.makeClient(RequestProviderRpcs)
    yield* client.CreateRequest({
      requestId,
      clientId: "client-1",
      schemaVersion: "1.0.0",
      requestJson: makeRequest(requestId),
      metadataJson: "{}",
      actorId: "client-1",
      summary: "created",
      sources: [sampleSource]
    })
    const state = yield* client.GetRequest({ requestId })
    const history = yield* client.GetRequestHistory({ requestId })
    expect(state.sourceAssets).toEqual([sampleSource])
    expect(history.currentRevision).toBe(2)
  }))
})

test("handlers: GetRequest for unknown request returns RequestNotFound", async () => {
  const requestId = makeRequestId(6)
  const { handlersLayer } = makeTestLayers()

  await runWith(handlersLayer, Effect.gen(function* () {
    const client = yield* RpcTest.makeClient(RequestProviderRpcs)
    const err = yield* client.GetRequest({ requestId }).pipe(Effect.flip)
    expect(err._tag).toBe("RequestNotFound")
    if (err._tag === "RequestNotFound") {
      expect(err.requestId).toBe(requestId)
    }
  }))
})

test("snapshot recovery: GetRequest loads state from snapshot store", async () => {
  const requestId = makeRequestId(7)
  const { handlersLayer, snapshotStore } = makeTestLayers()

  snapshotStore.set(requestId, {
    revision: 9,
    state: new RequestState({
      ...initialRequestState,
      status: "draft",
      requestId,
      clientId: "client-1",
      currentSchemaVersion: "1.0.0",
      requestJson: makeRequest(requestId),
      revision: 9
    })
  })

  await runWith(handlersLayer, Effect.gen(function* () {
    const client = yield* RpcTest.makeClient(RequestProviderRpcs)
    const state = yield* client.GetRequest({ requestId })
    expect(state.revision).toBe(9)
    expect(state.requestId).toBe(requestId)
  }))
})
