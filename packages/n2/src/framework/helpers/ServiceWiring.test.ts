import { test, expect } from "bun:test"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import { makeConsoleEventPublisherLayer, makeStandardOutboxWiring, makeStandardSnapshotWiring, type OutboxService, type SnapshotService } from "./ServiceWiring.js"

class TestMessage extends Schema.Class<TestMessage>("TestMessage")({
  id: Schema.String,
  topic: Schema.String,
  partitionKey: Schema.String,
  payload: Schema.Unknown
}) {}

class TestPublisher extends Context.Tag("TestPublisher")<
  TestPublisher,
  { readonly publish: (message: TestMessage) => Effect.Effect<void, unknown> }
>() {}

class TestSnapshots extends Context.Tag("TestSnapshots")<
  TestSnapshots,
  SnapshotService<{ readonly value: number }>
>() {}

test("makeStandardSnapshotWiring builds entity snapshot ops", async () => {
  const wiring = makeStandardSnapshotWiring({
    tag: TestSnapshots,
    table: "test_snapshots",
    stateSchema: Schema.Struct({ value: Schema.Number }),
    every: 10
  })
  const calls: Array<string> = []
  const layer = Layer.succeed(TestSnapshots, {
    load: (entityId) => Effect.sync(() => {
      calls.push(`load:${entityId}`)
      return Option.none()
    }),
    save: (entityId, state, revision) => Effect.sync(() => {
      calls.push(`save:${entityId}:${state.value}:${revision}`)
    })
  })

  await Effect.runPromise(
    Effect.gen(function* () {
      yield* wiring.ops.load("a")
      yield* wiring.ops.save("a", { value: 1 }, 2)
    }).pipe(Effect.provide(layer))
  )

  expect(wiring.ops.every).toBe(10)
  expect(calls).toEqual(["load:a", "save:a:1:2"])
})

test("makeConsoleEventPublisherLayer logs publish metadata", async () => {
  const layer = makeConsoleEventPublisherLayer(TestPublisher, "published")
  await Effect.runPromise(
    Effect.gen(function* () {
      const publisher = yield* TestPublisher
      yield* publisher.publish(new TestMessage({
        id: "m-1",
        topic: "events",
        partitionKey: "entity-1",
        payload: { ok: true }
      }))
    }).pipe(Effect.provide(layer))
  )
})

class WiringTestMessage extends Schema.Class<WiringTestMessage>("WiringTestMessage")({
  id: Schema.String,
  payload: Schema.Unknown
}) {}

class WiringTestOutbox extends Context.Tag("WiringTestOutbox")<
  WiringTestOutbox,
  OutboxService<WiringTestMessage>
>() {}

test("makeStandardOutboxWiring returns live, drainOnce, and workerLive", () => {
  const wiring = makeStandardOutboxWiring({
    tag: WiringTestOutbox,
    table: "test_outbox",
    schema: WiringTestMessage,
    publish: (_message) => Effect.void
  })
  expect(Layer.isLayer(wiring.live)).toBe(true)
  expect(Layer.isLayer(wiring.workerLive)).toBe(true)
  expect(typeof wiring.drainOnce).toBe("object")
})

test("makeStandardOutboxWiring drainOnce returns false when outbox is empty", async () => {
  const wiring = makeStandardOutboxWiring({
    tag: WiringTestOutbox,
    table: "test_outbox",
    schema: WiringTestMessage,
    publish: (_message) => Effect.void
  })
  const mockLayer = Layer.succeed(WiringTestOutbox, {
    enqueue: (_message) => Effect.void,
    claimPending: (_limit) => Effect.succeed([]),
    markDispatched: (_id) => Effect.void,
    markFailed: (_id, _retryCount, _error) => Effect.void,
    markDeadLetter: (_id, _error) => Effect.void
  })
  const result = await Effect.runPromise(
    wiring.drainOnce.pipe(Effect.provide(mockLayer))
  )
  expect(result).toBe(false)
})
