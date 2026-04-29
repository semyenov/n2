import { it, expect } from "@effect/vitest"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Ref from "effect/Ref"
import * as Schema from "effect/Schema"
import * as EventJournal from "@effect/experimental/EventJournal"
import { EventGroup } from "@effect/experimental"
import { eventPayloadSchema } from "./Definitions.js"
import {
  mergeAssetsByKey,
  makeToError,
  makeStateOverride,
  makeWriteThroughAfterCommitPublisher
} from "./EntityWiring.js"

it("mergeAssetsByKey dedupes by key, preserves order, last write wins", () => {
  type Asset = { readonly id: string; readonly value: number }
  const merge = mergeAssetsByKey<Asset>((a) => a.id)
  const result = merge(
    [{ id: "a", value: 1 }, { id: "b", value: 2 }],
    [{ id: "b", value: 99 }, { id: "c", value: 3 }]
  )
  expect(result).toEqual([
    { id: "a", value: 1 },
    { id: "b", value: 99 },
    { id: "c", value: 3 }
  ])
})

class FooError extends Error {
  readonly _tag = "FooError"
  constructor(props: { message: string }) {
    super(props.message)
  }
}

it("makeToError passes through an existing domain error instance", () => {
  const toError = makeToError(FooError)
  const original = new FooError({ message: "boom" })
  expect(toError(original)).toBe(original)
})

it("makeToError wraps unknown values with String(error) message", () => {
  const toError = makeToError(FooError)
  const wrapped = toError("some string")
  expect(wrapped).toBeInstanceOf(FooError)
  expect(wrapped.message).toBe("some string")
})

type State = { readonly id: string; readonly status: "empty" | "active"; readonly count: number }
type NotFound = { readonly _tag: "NotFound"; readonly id: string }
const notFound = (id: string): NotFound => ({ _tag: "NotFound", id })

it.effect("makeStateOverride returns state as-is when no project given", () => Effect.gen(function* () {
  const override = makeStateOverride<{ readonly id: string }, State, State, NotFound>(
    (c) => c.id,
    { isEmpty: (s) => s.status === "empty", notFound }
  )
  const ctx = { getState: (id: string) => Effect.succeed({ id, status: "active" as const, count: 5 }) }
  const result = yield* override({ id: "x" }, ctx)
  expect(result).toEqual({ id: "x", status: "active", count: 5 })
}))

it.effect("makeStateOverride applies project function for history-style overrides", () => Effect.gen(function* () {
  type History = { readonly entityId: string; readonly count: number }
  const override = makeStateOverride<{ readonly id: string }, State, History, NotFound>(
    (c) => c.id,
    {
      isEmpty: (s) => s.status === "empty",
      notFound,
      project: (s) => ({ entityId: s.id, count: s.count })
    }
  )
  const ctx = { getState: (id: string) => Effect.succeed({ id, status: "active" as const, count: 7 }) }
  const result = yield* override({ id: "x" }, ctx)
  expect(result).toEqual({ entityId: "x", count: 7 })
}))

it.effect("makeStateOverride fails with notFound when state is empty", () => Effect.gen(function* () {
  const override = makeStateOverride<{ readonly id: string }, State, State, NotFound>(
    (c) => c.id,
    { isEmpty: (s) => s.status === "empty", notFound }
  )
  const ctx = { getState: (id: string) => Effect.succeed({ id, status: "empty" as const, count: 0 }) }
  const exit = yield* Effect.exit(override({ id: "missing" }, ctx))
  expect(exit._tag).toBe("Failure")
  if (exit._tag === "Failure") {
    const err = (exit.cause as { error?: NotFound }).error
    expect(err).toEqual({ _tag: "NotFound", id: "missing" })
  }
}))

class TestCommitted extends Schema.TaggedClass<TestCommitted>()(
  "TestCommitted",
  {
    entityId: Schema.String,
    revision: Schema.Number
  }
) {}

const TestEventGroup = EventGroup.empty.add({
  tag: "TestCommitted",
  primaryKey: (event: { readonly entityId: string }) => event.entityId,
  payload: eventPayloadSchema(TestCommitted)
})

class TestProjectionStore extends Context.Tag("TestProjectionStore")<
  TestProjectionStore,
  { readonly dispatch: (event: TestCommitted) => Effect.Effect<void> }
>() {}

class TestOutbox extends Context.Tag("TestOutbox")<
  TestOutbox,
  { readonly enqueue: (message: { readonly id: string }) => Effect.Effect<void> }
>() {}

it.effect("makeWriteThroughAfterCommitPublisher writes journal, projection, and outbox", () =>
  Effect.gen(function* () {
    const calls = yield* Ref.make<Array<string>>([])
    const publisher = makeWriteThroughAfterCommitPublisher({
      group: TestEventGroup,
      storeTag: TestProjectionStore,
      outboxTag: TestOutbox,
      makeMessage: (event: TestCommitted) => ({ id: `${event.entityId}:${event.revision}` }),
      logPrefix: "[test]"
    })
    const layer = Layer.mergeAll(
      EventJournal.layerMemory,
      Layer.succeed(TestProjectionStore, {
        dispatch: (event) =>
          Ref.update(calls, (current) => [...current, `store:${event.revision}`])
      }),
      Layer.succeed(TestOutbox, {
        enqueue: (message) =>
          Ref.update(calls, (current) => [...current, `outbox:${message.id}`])
      })
    )

    yield* Effect.gen(function* () {
      yield* publisher({
        events: [new TestCommitted({ entityId: "entity-1", revision: 1 })]
      })
      const journal = yield* EventJournal.EventJournal
      const entries = yield* journal.entries
      expect(entries.map((entry) => entry.event)).toEqual(["TestCommitted"])
      expect(yield* Ref.get(calls)).toEqual(["store:1", "outbox:entity-1:1"])
    }).pipe(Effect.provide(layer))
  }))
