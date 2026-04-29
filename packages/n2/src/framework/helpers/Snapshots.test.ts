import { it, expect } from "@effect/vitest"
import * as Cause from "effect/Cause"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import { ParseError } from "effect/ParseResult"
import * as Schema from "effect/Schema"
import { SqlClient, type SqlClient as SqlClientInstance } from "@effect/sql/SqlClient"
import { makeSnapshotOps, makeSnapshotService, type SnapshotService } from "./Snapshots.js"

interface TestState {
  readonly value: number
}

class TestSnapshots extends Context.Tag("TestSnapshots")<
  TestSnapshots,
  SnapshotService<TestState>
>() {}

it.effect("makeSnapshotOps delegates load and save through the snapshot tag", () => Effect.gen(function* () {
  const calls: Array<string> = []
  const ops = makeSnapshotOps(TestSnapshots, 25)
  const layer = Layer.succeed(TestSnapshots, {
    load: (entityId) =>
      Effect.sync(() => {
        calls.push(`load:${entityId}`)
        return Option.some({ state: { value: 42 }, revision: 7 })
      }),
    save: (entityId, state, revision) =>
      Effect.sync(() => {
        calls.push(`save:${entityId}:${state.value}:${revision}`)
      })
  })

  const loaded = yield* Effect.gen(function* () {
    const snapshot = yield* ops.load("entity-1")
    yield* ops.save("entity-1", { value: 43 }, 8)
    return snapshot
  }).pipe(Effect.provide(layer))

  expect(ops.every).toBe(25)
  expect(Option.getOrUndefined(loaded)).toEqual({ state: { value: 42 }, revision: 7 })
  expect(calls).toEqual(["load:entity-1", "save:entity-1:43:8"])
}))

class TypedState extends Schema.Class<TypedState>("TypedState")({
  value: Schema.Number
}) {}

class TypedSnapshots extends Context.Tag("TypedSnapshots")<
  TypedSnapshots,
  SnapshotService<TypedState>
>() {}

it.effect("load surfaces ParseError as a typed failure on malformed state_json", () => Effect.gen(function* () {
  type FakeSql = {
    (strings: TemplateStringsArray | string, ...params: ReadonlyArray<unknown>): unknown
  }

  // value is a number per the schema; injecting a string forces a decode error.
  const malformed = JSON.stringify({ value: "not-a-number" })

  const fakeSql = ((strings: TemplateStringsArray | string, ..._params: ReadonlyArray<unknown>) => {
    if (typeof strings === "string") return { identifier: strings }
    const text = strings.join("?")
    if (text.includes("SELECT state_json, revision")) {
      return Effect.succeed([{ state_json: malformed, revision: 1 }])
    }
    return Effect.succeed([])
  }) as FakeSql

  const snapshots = makeSnapshotService({
    table: "test_snapshots",
    stateSchema: TypedState
  })

  const layer = Layer.provide(
    snapshots.makeLive(TypedSnapshots),
    Layer.succeed(SqlClient, fakeSql as unknown as SqlClientInstance)
  )

  const exit = yield* Effect.gen(function* () {
    const service = yield* TypedSnapshots
    return yield* service.load("entity-bad")
  }).pipe(
    Effect.provide(layer),
    Effect.exit
  )

  expect(Exit.isFailure(exit)).toBe(true)
  if (Exit.isFailure(exit)) {
    const failure = Cause.failureOption(exit.cause)
    expect(failure._tag).toBe("Some")
    if (failure._tag === "Some") {
      expect(failure.value).toBeInstanceOf(ParseError)
    }
    expect(Cause.defects(exit.cause).length).toBe(0)
  }
}))
