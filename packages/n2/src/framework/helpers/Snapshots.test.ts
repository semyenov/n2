import { test, expect } from "bun:test"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import { makeSnapshotOps, type SnapshotService } from "./Snapshots.js"

interface TestState {
  readonly value: number
}

class TestSnapshots extends Context.Tag("TestSnapshots")<
  TestSnapshots,
  SnapshotService<TestState>
>() {}

test("makeSnapshotOps delegates load and save through the snapshot tag", async () => {
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

  const loaded = await Effect.runPromise(
    Effect.gen(function* () {
      const snapshot = yield* ops.load("entity-1")
      yield* ops.save("entity-1", { value: 43 }, 8)
      return snapshot
    }).pipe(Effect.provide(layer))
  )

  expect(ops.every).toBe(25)
  expect(Option.getOrUndefined(loaded)).toEqual({ state: { value: 42 }, revision: 7 })
  expect(calls).toEqual(["load:entity-1", "save:entity-1:43:8"])
})
