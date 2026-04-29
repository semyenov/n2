import { test, expect } from "bun:test"
import * as Effect from "effect/Effect"
import { mergeAssetsByKey, makeToError, makeStateOverride } from "./EntityWiring.js"

test("mergeAssetsByKey dedupes by key, preserves order, last write wins", () => {
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

test("makeToError passes through an existing domain error instance", () => {
  const toError = makeToError(FooError)
  const original = new FooError({ message: "boom" })
  expect(toError(original)).toBe(original)
})

test("makeToError wraps unknown values with String(error) message", () => {
  const toError = makeToError(FooError)
  const wrapped = toError("some string")
  expect(wrapped).toBeInstanceOf(FooError)
  expect(wrapped.message).toBe("some string")
})

type State = { readonly id: string; readonly status: "empty" | "active"; readonly count: number }
type NotFound = { readonly _tag: "NotFound"; readonly id: string }
const notFound = (id: string): NotFound => ({ _tag: "NotFound", id })

test("makeStateOverride returns state as-is when no project given", async () => {
  const override = makeStateOverride<{ readonly id: string }, State, State, NotFound>(
    (c) => c.id,
    { isEmpty: (s) => s.status === "empty", notFound }
  )
  const ctx = { getState: (id: string) => Effect.succeed({ id, status: "active" as const, count: 5 }) }
  const result = await Effect.runPromise(override({ id: "x" }, ctx))
  expect(result).toEqual({ id: "x", status: "active", count: 5 })
})

test("makeStateOverride applies project function for history-style overrides", async () => {
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
  const result = await Effect.runPromise(override({ id: "x" }, ctx))
  expect(result).toEqual({ entityId: "x", count: 7 })
})

test("makeStateOverride fails with notFound when state is empty", async () => {
  const override = makeStateOverride<{ readonly id: string }, State, State, NotFound>(
    (c) => c.id,
    { isEmpty: (s) => s.status === "empty", notFound }
  )
  const ctx = { getState: (id: string) => Effect.succeed({ id, status: "empty" as const, count: 0 }) }
  const exit = await Effect.runPromiseExit(override({ id: "missing" }, ctx))
  expect(exit._tag).toBe("Failure")
  if (exit._tag === "Failure") {
    const err = (exit.cause as { error?: NotFound }).error
    expect(err).toEqual({ _tag: "NotFound", id: "missing" })
  }
})
