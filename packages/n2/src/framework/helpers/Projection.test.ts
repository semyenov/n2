/**
 * Behavior tests for `wireProjectionHandler` — the projection helper that
 * dispatches an event to a store method and (optionally) enqueues an outbox
 * message. Since broken behavior here corrupts read models silently, these
 * tests pin down: store dispatch, outbox skipping, and the documented `orDie`
 * failure mode.
 */
import { it, expect } from "@effect/vitest"
import * as Cause from "effect/Cause"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Layer from "effect/Layer"
import { wireProjectionHandler } from "./Projection.js"

// Minimal fake event class — runtime constructor that mimics Schema.TaggedClass.
class TestEvent {
  readonly _tag = "TestEvent" as const
  constructor(readonly payload: { readonly orderId: string; readonly amount: number }) {}
}

interface TestStoreShape {
  readonly onTestEvent: (event: TestEvent) => Effect.Effect<void, never, never>
}
class TestStore extends Context.Tag("TestStore")<TestStore, TestStoreShape>() {}

interface TestOutboxShape {
  readonly enqueue: (message: { readonly orderId: string; readonly value: number }) => Effect.Effect<void, never, never>
}
class TestOutbox extends Context.Tag("TestOutbox")<TestOutbox, TestOutboxShape>() {}

it.effect("wireProjectionHandler dispatches store method and enqueues outbox message", () => Effect.gen(function* () {
  const storeCalls: Array<TestEvent> = []
  const outboxCalls: Array<{ orderId: string; value: number }> = []

  const handler = wireProjectionHandler(
    TestStore,
    TestOutbox,
    TestEvent,
    "onTestEvent",
    (event: TestEvent) => ({ orderId: event.payload.orderId, value: event.payload.amount * 2 })
  )

  const layer = Layer.merge(
    Layer.succeed(TestStore, {
      onTestEvent: (event) => Effect.sync(() => { storeCalls.push(event) })
    }),
    Layer.succeed(TestOutbox, {
      enqueue: (message) => Effect.sync(() => { outboxCalls.push(message) })
    })
  )

  yield* handler({ payload: { orderId: "order-1", amount: 100 } }).pipe(Effect.provide(layer))

  expect(storeCalls).toHaveLength(1)
  expect(storeCalls[0]).toBeInstanceOf(TestEvent)
  expect(storeCalls[0]?.payload).toEqual({ orderId: "order-1", amount: 100 })
  expect(outboxCalls).toEqual([{ orderId: "order-1", value: 200 }])
}))

it.effect("wireProjectionHandler skips outbox when outboxTag is undefined", () => Effect.gen(function* () {
  const storeCalls: Array<TestEvent> = []

  const handler = wireProjectionHandler(
    TestStore,
    undefined,
    TestEvent,
    "onTestEvent"
  )

  const layer = Layer.succeed(TestStore, {
    onTestEvent: (event: TestEvent) => Effect.sync(() => { storeCalls.push(event) })
  })

  yield* handler({ payload: { orderId: "order-2", amount: 50 } }).pipe(Effect.provide(layer))

  expect(storeCalls).toHaveLength(1)
  expect(storeCalls[0]?.payload.orderId).toBe("order-2")
}))

it.effect("wireProjectionHandler dies on store failure (orDie behavior)", () => Effect.gen(function* () {
  const handler = wireProjectionHandler(
    TestStore,
    undefined,
    TestEvent,
    "onTestEvent"
  )

  const layer = Layer.succeed(TestStore, {
    onTestEvent: () => Effect.die("store exploded")
  })

  const exit = yield* handler({ payload: { orderId: "order-3", amount: 1 } }).pipe(
    Effect.provide(layer),
    Effect.exit
  )

  expect(Exit.isFailure(exit)).toBe(true)
  if (Exit.isFailure(exit)) {
    // The handler uses Effect.orDie internally — failures surface as defects, not typed errors.
    expect(Cause.defects(exit.cause).length).toBeGreaterThan(0)
    expect(Cause.failureOption(exit.cause)._tag).toBe("None")
  }
}))
