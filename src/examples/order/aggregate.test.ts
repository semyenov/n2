/**
 * Order aggregate behavior tests.
 * Uses handleCommand directly -- no AggregateRuntime, no EventLog, no Kafka layers.
 * Pure domain logic tests.
 */
import { test, expect } from "bun:test"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as DateTime from "effect/DateTime"
import { handleCommand, evolve } from "./aggregate.js"
import {
  type OrderState,
  CreateOrder,
  AddItem,
  SubmitOrder,
  CancelOrder,
  OrderCreated,
  ItemAdded,
  OrderSubmitted,
  initialOrderState as emptyState
} from "./contracts.js"
import * as TestClock from "../../framework/testing/TestClock.js"
import { N2ClockLive } from "../../framework/runtime/Clock.js"

// handleCommand needs N2Clock for timestamp generation
const clockLayer = TestClock.layer()

const run = <A>(effect: Effect.Effect<A, unknown, unknown>): Promise<A> =>
  // @ts-expect-error -- test provides clock layer
  effect.pipe(Effect.provide(clockLayer), Effect.runPromise) as Promise<A>

test("CreateOrder produces OrderCreated event", async () => {
  const result = await run(
    handleCommand(emptyState, new CreateOrder({ orderId: "order-1", customerId: "cust-1" }))
  )
  expect(result.events.length).toBe(1)
  const event = result.events[0] as OrderCreated
  expect(event._tag).toBe("OrderCreated")
  expect(event.customerId).toBe("cust-1")
  expect(event.orderId).toBe("order-1")
  expect(result.state.status).toBe("draft")
})

test("AddItem to draft order produces ItemAdded", async () => {
  const result = await run(
    Effect.gen(function*() {
      const { state: s1 } = yield* handleCommand(
        emptyState,
        new CreateOrder({ orderId: "order-2", customerId: "cust-1" })
      )
      return yield* handleCommand(
        s1,
        new AddItem({ orderId: "order-2", sku: "SKU-001", quantity: 2, price: 19.99 })
      )
    })
  )
  expect(result.events.length).toBe(1)
  const event = result.events[0] as ItemAdded
  expect(event._tag).toBe("ItemAdded")
  expect(event.sku).toBe("SKU-001")
  expect(result.state.items.length).toBe(1)
  expect(result.state.totalAmount).toBeCloseTo(39.98)
})

test("SubmitOrder with items produces OrderSubmitted", async () => {
  const result = await run(
    Effect.gen(function*() {
      const { state: s1 } = yield* handleCommand(
        emptyState,
        new CreateOrder({ orderId: "order-3", customerId: "cust-1" })
      )
      const { state: s2 } = yield* handleCommand(
        s1,
        new AddItem({ orderId: "order-3", sku: "SKU-001", quantity: 1, price: 10 })
      )
      return yield* handleCommand(s2, new SubmitOrder({ orderId: "order-3" }))
    })
  )
  expect(result.events.length).toBe(1)
  expect((result.events[0] as OrderSubmitted)._tag).toBe("OrderSubmitted")
  expect(result.state.status).toBe("submitted")
})

test("SubmitOrder with no items fails", async () => {
  const result = await run(
    Effect.gen(function*() {
      const { state: s1 } = yield* handleCommand(
        emptyState,
        new CreateOrder({ orderId: "order-4", customerId: "cust-1" })
      )
      return yield* handleCommand(s1, new SubmitOrder({ orderId: "order-4" })).pipe(Effect.flip)
    })
  )
  expect((result as { readonly _tag: string })._tag).toBe("OrderError")
  expect((result as { readonly message: string }).message).toContain("no items")
})

test("CancelOrder on cancelled order fails", async () => {
  const result = await run(
    Effect.gen(function*() {
      const { state: s1 } = yield* handleCommand(
        emptyState,
        new CreateOrder({ orderId: "order-5", customerId: "cust-1" })
      )
      const { state: s2 } = yield* handleCommand(
        s1,
        new CancelOrder({ orderId: "order-5", reason: "changed mind" })
      )
      return yield* handleCommand(
        s2,
        new CancelOrder({ orderId: "order-5", reason: "again" })
      ).pipe(Effect.flip)
    })
  )
  expect((result as { readonly _tag: string })._tag).toBe("OrderError")
  expect((result as { readonly message: string }).message).toContain("already cancelled")
})

test("CreateOrder on existing order fails", async () => {
  const result = await run(
    Effect.gen(function*() {
      const { state: s1 } = yield* handleCommand(
        emptyState,
        new CreateOrder({ orderId: "order-6", customerId: "cust-1" })
      )
      return yield* handleCommand(
        s1,
        new CreateOrder({ orderId: "order-6", customerId: "cust-2" })
      ).pipe(Effect.flip)
    })
  )
  expect((result as { readonly _tag: string })._tag).toBe("OrderError")
  expect((result as { readonly message: string }).message).toContain("already exists")
})

test("evolve produces correct state from events", () => {
  const state = emptyState
  const created = new OrderCreated({
    orderId: "order-7",
    customerId: "cust-1",
    createdAt: DateTime.unsafeMake(0)
  })
  const s1 = evolve(state, created)
  expect(s1.status).toBe("draft")
  expect(Option.getOrElse(s1.customerId, () => "")).toBe("cust-1")
})
