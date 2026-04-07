/**
 * Order aggregate behavior tests.
 * Pure domain logic -- no infrastructure layers needed.
 */
import { test, expect } from "bun:test"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as DateTime from "effect/DateTime"
import { Order, handleCommand, evolve } from "./aggregate.js"
import {
  CreateOrder,
  AddItem,
  SubmitOrder,
  CancelOrder,
  OrderCreated,
  ItemAdded,
  OrderSubmitted,
  initialOrderState as emptyState
} from "./contracts.js"

const run = <A, E>(effect: Effect.Effect<A, E, never>): Promise<A> =>
  Effect.runPromise(effect)

test("CreateOrder produces OrderCreated event", async () => {
  const result = await run(
    handleCommand(emptyState, new CreateOrder({ orderId: "order-1", customerId: "cust-1" }))
  )
  expect(result.events.length).toBe(1)
  const event = result.events[0] as OrderCreated
  expect(event._tag).toBe("OrderCreated")
  expect(event.customerId).toBe("cust-1")
  expect(result.state.status).toBe("draft")
})

test("AddItem to draft order produces ItemAdded", async () => {
  const result = await run(
    Order.run([
      new CreateOrder({ orderId: "o2", customerId: "c1" }),
      new AddItem({ orderId: "o2", sku: "SKU-001", quantity: 2, price: 19.99 })
    ], emptyState)
  )
  expect(result.events.length).toBe(2)
  expect((result.events[0] as OrderCreated)._tag).toBe("OrderCreated")
  expect((result.events[1] as ItemAdded)._tag).toBe("ItemAdded")
  expect(result.state.items.length).toBe(1)
})

test("SubmitOrder with items produces OrderSubmitted", async () => {
  const result = await run(
    Order.run([
      new CreateOrder({ orderId: "o3", customerId: "c1" }),
      new AddItem({ orderId: "o3", sku: "X", quantity: 1, price: 10 }),
      new SubmitOrder({ orderId: "o3" })
    ], emptyState)
  )
  expect(result.events.length).toBe(3)
  expect((result.events[0] as OrderCreated)._tag).toBe("OrderCreated")
  expect((result.events[1] as ItemAdded)._tag).toBe("ItemAdded")
  expect((result.events[2] as OrderSubmitted)._tag).toBe("OrderSubmitted")
  expect(result.state.status).toBe("submitted")
})

test("SubmitOrder with no items fails", async () => {
  const result = await run(
    Effect.gen(function*() {
      const { state: s1 } = yield* handleCommand(emptyState, new CreateOrder({ orderId: "o4", customerId: "c1" }))
      return yield* handleCommand(s1, new SubmitOrder({ orderId: "o4" })).pipe(Effect.flip)
    })
  )
  expect((result as { readonly _tag: string })._tag).toBe("OrderError")
})

test("CancelOrder on cancelled order fails", async () => {
  const result = await run(
    Effect.gen(function*() {
      const { state: s1 } = yield* handleCommand(emptyState, new CreateOrder({ orderId: "o5", customerId: "c1" }))
      const { state: s2 } = yield* handleCommand(s1, new CancelOrder({ orderId: "o5", reason: "r" }))
      return yield* handleCommand(s2, new CancelOrder({ orderId: "o5", reason: "r2" })).pipe(Effect.flip)
    })
  )
  expect((result as { readonly _tag: string })._tag).toBe("OrderError")
})

test("CreateOrder on existing order fails", async () => {
  const result = await run(
    Effect.gen(function*() {
      const { state: s1 } = yield* handleCommand(emptyState, new CreateOrder({ orderId: "o6", customerId: "c1" }))
      return yield* handleCommand(s1, new CreateOrder({ orderId: "o6", customerId: "c2" })).pipe(Effect.flip)
    })
  )
  expect((result as { readonly _tag: string })._tag).toBe("OrderError")
})

test("evolve produces correct state", () => {
  const created = new OrderCreated({ orderId: "o7", customerId: "c1", createdAt: DateTime.unsafeMake(0) })
  const s1 = evolve(emptyState, created)
  expect(s1.status).toBe("draft")
  expect(Option.getOrElse(s1.customerId, () => "")).toBe("c1")
})
