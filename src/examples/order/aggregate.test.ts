/**
 * Order aggregate behavior tests.
 */
import { test, expect } from "bun:test"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import { EntityId } from "@effect/cluster"
import { OrderAggregate } from "./aggregate.js"
import {
  type OrderState,
  CreateOrder,
  AddItem,
  SubmitOrder,
  CancelOrder,
  OrderCreated,
  ItemAdded,
  OrderSubmitted
} from "./contracts.js"
import * as AggregateRuntime from "../../framework/runtime/AggregateRuntime.js"
import { layerMemory as EventLogMemory } from "../../framework/runtime/EventJournalEventLog.js"
import * as InMemoryKafkaPublisher from "../../framework/testing/InMemoryKafkaPublisher.js"
import { layerMemory as SnapshotStoreMemory } from "../../framework/runtime/SnapshotStore.js"
import * as TestClock from "../../framework/testing/TestClock.js"
import * as DeterministicIdGenerator from "../../framework/testing/DeterministicIdGenerator.js"

const runtime = AggregateRuntime.make(OrderAggregate)
const eid = EntityId.make

const testLayer = Layer.mergeAll(
  EventLogMemory,
  SnapshotStoreMemory,
  InMemoryKafkaPublisher.layerSimple,
  TestClock.layer(),
  DeterministicIdGenerator.layer()
)

const runTest = <A>(effect: Effect.Effect<A, unknown, unknown>): Promise<A> =>
  // @ts-expect-error -- test helper erases R via testLayer
  effect.pipe(Effect.provide(testLayer), Effect.runPromise) as Promise<A>

test("CreateOrder produces OrderCreated event", async () => {
  const result = await runTest(
    runtime.handle(
      eid("order-1"),
      new CreateOrder({ customerId: "cust-1", orderId: "order-1" })
    )
  )
  expect(result.events.length).toBe(1)
  const event = result.events[0] as OrderCreated
  expect(event._tag).toBe("OrderCreated")
  expect(event.customerId).toBe("cust-1")
  expect(event.orderId).toBe("order-1")
  expect(Number(result.revision)).toBe(1)
})

test("AddItem to draft order produces ItemAdded event", async () => {
  const result = await runTest(
    Effect.gen(function* () {
      yield* runtime.handle(
        eid("order-2"),
        new CreateOrder({ customerId: "cust-1", orderId: "order-2" })
      )
      return yield* runtime.handle(
        eid("order-2"),
        new AddItem({
          orderId: "order-2",
          sku: "SKU-001",
          quantity: 2,
          price: 19.99
        })
      )
    })
  )
  expect(result.events.length).toBe(1)
  const event = result.events[0] as ItemAdded
  expect(event._tag).toBe("ItemAdded")
  expect(event.sku).toBe("SKU-001")
  expect(event.quantity).toBe(2)
  expect(Number(result.revision)).toBe(2)
})

test("SubmitOrder with items produces OrderSubmitted event", async () => {
  const result = await runTest(
    Effect.gen(function* () {
      yield* runtime.handle(
        eid("order-3"),
        new CreateOrder({ customerId: "cust-1", orderId: "order-3" })
      )
      yield* runtime.handle(
        eid("order-3"),
        new AddItem({ orderId: "order-3", sku: "SKU-001", quantity: 1, price: 10 })
      )
      return yield* runtime.handle(
        eid("order-3"),
        new SubmitOrder({ orderId: "order-3" })
      )
    })
  )
  expect(result.events.length).toBe(1)
  expect((result.events[0] as OrderSubmitted)._tag).toBe("OrderSubmitted")
  expect(result.state.status).toBe("submitted")
})

test("SubmitOrder with no items fails", async () => {
  const result = await runTest(
    Effect.gen(function* () {
      yield* runtime.handle(
        eid("order-4"),
        new CreateOrder({ customerId: "cust-1", orderId: "order-4" })
      )
      return yield* runtime.handle(
        eid("order-4"),
        new SubmitOrder({ orderId: "order-4" })
      ).pipe(Effect.flip)
    })
  )
  expect((result as { readonly _tag: string })._tag).toBe("OrderError")
  expect((result as { readonly message: string }).message).toContain("no items")
})

test("CancelOrder on cancelled order fails", async () => {
  const result = await runTest(
    Effect.gen(function* () {
      yield* runtime.handle(
        eid("order-5"),
        new CreateOrder({ customerId: "cust-1", orderId: "order-5" })
      )
      yield* runtime.handle(
        eid("order-5"),
        new CancelOrder({ orderId: "order-5", reason: "changed mind" })
      )
      return yield* runtime.handle(
        eid("order-5"),
        new CancelOrder({ orderId: "order-5", reason: "again" })
      ).pipe(Effect.flip)
    })
  )
  expect((result as { readonly _tag: string })._tag).toBe("OrderError")
  expect((result as { readonly message: string }).message).toContain("already cancelled")
})

test("CreateOrder on existing order fails", async () => {
  const result = await runTest(
    Effect.gen(function* () {
      yield* runtime.handle(
        eid("order-6"),
        new CreateOrder({ customerId: "cust-1", orderId: "order-6" })
      )
      return yield* runtime.handle(
        eid("order-6"),
        new CreateOrder({ customerId: "cust-2", orderId: "order-6" })
      ).pipe(Effect.flip)
    })
  )
  expect((result as { readonly _tag: string })._tag).toBe("OrderError")
  expect((result as { readonly message: string }).message).toContain("already exists")
})

test("hydrate returns correct state after events", async () => {
  const result = await runTest(
    Effect.gen(function* () {
      yield* runtime.handle(
        eid("order-7"),
        new CreateOrder({ customerId: "cust-1", orderId: "order-7" })
      )
      yield* runtime.handle(
        eid("order-7"),
        new AddItem({ orderId: "order-7", sku: "A", quantity: 3, price: 5 })
      )
      return yield* runtime.hydrate(eid("order-7"))
    })
  )
  const state = result.state as OrderState
  expect(state.status).toBe("draft")
  expect(Option.getOrElse(state.customerId, () => "")).toBe("cust-1")
  expect(state.items.length).toBe(1)
  expect(state.totalAmount).toBe(15)
  expect(Number(result.revision)).toBe(2)
})
