/**
 * Comprehensive benchmarks for the N2 framework.
 *
 * Covers: evolve, decide, handle, run (pure domain),
 * cluster entity operations, and RPC handler throughput.
 *
 * Run: bun src/examples/order/bench.ts
 */
import { bench, group, run as runBench } from "mitata"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as DateTime from "effect/DateTime"
import * as Option from "effect/Option"
import { Entity, ShardingConfig } from "@effect/cluster"

import { Order, evolve } from "./aggregate.js"
import {
  CreateOrder,
  AddItem,
  SubmitOrder,
  CancelOrder,
  OrderCreated,
  ItemAdded,
  OrderSubmitted,
  OrderState,
  LineItem,
  initialOrderState as emptyState,
  OrderEntity,
  CommandResult,
  OrderError
} from "./contracts.js"
import { OrderEntityLayer } from "./entity.js"
import { InfrastructureLayer } from "./layers.js"

import { Inventory } from "../inventory/aggregate.js"
import {
  ReserveStock,
  ReleaseStock,
  StockReserved,
  InventoryState,
  initialInventoryState,
} from "../inventory/contracts.js"

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const runEffect = <A, E>(effect: Effect.Effect<A, E, never>): Promise<A> =>
  Effect.runPromise(effect)

/** Pre-built state: a draft order with N items */
const makeDraftState = (itemCount: number): OrderState =>
  new OrderState({
    status: "draft",
    orderId: Option.some("bench-order"),
    customerId: Option.some("bench-cust"),
    items: Array.from({ length: itemCount }, (_, i) =>
      new LineItem({ sku: `SKU-${i}`, quantity: 1, price: 10 })
    ),
    totalAmount: itemCount * 10
  })

const draftWith5Items = makeDraftState(5)
const draftWith100Items = makeDraftState(100)

/** Pre-built inventory state with stock */
const stockedInventory = new InventoryState({ sku: "SKU-A", available: 10_000, reserved: 0 })

// Pre-built events and commands (avoid allocation noise in hot path)
const createdEvent = new OrderCreated({ orderId: "o1", customerId: "c1", occurredAt: DateTime.unsafeMake(0) })
const itemAddedEvent = new ItemAdded({ orderId: "o1", sku: "SKU-1", quantity: 2, price: 19.99, occurredAt: DateTime.unsafeMake(0) })
const submittedEvent = new OrderSubmitted({ orderId: "o1", occurredAt: DateTime.unsafeMake(1000) })
const createCmd = new CreateOrder({ orderId: "o1", customerId: "c1" })
const addItemCmd = new AddItem({ orderId: "o1", sku: "SKU-1", quantity: 2, price: 19.99 })
const submitCmd = new SubmitOrder({ orderId: "o1" })
const cancelCmd = new CancelOrder({ orderId: "o1", reason: "changed mind" })
const reserveCmd = new ReserveStock({ sku: "SKU-A", quantity: 1, orderId: "o1" })
const releaseCmd = new ReleaseStock({ sku: "SKU-A", quantity: 1, orderId: "o1" })

// ==========================================================================
// 1. EVOLVE — pure state transitions (synchronous)
// ==========================================================================

group("evolve (pure state transition)", () => {
  bench("OrderCreated → empty state", () => {
    evolve(emptyState, createdEvent)
  })

  bench("ItemAdded → draft state (5 items)", () => {
    evolve(draftWith5Items, itemAddedEvent)
  })

  bench("ItemAdded → draft state (100 items)", () => {
    evolve(draftWith100Items, itemAddedEvent)
  })

  bench("OrderSubmitted → draft state", () => {
    evolve(draftWith5Items, submittedEvent)
  })

  bench("StockReserved → inventory state", () => {
    Inventory.evolve(stockedInventory, new StockReserved({ sku: "SKU-A", occurredAt: DateTime.unsafeMake(0), quantity: 1, orderId: "o1" }))
  })
})

// ==========================================================================
// 2. DECIDE — command → events (effectful)
// ==========================================================================

group("decide (command → events)", () => {
  bench("CreateOrder (success)", async () => {
    await runEffect(Order.decide(emptyState, createCmd))
  })

  bench("AddItem (success, 5 items)", async () => {
    await runEffect(Order.decide(draftWith5Items, addItemCmd))
  })

  bench("SubmitOrder (success, 5 items)", async () => {
    await runEffect(Order.decide(draftWith5Items, submitCmd))
  })

  bench("CancelOrder (success)", async () => {
    await runEffect(Order.decide(draftWith5Items, cancelCmd))
  })

  bench("CreateOrder (failure — already exists)", async () => {
    await runEffect(Order.decide(draftWith5Items, createCmd).pipe(Effect.flip))
  })

  bench("SubmitOrder (failure — no items)", async () => {
    const emptyDraft = makeDraftState(0)
    await runEffect(Order.decide(emptyDraft, submitCmd).pipe(Effect.flip))
  })

  bench("ReserveStock (success)", async () => {
    await runEffect(Inventory.decide(stockedInventory, reserveCmd))
  })

  bench("ReserveStock (failure — insufficient)", async () => {
    await runEffect(Inventory.decide(initialInventoryState, reserveCmd).pipe(Effect.flip))
  })
})

// ==========================================================================
// 3. HANDLE — decide + evolve combined
// ==========================================================================

group("handle (decide + evolve)", () => {
  bench("CreateOrder", async () => {
    await runEffect(Order.handle(emptyState, createCmd))
  })

  bench("AddItem (5 existing items)", async () => {
    await runEffect(Order.handle(draftWith5Items, addItemCmd))
  })

  bench("AddItem (100 existing items)", async () => {
    await runEffect(Order.handle(draftWith100Items, addItemCmd))
  })

  bench("ReserveStock", async () => {
    await runEffect(Inventory.handle(stockedInventory, reserveCmd))
  })
})

// ==========================================================================
// 4. RUN — multi-command sequences
// ==========================================================================

group("run (multi-command sequence)", () => {
  bench("3 commands: create → add → submit", async () => {
    await runEffect(
      Order.run([createCmd, addItemCmd, submitCmd])
    )
  })

  bench("create → 10 addItems → submit", async () => {
    const items = Array.from({ length: 10 }, (_, i) =>
      new AddItem({ orderId: "o1", sku: `SKU-${i}`, quantity: 1, price: 10 })
    )
    await runEffect(Order.run([createCmd, ...items, submitCmd]))
  })

  bench("create → 100 addItems → submit", async () => {
    const items = Array.from({ length: 100 }, (_, i) =>
      new AddItem({ orderId: "o1", sku: `SKU-${i}`, quantity: 1, price: 10 })
    )
    await runEffect(Order.run([createCmd, ...items, submitCmd]))
  })

  bench("create → 1000 addItems → submit", async () => {
    const items = Array.from({ length: 1000 }, (_, i) =>
      new AddItem({ orderId: "o1", sku: `SKU-${i}`, quantity: 1, price: 10 })
    )
    await runEffect(Order.run([createCmd, ...items, submitCmd]))
  })

  bench("inventory: 50 reserve → 50 release", async () => {
    const cmds = Array.from({ length: 100 }, (_, i) =>
      i < 50
        ? new ReserveStock({ sku: "SKU-A", quantity: 1, orderId: `o-${i}` })
        : new ReleaseStock({ sku: "SKU-A", quantity: 1, orderId: `o-${i}` })
    )
    await runEffect(Inventory.run(cmds, stockedInventory))
  })
})

// ==========================================================================
// 5. CLUSTER ENTITY — end-to-end through entity layer
// ==========================================================================

group("cluster entity (end-to-end)", () => {
  const EntityBehaviorWithInfra = Layer.provide(OrderEntityLayer, InfrastructureLayer)
  const shardingConfig = ShardingConfig.layer({})

  bench("order: create → add → submit (single entity)", async () => {
    await Effect.gen(function* () {
      const makeClient = yield* Entity.makeTestClient(OrderEntity, EntityBehaviorWithInfra)
      const client = yield* makeClient("bench-order")
      yield* client.CreateOrder!({ orderId: "bench-order", customerId: "c1" })
      yield* client.AddItem!({ orderId: "bench-order", sku: "SKU-1", quantity: 1, price: 10 })
      yield* client.SubmitOrder!({ orderId: "bench-order" })
    }).pipe(
      Effect.scoped,
      Effect.provide(shardingConfig),
      Effect.runPromise
    )
  })

  bench("order: create + 10 items + submit (single entity)", async () => {
    await Effect.gen(function* () {
      const makeClient = yield* Entity.makeTestClient(OrderEntity, EntityBehaviorWithInfra)
      const client = yield* makeClient("bench-order-10")
      yield* client.CreateOrder!({ orderId: "bench-order-10", customerId: "c1" })
      for (let i = 0; i < 10; i++) {
        yield* client.AddItem!({ orderId: "bench-order-10", sku: `SKU-${i}`, quantity: 1, price: 10 })
      }
      yield* client.SubmitOrder!({ orderId: "bench-order-10" })
    }).pipe(
      Effect.scoped,
      Effect.provide(shardingConfig),
      Effect.runPromise
    )
  })

  bench("order: 5 independent entities (parallel create)", async () => {
    await Effect.gen(function* () {
      const makeClient = yield* Entity.makeTestClient(OrderEntity, EntityBehaviorWithInfra)
      yield* Effect.all(
        Array.from({ length: 5 }, (_, i) =>
          Effect.gen(function* () {
            const client = yield* makeClient(`parallel-${i}`)
            yield* client.CreateOrder!({ orderId: `parallel-${i}`, customerId: `c-${i}` })
            yield* client.AddItem!({ orderId: `parallel-${i}`, sku: "X", quantity: 1, price: 10 })
            yield* client.SubmitOrder!({ orderId: `parallel-${i}` })
          })
        ),
        { concurrency: 5 }
      )
    }).pipe(
      Effect.scoped,
      Effect.provide(shardingConfig),
      Effect.runPromise
    )
  })
})

// ==========================================================================
// 6. OBJECT CONSTRUCTION — schema class instantiation overhead
// ==========================================================================

group("schema class construction", () => {
  bench("CreateOrder (command)", () => {
    new CreateOrder({ orderId: "o1", customerId: "c1" })
  })

  bench("AddItem (command)", () => {
    new AddItem({ orderId: "o1", sku: "SKU-1", quantity: 2, price: 19.99 })
  })

  bench("OrderCreated (event)", () => {
    new OrderCreated({ orderId: "o1", customerId: "c1", occurredAt: DateTime.unsafeMake(0) })
  })

  bench("OrderState (state)", () => {
    new OrderState({
      status: "draft",
      orderId: Option.some("o1"),
      customerId: Option.some("c1"),
      items: [],
      totalAmount: 0
    })
  })

  bench("LineItem", () => {
    new LineItem({ sku: "SKU-1", quantity: 2, price: 19.99 })
  })
})

// ==========================================================================
// 7. EFFECT OVERHEAD — baseline Effect.runPromise cost
// ==========================================================================

group("effect overhead (baseline)", () => {
  bench("Effect.succeed → runPromise", async () => {
    await Effect.runPromise(Effect.succeed(42))
  })

  bench("Effect.gen (single yield)", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        return yield* Effect.succeed(42)
      })
    )
  })

  bench("Effect.gen (3 yields)", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const a = yield* Effect.succeed(1)
        const b = yield* Effect.succeed(2)
        const c = yield* Effect.succeed(3)
        return a + b + c
      })
    )
  })

  bench("DateTime.now (via Effect)", async () => {
    await Effect.runPromise(DateTime.now)
  })
})

// ==========================================================================
// Run
// ==========================================================================

await runBench()
