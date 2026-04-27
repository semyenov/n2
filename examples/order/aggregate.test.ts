/**
 * Order service tests.
 *
 * Two layers of coverage:
 *
 *   1. Pure aggregate  — handle/evolve with no infrastructure.
 *   2. Handler layer   — RpcTest.makeClient wired to in-memory services.
 *
 * No PostgreSQL required: EventJournal.layerMemory, WorkflowEngine.layerMemory,
 * and an in-memory Map for snapshots replace all SQL dependencies.
 *
 * RpcTest.makeClient(group) creates a direct-call client: it bypasses HTTP and
 * calls handler functions from the Layer in-process. Requires Scope (from
 * Effect.scoped) + Rpc.ToHandler<Rpcs> (from handlersLayer).
 */
import { test, expect } from "bun:test"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as ExpEventJournal from "@effect/experimental/EventJournal"
import { EventLog, Identity } from "@effect/experimental/EventLog"
import * as EventLogApi from "@effect/experimental/EventLog"
import { EventLog as EL } from "@effect/experimental"
import { WorkflowEngine } from "@effect/workflow"
import { RpcTest } from "@effect/rpc"
import { handle, initialOrderState } from "./aggregate.js"
import {
  CreateOrder, AddItem, SubmitOrder, CancelOrder,
  OrderState, LineItem, OrderError, OrderNotFound,
  OrderRpcs
} from "./contracts.js"
import { OrderEventGroup, OrderEventLogSchema } from "./events.js"
import { OrderSnapshots, type SnapshotEntry } from "./snapshots.js"
import { OrderFulfillmentHandlers } from "./workflows.js"
import { OrderHandlersRaw } from "./entity.js"

// ---------------------------------------------------------------------------
// Test infrastructure
//
// Named constants ensure Effect deduplicates shared layers in the layer graph
// (same pattern as the production InfrastructureLayer in layers.ts).
//
// NoOpProjection: publish events into the memory journal without SQL upserts.
// makeTestLayers(): isolated snapshot store + handlers layer per test.
// ---------------------------------------------------------------------------

const NoOpProjection = EL.group(
  OrderEventGroup,
  (handlers) =>
    handlers
      .handle("OrderCreated",   (_) => Effect.void)
      .handle("ItemAdded",      (_) => Effect.void)
      .handle("OrderSubmitted", (_) => Effect.void)
      .handle("OrderCancelled", (_) => Effect.void)
)

const testJournalLayer  = ExpEventJournal.layerMemory
const testIdentityLayer = Layer.succeed(Identity, Identity.makeRandom())
const testWorkflowLayer = Layer.provideMerge(OrderFulfillmentHandlers, WorkflowEngine.layerMemory)

const testEventLogLayer = EventLogApi.layer(OrderEventLogSchema).pipe(
  Layer.provide(NoOpProjection),
  Layer.provide(Layer.merge(testJournalLayer, testIdentityLayer))
)

const makeTestLayers = () => {
  const snapshotStore = new Map<string, SnapshotEntry>()

  const snapshotsLayer = Layer.succeed(OrderSnapshots, {
    load:  (orderId) => Effect.succeed(Option.fromNullable(snapshotStore.get(orderId))),
    save:  (orderId, state, revision) =>
      Effect.sync(() => { snapshotStore.set(orderId, { state, revision }) })
  })

  const infraLayer = Layer.mergeAll(
    testJournalLayer,
    testIdentityLayer,
    testWorkflowLayer,
    testEventLogLayer,
    snapshotsLayer
  )

  return {
    snapshotStore,
    handlersLayer: Layer.provide(OrderHandlersRaw, infraLayer)
  }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// Pure tests — no layer requirements.
const run = <A, E>(effect: Effect.Effect<A, E, never>): Promise<A> =>
  Effect.runPromise(effect)

const runProvided = <A, E, R>(
  handlersLayer: Layer.Layer<never, never, never>,
  program: Effect.Effect<A, E, R>
): Promise<A> =>
  Effect.runPromise(
    // The integration test layer fully provides RpcTest handlers and Scope, but
    // the generic program/layer relationship is assembled dynamically here.
    Effect.provide(Effect.scoped(program), handlersLayer) as unknown as Effect.Effect<A, E, never>
  )

// Integration tests — scoped client + provided handlers layer.
const runWith = <A, E, R>(
  handlersLayer: Layer.Layer<never, never, never>,
  program: Effect.Effect<A, E, R>
): Promise<A> =>
  runProvided(handlersLayer, program)

// ---------------------------------------------------------------------------
// 1 — Pure aggregate: handle / evolve
// ---------------------------------------------------------------------------

test("CreateOrder produces draft state", async () => {
  const { state, events } = await run(
    handle(initialOrderState, new CreateOrder({ orderId: "o-1", customerId: "c-1" }))
  )
  expect(state.status).toBe("draft")
  expect(Option.getOrThrow(state.orderId)).toBe("o-1")
  expect(Option.getOrThrow(state.customerId)).toBe("c-1")
  expect(events.length).toBe(1)
  expect(events[0]!._tag).toBe("OrderCreated")
})

test("AddItem accumulates items and totalAmount", async () => {
  const { state: s1 } = await run(handle(initialOrderState, new CreateOrder({ orderId: "o-2", customerId: "c-1" })))
  const { state: s2 } = await run(handle(s1, new AddItem({ orderId: "o-2", sku: "A", quantity: 2, price: 10 })))
  const { state: s3 } = await run(handle(s2, new AddItem({ orderId: "o-2", sku: "B", quantity: 1, price: 5 })))
  expect(s3.items.length).toBe(2)
  expect(s3.totalAmount).toBe(25)
})

test("SubmitOrder with items produces submitted state", async () => {
  const { state: s1 } = await run(handle(initialOrderState, new CreateOrder({ orderId: "o-3", customerId: "c-1" })))
  const { state: s2 } = await run(handle(s1, new AddItem({ orderId: "o-3", sku: "X", quantity: 1, price: 1 })))
  const { state, events } = await run(handle(s2, new SubmitOrder({ orderId: "o-3" })))
  expect(state.status).toBe("submitted")
  expect(events[0]!._tag).toBe("OrderSubmitted")
})

test("SubmitOrder with no items fails with OrderError", async () => {
  const { state: s1 } = await run(handle(initialOrderState, new CreateOrder({ orderId: "o-4", customerId: "c-1" })))
  const err = await run(handle(s1, new SubmitOrder({ orderId: "o-4" })).pipe(Effect.flip))
  expect(err._tag).toBe("OrderError")
  expect((err as OrderError).message).toMatch(/no items/)
})

test("CreateOrder on existing order fails", async () => {
  const { state: s1 } = await run(handle(initialOrderState, new CreateOrder({ orderId: "o-5", customerId: "c-1" })))
  const err = await run(handle(s1, new CreateOrder({ orderId: "o-5", customerId: "c-2" })).pipe(Effect.flip))
  expect(err._tag).toBe("OrderError")
})

test("CancelOrder on cancelled order fails", async () => {
  const { state: s1 } = await run(handle(initialOrderState, new CreateOrder({ orderId: "o-6", customerId: "c-1" })))
  const { state: s2 } = await run(handle(s1, new CancelOrder({ orderId: "o-6", reason: "reason" })))
  expect(s2.status).toBe("cancelled")
  expect(Option.isSome(s2.cancelledAt)).toBe(true)
  const err = await run(handle(s2, new CancelOrder({ orderId: "o-6", reason: "again" })).pipe(Effect.flip))
  expect(err._tag).toBe("OrderError")
})

// ---------------------------------------------------------------------------
// 2 — Handler layer integration: state persists across calls in the same layer
//
// RpcTest.makeClient creates an in-process client backed by the handler layer.
// Each test gets its own makeTestLayers() call → fresh SynchronizedRef + Map.
// ---------------------------------------------------------------------------

test("handlers: Create → AddItem → GetOrder persists state", async () => {
  const { handlersLayer } = makeTestLayers()
  await runWith(handlersLayer, Effect.gen(function* () {
    const client = yield* RpcTest.makeClient(OrderRpcs)
    yield* client.CreateOrder({ orderId: "h-1", customerId: "c-1" })
    yield* client.AddItem({ orderId: "h-1", sku: "SKU-1", quantity: 3, price: 10 })
    const state = yield* client.GetOrder({ orderId: "h-1" })
    expect(state.status).toBe("draft")
    expect(state.items.length).toBe(1)
    expect(state.totalAmount).toBe(30)
  }))
})

test("handlers: Submit changes status to submitted", async () => {
  const { handlersLayer } = makeTestLayers()
  await runWith(handlersLayer, Effect.gen(function* () {
    const client = yield* RpcTest.makeClient(OrderRpcs)
    yield* client.CreateOrder({ orderId: "h-2", customerId: "c-1" })
    yield* client.AddItem({ orderId: "h-2", sku: "X", quantity: 1, price: 5 })
    const result = yield* client.SubmitOrder({ orderId: "h-2" })
    expect(result.orderId).toBe("h-2")
    const state = yield* client.GetOrder({ orderId: "h-2" })
    expect(state.status).toBe("submitted")
  }))
})

test("handlers: GetOrder for unknown order returns OrderNotFound", async () => {
  const { handlersLayer } = makeTestLayers()
  await runWith(handlersLayer, Effect.gen(function* () {
    const client = yield* RpcTest.makeClient(OrderRpcs)
    const err = yield* client.GetOrder({ orderId: "unknown" }).pipe(Effect.flip)
    expect(err._tag).toBe("OrderNotFound")
    if (err._tag === "OrderNotFound") {
      expect(err.orderId).toBe("unknown")
    }
  }))
})

test("handlers: SubmitOrder with no items fails", async () => {
  const { handlersLayer } = makeTestLayers()
  await runWith(handlersLayer, Effect.gen(function* () {
    const client = yield* RpcTest.makeClient(OrderRpcs)
    yield* client.CreateOrder({ orderId: "h-3", customerId: "c-1" })
    const err = yield* client.SubmitOrder({ orderId: "h-3" }).pipe(Effect.flip)
    expect(err._tag).toBe("OrderError")
  }))
})

test("handlers: Cancel submitted order succeeds", async () => {
  const { handlersLayer } = makeTestLayers()
  await runWith(handlersLayer, Effect.gen(function* () {
    const client = yield* RpcTest.makeClient(OrderRpcs)
    yield* client.CreateOrder({ orderId: "h-4", customerId: "c-1" })
    yield* client.AddItem({ orderId: "h-4", sku: "Y", quantity: 1, price: 1 })
    yield* client.SubmitOrder({ orderId: "h-4" })
    yield* client.CancelOrder({ orderId: "h-4", reason: "changed mind" })
    const state = yield* client.GetOrder({ orderId: "h-4" })
    expect(state.status).toBe("cancelled")
  }))
})

// ---------------------------------------------------------------------------
// 3 — Snapshot recovery: GetOrder loads from snapshot when not in memory
//
// Pre-seed snapshotStore directly (simulates a server restart).
// The handler's SynchronizedRef starts empty — it must fall back to the snapshot.
// ---------------------------------------------------------------------------

test("snapshot recovery: GetOrder loads state from snapshot store", async () => {
  const { snapshotStore, handlersLayer } = makeTestLayers()

  snapshotStore.set("snap-1", {
    state: new OrderState({
      status: "draft",
      orderId: Option.some("snap-1"),
      customerId: Option.some("c-99"),
      items: [new LineItem({ sku: "SKU-SNAP", quantity: 2, price: 15 })],
      totalAmount: 30,
      cancelledAt: Option.none()
    }),
    revision: 10
  })

  await runWith(handlersLayer, Effect.gen(function* () {
    const client = yield* RpcTest.makeClient(OrderRpcs)
    const state = yield* client.GetOrder({ orderId: "snap-1" })
    expect(state.status).toBe("draft")
    expect(state.items.length).toBe(1)
    expect(state.totalAmount).toBe(30)
    expect(Option.getOrThrow(state.customerId)).toBe("c-99")
  }))
})
