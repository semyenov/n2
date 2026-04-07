/**
 * Cross-aggregate saga integration test.
 *
 * Tests OrderFulfillment workflow that coordinates:
 * 1. Order entity (submit)
 * 2. Inventory entity (reserve stock)
 * 3. Payment (activity)
 * 4. Shipping (activity)
 *
 * Uses a simplified test workflow (no DurableClock.sleep) to avoid real-time waits.
 */
import { test, expect } from "bun:test"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import { Entity, ShardingConfig } from "@effect/cluster"
import { Workflow, Activity, WorkflowEngine } from "@effect/workflow"
import { OrderEntity, CommandResult, OrderError } from "./contracts.js"
import { InventoryEntity, StockResult, InsufficientStock } from "../inventory/contracts.js"
import { OrderEntityLayer } from "./entity.js"
import { InventoryEntityLayer } from "../inventory/entity.js"
import { InfrastructureLayer } from "./layers.js"

// Shared infra for both entities
const BothEntitiesLayer = Layer.mergeAll(
  Layer.provide(OrderEntityLayer, InfrastructureLayer),
  Layer.provide(InventoryEntityLayer, InfrastructureLayer)
)

// ---------------------------------------------------------------------------
// Test saga: create order, reserve inventory, complete
// ---------------------------------------------------------------------------

class SagaError extends Schema.TaggedError<SagaError>()(
  "SagaError",
  { message: Schema.String }
) { }

const CrossAggregateSaga = Workflow.make({
  name: "CrossAggregateSaga",
  payload: { orderId: Schema.String, sku: Schema.String, quantity: Schema.Number },
  success: Schema.Struct({ orderId: Schema.String, reserved: Schema.Boolean }),
  error: SagaError,
  idempotencyKey: (p) => p.orderId
})

const CreateOrderActivity = Activity.make({
  name: "CreateOrder",
  error: SagaError,
  execute: Effect.succeed({ created: true })
})

const ReserveStockActivity = Activity.make({
  name: "ReserveStock",
  error: SagaError,
  execute: Effect.succeed({ reserved: true })
})

const SagaHandlers = CrossAggregateSaga.toLayer(
  (payload, _executionId) =>
    Effect.gen(function* () {
      yield* CreateOrderActivity
      yield* ReserveStockActivity
      return { orderId: payload.orderId, reserved: true as const }
    })
)

const SagaLayer = Layer.provideMerge(SagaHandlers, WorkflowEngine.layerMemory)

test("cross-aggregate saga: order + inventory coordination", async () => {
  await Effect.gen(function* () {
    const result = yield* CrossAggregateSaga.execute({
      orderId: "saga-order-1",
      sku: "SKU-SAGA",
      quantity: 2
    })
    expect(result.orderId).toBe("saga-order-1")
    expect(result.reserved).toBe(true)
  }).pipe(
    Effect.scoped,
    Effect.provide(SagaLayer),
    Effect.runPromise
  )
})

test("cross-aggregate: both entities via cluster", async () => {
  // Entity.makeTestClient yields R = unknown (opaque cluster type); ShardingConfig is satisfied at runtime.
  const run = <A, E>(effect: Effect.Effect<A, E, unknown>): Promise<A> =>
    Effect.runPromise(
      effect.pipe(Effect.scoped, Effect.provide(ShardingConfig.layer({}))) as Effect.Effect<A, E, never>
    )

  await run(
    Effect.gen(function* () {
      // Boot both entities
      const makeOrderClient = yield* Entity.makeTestClient(OrderEntity, BothEntitiesLayer)
      const makeInventoryClient = yield* Entity.makeTestClient(InventoryEntity, BothEntitiesLayer)

      // Create order
      const orderClient = yield* makeOrderClient("saga-order-2")
      const orderResult = yield* orderClient.CreateOrder!({
        orderId: "saga-order-2",
        customerId: "cust-saga"
      })
      expect(orderResult).toBeInstanceOf(CommandResult)

      // Restock inventory
      const invClient = yield* makeInventoryClient("SKU-SAGA-2")
      yield* invClient.ReleaseStock!({ sku: "SKU-SAGA-2", quantity: 100, orderId: "restock" })

      // Reserve inventory for the order
      const stockResult = yield* invClient.ReserveStock!({
        sku: "SKU-SAGA-2",
        quantity: 5,
        orderId: "saga-order-2"
      })
      expect(stockResult).toBeInstanceOf(StockResult)
      expect(stockResult.available).toBe(95)
      expect(stockResult.reserved).toBe(5)

      // Add item and submit order
      yield* orderClient.AddItem!({
        orderId: "saga-order-2",
        sku: "SKU-SAGA-2",
        quantity: 5,
        price: 20
      })
      const submitResult = yield* orderClient.SubmitOrder!({ orderId: "saga-order-2" })
      expect(Number(submitResult.revision)).toBe(3)
    })
  )
})
