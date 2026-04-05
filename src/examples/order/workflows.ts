/**
 * Order fulfillment saga: orchestrates multi-step order processing
 * using @effect/workflow for durable execution.
 *
 * Demonstrates cross-aggregate coordination:
 * 1. Reserve inventory (Inventory aggregate)
 * 2. Charge payment
 * 3. Schedule shipping
 * With compensation on failure (release inventory, refund payment).
 */
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import { Workflow, Activity, DurableClock } from "@effect/workflow"

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

export class FulfillmentError extends Schema.TaggedError<FulfillmentError>()(
  "FulfillmentError",
  { message: Schema.String }
) {}

// ---------------------------------------------------------------------------
// Workflow definition
// ---------------------------------------------------------------------------

export const OrderFulfillmentWorkflow = Workflow.make({
  name: "OrderFulfillment",
  payload: { orderId: Schema.String, sku: Schema.String, quantity: Schema.Number },
  success: Schema.Struct({
    orderId: Schema.String,
    shipped: Schema.Boolean
  }),
  error: FulfillmentError,
  idempotencyKey: (payload) => payload.orderId
})

// ---------------------------------------------------------------------------
// Activities
// ---------------------------------------------------------------------------

export const ReserveInventory = Activity.make({
  name: "ReserveInventory",
  error: FulfillmentError,
  execute: Effect.gen(function*() {
    // In production: call Inventory entity via cluster client
    // yield* inventoryClient(sku).ReserveStock({ sku, quantity, orderId })
    yield* Effect.log("Reserving inventory...")
    return { reserved: true }
  })
})

export const ChargePayment = Activity.make({
  name: "ChargePayment",
  error: FulfillmentError,
  execute: Effect.gen(function*() {
    yield* Effect.log("Charging payment...")
    return { charged: true }
  })
})

export const ScheduleShipping = Activity.make({
  name: "ScheduleShipping",
  error: FulfillmentError,
  execute: Effect.gen(function*() {
    yield* Effect.log("Scheduling shipping...")
    return { scheduled: true }
  })
})

export const ReleaseInventory = Activity.make({
  name: "ReleaseInventory",
  error: FulfillmentError,
  execute: Effect.gen(function*() {
    yield* Effect.log("Releasing inventory (compensation)...")
    return { released: true }
  })
})

export const RefundPayment = Activity.make({
  name: "RefundPayment",
  error: FulfillmentError,
  execute: Effect.gen(function*() {
    yield* Effect.log("Refunding payment (compensation)...")
    return { refunded: true }
  })
})

// ---------------------------------------------------------------------------
// Workflow implementation
// ---------------------------------------------------------------------------

export const OrderFulfillmentHandlers = OrderFulfillmentWorkflow.toLayer(
  (payload, _executionId) =>
    Effect.gen(function*() {
      yield* Effect.log(`Starting fulfillment for order ${payload.orderId}`)

      // Step 1: Reserve inventory (with compensation)
      yield* ReserveInventory.pipe(
        OrderFulfillmentWorkflow.withCompensation(() =>
          ReleaseInventory.pipe(Effect.ignore)
        )
      )

      // Step 2: Charge payment (with compensation)
      yield* ChargePayment.pipe(
        OrderFulfillmentWorkflow.withCompensation(() =>
          RefundPayment.pipe(Effect.ignore)
        )
      )

      // Step 3: Wait before shipping (durable sleep)
      yield* DurableClock.sleep({
        name: "pre-shipping-delay",
        duration: "5 seconds"
      })

      // Step 4: Schedule shipping (no compensation -- point of no return)
      yield* ScheduleShipping

      yield* Effect.log(`Fulfillment complete for order ${payload.orderId}`)

      return { orderId: payload.orderId, shipped: true as const }
    })
)
