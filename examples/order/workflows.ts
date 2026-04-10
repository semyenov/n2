/**
 * Order fulfillment saga.
 *
 * Demonstrates @effect/workflow for durable multi-step orchestration:
 *
 *   1. Reserve inventory  ← compensated by: Release inventory
 *   2. Charge payment     ← compensated by: Refund payment
 *   3. DurableClock.sleep — durable pause (no thread held; survives restarts)
 *   4. Schedule shipping  ← no compensation (point of no return)
 *
 * Compensation runs automatically if any step after it fails.
 * Activities are idempotent — safe to replay on retry.
 */
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import { Activity, DurableClock, Workflow } from "@effect/workflow"

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

export class FulfillmentError extends Schema.TaggedError<FulfillmentError>()(
  "FulfillmentError",
  { message: Schema.String }
) {}

// ---------------------------------------------------------------------------
// Workflow definition
//
// idempotencyKey ensures a second execute() call with the same orderId
// joins the running workflow instead of starting a duplicate.
// ---------------------------------------------------------------------------

export const OrderFulfillmentWorkflow = Workflow.make({
  name: "OrderFulfillment",
  payload: { orderId: Schema.String, sku: Schema.String, quantity: Schema.Number },
  success: Schema.Struct({ orderId: Schema.String, shipped: Schema.Boolean }),
  error: FulfillmentError,
  idempotencyKey: (payload) => payload.orderId
})

// ---------------------------------------------------------------------------
// Activities (forward steps)
// ---------------------------------------------------------------------------

export const ReserveInventory = Activity.make({
  name: "ReserveInventory",
  error: FulfillmentError,
  execute: Effect.gen(function* () {
    yield* Effect.log("Reserving inventory...")
    // In production: yield* InventoryEntity.reserve({ sku, quantity, orderId })
    return { reserved: true }
  })
})

export const ChargePayment = Activity.make({
  name: "ChargePayment",
  error: FulfillmentError,
  execute: Effect.gen(function* () {
    yield* Effect.log("Charging payment...")
    return { charged: true }
  })
})

export const ScheduleShipping = Activity.make({
  name: "ScheduleShipping",
  error: FulfillmentError,
  execute: Effect.gen(function* () {
    yield* Effect.log("Scheduling shipping...")
    return { scheduled: true }
  })
})

// ---------------------------------------------------------------------------
// Compensation activities (run automatically on failure)
// ---------------------------------------------------------------------------

export const ReleaseInventory = Activity.make({
  name: "ReleaseInventory",
  error: FulfillmentError,
  execute: Effect.gen(function* () {
    yield* Effect.log("Releasing inventory (compensation)...")
    return { released: true }
  })
})

export const RefundPayment = Activity.make({
  name: "RefundPayment",
  error: FulfillmentError,
  execute: Effect.gen(function* () {
    yield* Effect.log("Refunding payment (compensation)...")
    return { refunded: true }
  })
})

// ---------------------------------------------------------------------------
// Workflow implementation
// ---------------------------------------------------------------------------

export const OrderFulfillmentHandlers = OrderFulfillmentWorkflow.toLayer(
  (payload, _executionId) =>
    Effect.gen(function* () {
      yield* Effect.log(`Starting fulfillment for order ${payload.orderId}`)

      // Step 1: Reserve inventory
      // withCompensation registers ReleaseInventory to run if a later step fails.
      yield* ReserveInventory.pipe(
        OrderFulfillmentWorkflow.withCompensation(() =>
          ReleaseInventory.pipe(Effect.ignore)
        )
      )

      // Step 2: Charge payment
      yield* ChargePayment.pipe(
        OrderFulfillmentWorkflow.withCompensation(() =>
          RefundPayment.pipe(Effect.ignore)
        )
      )

      // Step 3: Durable sleep
      // The workflow fiber suspends and releases its thread.
      // After the duration, it resumes — even if the process restarted.
      yield* DurableClock.sleep({ name: "pre-shipping-delay", duration: "5 seconds" })

      // Step 4: Point of no return — no compensation registered
      yield* ScheduleShipping

      yield* Effect.log(`Fulfillment complete for order ${payload.orderId}`)

      return { orderId: payload.orderId, shipped: true as const }
    })
)
