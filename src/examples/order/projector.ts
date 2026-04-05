/**
 * Typed native projector using @effect/experimental EventLog.group.
 *
 * Each event handler receives fully typed payload -- no runtime type checking.
 * Compare with projector.ts which uses raw `envelope.payload` casting.
 */
import * as Effect from "effect/Effect"
import * as HashMap from "effect/HashMap"
import * as Schema from "effect/Schema"
import { EventLog } from "@effect/experimental"
import { OrderEventGroup } from "./events.js"

// ---------------------------------------------------------------------------
// Read model (same as projector.ts)
// ---------------------------------------------------------------------------

export class OrderView extends Schema.Class<OrderView>("OrderView")({
  orderId: Schema.String,
  customerId: Schema.String,
  status: Schema.String,
  itemCount: Schema.Number,
  totalAmount: Schema.Number
}) { }

// ---------------------------------------------------------------------------
// Typed event handlers via EventLog.group
//
// Each handler receives `{ payload, entry, conflicts }` with full type
// inference on `payload` -- no casts, no type guards, no runtime checks.
// ---------------------------------------------------------------------------

export const OrderProjectionHandlers = EventLog.group(
  OrderEventGroup,
  (handlers) =>
    handlers
      .handle("OrderCreated", ({ payload }) =>
        Effect.gen(function* () {
          yield* Effect.log(`Order created: ${payload.orderId} for ${payload.customerId}`)
        })
      )
      .handle("ItemAdded", ({ payload }) =>
        Effect.gen(function* () {
          yield* Effect.log(`Item added to ${payload.orderId}: ${payload.sku} x${payload.quantity}`)
        })
      )
      .handle("OrderSubmitted", ({ payload }) =>
        Effect.gen(function* () {
          yield* Effect.log(`Order submitted: ${payload.orderId}`)
        })
      )
      .handle("OrderCancelled", ({ payload }) =>
        Effect.gen(function* () {
          yield* Effect.log(`Order cancelled: ${payload.orderId} -- ${payload.reason}`)
        })
      )
)
