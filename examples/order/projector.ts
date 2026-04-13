/**
 * Order event projection — materialises a read model from the EventLog stream.
 *
 * EventLog.group registers handlers that run whenever a matching event is
 * appended to the journal (SqlEventJournal in production, layerMemory in tests).
 *
 * These handlers are stubs that log each event. In production, replace the
 * Effect.log calls with SQL upserts into a denormalised `orders_read` table
 * so queries can read directly without replaying the event stream.
 *
 * The layer returned by EventLog.group requires EventLog and Identity (both
 * provided by InfrastructureLayer / ClusterInfrastructureLayer).
 */
import * as Effect from "effect/Effect"
import { EventLog } from "@effect/experimental"
import { OrderEventGroup } from "./events.js"

export const OrderProjectionLayer = EventLog.group(
  OrderEventGroup,
  (handlers) =>
    handlers
      .handle("OrderCreated", ({ payload }) =>
        Effect.log(`[projection] order created: ${payload.orderId} for customer ${payload.customerId}`)
      )
      .handle("ItemAdded", ({ payload }) =>
        Effect.log(`[projection] item added to ${payload.orderId}: ${payload.sku} ×${payload.quantity} @ ${payload.price}`)
      )
      .handle("OrderSubmitted", ({ payload }) =>
        Effect.log(`[projection] order submitted: ${payload.orderId}`)
      )
      .handle("OrderCancelled", ({ payload }) =>
        Effect.log(`[projection] order cancelled: ${payload.orderId} — ${payload.reason}`)
      )
)
