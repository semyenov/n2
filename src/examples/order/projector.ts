/**
 * Orders view projection: builds a read-side view from order events.
 */
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as HashMap from "effect/HashMap"
import * as ProjectorDefinition from "../../framework/projection/ProjectorDefinition.js"
import type { EventEnvelope } from "../../framework/contracts/EventEnvelope.js"
import {
  type OrderEvent,
  OrderCreated,
  ItemAdded,
  OrderSubmitted,
  OrderCancelled
} from "./contracts.js"

// ---------------------------------------------------------------------------
// Read model
// ---------------------------------------------------------------------------

export class OrderView extends Schema.Class<OrderView>("OrderView")({
  orderId: Schema.String,
  customerId: Schema.String,
  status: Schema.String,
  itemCount: Schema.Number,
  totalAmount: Schema.Number
}) {}

// ---------------------------------------------------------------------------
// Projector
// ---------------------------------------------------------------------------

export type OrdersViewState = HashMap.HashMap<string, OrderView>

/**
 * Type guard for order events. The payload stored in EventEnvelope
 * is the Type form (not encoded), so we check _tag presence.
 */
const isOrderEvent = (u: unknown): u is OrderEvent =>
  typeof u === "object" && u !== null && "_tag" in u &&
  typeof (u as Record<string, unknown>)["_tag"] === "string" &&
  ["OrderCreated", "ItemAdded", "OrderSubmitted", "OrderCancelled"].includes(
    (u as Record<string, unknown>)["_tag"] as string
  )

export const OrdersViewProjector = ProjectorDefinition.define<OrdersViewState>({
  name: "orders_view",
  initialState: HashMap.empty(),

  handle: (
    state: OrdersViewState,
    envelope: EventEnvelope
  ): Effect.Effect<OrdersViewState> =>
    Effect.sync(() => {
      if (!isOrderEvent(envelope.payload)) return state
      const event = envelope.payload

      switch (event._tag) {
        case "OrderCreated": {
          const view = new OrderView({
            orderId: event.orderId,
            customerId: event.customerId,
            status: "draft",
            itemCount: 0,
            totalAmount: 0
          })
          return HashMap.set(state, event.orderId, view)
        }
        case "ItemAdded": {
          const existing = HashMap.get(state, event.orderId)
          if (existing._tag === "None") return state
          const current = existing.value
          return HashMap.set(
            state,
            event.orderId,
            new OrderView({
              ...current,
              itemCount: current.itemCount + 1,
              totalAmount:
                current.totalAmount + event.price * event.quantity
            })
          )
        }
        case "OrderSubmitted": {
          const existing = HashMap.get(state, event.orderId)
          if (existing._tag === "None") return state
          return HashMap.set(
            state,
            event.orderId,
            new OrderView({
              ...existing.value,
              status: "submitted"
            })
          )
        }
        case "OrderCancelled": {
          const existing = HashMap.get(state, event.orderId)
          if (existing._tag === "None") return state
          return HashMap.set(
            state,
            event.orderId,
            new OrderView({
              ...existing.value,
              status: "cancelled"
            })
          )
        }
      }
    })
})
