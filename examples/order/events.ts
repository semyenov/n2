/**
 * Order EventGroup — bridges domain events to the @effect/experimental EventLog system.
 *
 * defineEvents(...).toEventGroup(...) derives each event's tag and payload
 * schema from contracts.ts, then attaches the EventLog primary key.
 *
 * OrderEventLogSchema is exported as a single instance shared by:
 *   - entity.ts  → EventLog.makeClient(OrderEventLogSchema)   (publish)
 *   - layers.ts  → EventLog.layer(OrderEventLogSchema)        (dispatch)
 * Both sides must use the same object reference so EventLog can match published
 * events to the correct dispatch watcher.
 */
import * as EventLogApi from "@effect/experimental/EventLog"
import * as N2 from "@semyenov/n2/helpers"
import {
  ItemAdded,
  OrderCancelled,
  OrderCreated,
  OrderSubmitted
} from "./contracts.js"

export const OrderEvents = N2.defineEvents(
  OrderCreated,
  ItemAdded,
  OrderSubmitted,
  OrderCancelled
)

export const OrderEventGroup = OrderEvents.toEventGroup((p) => p.orderId)

export const OrderEventLogSchema = EventLogApi.schema(OrderEventGroup)
