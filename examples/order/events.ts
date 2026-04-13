/**
 * Order EventGroup — bridges domain events to the @effect/experimental EventLog system.
 *
 * EventGroup.empty.add(...) defines each event's tag, primaryKey, and payload schema.
 * The primaryKey determines which EventLog stream an event belongs to (one per orderId).
 *
 * This is separate from contracts.ts because EventGroup uses its own schema types
 * (plain Schema.Struct fields) rather than Schema.TaggedClass instances.
 * The payloads are structurally identical — just not the same class objects.
 *
 * OrderEventLogSchema is exported as a single instance shared by:
 *   - entity.ts  → EventLog.makeClient(OrderEventLogSchema)   (publish)
 *   - layers.ts  → EventLog.layer(OrderEventLogSchema)        (dispatch)
 * Both sides must use the same object reference so EventLog can match published
 * events to the correct dispatch watcher.
 */
import * as Schema from "effect/Schema"
import { EventGroup } from "@effect/experimental"
import * as EventLogApi from "@effect/experimental/EventLog"

export const OrderEventGroup = EventGroup.empty
  .add({
    tag: "OrderCreated",
    primaryKey: (p: { orderId: string }) => p.orderId,
    payload: Schema.Struct({
      orderId: Schema.String,
      customerId: Schema.String,
      createdAt: Schema.DateTimeUtc
    })
  })
  .add({
    tag: "ItemAdded",
    primaryKey: (p: { orderId: string }) => p.orderId,
    payload: Schema.Struct({
      orderId: Schema.String,
      sku: Schema.String,
      quantity: Schema.Number,
      price: Schema.Number
    })
  })
  .add({
    tag: "OrderSubmitted",
    primaryKey: (p: { orderId: string }) => p.orderId,
    payload: Schema.Struct({
      orderId: Schema.String,
      submittedAt: Schema.DateTimeUtc
    })
  })
  .add({
    tag: "OrderCancelled",
    primaryKey: (p: { orderId: string }) => p.orderId,
    payload: Schema.Struct({
      orderId: Schema.String,
      reason: Schema.String,
      cancelledAt: Schema.DateTimeUtc
    })
  })

export const OrderEventLogSchema = EventLogApi.schema(OrderEventGroup)
