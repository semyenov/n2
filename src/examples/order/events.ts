/**
 * Order events defined using @effect/experimental EventGroup.
 *
 * This is the preferred way to define events when using the experimental
 * EventLog system. The EventGroup provides:
 * - Typed event definitions with primaryKey
 * - Built-in MsgPack serialization
 * - EventLog.group() for handler registration
 * - Compaction and reactivity support
 */
import * as Schema from "effect/Schema"
import { EventGroup } from "@effect/experimental"

/**
 * Order event payloads defined as Effect Schemas.
 */
export const OrderCreatedPayload = Schema.Struct({
  orderId: Schema.String,
  customerId: Schema.String,
  createdAt: Schema.DateTimeUtc
})

export const ItemAddedPayload = Schema.Struct({
  orderId: Schema.String,
  sku: Schema.String,
  quantity: Schema.Number,
  price: Schema.Number
})

export const OrderSubmittedPayload = Schema.Struct({
  orderId: Schema.String,
  submittedAt: Schema.DateTimeUtc
})

export const OrderCancelledPayload = Schema.Struct({
  orderId: Schema.String,
  reason: Schema.String,
  cancelledAt: Schema.DateTimeUtc
})

/**
 * Order events as an @effect/experimental EventGroup.
 *
 * @example
 * ```ts
 * // Define handlers:
 * const OrderEventHandlers = EventLog.group(OrderEventGroup, (handlers) =>
 *   handlers
 *     .handle("OrderCreated", ({ payload }) => Effect.gen(function*() { ... }))
 *     .handle("ItemAdded", ({ payload }) => Effect.gen(function*() { ... }))
 *     .handle("OrderSubmitted", ({ payload }) => Effect.gen(function*() { ... }))
 *     .handle("OrderCancelled", ({ payload }) => Effect.gen(function*() { ... }))
 * )
 *
 * // Use client:
 * const client = yield* EventLog.makeClient(schema)
 * yield* client("OrderCreated", { orderId: "123", customerId: "456", createdAt: now })
 * ```
 */
export const OrderEventGroup = EventGroup.empty
  .add({
    tag: "OrderCreated",
    primaryKey: (p: typeof OrderCreatedPayload.Type) => p.orderId,
    payload: OrderCreatedPayload
  })
  .add({
    tag: "ItemAdded",
    primaryKey: (p: typeof ItemAddedPayload.Type) => p.orderId,
    payload: ItemAddedPayload
  })
  .add({
    tag: "OrderSubmitted",
    primaryKey: (p: typeof OrderSubmittedPayload.Type) => p.orderId,
    payload: OrderSubmittedPayload
  })
  .add({
    tag: "OrderCancelled",
    primaryKey: (p: typeof OrderCancelledPayload.Type) => p.orderId,
    payload: OrderCancelledPayload
  })
