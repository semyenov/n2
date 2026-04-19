/**
 * Order events defined using @effect/experimental EventGroup.
 *
 * Payload schemas are derived from the TaggedClass definitions in contracts.ts
 * via eventPayloadSchema(), keeping contracts.ts as the single source of truth.
 */
import { EventGroup } from "@effect/experimental"
import * as N2 from "../../framework/helpers/index.js"
import { OrderCreated, ItemAdded, OrderSubmitted, OrderCancelled } from "./contracts.js"

export const OrderEventGroup = EventGroup.empty
  .add({
    tag: "OrderCreated",
    primaryKey: (p) => p.orderId,
    payload: N2.eventPayloadSchema(OrderCreated)
  })
  .add({
    tag: "ItemAdded",
    primaryKey: (p) => p.orderId,
    payload: N2.eventPayloadSchema(ItemAdded)
  })
  .add({
    tag: "OrderSubmitted",
    primaryKey: (p) => p.orderId,
    payload: N2.eventPayloadSchema(OrderSubmitted)
  })
  .add({
    tag: "OrderCancelled",
    primaryKey: (p) => p.orderId,
    payload: N2.eventPayloadSchema(OrderCancelled)
  })
