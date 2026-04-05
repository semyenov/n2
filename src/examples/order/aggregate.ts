/**
 * Order aggregate: evolve + command procedures.
 *
 * `evolve` is pure (for event replay).
 * Each command handler produces events; `evolve` folds them into new state.
 */
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as DateTime from "effect/DateTime"
import {
  type OrderState,
  type OrderEvent,
  type OrderCommand,
  OrderState as OrderStateSchema,
  initialOrderState,
  OrderCreated,
  ItemAdded,
  OrderSubmitted,
  OrderCancelled,
  OrderError,
  LineItem,
  type CreateOrder,
  type AddItem,
  type SubmitOrder,
  type CancelOrder
} from "./contracts.js"

// ---------------------------------------------------------------------------
// Evolve (pure state transition, used for replay)
// ---------------------------------------------------------------------------

export const evolve = (state: OrderState, event: OrderEvent): OrderState => {
  switch (event._tag) {
    case "OrderCreated":
      return new OrderStateSchema({
        ...state,
        status: "draft",
        orderId: Option.some(event.orderId),
        customerId: Option.some(event.customerId)
      })
    case "ItemAdded":
      return new OrderStateSchema({
        ...state,
        items: [
          ...state.items,
          new LineItem({ sku: event.sku, quantity: event.quantity, price: event.price })
        ],
        totalAmount: state.totalAmount + event.price * event.quantity
      })
    case "OrderSubmitted":
      return new OrderStateSchema({ ...state, status: "submitted" })
    case "OrderCancelled":
      return new OrderStateSchema({ ...state, status: "cancelled" })
  }
}

// ---------------------------------------------------------------------------
// Command procedures
// ---------------------------------------------------------------------------

export const handleCreateOrder = (state: OrderState, cmd: CreateOrder) =>
  Effect.gen(function*() {
    if (state.status !== "empty") return yield* new OrderError({ message: "Order already exists" })
    const now = yield* DateTime.now
    return [new OrderCreated({ orderId: cmd.orderId, customerId: cmd.customerId, createdAt: now })]
  })

export const handleAddItem = (state: OrderState, cmd: AddItem) =>
  Effect.gen(function*() {
    if (state.status !== "draft") return yield* new OrderError({ message: `Cannot add items in status "${state.status}"` })
    return [new ItemAdded({ orderId: cmd.orderId, sku: cmd.sku, quantity: cmd.quantity, price: cmd.price })]
  })

export const handleSubmitOrder = (state: OrderState, cmd: SubmitOrder) =>
  Effect.gen(function*() {
    if (state.status !== "draft") return yield* new OrderError({ message: `Cannot submit in status "${state.status}"` })
    if (state.items.length === 0) return yield* new OrderError({ message: "Cannot submit order with no items" })
    const now = yield* DateTime.now
    return [new OrderSubmitted({ orderId: cmd.orderId, submittedAt: now })]
  })

export const handleCancelOrder = (state: OrderState, cmd: CancelOrder) =>
  Effect.gen(function*() {
    if (state.status === "cancelled") return yield* new OrderError({ message: "Order is already cancelled" })
    if (state.status === "empty") return yield* new OrderError({ message: "Order does not exist" })
    const now = yield* DateTime.now
    return [new OrderCancelled({ orderId: cmd.orderId, reason: cmd.reason, cancelledAt: now })]
  })

// ---------------------------------------------------------------------------
// Dispatch + handleCommand
// ---------------------------------------------------------------------------

export const decide = (state: OrderState, command: OrderCommand) => {
  switch (command._tag) {
    case "CreateOrder": return handleCreateOrder(state, command)
    case "AddItem": return handleAddItem(state, command)
    case "SubmitOrder": return handleSubmitOrder(state, command)
    case "CancelOrder": return handleCancelOrder(state, command)
  }
}

export const handleCommand = (state: OrderState, command: OrderCommand) =>
  Effect.gen(function*() {
    const events = yield* decide(state, command)
    let newState = state
    for (const event of events) newState = evolve(newState, event)
    return { events, state: newState }
  })

export { initialOrderState }
