/**
 * Order aggregate — pure business logic, no framework.
 *
 * Three plain functions implement the event-sourced state machine:
 *
 *   evolve : (state, event)   → state        — pure fold, no effects
 *   decide : (state, command) → Effect<events, error>  — guards + domain rules
 *   handle : (state, command) → Effect<{events, state}>  — decide then evolve
 */
import * as DateTime from "effect/DateTime"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import {
  type OrderEvent,
  type OrderCommand,
  type OrderState,
  initialOrderState,
  OrderCreated,
  ItemAdded,
  OrderSubmitted,
  OrderCancelled,
  LineItem,
  OrderError
} from "./contracts.js"

export { initialOrderState }

// ---------------------------------------------------------------------------
// Evolve — pure state transitions
//
// Called once per event. Accumulate by folding over an array of events:
//   events.reduce(evolve, state)
// ---------------------------------------------------------------------------

export const evolve = (state: OrderState, event: OrderEvent): OrderState => {
  switch (event._tag) {
    case "OrderCreated":
      return {
        ...state,
        status: "draft" as const,
        orderId: Option.some(event.orderId),
        customerId: Option.some(event.customerId)
      }
    case "ItemAdded":
      return {
        ...state,
        items: [
          ...state.items,
          new LineItem({ sku: event.sku, quantity: event.quantity, price: event.price })
        ],
        totalAmount: state.totalAmount + event.price * event.quantity
      }
    case "OrderSubmitted":
      return { ...state, status: "submitted" as const }
    case "OrderCancelled":
      return { ...state, status: "cancelled" as const, cancelledAt: Option.some(event.cancelledAt) }
  }
}

// ---------------------------------------------------------------------------
// Decide — business rules
//
// Returns the events that the command produces, or fails with OrderError.
// No infrastructure — DateTime is the only effect used here.
// GetOrder is a read — it never reaches decide; the entity layer handles it directly.
// ---------------------------------------------------------------------------

export const decide = (
  state: OrderState,
  command: OrderCommand
): Effect.Effect<ReadonlyArray<OrderEvent>, OrderError> => {
  switch (command._tag) {
    case "CreateOrder":
      return Effect.gen(function* () {
        if (state.status !== "empty") {
          return yield* new OrderError({ message: "Order already exists" })
        }
        const now = yield* DateTime.now
        return [new OrderCreated({ orderId: command.orderId, customerId: command.customerId, createdAt: now })]
      })

    case "AddItem":
      return Effect.gen(function* () {
        if (state.status !== "draft") {
          return yield* new OrderError({ message: `Cannot add items in status "${state.status}"` })
        }
        return [new ItemAdded({ orderId: command.orderId, sku: command.sku, quantity: command.quantity, price: command.price })]
      })

    case "SubmitOrder":
      return Effect.gen(function* () {
        if (state.status !== "draft") {
          return yield* new OrderError({ message: `Cannot submit in status "${state.status}"` })
        }
        if (state.items.length === 0) {
          return yield* new OrderError({ message: "Cannot submit order with no items" })
        }
        const now = yield* DateTime.now
        return [new OrderSubmitted({ orderId: command.orderId, submittedAt: now })]
      })

    case "CancelOrder":
      return Effect.gen(function* () {
        if (state.status === "empty") {
          return yield* new OrderError({ message: "Order does not exist" })
        }
        if (state.status === "cancelled") {
          return yield* new OrderError({ message: "Order is already cancelled" })
        }
        const now = yield* DateTime.now
        return [new OrderCancelled({ orderId: command.orderId, reason: command.reason, cancelledAt: now })]
      })

    case "GetOrder":
      // GetOrder is a read — it never reaches decide.
      // The entity layer reads directly from stateRef / the in-memory map.
      return Effect.succeed([])
  }
}

// ---------------------------------------------------------------------------
// Handle — decide then evolve
//
// Produces both the events and the resulting state in a single call.
// Used by dev-mode in-memory handlers and by the cluster entity layer.
// ---------------------------------------------------------------------------

export const handle = (state: OrderState, command: OrderCommand) =>
  Effect.gen(function* () {
    const events = yield* decide(state, command)
    return { events, state: events.reduce(evolve, state) }
  })
