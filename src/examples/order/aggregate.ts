/**
 * Order aggregate: evolve + command procedures.
 *
 * `evolve` is pure (for event replay).
 * Each command handler produces events; `evolve` folds them into new state.
 * The AggregateDefinition wraps this for AggregateRuntime compatibility.
 */
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as AggregateDefinition from "../../framework/domain/AggregateDefinition.js"
import { N2Clock } from "../../framework/runtime/Clock.js"
import {
  type OrderState,
  type OrderEvent,
  type OrderCommand,
  OrderState as OrderStateSchema,
  OrderEvent as OrderEventSchema,
  OrderErrors as OrderErrorsSchema,
  initialOrderState,
  OrderCreated,
  ItemAdded,
  OrderSubmitted,
  OrderCancelled,
  OrderError,
  LineItem,
  CreateOrder,
  AddItem,
  SubmitOrder,
  CancelOrder
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
// Command procedures (produce events from state + command)
// Each procedure matches a Schema.TaggedRequest from contracts.ts
// ---------------------------------------------------------------------------

export const handleCreateOrder = (
  state: OrderState,
  cmd: CreateOrder
): Effect.Effect<ReadonlyArray<OrderEvent>, OrderError, N2Clock> =>
  Effect.gen(function* () {
    if (state.status !== "empty") {
      return yield* new OrderError({ message: "Order already exists" })
    }
    const clock = yield* N2Clock
    const now = yield* clock.now
    return [new OrderCreated({ orderId: cmd.orderId, customerId: cmd.customerId, createdAt: now })]
  })

export const handleAddItem = (
  state: OrderState,
  cmd: AddItem
): Effect.Effect<ReadonlyArray<OrderEvent>, OrderError> =>
  Effect.gen(function* () {
    if (state.status !== "draft") {
      return yield* new OrderError({ message: `Cannot add items in status "${state.status}"` })
    }
    return [new ItemAdded({ orderId: cmd.orderId, sku: cmd.sku, quantity: cmd.quantity, price: cmd.price })]
  })

export const handleSubmitOrder = (
  state: OrderState,
  cmd: SubmitOrder
): Effect.Effect<ReadonlyArray<OrderEvent>, OrderError, N2Clock> =>
  Effect.gen(function* () {
    if (state.status !== "draft") {
      return yield* new OrderError({ message: `Cannot submit in status "${state.status}"` })
    }
    if (state.items.length === 0) {
      return yield* new OrderError({ message: "Cannot submit order with no items" })
    }
    const clock = yield* N2Clock
    const now = yield* clock.now
    return [new OrderSubmitted({ orderId: cmd.orderId, submittedAt: now })]
  })

export const handleCancelOrder = (
  state: OrderState,
  cmd: CancelOrder
): Effect.Effect<ReadonlyArray<OrderEvent>, OrderError, N2Clock> =>
  Effect.gen(function* () {
    if (state.status === "cancelled") {
      return yield* new OrderError({ message: "Order is already cancelled" })
    }
    if (state.status === "empty") {
      return yield* new OrderError({ message: "Order does not exist" })
    }
    const clock = yield* N2Clock
    const now = yield* clock.now
    return [new OrderCancelled({ orderId: cmd.orderId, reason: cmd.reason, cancelledAt: now })]
  })

// ---------------------------------------------------------------------------
// Dispatch (routes a command union to the right handler)
// ---------------------------------------------------------------------------

export const decide = (
  state: OrderState,
  command: OrderCommand
): Effect.Effect<ReadonlyArray<OrderEvent>, OrderError, N2Clock> => {
  switch (command._tag) {
    case "CreateOrder": return handleCreateOrder(state, command)
    case "AddItem": return handleAddItem(state, command)
    case "SubmitOrder": return handleSubmitOrder(state, command)
    case "CancelOrder": return handleCancelOrder(state, command)
  }
}

// ---------------------------------------------------------------------------
// handleCommand: decide + evolve in one step
// This is the primary API for entity handlers and tests.
// ---------------------------------------------------------------------------

export const handleCommand = (
  state: OrderState,
  command: OrderCommand
): Effect.Effect<
  { readonly events: ReadonlyArray<OrderEvent>; readonly state: OrderState },
  OrderError,
  N2Clock
> =>
  Effect.gen(function*() {
    const events = yield* decide(state, command)
    let newState = state
    for (const event of events) {
      newState = evolve(newState, event)
    }
    return { events, state: newState }
  })

// ---------------------------------------------------------------------------
// AggregateDefinition (kept for backward compat with AggregateRuntime)
// ---------------------------------------------------------------------------

/** @deprecated Use handleCommand + decide + evolve directly */
export const OrderAggregate = AggregateDefinition.define({
  name: "Order" as const,
  initialState: initialOrderState,
  decide,
  evolve,
  schemas: {
    state: OrderStateSchema,
    command: Schema.Union(CreateOrder, AddItem, SubmitOrder, CancelOrder),
    event: OrderEventSchema,
    error: OrderErrorsSchema
  }
})
