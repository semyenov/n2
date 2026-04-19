/**
 * Order aggregate.
 */
import * as DateTime from "effect/DateTime"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as N2 from "../../framework/helpers/index.js"
import * as C from "./contracts.js"

export const Order = N2.define<C.OrderEvent, C.OrderCommand>()({
  initialState: C.initialOrderState,
  commands: C.OrderCommands.constructors,
  evolve: {
    OrderCreated: (state, event) => ({
      ...state,
      status: "draft" as const,
      orderId: Option.some(event.orderId),
      customerId: Option.some(event.customerId)
    }),
    ItemAdded: (state, event) => ({
      ...state,
      items: [
        ...state.items,
        new C.LineItem({
          sku: event.sku,
          quantity: event.quantity,
          price: event.price
        })
      ],
      totalAmount: state.totalAmount + event.price * event.quantity
    }),
    OrderSubmitted: (state) => ({
      ...state,
      status: "submitted" as const
    }),
    OrderCancelled: (state) => ({
      ...state,
      status: "cancelled" as const
    })
  },
  decide: {
    CreateOrder: (state, command) =>
      Effect.gen(function* () {
        if (state.status !== "empty") {
          return yield* new C.OrderError({ message: "Order already exists" })
        }

        const now = yield* DateTime.now
        return [
          new C.OrderCreated({
            orderId: command.orderId,
            customerId: command.customerId,
            occurredAt: now
          })
        ]
      }),
    AddItem: (state, command) =>
      Effect.gen(function* () {
        if (state.status !== "draft") {
          return yield* new C.OrderError({
            message: `Cannot add items in status "${state.status}"`
          })
        }

        const now = yield* DateTime.now
        return [
          new C.ItemAdded({
            orderId: command.orderId,
            occurredAt: now,
            sku: command.sku,
            quantity: command.quantity,
            price: command.price
          })
        ]
      }),
    SubmitOrder: (state, command) =>
      Effect.gen(function* () {
        if (state.status !== "draft") {
          return yield* new C.OrderError({
            message: `Cannot submit in status "${state.status}"`
          })
        }

        if (state.items.length === 0) {
          return yield* new C.OrderError({
            message: "Cannot submit order with no items"
          })
        }

        const now = yield* DateTime.now
        return [
          new C.OrderSubmitted({ orderId: command.orderId, occurredAt: now })
        ]
      }),
    CancelOrder: (state, command) =>
      Effect.gen(function* () {
        if (state.status === "cancelled") {
          return yield* new C.OrderError({
            message: "Order is already cancelled"
          })
        }

        if (state.status === "empty") {
          return yield* new C.OrderError({ message: "Order does not exist" })
        }

        const now = yield* DateTime.now
        return [
          new C.OrderCancelled({
            orderId: command.orderId,
            reason: command.reason,
            occurredAt: now
          })
        ]
      }),
    GetOrder: (_state, _command) => Effect.succeed([]),
    FulfillOrder: (_state, _command) => Effect.succeed([])
  }
})

export const {
  evolve,
  decide,
  handle: handleCommand,
  run,
  initialState: initialOrderState,
} = Order;
