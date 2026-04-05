/**
 * Order aggregate.
 */
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as DateTime from "effect/DateTime";
import * as N2 from "../../framework/helpers/index.js";
import * as C from "./contracts.js";

const Order = N2.Aggregate.define<C.OrderEvent, C.OrderCommand>()(
  C.initialOrderState,
  {
    evolve: {
      OrderCreated: (state, e) => ({
        ...state,
        status: "draft" as const,
        orderId: Option.some(e.orderId),
        customerId: Option.some(e.customerId),
      }),
      ItemAdded: (state, e) => ({
        ...state,
        items: [
          ...state.items,
          new C.LineItem({
            sku: e.sku,
            quantity: e.quantity,
            price: e.price,
          }),
        ],
        totalAmount: state.totalAmount + e.price * e.quantity,
      }),
      OrderSubmitted: (state) => ({
        ...state,
        status: "submitted" as const,
      }),
      OrderCancelled: (state) => ({
        ...state,
        status: "cancelled" as const,
      }),
    },
    decide: {
      CreateOrder: (state, cmd) =>
        Effect.gen(function* () {
          if (state.status !== "empty")
            return yield* new C.OrderError({ message: "Order already exists" });
          const now = yield* DateTime.now;
          return [
            new C.OrderCreated({
              orderId: cmd.orderId,
              customerId: cmd.customerId,
              createdAt: now,
            }),
          ];
        }),
      AddItem: (state, cmd) =>
        Effect.gen(function* () {
          if (state.status !== "draft")
            return yield* new C.OrderError({
              message: `Cannot add items in status "${state.status}"`,
            });
          return [
            new C.ItemAdded({
              orderId: cmd.orderId,
              sku: cmd.sku,
              quantity: cmd.quantity,
              price: cmd.price,
            }),
          ];
        }),
      SubmitOrder: (state, cmd) =>
        Effect.gen(function* () {
          if (state.status !== "draft")
            return yield* new C.OrderError({
              message: `Cannot submit in status "${state.status}"`,
            });
          if (state.items.length === 0)
            return yield* new C.OrderError({
              message: "Cannot submit order with no items",
            });
          const now = yield* DateTime.now;
          return [
            new C.OrderSubmitted({ orderId: cmd.orderId, submittedAt: now }),
          ];
        }),
      CancelOrder: (state, cmd) =>
        Effect.gen(function* () {
          if (state.status === "cancelled")
            return yield* new C.OrderError({
              message: "Order is already cancelled",
            });
          if (state.status === "empty")
            return yield* new C.OrderError({ message: "Order does not exist" });
          const now = yield* DateTime.now;
          return [
            new C.OrderCancelled({
              orderId: cmd.orderId,
              reason: cmd.reason,
              cancelledAt: now,
            }),
          ];
        }),
    },
  },
);

export const {
  evolve,
  decide,
  handleCommand,
  initialState: initialOrderState,
} = Order;
