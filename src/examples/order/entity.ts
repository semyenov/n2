/**
 * Order cluster entity + direct RPC handlers.
 * Uses the Order definition and framework lifecycle hooks.
 */
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import { EntityProxy, EntityProxyServer } from "@effect/cluster"
import { OrderFulfillmentWorkflow } from "./workflows.js"
import { WorkflowLayer } from "./layers.js"
import { Order } from "./aggregate.js"
import {
  type OrderCommand,
  OrderEntity,
  OrderRpcs,
  CommandResult,
  FulfillmentResult,
  OrderError,
  OrderNotFound
} from "./contracts.js"

// ---------------------------------------------------------------------------
// Cluster Entity: stateful per entity ID with read query overrides
// ---------------------------------------------------------------------------

export const OrderEntityLayer = Order.toEntityLayer(
  OrderEntity,
  {
    toResult: ({ entityId, revision }) =>
      new CommandResult({ orderId: entityId, revision }),
    toError: (error) =>
      error instanceof OrderError
        ? error
        : new OrderError({ message: String(error) }),
    overrides: {
      GetOrder: (command, ctx) =>
        ctx.getState(command.orderId).pipe(
          Effect.flatMap((state) =>
            Option.isNone(state.orderId)
              ? Effect.fail(new OrderNotFound({ orderId: command.orderId }))
              : Effect.succeed(state)
          )
        ),
      FulfillOrder: (command) =>
        OrderFulfillmentWorkflow.execute({ orderId: command.orderId, sku: command.sku, quantity: command.quantity }).pipe(
          Effect.map(({ orderId, shipped }) => new FulfillmentResult({ orderId, shipped })),
          Effect.mapError((e) => new OrderError({ message: e.message })),
          Effect.scoped
        )
    }
  },
  { maxIdleTime: "10 minutes", concurrency: "unbounded" }
)

// ---------------------------------------------------------------------------
// EntityProxy (auto-derived from Entity)
// ---------------------------------------------------------------------------

export const OrderProxyRpcs = EntityProxy.toRpcGroup(OrderEntity)
export const OrderProxyHandlers = EntityProxyServer.layerRpcHandlers(OrderEntity)

// ---------------------------------------------------------------------------
// Direct RPC handlers — stateful in-memory (dev server)
// ---------------------------------------------------------------------------

const OrderHandlersRaw = Order.toStatefulRpcHandlers(
  OrderRpcs,
  {
    entityId: (command: OrderCommand) => command.orderId,
    toResult: ({ entityId, revision }) =>
      new CommandResult({ orderId: entityId, revision }),
    toError: (error) =>
      error instanceof OrderError
        ? error
        : new OrderError({ message: String(error) }),
    overrides: {
      GetOrder: (command, ctx) =>
        ctx.getState(command.orderId).pipe(
          Effect.flatMap((state) =>
            Option.isNone(state.orderId)
              ? Effect.fail(new OrderNotFound({ orderId: command.orderId }))
              : Effect.succeed(state)
          )
        ),
      FulfillOrder: (command) =>
        OrderFulfillmentWorkflow.execute({ orderId: command.orderId, sku: command.sku, quantity: command.quantity }).pipe(
          Effect.map(({ orderId, shipped }) => new FulfillmentResult({ orderId, shipped })),
          Effect.mapError((e) => new OrderError({ message: e.message })),
          Effect.scoped
        )
    }
  }
)

export const OrderHandlers = Layer.provide(OrderHandlersRaw, WorkflowLayer)
