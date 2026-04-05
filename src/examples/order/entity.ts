/**
 * Order cluster entity + direct RPC handlers.
 * Uses the Order definition as the single source of truth.
 */
import { EntityProxy, EntityProxyServer } from "@effect/cluster"
import { Order } from "./aggregate.js"
import {
  OrderEntity,
  OrderRpcs,
  CommandResult,
  OrderError
} from "./contracts.js"

// ---------------------------------------------------------------------------
// Cluster Entity: stateful per entity ID
// ---------------------------------------------------------------------------

export const OrderEntityLayer = Order.toEntityLayer(
  OrderEntity,
  {
    toResult: ({ entityId, revision }) =>
      new CommandResult({ orderId: entityId, revision }),
    toError: (error) =>
      error instanceof OrderError
        ? error
        : new OrderError({ message: String(error) })
  },
  {
    maxIdleTime: "10 minutes",
    concurrency: "unbounded"
  }
)

// ---------------------------------------------------------------------------
// EntityProxy (auto-derived from Entity)
// ---------------------------------------------------------------------------

export const OrderProxyRpcs = EntityProxy.toRpcGroup(OrderEntity)
export const OrderProxyHandlers = EntityProxyServer.layerRpcHandlers(OrderEntity)

// ---------------------------------------------------------------------------
// Direct RPC handlers (non-cluster, stateless per request)
// ---------------------------------------------------------------------------

export const OrderHandlers = Order.toRpcHandlers(OrderRpcs, {
  toResult: ({ command, events }) =>
    new CommandResult({
      orderId: command.orderId,
      revision: events.length
    }),
  toError: (error) =>
    error instanceof OrderError
      ? error
      : new OrderError({
        message: String(error),
      })
})
