/**
 * Order cluster entity + direct RPC handlers.
 * Uses N2 helpers to eliminate boilerplate.
 */
import { EntityProxy, EntityProxyServer } from "@effect/cluster"
import * as N2 from "../../framework/helpers/index.js"
import { handleCommand } from "./aggregate.js"
import {
  OrderEntity,
  OrderRpcs,
  CommandResult,
  OrderError,
  CreateOrder,
  AddItem,
  SubmitOrder,
  CancelOrder,
  initialOrderState
} from "./contracts.js"

// ---------------------------------------------------------------------------
// Cluster Entity: stateful per entity ID
// ---------------------------------------------------------------------------

export const OrderEntityLayer = N2.Entity.makeEntityLayer(OrderEntity, {
  handleCommand,
  initialState: initialOrderState,
  toResult: (entityId, revision) => new CommandResult({ orderId: entityId, revision }),
  toError: (err) => new OrderError({ message: String(err) }),
  commands: { CreateOrder, AddItem, SubmitOrder, CancelOrder }
}, { maxIdleTime: "10 minutes" })

// ---------------------------------------------------------------------------
// EntityProxy (auto-derived from Entity)
// ---------------------------------------------------------------------------

export const OrderProxyRpcs = EntityProxy.toRpcGroup(OrderEntity)
export const OrderProxyHandlers = EntityProxyServer.layerRpcHandlers(OrderEntity)

// ---------------------------------------------------------------------------
// Direct RPC handlers (non-cluster, stateless per request)
// ---------------------------------------------------------------------------

export const OrderHandlers = N2.Entity.makeRpcHandlers(OrderRpcs, {
  handleCommand,
  initialState: initialOrderState,
  toResult: (payload, events) => new CommandResult({ orderId: (payload as { orderId: string }).orderId, revision: events.length }),
  toError: (err) => new OrderError({ message: String(err) }),
  commands: { CreateOrder, AddItem, SubmitOrder, CancelOrder }
})
