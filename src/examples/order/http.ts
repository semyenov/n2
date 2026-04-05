/**
 * HTTP route setup for the Order service.
 *
 * Two modes:
 * 1. Cluster mode: EntityProxy RPC group + EntityProxyServer handlers
 * 2. Direct mode: OrderRpcs + manual handlers
 */
import * as Layer from "effect/Layer"
import { RpcSerialization, RpcServer } from "@effect/rpc"
import { HttpLayerRouter } from "@effect/platform"
import { OrderRpcs } from "./contracts.js"
import {
  OrderHandlers,
  OrderProxyRpcs,
  OrderProxyHandlers,
  OrderEntityLayer
} from "./entity.js"

/**
 * Direct RPC route (non-cluster, for local dev).
 * Uses OrderRpcs + manual handlers.
 */
export const OrderRpcRoute = RpcServer
  .layerHttpRouter({
    group: OrderRpcs,
    path: "/rpc/orders"
  })
  .pipe(
    Layer.provide(OrderHandlers),
    Layer.provide(RpcSerialization.layerJson),
    Layer.provide(HttpLayerRouter.cors())
  )

/**
 * Cluster RPC route (production).
 * Uses EntityProxy-derived RpcGroup + EntityProxyServer handlers.
 * Requires Sharding layer.
 */
export const OrderClusterRpcRoute = RpcServer
  .layerHttpRouter({
    group: OrderProxyRpcs,
    path: "/rpc/orders"
  })
  .pipe(
    Layer.provide(OrderProxyHandlers),
    Layer.provide(OrderEntityLayer),
    Layer.provide(RpcSerialization.layerJson),
    Layer.provide(HttpLayerRouter.cors())
  )
