/**
 * @since 1.0.0
 * @module N2.Http
 *
 * Helper for creating RPC HTTP routes with standard defaults.
 */
import * as Layer from "effect/Layer"
import type { Rpc, RpcGroup } from "@effect/rpc"
import { RpcSerialization, RpcServer } from "@effect/rpc"
import { HttpLayerRouter } from "@effect/platform"

/**
 * Creates an RPC HTTP route with JSON serialization and CORS.
 *
 * @example
 * ```ts
 * export const OrderRpcRoute = N2.Http.makeRoute(OrderRpcs, "/rpc/orders", OrderHandlers)
 * ```
 *
 * @since 1.0.0
 */
export const makeRoute = <Rpcs extends Rpc.Any>(
  group: RpcGroup.RpcGroup<Rpcs>,
  path: `/${string}`,
  handlers: Layer.Layer<Rpc.ToHandler<Rpcs>>
) =>
  RpcServer
    .layerHttpRouter({ group, path })
    .pipe(
      Layer.provide(handlers),
      Layer.provide(RpcSerialization.layerJson),
      Layer.provide(HttpLayerRouter.cors())
    )
