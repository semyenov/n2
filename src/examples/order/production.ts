/**
 * Production deployment: clustered Order service.
 */
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import { BunHttpServer } from "@effect/platform-bun"
import { HttpLayerRouter } from "@effect/platform"
import { RpcSerialization, RpcServer } from "@effect/rpc"
import {
  ShardingConfig,
  EntityProxy,
  EntityProxyServer,
  TestRunner
} from "@effect/cluster"

import { OrderEntity } from "./contracts.js"
import { OrderEntityLayer } from "./entity.js"
import { InventoryEntityLayer } from "../inventory/entity.js"
import { InfrastructureLayer } from "./layers.js"

const EntitiesLayer = Layer.mergeAll(
  Layer.provide(OrderEntityLayer, InfrastructureLayer),
  Layer.provide(InventoryEntityLayer, InfrastructureLayer)
)

const OrderProxyRpcs = EntityProxy.toRpcGroup(OrderEntity)
const OrderProxyHandlers = EntityProxyServer.layerRpcHandlers(OrderEntity)

const ApiRoute = RpcServer
  .layerHttpRouter({
    group: OrderProxyRpcs,
    path: "/rpc/orders"
  })
  .pipe(
    Layer.provide(OrderProxyHandlers),
    Layer.provide(RpcSerialization.layerJson),
    Layer.provide(HttpLayerRouter.cors())
  )

const main = Effect.gen(function* () {
  yield* Effect.log("Order service on port 3000")
  yield* Effect.never
}).pipe(
  Effect.provide(
    Layer.mergeAll(
      HttpLayerRouter.serve(ApiRoute),
      EntitiesLayer
    )
  ),
  Effect.provide(
    Layer.mergeAll(
      BunHttpServer.layer({ port: 3000 }),
      InfrastructureLayer,
      TestRunner.layer,
      ShardingConfig.layer({})
    )
  )
)

Effect.runPromise(main)
