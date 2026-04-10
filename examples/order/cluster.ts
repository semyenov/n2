/**
 * Production entry point — clustered Order service.
 *
 * Run: bun examples/order/cluster.ts
 * Test:
 *   curl -s -X POST http://localhost:3000/rpc/orders \
 *     -H 'Content-Type: application/json' \
 *     -d '{"jsonrpc":"2.0","method":"CreateOrder","params":{"orderId":"o-1","customerId":"c-1"},"id":1}'
 *
 * Architecture:
 *   - Each Order ID is a separate sharded actor with its own Ref<OrderState>
 *   - EntityProxy translates HTTP JSON-RPC → cluster Sharding messages
 *   - TestRunner.layer runs a single-process in-memory shard manager (no external shard server needed)
 *   - Swap TestRunner.layer → real shard manager for multi-process deployment
 */
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import { BunHttpServer } from "@effect/platform-bun"
import { HttpLayerRouter } from "@effect/platform"
import { RpcSerialization, RpcServer } from "@effect/rpc"
import { ShardingConfig, TestRunner } from "@effect/cluster"
import { OrderEntityLayer, OrderProxyRpcs, OrderProxyHandlers } from "./entity.js"
import { InfrastructureLayer } from "./layers.js"

// Entity layer: each Order ID is a separate sharded actor with its own state Ref.
// InfrastructureLayer is provided here so EntitiesLayer has R = never.
const EntitiesLayer = Layer.provide(OrderEntityLayer, InfrastructureLayer)

// HTTP route backed by EntityProxy.
// EntityProxy.toRpcGroup derives an RpcGroup from the entity definition.
// EntityProxyServer.layerRpcHandlers generates handlers that forward
// RPC calls as Sharding messages to the correct shard.
const ApiRoute = RpcServer
  .layerHttpRouter({ group: OrderProxyRpcs, path: "/rpc/orders", protocol: "http" })
  .pipe(
    Layer.provide(OrderProxyHandlers),
    Layer.provide(RpcSerialization.layerJsonRpc())
  )

Effect.runPromise(
  Effect.gen(function* () {
    yield* Effect.log("Order service (cluster mode) on http://localhost:3000")
    yield* Effect.log("Endpoints: CreateOrder | AddItem | SubmitOrder")
    yield* Effect.never
  }).pipe(
    // inner: domain services (entities + HTTP routes)
    Effect.provide(Layer.mergeAll(
      HttpLayerRouter.serve(ApiRoute),
      EntitiesLayer
    )),
    // outer: infrastructure (HTTP server, sharding runtime, workflow engine)
    Effect.provide(Layer.mergeAll(
      BunHttpServer.layer({ port: 3000 }),
      InfrastructureLayer,
      TestRunner.layer,        // single-process in-memory shard manager
      ShardingConfig.layer({}) // cluster config (pods, replication, etc.)
    ))
  )
)
