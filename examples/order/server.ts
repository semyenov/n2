/**
 * Dev server — in-memory state, no cluster overhead.
 *
 * Run: bun examples/order/server.ts
 * Test:
 *   curl -s -X POST http://localhost:4000/rpc/orders \
 *     -H 'Content-Type: application/json' \
 *     -d '{"jsonrpc":"2.0","method":"CreateOrder","params":{"orderId":"o-1","customerId":"c-1"},"id":1}'
 *
 * RpcServer.layerHttpRouter registers an HTTP route for the RpcGroup.
 *   protocol: "http"          — use HTTP request/response (not WebSocket)
 *   RpcSerialization.layerJsonRpc() — translate standard JSON-RPC 2.0
 *     {jsonrpc,method,params,id} ↔ @effect/rpc internal format
 */
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import { BunHttpServer } from "@effect/platform-bun"
import { HttpLayerRouter } from "@effect/platform"
import { RpcSerialization, RpcServer } from "@effect/rpc"
import { OrderRpcs } from "./contracts.js"
import { OrderHandlers } from "./entity.js"
import { InfrastructureLayer } from "./layers.js"

const OrderRpcRoute = RpcServer
  .layerHttpRouter({ group: OrderRpcs, path: "/rpc/orders", protocol: "http" })
  .pipe(
    Layer.provide(OrderHandlers),
    Layer.provide(RpcSerialization.layerJsonRpc())
  )

const ServerLayer = HttpLayerRouter.serve(OrderRpcRoute).pipe(
  Layer.provide(BunHttpServer.layer({ port: 4000 })),
  Layer.provide(InfrastructureLayer)
)

Effect.runPromise(
  Effect.gen(function* () {
    yield* Effect.log("Order service (dev mode) on http://localhost:4000")
    yield* Effect.log("Endpoints: CreateOrder | AddItem | SubmitOrder")
    yield* Effect.never
  }).pipe(Effect.provide(ServerLayer))
)
