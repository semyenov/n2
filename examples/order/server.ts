/**
 * Dev server — in-memory state, no cluster overhead.
 *
 * Run: PORT=4000 bun examples/order/server.ts
 * Test:
 *   curl -s http://localhost:4000/health
 *   curl -s -X POST http://localhost:4000/rpc/orders \
 *     -H 'Content-Type: application/json' \
 *     -d '{"jsonrpc":"2.0","method":"CreateOrder","params":{"orderId":"o-1","customerId":"c-1"},"id":1}'
 *
 * RpcServer.layerHttpRouter registers an HTTP route for the RpcGroup.
 *   protocol: "http"          — use HTTP request/response (not WebSocket)
 *   RpcSerialization.layerJsonRpc() — translate standard JSON-RPC 2.0
 *     {jsonrpc,method,params,id} ↔ @effect/rpc internal format
 */
import * as Config from "effect/Config"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import { BunHttpServer, BunRuntime } from "@effect/platform-bun"
import { HttpLayerRouter, HttpServerResponse } from "@effect/platform"
import { RpcSerialization, RpcServer } from "@effect/rpc"
import { PgClient } from "@effect/sql-pg"
import { OrderRpcs } from "./contracts.js"
import { OrderHandlers } from "./entity.js"
import { InfrastructureLayer } from "./layers.js"

const SqlLayer = PgClient.layerConfig(
  Config.map(Config.redacted("DATABASE_URL"), (url) => ({ url }))
)

const OrderRpcRoute = RpcServer
  .layerHttpRouter({ group: OrderRpcs, path: "/rpc/orders", protocol: "http" })
  .pipe(
    Layer.provide(OrderHandlers),
    Layer.provide(RpcSerialization.layerJsonRpc())
  )

const HealthRoute = HttpLayerRouter.add(
  "GET",
  "/health",
  HttpServerResponse.json({ status: "ok" })
)

const ServerLayer = HttpLayerRouter.serve(
  Layer.mergeAll(OrderRpcRoute, HealthRoute)
).pipe(
  Layer.provide(BunHttpServer.layerConfig(
    Config.map(
      Config.integer("PORT").pipe(Config.withDefault(4000)),
      (port) => ({ port })
    )
  )),
  Layer.provide(InfrastructureLayer),
  Layer.provide(SqlLayer)
)

BunRuntime.runMain(
  Effect.gen(function* () {
    const port = yield* Config.integer("PORT").pipe(Config.withDefault(4000))
    yield* Effect.log(`Order service (dev mode) on http://localhost:${port}`)
    yield* Effect.log("Endpoints: CreateOrder | AddItem | SubmitOrder | GET /health")
    yield* Effect.never
  }).pipe(Effect.provide(ServerLayer))
)
