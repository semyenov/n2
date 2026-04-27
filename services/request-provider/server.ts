/**
 * Dev server — in-memory state, PostgreSQL infrastructure, and ClickHouse read projections.
 *
 * Required:
 *   DATABASE_URL    PostgreSQL connection string
 *   CLICKHOUSE_URL  ClickHouse HTTP URL, e.g. http://localhost:8123
 *
 * Optional:
 *   CLICKHOUSE_DATABASE  ClickHouse database (default: default)
 *   PORT                 Public JSON-RPC / health port (default: 4110)
 */
import * as Config from "effect/Config"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import { BunHttpServer, BunRuntime } from "@effect/platform-bun"
import { HttpLayerRouter, HttpServerResponse } from "@effect/platform"
import { RpcSerialization, RpcServer } from "@effect/rpc"
import { PgClient } from "@effect/sql-pg"
import { RequestProviderHandlers } from "./entity.js"
import { RequestProviderRpcs } from "./contracts.js"
import { InfrastructureLayer } from "./layers.js"
import { MigrationsLayer } from "./migrate.js"

const SqlLayer = PgClient.layerConfig(
  Config.map(Config.redacted("DATABASE_URL"), (url) => ({ url }))
)

const RequestProviderRpcRoute = RpcServer
  .layerHttpRouter({ group: RequestProviderRpcs, path: "/rpc/request-provider", protocol: "http" })
  .pipe(
    Layer.provide(RequestProviderHandlers),
    Layer.provide(RpcSerialization.layerJsonRpc())
  )

const HealthRoute = HttpLayerRouter.add(
  "GET",
  "/health",
  HttpServerResponse.json({ status: "ok" })
)

const ServerLayer = HttpLayerRouter.serve(
  Layer.mergeAll(RequestProviderRpcRoute, HealthRoute)
).pipe(
  Layer.provide(BunHttpServer.layerConfig(
    Config.map(
      Config.integer("PORT").pipe(Config.withDefault(4110)),
      (port) => ({ port })
    )
  )),
  Layer.provide(MigrationsLayer),
  Layer.provide(InfrastructureLayer),
  Layer.provide(SqlLayer)
)

BunRuntime.runMain(
  Effect.gen(function* () {
    const port = yield* Config.integer("PORT").pipe(Config.withDefault(4110))
    yield* Effect.log(`Request provider service (dev mode) on http://localhost:${port}`)
    yield* Effect.log("Endpoints: CreateRequest | UpdateRequest | CreateRequestSnapshot")
    yield* Effect.never
  }).pipe(Effect.provide(ServerLayer))
)
