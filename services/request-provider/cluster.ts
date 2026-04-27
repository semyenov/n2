/**
 * Cluster entrypoint for request provider.
 *
 * Required:
 *   DATABASE_URL    PostgreSQL connection string
 *   CLICKHOUSE_URL  ClickHouse HTTP URL, e.g. http://localhost:8123
 *
 * Optional:
 *   CLICKHOUSE_DATABASE  ClickHouse database (default: default)
 *   HOST                 Runner advertised host (default: 127.0.0.1)
 *   PORT                 Runner cluster port
 *   API_PORT             Public JSON-RPC / health port (default: 4110)
 */
import * as Config from "effect/Config"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import { BunClusterHttp, BunHttpServer, BunRuntime } from "@effect/platform-bun"
import { HttpLayerRouter, HttpServerResponse } from "@effect/platform"
import { RpcSerialization, RpcServer } from "@effect/rpc"
import { PgClient } from "@effect/sql-pg"
import { RequestProviderEntityLayer, RequestProviderProxyHandlers, RequestProviderProxyRpcs } from "./entity.js"
import { ClusterInfrastructureLayer } from "./layers.js"
import { MigrationsLayer } from "./migrate.js"

const RequestProviderRpcRoute = RpcServer
  .layerHttpRouter({
    group: RequestProviderProxyRpcs,
    path: "/rpc/request-provider",
    protocol: "http"
  }).pipe(
    Layer.provide(RequestProviderProxyHandlers),
    Layer.provide(RpcSerialization.layerJsonRpc())
  )

const HealthRoute = HttpLayerRouter.add(
  "GET",
  "/health",
  HttpServerResponse.json({ status: "ok" })
)

const SqlLayer = PgClient.layerConfig(
  Config.map(Config.redacted("DATABASE_URL"), (url) => ({
    url,
    minConnections: 4
  }))
)

const ShardingLayer = BunClusterHttp.layer({
  transport: "http",
  storage: "sql"
}).pipe(
  Layer.provide(SqlLayer),
)

const EntitiesLayer = Layer.provide(
  RequestProviderEntityLayer,
  Layer.provide(
    Layer.merge(ClusterInfrastructureLayer, MigrationsLayer),
    Layer.merge(SqlLayer, ShardingLayer)
  )
)

BunRuntime.runMain(
  Effect.gen(function* () {
    const apiPort = yield* Config.integer("API_PORT").pipe(Config.withDefault(4110))
    yield* Effect.log(`Request provider service (cluster mode) on http://localhost:${apiPort}/rpc/request-provider`)
    yield* Effect.log("Runner cluster port is configured by HOST / PORT")
    yield* Effect.never
  }).pipe(
    Effect.provide(Layer.mergeAll(
      HttpLayerRouter.serve(Layer.mergeAll(RequestProviderRpcRoute, HealthRoute)),
      EntitiesLayer
    )),
    Effect.provide(
      Layer.mergeAll(
        BunHttpServer.layerConfig(
          Config.map(
            Config.integer("API_PORT").pipe(Config.withDefault(4110)),
            (port) => ({ port })
          )
        ),
        ShardingLayer
      )
    )
  )
)
