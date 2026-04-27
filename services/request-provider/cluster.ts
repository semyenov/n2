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
import { BunHttpServer, BunRuntime } from "@effect/platform-bun"
import { HttpLayerRouter } from "@effect/platform"
import {
  makeClusterShardingLayer,
  makeHealthRoute,
  makePgSqlLayer,
  makeRpcHttpRoute
} from "@semyenov/n2/runtime"
import { RequestProviderEntityLayer, RequestProviderProxyHandlers, RequestProviderProxyRpcs } from "./entity.js"
import { ClusterInfrastructureLayer } from "./layers.js"
import { MigrationsLayer } from "./migrate.js"

const RequestProviderRpcRoute = makeRpcHttpRoute({
  group: RequestProviderProxyRpcs,
  path: "/rpc/request-provider",
  handlers: RequestProviderProxyHandlers
})

const HealthRoute = makeHealthRoute()

const SqlLayer = makePgSqlLayer({ minConnections: 4 })

const ShardingLayer = makeClusterShardingLayer(SqlLayer)

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
