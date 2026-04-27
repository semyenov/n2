import * as Config from "effect/Config"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import { BunClusterHttp, BunHttpServer, BunRuntime } from "@effect/platform-bun"
import { HttpLayerRouter, HttpServerResponse } from "@effect/platform"
import { RpcSerialization, RpcServer } from "@effect/rpc"
import { PgClient } from "@effect/sql-pg"
import { ProfileProviderEntityLayer, ProfileProviderProxyHandlers, ProfileProviderProxyRpcs } from "./entity.js"
import { ClusterInfrastructureLayer } from "./layers.js"
import { MigrationsLayer } from "./migrate.js"

/**
 * Cluster entrypoint for the profile provider service.
 *
 * Required infrastructure:
 *   DATABASE_URL    PostgreSQL connection string
 *   CLICKHOUSE_URL  ClickHouse HTTP URL, e.g. http://localhost:8123
 *
 * Optional:
 *   CLICKHOUSE_DATABASE  ClickHouse database (default: default)
 *   HOST                 This runner's advertised host (default: 127.0.0.1)
 *   PORT                 This runner's cluster port
 *   API_PORT             Public JSON-RPC / health port (default: 4100)
 *   SHARDS_PER_GROUP     Total shard count; keep identical across runners
 *
 * Single runner:
 *   DATABASE_URL=postgres://user:pass@localhost:5432/n2 \
 *   CLICKHOUSE_URL=http://localhost:8123 \
 *   HOST=127.0.0.1 PORT=34431 API_PORT=4100 \
 *     bun services/profiler/cluster.ts
 *
 * Two local runners:
 *   # Terminal 1
 *   DATABASE_URL=postgres://... CLICKHOUSE_URL=http://localhost:8123 \
 *   HOST=127.0.0.1 PORT=34431 API_PORT=4100 \
 *     bun services/profiler/cluster.ts
 *
 *   # Terminal 2
 *   DATABASE_URL=postgres://... CLICKHOUSE_URL=http://localhost:8123 \
 *   HOST=127.0.0.1 PORT=34432 API_PORT=4101 \
 *     bun services/profiler/cluster.ts
 *
 * PostgreSQL backs sharding, snapshots, event journal, and outbox.
 * ClickHouse backs read projections.
 * JSON-RPC endpoint: /rpc/profile-provider
 * Health endpoint:   /health
 */

const ProfileProviderRpcRoute = RpcServer
  .layerHttpRouter({
    group: ProfileProviderProxyRpcs,
    path: "/rpc/profile-provider",
    protocol: "http"
  }).pipe(
    Layer.provide(ProfileProviderProxyHandlers),
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
  ProfileProviderEntityLayer,
  Layer.provide(
    Layer.merge(ClusterInfrastructureLayer, MigrationsLayer),
    Layer.merge(SqlLayer, ShardingLayer)
  )
)

BunRuntime.runMain(
  Effect.gen(function* () {
    const apiPort = yield* Config.integer("API_PORT").pipe(Config.withDefault(4100))
    yield* Effect.log(`Profile provider service (cluster mode) on http://localhost:${apiPort}/rpc/profile-provider`)
    yield* Effect.log("Runner cluster port is configured by HOST / PORT")
    yield* Effect.never
  }).pipe(
    Effect.provide(Layer.mergeAll(
      HttpLayerRouter.serve(Layer.mergeAll(ProfileProviderRpcRoute, HealthRoute)),
      EntitiesLayer
    )),
    Effect.provide(
      Layer.mergeAll(
        BunHttpServer.layerConfig(
          Config.map(
            Config.integer("API_PORT").pipe(Config.withDefault(4100)),
            (port) => ({ port })
          )
        ),
        ShardingLayer
      )
    )
  )
)
