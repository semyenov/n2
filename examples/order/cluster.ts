/**
 * Production cluster entrypoint — multiple processes, PostgreSQL coordination.
 *
 * Environment variables:
 *   DATABASE_URL   PostgreSQL connection string  (required)
 *                  e.g. postgres://user:pass@localhost:5432/orders
 *   HOST           This runner's hostname / IP   (default: 127.0.0.1)
 *   PORT           This runner's cluster port    (default: 34431)
 *                  Other runners forward shard messages here.
 *   API_PORT       HTTP port for the JSON-RPC API (default: 3000)
 *
 * Running a two-process cluster locally:
 *
 *   # Terminal 1
 *   DATABASE_URL=postgres://... HOST=127.0.0.1 PORT=34431 API_PORT=3000 \
 *     bun examples/order/cluster.ts
 *
 *   # Terminal 2 — different cluster port and API port
 *   DATABASE_URL=postgres://... HOST=127.0.0.1 PORT=34432 API_PORT=3001 \
 *     bun examples/order/cluster.ts
 *
 *   Both processes share the same PostgreSQL database.
 *   Shards are distributed across both runners automatically.
 *   A request to either API port is forwarded to the shard owner.
 *
 * How it works:
 *   - Each process registers itself in RunnerStorage (PostgreSQL).
 *   - ShardingConfig assigns shards across all registered runners.
 *   - When a request arrives, EntityProxy looks up the shard owner
 *     and forwards the message via HTTP to that runner's PORT.
 *   - Each Order entity (Ref<OrderState> per orderId) lives in memory
 *     on the owning runner; SqlMessageStorage queues durably.
 *
 * For single-process local testing without PostgreSQL, replace
 * BunClusterHttp.layer with TestRunner.layer + ShardingConfig.layer({}).
 */
import * as Config from "effect/Config"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import { BunClusterHttp, BunHttpServer, BunRuntime } from "@effect/platform-bun"
import { HttpLayerRouter, HttpServerResponse } from "@effect/platform"
import { RpcSerialization, RpcServer } from "@effect/rpc"
import { PgClient } from "@effect/sql-pg"
import { OrderEntityLayer, OrderProxyRpcs, OrderProxyHandlers } from "./entity.js"
import { ClusterInfrastructureLayer } from "./layers.js"

// ---------------------------------------------------------------------------
// API route — EntityProxy translates JSON-RPC → Sharding messages
//
// EntityProxy.toRpcGroup + EntityProxyServer.layerRpcHandlers (from entity.ts)
// forward each RPC call to the runner that owns the relevant shard.
// ---------------------------------------------------------------------------

const OrderRpcRoute = RpcServer
  .layerHttpRouter({ group: OrderProxyRpcs, path: "/rpc/orders", protocol: "http" })
  .pipe(
    Layer.provide(OrderProxyHandlers),
    Layer.provide(RpcSerialization.layerJsonRpc())
  )

const HealthRoute = HttpLayerRouter.add(
  "GET",
  "/health",
  HttpServerResponse.json({ status: "ok" })
)

// ---------------------------------------------------------------------------
// SQL layer — PostgreSQL client (shared by message + runner storage)
// ---------------------------------------------------------------------------

const SqlLayer = PgClient.layerConfig(
  Config.map(Config.redacted("DATABASE_URL"), (url) => ({
    url,
    // Pre-allocate connections so concurrent init queries (SqlMessageStorage +
    // SqlRunnerStorage schema creation) each get their own connection and avoid
    // the pg "client already executing a query" deprecation warning.
    minConnections: 4
  }))
)

// ---------------------------------------------------------------------------
// Cluster (Sharding) layer
//
// BunClusterHttp.layer is a batteries-included wrapper that configures:
//   HttpRunner      — inter-runner HTTP server on HOST:PORT
//   SqlMessageStorage — durable message queue in PostgreSQL
//   SqlRunnerStorage  — shard ownership registry in PostgreSQL
//   RunnerHealth    — ping-based health checks between runners
//   ShardingConfig  — reads HOST / PORT from environment variables
//
// storage: "sql"      — requires SqlClient (provided by SqlLayer below)
// transport: "http"   — runners communicate over HTTP
//
// ShardingConfig reads these env vars (via ConfigProvider.constantCase):
//   HOST             → runner address host   (default: 127.0.0.1)
//   PORT             → runner address port   (default: 34431)
//   SHARDS_PER_GROUP → total shards          (default: 300, must match across all runners)
// ---------------------------------------------------------------------------

const ShardingLayer = BunClusterHttp.layer({
  transport: "http",
  storage: "sql"
}).pipe(Layer.provide(SqlLayer))

// Entity layer: each Order ID is a sharded actor with its own Ref<OrderState>.
// ClusterInfrastructureLayer requires SqlClient | Sharding | MessageStorage —
// both SqlLayer and ShardingLayer are provided here. Effect deduplicates SqlLayer
// at runtime: same reference used inside ShardingLayer, so one pool is built.
const EntitiesLayer = Layer.provide(
  OrderEntityLayer,
  Layer.provide(ClusterInfrastructureLayer, Layer.merge(SqlLayer, ShardingLayer))
)

// ---------------------------------------------------------------------------
// Main program
// ---------------------------------------------------------------------------

BunRuntime.runMain(
  Effect.gen(function* () {
    const apiPort = yield* Config.integer("API_PORT").pipe(Config.withDefault(3000))
    yield* Effect.log(`Order service (cluster mode) — API on http://localhost:${apiPort}/rpc/orders`)
    yield* Effect.log("Runner cluster port set via HOST / PORT env vars (default: 127.0.0.1:34431)")
    yield* Effect.never
  }).pipe(
    // inner: domain services (entities + API routes)
    Effect.provide(Layer.mergeAll(
      HttpLayerRouter.serve(Layer.mergeAll(OrderRpcRoute, HealthRoute)),
      EntitiesLayer
    )),
    // outer: infrastructure (API HTTP server + cluster)
    // InfrastructureLayer is omitted here — it is fully satisfied inside EntitiesLayer.
    // Both EntitiesLayer and ShardingLayer reference the same SqlLayer constant, so
    // Effect deduplicates the PgClient pool at runtime even though it appears in two places.
    Effect.provide(
      Layer.mergeAll(
        BunHttpServer.layerConfig(
          Config.map(
            Config.integer("API_PORT").pipe(Config.withDefault(3000)),
            (port) => ({ port })
          )
        ),
        ShardingLayer
      )
    )
  )
)
