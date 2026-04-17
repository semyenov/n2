/**
 * Dev server — in-memory state, no cluster overhead.
 *
 * Run: PORT=4100 bun examples/profile-provider/server.ts
 * Test:
 *   curl -s http://localhost:4100/health
 *   curl -s -X POST http://localhost:4100/rpc/profile-provider \
 *     -H 'Content-Type: application/json' \
 *     -d '{"jsonrpc":"2.0","method":"CreateProfile","params":{"profileId":"00000000-0000-4000-8000-000000000001","ownerAgentId":"a-1","branchId":"main","schemaVersion":"1.0","maskedProfileJson":{"uuid":"00000000-0000-4000-8000-000000000001","created_at":"2026-01-01T00:00:00.000Z","user_data":{"personal_info":{"first_name":"Ada","last_name":"Lovelace","relevant_position":"Platform Engineer"},"salary_expectations":{"currency":"USD","amount_from":1000},"skills":[{"name":"TypeScript","level":"advanced"}],"education":[{"degree":"Bachelor","field_of_study":"Computer Science","institution":"Analytical Engine Institute"}]},"user_meta_data":{"version":1}},"metadataJson":"{}","piiStorageKey":"","piiJson":"","piiJurisdiction":"","actorId":"a-1","summary":"init","sources":[]},"id":1}'
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
import { ProfileProviderHandlers } from "./entity.js"
import { ProfileProviderRpcs } from "./contracts.js"
import { InfrastructureLayer } from "./layers.js"
import { MigrationsLayer } from "./migrate.js"

const SqlLayer = PgClient.layerConfig(
  Config.map(Config.redacted("DATABASE_URL"), (url) => ({ url }))
)

const ProfileProviderRpcRoute = RpcServer
  .layerHttpRouter({ group: ProfileProviderRpcs, path: "/rpc/profile-provider", protocol: "http" })
  .pipe(
    Layer.provide(ProfileProviderHandlers),
    Layer.provide(RpcSerialization.layerJsonRpc())
  )

const HealthRoute = HttpLayerRouter.add(
  "GET",
  "/health",
  HttpServerResponse.json({ status: "ok" })
)

const ServerLayer = HttpLayerRouter.serve(
  Layer.mergeAll(ProfileProviderRpcRoute, HealthRoute)
).pipe(
  Layer.provide(BunHttpServer.layerConfig(
    Config.map(
      Config.integer("PORT").pipe(Config.withDefault(4100)),
      (port) => ({ port })
    )
  )),
  Layer.provide(MigrationsLayer),
  Layer.provide(InfrastructureLayer),
  Layer.provide(SqlLayer)
)

BunRuntime.runMain(
  Effect.gen(function* () {
    const port = yield* Config.integer("PORT").pipe(Config.withDefault(4100))
    yield* Effect.log(`Profile provider service (dev mode) on http://localhost:${port}`)
    yield* Effect.log("Endpoints: CreateProfile | MergeProfileData | CreateProfileSnapshot | PublishProfileSnapshot")
    yield* Effect.never
  }).pipe(Effect.provide(ServerLayer)) as Effect.Effect<void, never, never>
)
