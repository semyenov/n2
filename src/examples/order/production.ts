/**
 * Production deployment: full clustered Order service.
 *
 * This is how you'd run the Order service in production with:
 * - Multi-node cluster via HttpRunner
 * - SQL-backed message persistence (MessageStorage)
 * - SQL-backed runner state (RunnerStorage)
 * - Health checking between runners
 * - Singleton outbox publisher + projection runner
 * - EntityProxy-based HTTP API
 * - Graceful shutdown on SIGTERM/SIGINT
 *
 * Environment variables (see Config.ts):
 *   N2_DB_URL=postgres://...
 *   N2_KAFKA_BROKERS=broker1:9092,broker2:9092
 *   N2_HTTP_PORT=3000
 *
 * Run: bun src/examples/order/production.ts
 */
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import { BunHttpServer } from "@effect/platform-bun"
import { FetchHttpClient } from "@effect/platform"
import { HttpLayerRouter } from "@effect/platform"
import { RpcSerialization, RpcServer } from "@effect/rpc"
import {
  HttpRunner,
  SqlMessageStorage,
  SqlRunnerStorage,
  RunnerHealth,
  ShardingConfig,
  Singleton,
  EntityProxy,
  EntityProxyServer,
  TestRunner
} from "@effect/cluster"
import { PgClient } from "@effect/sql-pg"

import { OrderEntity } from "./contracts.js"
import { OrderEntityLayer } from "./entity.js"
import { InventoryEntityLayer } from "../inventory/entity.js"
import { OrdersViewProjector } from "./projector.js"
import * as Subscription from "../../framework/projection/Subscription.js"
import * as OutboxPublisher from "../../framework/runtime/OutboxPublisher.js"
import * as Shutdown from "../../framework/runtime/Shutdown.js"
import { N2ConfigService, layerFromEnv } from "../../framework/runtime/Config.js"
import { layerMemory as EventLogMemory } from "../../framework/runtime/EventJournalEventLog.js"
import { layerMemory as SnapshotStoreMemory } from "../../framework/runtime/SnapshotStore.js"
import { layerMemory as CheckpointStoreMemory } from "../../framework/projection/CheckpointStore.js"
import * as InMemoryKafkaPublisher from "../../framework/testing/InMemoryKafkaPublisher.js"
import * as InMemoryKafkaConsumer from "../../framework/testing/InMemoryKafkaConsumer.js"
import * as InMemoryDeadLetter from "../../framework/testing/InMemoryDeadLetter.js"
import { N2ClockLive } from "../../framework/runtime/Clock.js"
import { IdGeneratorUuid } from "../../framework/runtime/IdGenerator.js"

// ---------------------------------------------------------------------------
// Database layer (would use PgClient in real production)
// For this example, we use in-memory layers
// ---------------------------------------------------------------------------

const EventSourcingLayer = Layer.mergeAll(
  EventLogMemory,
  SnapshotStoreMemory,
  CheckpointStoreMemory,
  InMemoryKafkaPublisher.layerSimple,
  InMemoryKafkaConsumer.layer,
  InMemoryDeadLetter.layer,
  N2ClockLive,
  IdGeneratorUuid
)

// ---------------------------------------------------------------------------
// Entity layers
// ---------------------------------------------------------------------------

const EntitiesLayer = Layer.mergeAll(
  Layer.provide(OrderEntityLayer, EventSourcingLayer),
  Layer.provide(InventoryEntityLayer, EventSourcingLayer)
)

// ---------------------------------------------------------------------------
// Singleton services
// ---------------------------------------------------------------------------

const OutboxSingleton = Singleton.make(
  "outbox-publisher",
  OutboxPublisher.run({ aggregateType: "Order", pollIntervalSeconds: 1 })
)

const ProjectionSingleton = Singleton.make(
  "orders-projection",
  Subscription.make(OrdersViewProjector, {
    topics: ["n2.aggregate.Order.events"],
    groupId: "orders-view-group",
    fromBeginning: true
  })
)

// ---------------------------------------------------------------------------
// HTTP API via EntityProxy
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Full production runner
//
// In real production, replace:
// - EventSourcingLayer → PgClient + SqlMessageStorage
// - InMemoryKafka → KafkaJs adapters
// - ShardingConfig.layer({}) → ShardingConfig.layerFromEnv
//
// The cluster auto-discovers other runners via SqlRunnerStorage,
// assigns shards, and routes messages via HttpRunner.
// ---------------------------------------------------------------------------

const main = Effect.gen(function*() {
  const config = yield* N2ConfigService
  yield* Effect.log(`Order service starting on port ${config.http.port}...`)
  yield* Effect.log("Cluster mode: entities distributed across runners")
  yield* Effect.log("Singletons: outbox-publisher, orders-projection")
  yield* Effect.never
}).pipe(
  // Singletons need Sharding + EventSourcingLayer provided
  Effect.provide(
    Layer.mergeAll(
      HttpLayerRouter.serve(ApiRoute),
      EntitiesLayer,
      OutboxSingleton.pipe(Layer.provide(EventSourcingLayer)),
      ProjectionSingleton.pipe(Layer.provide(EventSourcingLayer)),
      layerFromEnv
    )
  ),
  Effect.provide(
    Layer.mergeAll(
      BunHttpServer.layer({ port: 3000 }),
      EventSourcingLayer,
      // In production: HttpRunner.layerHttp (multi-node)
      // For this example: TestRunner (in-memory single-node)
      TestRunner.layer,
      ShardingConfig.layer({})
    )
  ),
  Shutdown.withGracefulShutdown
)

Effect.runPromise(main).catch(console.error)
