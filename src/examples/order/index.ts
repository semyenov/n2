/**
 * Order service entry point.
 *
 * Demonstrates:
 * - HTTP RPC server for commands (direct mode)
 * - Singleton projection runner (cluster mode)
 * - Outbox publisher (cluster mode)
 */
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import { BunHttpServer } from "@effect/platform-bun"
import { HttpLayerRouter } from "@effect/platform"
import { Singleton } from "@effect/cluster"
import { OrderRpcRoute } from "./http.js"
import { InfrastructureLayer } from "./layers.js"
import * as Subscription from "../../framework/projection/Subscription.js"
import * as OutboxPublisher from "../../framework/runtime/OutboxPublisher.js"
import { OrdersViewProjector } from "./projector.js"

// ---------------------------------------------------------------------------
// HTTP server (direct mode, no cluster)
// ---------------------------------------------------------------------------

const ServerLayer = HttpLayerRouter.serve(OrderRpcRoute).pipe(
  Layer.provide(BunHttpServer.layer({ port: 3000 })),
  Layer.provide(InfrastructureLayer)
)

// ---------------------------------------------------------------------------
// Singleton projection runner (cluster mode)
// Runs exactly one instance of the orders_view projection across the cluster.
// ---------------------------------------------------------------------------

export const OrdersProjectionSingleton = Singleton.make(
  "orders-projection",
  Subscription.make(OrdersViewProjector, {
    topics: ["n2.aggregate.Order.events"],
    groupId: "orders-view-group",
    fromBeginning: true
  })
)

// ---------------------------------------------------------------------------
// Singleton outbox publisher (cluster mode)
// Polls EventLog and publishes to Kafka. At-least-once delivery.
// ---------------------------------------------------------------------------

export const OutboxSingleton = Singleton.make(
  "outbox-publisher",
  OutboxPublisher.run({ aggregateType: "Order", pollIntervalSeconds: 1 })
)

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

const main = Effect.gen(function*() {
  yield* Effect.log("Order service starting on port 3000...")
  yield* Effect.log("RPC endpoint: POST http://localhost:3000/rpc/orders")
  yield* Effect.never
}).pipe(Effect.provide(ServerLayer))

Effect.runPromise(main)
