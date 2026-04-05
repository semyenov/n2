/**
 * Order cluster entity with EntityProxy for automatic RPC bridging.
 *
 * Two modes:
 * 1. Cluster mode: Entity.toLayer + EntityProxyServer (automatic routing)
 * 2. Direct mode: OrderRpcs + manual handlers (non-cluster, HTTP-only)
 */
import * as Effect from "effect/Effect"
import * as Duration from "effect/Duration"
import { Entity, EntityId, EntityProxy, EntityProxyServer } from "@effect/cluster"
import * as AggregateEntity from "../../framework/cluster/AggregateEntity.js"
import { OrderAggregate } from "./aggregate.js"
import {
  OrderEntity,
  OrderRpcs,
  CommandResult,
  OrderError,
  CreateOrder,
  AddItem,
  SubmitOrder,
  CancelOrder
} from "./contracts.js"

// ---------------------------------------------------------------------------
// Aggregate runtime (shared by both modes)
// ---------------------------------------------------------------------------

const { runtime: orderRuntime } = AggregateEntity.make(
  OrderAggregate,
  OrderEntity.protocol,
  { maxIdleTime: Duration.minutes(10), snapshotEvery: 50 }
)

// ---------------------------------------------------------------------------
// Mode 1: Cluster Entity with EntityProxy (recommended for production)
//
// Entity.toLayer registers the entity with the cluster.
// EntityProxy.toRpcGroup derives an RpcGroup from the entity.
// EntityProxyServer.layerRpcHandlers wires the proxy handlers.
// ---------------------------------------------------------------------------

/** Entity behavior: each RPC handler delegates to the aggregate runtime. */
export const OrderEntityLayer = OrderEntity.toLayer(
  Effect.gen(function* () {
    const address = yield* Entity.CurrentAddress
    return OrderEntity.of({
      CreateOrder: (req) =>
        orderRuntime.handle(
          address.entityId,
          new CreateOrder({ orderId: req.payload.orderId, customerId: req.payload.customerId })
        ).pipe(
          Effect.map((r) => new CommandResult({ orderId: address.entityId, revision: r.revision })),
          Effect.catchAll((err) => Effect.fail(new OrderError({ message: String(err) })))
        ),

      AddItem: (req) =>
        orderRuntime.handle(
          address.entityId,
          new AddItem({
            orderId: req.payload.orderId,
            sku: req.payload.sku,
            quantity: req.payload.quantity,
            price: req.payload.price
          })
        ).pipe(
          Effect.map((r) => new CommandResult({ orderId: address.entityId, revision: r.revision })),
          Effect.catchAll((err) => Effect.fail(new OrderError({ message: String(err) })))
        ),

      SubmitOrder: (req) =>
        orderRuntime.handle(
          address.entityId,
          new SubmitOrder({ orderId: req.payload.orderId })
        ).pipe(
          Effect.map((r) => new CommandResult({ orderId: address.entityId, revision: r.revision })),
          Effect.catchAll((err) => Effect.fail(new OrderError({ message: String(err) })))
        ),

      CancelOrder: (req) =>
        orderRuntime.handle(
          address.entityId,
          new CancelOrder({ orderId: req.payload.orderId, reason: req.payload.reason })
        ).pipe(
          Effect.map((r) => new CommandResult({ orderId: address.entityId, revision: r.revision })),
          Effect.catchAll((err) => Effect.fail(new OrderError({ message: String(err) })))
        )
    })
  }),
  { maxIdleTime: Duration.minutes(10) }
)

/**
 * RPC group derived from the Entity via EntityProxy.
 * Automatically adds entityId to each RPC payload and wraps errors.
 */
export const OrderProxyRpcs = EntityProxy.toRpcGroup(OrderEntity)

/**
 * RPC handler layer via EntityProxyServer.
 * Automatically routes RPC calls to the correct entity instance via Sharding.
 */
export const OrderProxyHandlers = EntityProxyServer.layerRpcHandlers(OrderEntity)

// ---------------------------------------------------------------------------
// Mode 2: Direct handlers (non-cluster, for HTTP-only or testing)
// ---------------------------------------------------------------------------

/** Direct handlers for OrderRpcs (no cluster dependency). */
export const OrderHandlers = OrderRpcs.toLayer(
  OrderRpcs.of({
    CreateOrder: (payload) =>
      Effect.gen(function* () {
        const result = yield* orderRuntime.handle(
          EntityId.make(payload.orderId),
          new CreateOrder({ customerId: payload.customerId, orderId: payload.orderId })
        )
        return new CommandResult({ orderId: payload.orderId, revision: result.revision })
      }).pipe(
        Effect.catchAll((err) => Effect.fail(new OrderError({ message: String(err) })))
      ),

    AddItem: (payload) =>
      orderRuntime.handle(
        EntityId.make(payload.orderId),
        new AddItem({ orderId: payload.orderId, sku: payload.sku, quantity: payload.quantity, price: payload.price })
      ).pipe(
        Effect.map((r) => new CommandResult({ orderId: payload.orderId, revision: r.revision })),
        Effect.catchAll((err) => Effect.fail(new OrderError({ message: String(err) })))
      ),

    SubmitOrder: (payload) =>
      orderRuntime.handle(
        EntityId.make(payload.orderId),
        new SubmitOrder({ orderId: payload.orderId })
      ).pipe(
        Effect.map((r) => new CommandResult({ orderId: payload.orderId, revision: r.revision })),
        Effect.catchAll((err) => Effect.fail(new OrderError({ message: String(err) })))
      ),

    CancelOrder: (payload) =>
      orderRuntime.handle(
        EntityId.make(payload.orderId),
        new CancelOrder({ orderId: payload.orderId, reason: payload.reason })
      ).pipe(
        Effect.map((r) => new CommandResult({ orderId: payload.orderId, revision: r.revision })),
        Effect.catchAll((err) => Effect.fail(new OrderError({ message: String(err) })))
      )
  })
)

export { orderRuntime, OrderEntity }
