/**
 * Order entity wiring — two modes side by side.
 *
 * ┌─────────────────────────────────────────────────────────────────────┐
 * │  Cluster entity (production)                                         │
 * │  State lives in a Ref<OrderState> per entity ID.                    │
 * │  Entity.toLayer builds one actor per entityId, initialised on       │
 * │  first access and evicted after maxIdleTime of inactivity.          │
 * ├─────────────────────────────────────────────────────────────────────┤
 * │  In-memory handlers (dev / testing)                                  │
 * │  State in module-level Maps. No cluster, no network.                │
 * └─────────────────────────────────────────────────────────────────────┘
 */
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Ref from "effect/Ref"
import * as SynchronizedRef from "effect/SynchronizedRef"
import { Entity, EntityProxy, EntityProxyServer } from "@effect/cluster"
import * as EventLogApi from "@effect/experimental/EventLog"
import { InfrastructureLayer } from "./layers.js"
import { handle, initialOrderState } from "./aggregate.js"
import { OrderFulfillmentWorkflow } from "./workflows.js"
import { OrderEventGroup, OrderEventLogSchema } from "./events.js"
import {
  type OrderEvent,
  OrderEntity,
  OrderRpcs,
  CommandResult,
  OrderError,
  OrderNotFound,
  OrderState,
  CreateOrder,
  AddItem,
  SubmitOrder,
  CancelOrder,
  GetOrder
} from "./contracts.js"

// ---------------------------------------------------------------------------
// Cluster entity layer (production)
//
// Entity.toLayer registers this entity type with the Sharding runtime.
// The Effect passed to toLayer is executed once per entityId:
//   - Entity.CurrentAddress yields the entityId for this actor instance
//   - Ref.make creates a mutable state cell private to this actor
//   - The returned object maps each Rpc tag to its handler
//
// Handlers receive Entity.Request<Rpc> — an envelope that includes:
//   - req.payload  — the decoded request payload (orderId, sku, etc.)
//   - req.headers  — transport headers
//
// ClusterSchema.Persisted (set in contracts.ts) means each state change
// is written to EventJournal before the response is sent.
// ---------------------------------------------------------------------------

export const OrderEntityLayer = OrderEntity.toLayer(
  Effect.gen(function* () {
    const address = yield* Entity.CurrentAddress
    const stateRef = yield* Ref.make(initialOrderState)
    let revision = 0

    // Helper: apply a command to the current state and persist the result.
    const dispatch = (command: CreateOrder | AddItem | SubmitOrder | CancelOrder) =>
      Effect.gen(function* () {
        const state = yield* Ref.get(stateRef)
        const result = yield* handle(state, command).pipe(
          Effect.mapError((e) =>
            e instanceof OrderError ? e : new OrderError({ message: String(e) })
          )
        )
        yield* Ref.set(stateRef, result.state)
        revision += result.events.length
        return new CommandResult({ orderId: address.entityId, revision })
      })

    return OrderEntity.of({
      CreateOrder: (req) => dispatch(new CreateOrder(req.payload)),
      AddItem:     (req) => dispatch(new AddItem(req.payload)),
      CancelOrder: (req) => dispatch(new CancelOrder(req.payload)),

      // SubmitOrder: dispatch the command, then fire the fulfillment workflow.
      // discard: true — don't block the RPC response waiting for the workflow.
      // idempotencyKey is orderId so a duplicate SubmitOrder joins the running workflow.
      SubmitOrder: (req) =>
        Effect.gen(function* () {
          const result = yield* dispatch(new SubmitOrder(req.payload))
          const state = yield* Ref.get(stateRef)
          const firstItem = state.items[0]
          if (firstItem !== undefined) {
            yield* OrderFulfillmentWorkflow.execute(
              { orderId: req.payload.orderId, sku: firstItem.sku, quantity: firstItem.quantity },
              { discard: true }
            )
          }
          return result
        }),

      // GetOrder: read-only — no events, no journal write.
      GetOrder: (_req) =>
        Effect.gen(function* () {
          const state = yield* Ref.get(stateRef)
          if (state.status === "empty") {
            return yield* new OrderNotFound({ orderId: address.entityId })
          }
          return state
        })
    })
  }),
  {
    maxIdleTime: "10 minutes",
    concurrency: "unbounded"
  }
)

// ---------------------------------------------------------------------------
// EntityProxy — cluster → HTTP bridge
//
// EntityProxy.toRpcGroup derives an RpcGroup from the entity definition.
// EntityProxyServer.layerRpcHandlers generates handlers that forward each
// incoming RPC call as a Sharding message to the correct shard.
// ---------------------------------------------------------------------------

export const OrderProxyRpcs    = EntityProxy.toRpcGroup(OrderEntity)
export const OrderProxyHandlers = EntityProxyServer.layerRpcHandlers(OrderEntity)

// ---------------------------------------------------------------------------
// In-memory handlers (dev mode)
//
// State is stored in a SynchronizedRef<Map> created inside the Layer.
// This keeps state scoped to the layer's lifetime (no global mutable data)
// and ensures atomic read-then-update per orderId: SynchronizedRef.modifyEffect
// holds the ref's lock for the duration of the Effect, so concurrent commands
// on the same orderId are serialised automatically.
//
// OrderRpcs.toLayer accepts an Effect<Handlers> — the SynchronizedRef is
// allocated once when the layer builds, then shared across all handlers.
// ---------------------------------------------------------------------------

type OrderEntry = { readonly state: OrderState; readonly revision: number }

const OrderHandlersRaw = OrderRpcs.toLayer(
  Effect.gen(function* () {
    const store   = yield* SynchronizedRef.make(new Map<string, OrderEntry>())

    // Obtain the publish function once at layer-build time.
    // makeClient requires EventLog in context (provided by InfrastructureLayer below).
    // The returned publish fn has R = never — EventLog is consumed at acquire time.
    const publish = yield* EventLogApi.makeClient(OrderEventLogSchema)

    // Map a domain event to an EventLog publish call.
    const publishEvent = (event: OrderEvent) => {
      switch (event._tag) {
        case "OrderCreated":
          return publish("OrderCreated",   { orderId: event.orderId, customerId: event.customerId, createdAt: event.createdAt })
        case "ItemAdded":
          return publish("ItemAdded",      { orderId: event.orderId, sku: event.sku, quantity: event.quantity, price: event.price })
        case "OrderSubmitted":
          return publish("OrderSubmitted", { orderId: event.orderId, submittedAt: event.submittedAt })
        case "OrderCancelled":
          return publish("OrderCancelled", { orderId: event.orderId, reason: event.reason, cancelledAt: event.cancelledAt })
      }
    }

    // Apply a command, update state, then publish resulting events outside the lock.
    // SynchronizedRef.modifyEffect returns [CommandResult, events]; the lock is
    // released before the publish I/O starts.
    const runCommand = (orderId: string, command: CreateOrder | AddItem | SubmitOrder | CancelOrder) =>
      Effect.gen(function* () {
        const [result, events] = yield* SynchronizedRef.modifyEffect(store, (map) => {
          const { state, revision } = map.get(orderId) ?? { state: initialOrderState, revision: 0 }
          return handle(state, command).pipe(
            Effect.mapError((e) =>
              e instanceof OrderError ? e : new OrderError({ message: String(e) })
            ),
            Effect.map(({ events, state: next }) => {
              const nextRevision = revision + events.length
              const nextMap = new Map(map).set(orderId, { state: next, revision: nextRevision })
              return [[new CommandResult({ orderId, revision: nextRevision }), events] as const, nextMap] as const
            })
          )
        })
        // Publish events fire-and-forget: journal errors don't fail the command.
        yield* Effect.forEach(events, publishEvent, { discard: true }).pipe(Effect.ignore)
        return result
      })

    return OrderRpcs.of({
      CreateOrder: (p) => runCommand(p.orderId, new CreateOrder(p)),
      AddItem:     (p) => runCommand(p.orderId, new AddItem(p)),
      CancelOrder: (p) => runCommand(p.orderId, new CancelOrder(p)),

      // SubmitOrder: dispatch, publish events, then fire the fulfillment workflow.
      SubmitOrder: (p) =>
        Effect.gen(function* () {
          const result = yield* runCommand(p.orderId, new SubmitOrder(p))
          const entry = yield* SynchronizedRef.get(store).pipe(
            Effect.map((map) => map.get(p.orderId))
          )
          const firstItem = entry?.state.items[0]
          if (firstItem !== undefined) {
            yield* OrderFulfillmentWorkflow.execute(
              { orderId: p.orderId, sku: firstItem.sku, quantity: firstItem.quantity },
              { discard: true }
            )
          }
          return result
        }),

      // GetOrder: read from the map without modifying it.
      GetOrder: (p) =>
        SynchronizedRef.modifyEffect(store, (map) => {
          const entry = map.get(p.orderId)
          if (!entry || entry.state.status === "empty") {
            return Effect.fail(new OrderNotFound({ orderId: p.orderId }))
          }
          return Effect.succeed([entry.state, map] as const)
        })
    })
  })
)

// InfrastructureLayer provides EventLog (required by makeClient above) plus
// WorkflowEngine. OrderHandlers has R = SqlClient — server.ts provides SqlLayer.
export const OrderHandlers = Layer.provide(OrderHandlersRaw, InfrastructureLayer)
