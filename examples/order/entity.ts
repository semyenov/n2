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
import * as Metric from "effect/Metric"
import * as Option from "effect/Option"
import * as Ref from "effect/Ref"
import * as Schedule from "effect/Schedule"
import * as SynchronizedRef from "effect/SynchronizedRef"
import { Entity, EntityProxy, EntityProxyServer } from "@effect/cluster"
import * as EventLogApi from "@effect/experimental/EventLog"
import { InfrastructureLayer } from "./layers.js"
import { handle, initialOrderState } from "./aggregate.js"
import { OrderFulfillmentWorkflow } from "./workflows.js"
import { OrderEventGroup, OrderEventLogSchema } from "./events.js"
import { OrderSnapshots, SNAPSHOT_EVERY, type SnapshotEntry } from "./snapshots.js"
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
    const address  = yield* Entity.CurrentAddress
    const snapshots = yield* OrderSnapshots

    // Load snapshot on entity init — recover state without replaying every event.
    // Falls back to initialOrderState if no snapshot exists or load fails.
    const initial = yield* snapshots.load(address.entityId).pipe(
      Effect.orElse(() => Effect.succeed(Option.none<SnapshotEntry>()))
    )
    const stateRef = yield* Ref.make(
      Option.match(initial, { onNone: () => initialOrderState, onSome: ({ state }) => state })
    )
    let revision = Option.match(initial, { onNone: () => 0, onSome: ({ revision: r }) => r })

    // Helper: apply a command to the current state and persist the result.
    // Saves a snapshot every SNAPSHOT_EVERY events (fire-and-forget).
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
        if (revision % SNAPSHOT_EVERY === 0) {
          yield* snapshots.save(address.entityId, result.state, revision).pipe(
            Effect.tapError((e) => Effect.logWarning(`[entity] snapshot save failed: ${String(e)}`)),
            Effect.ignore
          )
        }
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

// ---------------------------------------------------------------------------
// Metrics — zero-context global values (R = never), created once at module load.
// Identified by name + tags; Effect deduplicates by name+tags in the registry.
// ---------------------------------------------------------------------------

const commandTotal   = Metric.counter("order.commands.total",   { incremental: true })
const commandErrors  = Metric.counter("order.commands.errors",  { incremental: true })
const commandLatency = Metric.timer("order.command.duration_ms", "milliseconds")

// ---------------------------------------------------------------------------
// Retry schedule for event publishing — exponential backoff with jitter.
// Retries up to 3 extra attempts (4 total) before giving up.
// ---------------------------------------------------------------------------

// exponential backoff with jitter, capped at 3 extra attempts (4 total).
// intersect stops when EITHER schedule stops — recurs(3) caps after 3 iterations.
const publishRetry = Schedule.exponential("100 millis").pipe(
  Schedule.jittered,
  Schedule.intersect(Schedule.recurs(3))
)

export const OrderHandlersRaw = OrderRpcs.toLayer(
  Effect.gen(function* () {
    const store     = yield* SynchronizedRef.make(new Map<string, OrderEntry>())
    const snapshots = yield* OrderSnapshots

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

    // Get entry from map or fall back to snapshot (then initialOrderState).
    // Called inside SynchronizedRef.modifyEffect so results flow into the atomic update.
    const getOrLoad = (map: Map<string, OrderEntry>, orderId: string): Effect.Effect<OrderEntry> =>
      map.has(orderId)
        ? Effect.succeed(map.get(orderId)!)
        : snapshots.load(orderId).pipe(
            Effect.map(Option.getOrElse(() => ({ state: initialOrderState, revision: 0 }))),
            Effect.orElse(() => Effect.succeed({ state: initialOrderState, revision: 0 }))
          )

    // Apply a command, update state, then publish resulting events outside the lock.
    // SynchronizedRef.modifyEffect holds the lock only for the state mutation;
    // event publishing and snapshot saving happen after it is released.
    //
    // Metrics: commandLatency (histogram), commandTotal / commandErrors (counters).
    // Publish retry: up to 3 extra attempts with exponential+jittered backoff.
    const runCommand = (orderId: string, command: CreateOrder | AddItem | SubmitOrder | CancelOrder) => {
      const tag = command._tag
      return Effect.gen(function* () {
        const [result, events, nextState] = yield* SynchronizedRef.modifyEffect(store, (map) =>
          getOrLoad(map, orderId).pipe(
            Effect.flatMap(({ state, revision }) =>
              handle(state, command).pipe(
                Effect.mapError((e) =>
                  e instanceof OrderError ? e : new OrderError({ message: String(e) })
                ),
                Effect.map(({ events, state: next }) => {
                  const nextRevision = revision + events.length
                  const nextMap = new Map(map).set(orderId, { state: next, revision: nextRevision })
                  return [
                    [new CommandResult({ orderId, revision: nextRevision }), events, next] as const,
                    nextMap
                  ] as const
                })
              )
            )
          )
        )
        yield* Effect.forEach(events, publishEvent, { discard: true }).pipe(
          Effect.retry(publishRetry),
          Effect.tapError((e) => Effect.logError(`[entity] event publish failed: ${String(e)}`)),
          Effect.ignore
        )
        if (result.revision % SNAPSHOT_EVERY === 0) {
          yield* snapshots.save(orderId, nextState, result.revision).pipe(
            Effect.tapError((e) => Effect.logWarning(`[entity] snapshot save failed: ${String(e)}`)),
            Effect.ignore
          )
        }
        return result
      }).pipe(
        Metric.trackDuration(Metric.tagged(commandLatency, "command", tag)),
        Effect.tap(() => Metric.increment(Metric.tagged(commandTotal, "command", tag))),
        Effect.tapError(() => Metric.increment(Metric.tagged(commandErrors, "command", tag)))
      )
    }

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

      // GetOrder: read from map; if not in memory, try snapshot (hydrates map for future reads).
      GetOrder: (p) =>
        SynchronizedRef.modifyEffect(store, (map) => {
          if (map.has(p.orderId)) {
            const entry = map.get(p.orderId)!
            if (entry.state.status === "empty") {
              return Effect.fail(new OrderNotFound({ orderId: p.orderId }))
            }
            return Effect.succeed([entry.state, map] as const)
          }
          // Not in memory — check snapshot to recover state after server restart.
          return snapshots.load(p.orderId).pipe(
            Effect.orElse(() => Effect.succeed(Option.none<SnapshotEntry>())),
            Effect.flatMap(Option.match({
              onNone: () => Effect.fail(new OrderNotFound({ orderId: p.orderId })),
              onSome: ({ state, revision }) => {
                if (state.status === "empty") {
                  return Effect.fail(new OrderNotFound({ orderId: p.orderId }))
                }
                const updatedMap = new Map(map).set(p.orderId, { state, revision })
                return Effect.succeed([state, updatedMap] as const)
              }
            }))
          )
        })
    })
  })
)

// InfrastructureLayer provides EventLog (required by makeClient above) plus
// WorkflowEngine. OrderHandlers has R = SqlClient — server.ts provides SqlLayer.
export const OrderHandlers = Layer.provide(OrderHandlersRaw, InfrastructureLayer)
