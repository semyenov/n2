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
import { Entity, EntityProxy, EntityProxyServer } from "@effect/cluster"
import { WorkflowLayer } from "./layers.js"
import { handle, initialOrderState } from "./aggregate.js"
import {
  OrderEntity,
  OrderRpcs,
  CommandResult,
  OrderError,
  OrderState,
  CreateOrder,
  AddItem,
  SubmitOrder
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
    const dispatch = (command: CreateOrder | AddItem | SubmitOrder) =>
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
      SubmitOrder: (req) => dispatch(new SubmitOrder(req.payload))
    })
  }),
  { maxIdleTime: "10 minutes", concurrency: "unbounded" }
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
// runCommand: reads current state → calls handle → writes back.
// Each command sees the state left by prior commands on the same orderId.
//
// OrderRpcs.of({ ... }) is typed by the RpcGroup derived from OrderEntity —
// TypeScript enforces that every Rpc tag has a handler.
//
// To trigger a workflow from a handler:
//   OrderFulfillmentWorkflow.execute({ orderId, sku, quantity }).pipe(Effect.scoped)
// ---------------------------------------------------------------------------

const stateMap    = new Map<string, OrderState>()
const revisionMap = new Map<string, number>()

const runCommand = (orderId: string, command: CreateOrder | AddItem | SubmitOrder) =>
  handle(stateMap.get(orderId) ?? initialOrderState, command).pipe(
    Effect.map(({ events, state }) => {
      stateMap.set(orderId, state)
      const revision = (revisionMap.get(orderId) ?? 0) + events.length
      revisionMap.set(orderId, revision)
      return new CommandResult({ orderId, revision })
    }),
    Effect.mapError((e) =>
      e instanceof OrderError ? e : new OrderError({ message: String(e) })
    )
  )

const OrderHandlersRaw = OrderRpcs.toLayer(
  OrderRpcs.of({
    CreateOrder: (p) => runCommand(p.orderId, new CreateOrder(p)),
    AddItem:     (p) => runCommand(p.orderId, new AddItem(p)),
    SubmitOrder: (p) => runCommand(p.orderId, new SubmitOrder(p))
  })
)

// WorkflowLayer provided here so OrderHandlers has R = never,
// making it composable into any server layer without extra wiring.
// Effect deduplicates WorkflowLayer — it is built once even though
// entity.ts and layers.ts both reference it.
export const OrderHandlers = Layer.provide(OrderHandlersRaw, WorkflowLayer)
