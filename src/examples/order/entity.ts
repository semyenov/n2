/**
 * Order cluster entity + direct RPC handlers.
 * Uses the Order definition as the single source of truth.
 */
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import { EntityProxy, EntityProxyServer } from "@effect/cluster"
import { OrderFulfillmentWorkflow } from "./workflows.js"
import { WorkflowLayer } from "./layers.js"
import { Order, initialOrderState } from "./aggregate.js"
import {
  OrderEntity,
  OrderRpcs,
  CommandResult,
  FulfillmentResult,
  OrderError,
  OrderNotFound,
  OrderState,
  CreateOrder,
  AddItem,
  SubmitOrder,
  CancelOrder,
  FulfillOrder,
  type OrderCommand
} from "./contracts.js"

// ---------------------------------------------------------------------------
// Cluster Entity: stateful per entity ID
// ---------------------------------------------------------------------------

export const OrderEntityLayer = Order.toEntityLayer(
  OrderEntity,
  {
    toResult: ({ entityId, revision }) =>
      new CommandResult({ orderId: entityId, revision }),
    toError: (error) =>
      error instanceof OrderError
        ? error
        : new OrderError({ message: String(error) })
  },
  {
    maxIdleTime: "10 minutes",
    concurrency: "unbounded"
  }
)

// ---------------------------------------------------------------------------
// EntityProxy (auto-derived from Entity)
// ---------------------------------------------------------------------------

export const OrderProxyRpcs = EntityProxy.toRpcGroup(OrderEntity)
export const OrderProxyHandlers = EntityProxyServer.layerRpcHandlers(OrderEntity)

// ---------------------------------------------------------------------------
// Direct RPC handlers — stateful in-memory (dev server)
//
// Each command reads current state, processes, writes back.
// GetOrder reads without mutating.
// ---------------------------------------------------------------------------

const stateMap    = new Map<string, OrderState>()
const revisionMap = new Map<string, number>()

const runCommand = (orderId: string, command: OrderCommand) =>
  Order.handle(stateMap.get(orderId) ?? initialOrderState, command).pipe(
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
    SubmitOrder: (p) => runCommand(p.orderId, new SubmitOrder(p)),
    CancelOrder: (p) => runCommand(p.orderId, new CancelOrder(p)),
    GetOrder: ({ orderId }) => {
      const state = stateMap.get(orderId)
      return state
        ? Effect.succeed(state)
        : Effect.fail(new OrderNotFound({ orderId }))
    },
    FulfillOrder: (p) =>
      OrderFulfillmentWorkflow.execute({ orderId: p.orderId, sku: p.sku, quantity: p.quantity }).pipe(
        Effect.map(({ orderId, shipped }) => new FulfillmentResult({ orderId, shipped })),
        Effect.mapError((e) => new OrderError({ message: e.message })),
        Effect.scoped
      )
  })
)

// Satisfy the WorkflowEngine requirement inline so the route layer stays R = never.
// WorkflowLayer is the same reference used in InfrastructureLayer — Effect deduplicates
// it to a single engine instance shared across the whole server.
export const OrderHandlers = Layer.provide(OrderHandlersRaw, WorkflowLayer)
