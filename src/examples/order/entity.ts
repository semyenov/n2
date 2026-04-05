/**
 * Order cluster entity.
 * Stateful -- maintains state via Ref between commands.
 * Uses handleCommand (decide+evolve) directly.
 */
import * as Effect from "effect/Effect"
import * as Ref from "effect/Ref"
import * as Duration from "effect/Duration"
import { Entity, EntityProxy, EntityProxyServer } from "@effect/cluster"
import { handleCommand } from "./aggregate.js"
import {
  type OrderState,
  type OrderCommand,
  OrderEntity,
  OrderRpcs,
  CommandResult,
  OrderError,
  CreateOrder,
  AddItem,
  SubmitOrder,
  CancelOrder,
  initialOrderState
} from "./contracts.js"

// ---------------------------------------------------------------------------
// Cluster Entity: stateful per entity ID
// ---------------------------------------------------------------------------

export const OrderEntityLayer = OrderEntity.toLayer(
  Effect.gen(function*() {
    const address = yield* Entity.CurrentAddress
    const stateRef = yield* Ref.make<OrderState>(initialOrderState)
    let revision = 0

    const dispatch = (command: OrderCommand) =>
      Effect.gen(function*() {
        const state = yield* Ref.get(stateRef)
        const result = yield* handleCommand(state, command)
        yield* Ref.set(stateRef, result.state)
        revision += result.events.length
        return new CommandResult({ orderId: address.entityId, revision })
      }).pipe(
        Effect.catchAll((err) => Effect.fail(new OrderError({ message: String(err) })))
      )

    return OrderEntity.of({
      CreateOrder: (req) => dispatch(new CreateOrder(req.payload)),
      AddItem: (req) => dispatch(new AddItem(req.payload)),
      SubmitOrder: (req) => dispatch(new SubmitOrder(req.payload)),
      CancelOrder: (req) => dispatch(new CancelOrder(req.payload))
    })
  }),
  { maxIdleTime: Duration.minutes(10) }
)

// ---------------------------------------------------------------------------
// EntityProxy
// ---------------------------------------------------------------------------

export const OrderProxyRpcs = EntityProxy.toRpcGroup(OrderEntity)
export const OrderProxyHandlers = EntityProxyServer.layerRpcHandlers(OrderEntity)

// ---------------------------------------------------------------------------
// Direct RPC handlers (non-cluster, stateless per request)
// ---------------------------------------------------------------------------

export const OrderHandlers = OrderRpcs.toLayer(
  OrderRpcs.of({
    CreateOrder: (payload) =>
      handleCommand(initialOrderState, new CreateOrder(payload)).pipe(
        Effect.map(({ events }) => new CommandResult({ orderId: payload.orderId, revision: events.length })),
        Effect.catchAll((err) => Effect.fail(new OrderError({ message: String(err) })))
      ),
    AddItem: (payload) =>
      handleCommand(initialOrderState, new AddItem(payload)).pipe(
        Effect.map(({ events }) => new CommandResult({ orderId: payload.orderId, revision: events.length })),
        Effect.catchAll((err) => Effect.fail(new OrderError({ message: String(err) })))
      ),
    SubmitOrder: (payload) =>
      handleCommand(initialOrderState, new SubmitOrder(payload)).pipe(
        Effect.map(({ events }) => new CommandResult({ orderId: payload.orderId, revision: events.length })),
        Effect.catchAll((err) => Effect.fail(new OrderError({ message: String(err) })))
      ),
    CancelOrder: (payload) =>
      handleCommand(initialOrderState, new CancelOrder(payload)).pipe(
        Effect.map(({ events }) => new CommandResult({ orderId: payload.orderId, revision: events.length })),
        Effect.catchAll((err) => Effect.fail(new OrderError({ message: String(err) })))
      )
  })
)
