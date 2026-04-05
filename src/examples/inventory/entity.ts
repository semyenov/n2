/**
 * Inventory cluster entity. Stateful via Ref.
 */
import * as Effect from "effect/Effect"
import * as Ref from "effect/Ref"
import * as Duration from "effect/Duration"
import { Entity } from "@effect/cluster"
import { handleCommand } from "./aggregate.js"
import {
  type InventoryState,
  type InventoryCommand,
  InventoryEntity,
  StockResult,
  InsufficientStock,
  ReserveStock,
  ReleaseStock,
  initialInventoryState
} from "./contracts.js"

export const InventoryEntityLayer = InventoryEntity.toLayer(
  Effect.gen(function*() {
    const address = yield* Entity.CurrentAddress
    const stateRef = yield* Ref.make<InventoryState>(initialInventoryState)
    let revision = 0

    const dispatch = (command: InventoryCommand) =>
      Effect.gen(function*() {
        const state = yield* Ref.get(stateRef)
        const result = yield* handleCommand(state, command)
        yield* Ref.set(stateRef, result.state)
        revision += result.events.length
        return result
      })

    return InventoryEntity.of({
      ReserveStock: (req) =>
        dispatch(new ReserveStock(req.payload)).pipe(
          Effect.map(({ state }) => new StockResult({
            sku: req.payload.sku,
            available: state.available,
            reserved: state.reserved
          })),
          Effect.catchAll((err) => Effect.fail(
            err instanceof InsufficientStock
              ? err
              : new InsufficientStock({ sku: req.payload.sku, requested: 0, available: 0 })
          ))
        ),
      ReleaseStock: (req) =>
        dispatch(new ReleaseStock(req.payload)).pipe(
          Effect.map(({ state }) => new StockResult({
            sku: req.payload.sku,
            available: state.available,
            reserved: state.reserved
          })),
          Effect.catchAll((err) => Effect.fail(
            err instanceof InsufficientStock
              ? err
              : new InsufficientStock({ sku: req.payload.sku, requested: 0, available: 0 })
          ))
        )
    })
  }),
  { maxIdleTime: Duration.minutes(30) }
)

export { InventoryEntity }
