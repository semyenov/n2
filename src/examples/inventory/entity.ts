/**
 * Inventory cluster entity.
 */
import * as Effect from "effect/Effect"
import * as Duration from "effect/Duration"
import { Entity } from "@effect/cluster"
import * as AggregateEntity from "../../framework/cluster/AggregateEntity.js"
import { InventoryAggregate } from "./aggregate.js"
import {
  InventoryEntity,
  StockResult,
  InsufficientStock,
  ReserveStock,
  ReleaseStock
} from "./contracts.js"

const { runtime: inventoryRuntime } = AggregateEntity.make(
  InventoryAggregate,
  InventoryEntity.protocol,
  { maxIdleTime: Duration.minutes(30) }
)

export const InventoryEntityLayer = InventoryEntity.toLayer(
  Effect.gen(function* () {
    const address = yield* Entity.CurrentAddress
    return InventoryEntity.of({
      ReserveStock: (req) =>
        inventoryRuntime.handle(
          address.entityId,
          new ReserveStock({
            sku: req.payload.sku,
            quantity: req.payload.quantity,
            orderId: req.payload.orderId
          })
        ).pipe(
          Effect.map((r) => new StockResult({
            sku: req.payload.sku,
            available: r.state.available,
            reserved: r.state.reserved
          })),
          Effect.catchAll((err) => Effect.fail(
            err instanceof InsufficientStock
              ? err
              : new InsufficientStock({ sku: req.payload.sku, requested: 0, available: 0 })
          ))
        ),

      ReleaseStock: (req) =>
        inventoryRuntime.handle(
          address.entityId,
          new ReleaseStock({
            sku: req.payload.sku,
            quantity: req.payload.quantity,
            orderId: req.payload.orderId
          })
        ).pipe(
          Effect.map((r) => new StockResult({
            sku: req.payload.sku,
            available: r.state.available,
            reserved: r.state.reserved
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

export { inventoryRuntime, InventoryEntity }
