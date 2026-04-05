/**
 * Inventory cluster entity. Uses the Inventory definition directly.
 */
import { Inventory } from "./aggregate.js"
import {
  InventoryEntity,
  StockResult,
  InsufficientStock
} from "./contracts.js"

export const InventoryEntityLayer = Inventory.toEntityLayer(
  InventoryEntity,
  {
    toResult: ({ state }) => new StockResult({
      sku: state.sku,
      available: state.available,
      reserved: state.reserved
    }),
    toError: (error) =>
      error instanceof InsufficientStock
        ? error
        : new InsufficientStock({ sku: "", requested: 0, available: 0 })
  },
  {
    maxIdleTime: "30 minutes",
    concurrency: "unbounded"
  }
)

export { InventoryEntity }
