/**
 * Inventory cluster entity. Uses N2 helpers.
 */
import * as N2 from "../../framework/helpers/index.js"
import { handleCommand } from "./aggregate.js"
import {
  InventoryEntity,
  StockResult,
  InsufficientStock,
  ReserveStock,
  ReleaseStock,
  initialInventoryState
} from "./contracts.js"

export const InventoryEntityLayer = N2.Entity.makeEntityLayer(InventoryEntity, {
  handleCommand,
  initialState: initialInventoryState,
  toResult: (_entityId, _revision, state) => new StockResult({
    sku: state.sku,
    available: state.available,
    reserved: state.reserved
  }),
  toError: (err) =>
    err instanceof InsufficientStock
      ? err
      : new InsufficientStock({ sku: "", requested: 0, available: 0 }),
  commands: { ReserveStock, ReleaseStock }
}, { maxIdleTime: "30 minutes" })

export { InventoryEntity }
