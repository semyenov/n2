/**
 * Inventory cluster entity + direct RPC handlers.
 */
import { EntityProxy, EntityProxyServer } from "@effect/cluster"
import { Inventory } from "./aggregate.js"
import {
  type InventoryCommand,
  InventoryEntity,
  InventoryRpcs,
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
  { maxIdleTime: "30 minutes", concurrency: "unbounded" }
)

export const InventoryProxyRpcs = EntityProxy.toRpcGroup(InventoryEntity)
export const InventoryProxyHandlers = EntityProxyServer.layerRpcHandlers(InventoryEntity)

export const InventoryHandlersRaw = Inventory.toStatefulRpcHandlers(
  InventoryRpcs,
  {
    entityId: (command: InventoryCommand) => command.sku,
    toResult: ({ state }) => new StockResult({
      sku: state.sku,
      available: state.available,
      reserved: state.reserved
    }),
    toError: (error) =>
      error instanceof InsufficientStock
        ? error
        : new InsufficientStock({ sku: "", requested: 0, available: 0 })
  }
)

export { InventoryEntity }
