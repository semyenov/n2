/**
 * Inventory EventGroup — bridges domain events to @effect/experimental EventLog.
 * Payload schemas derived from contracts.ts via eventPayloadSchema().
 */
import { EventGroup } from "@effect/experimental"
import * as EventLogApi from "@effect/experimental/EventLog"
import * as N2 from "../../framework/helpers/index.js"
import { StockReserved, StockReleased } from "./contracts.js"

export const InventoryEventGroup = EventGroup.empty
  .add({ tag: "StockReserved", primaryKey: (p: { sku: string }) => p.sku, payload: N2.eventPayloadSchema(StockReserved) })
  .add({ tag: "StockReleased", primaryKey: (p: { sku: string }) => p.sku, payload: N2.eventPayloadSchema(StockReleased) })

export const InventoryEventLogSchema = EventLogApi.schema(InventoryEventGroup)
