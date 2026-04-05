/**
 * Inventory aggregate contracts.
 * Commands are Schema.TaggedRequest.
 */
import * as Schema from "effect/Schema"
import * as N2 from "../../framework/helpers/index.js"

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

export class StockReserved extends Schema.TaggedClass<StockReserved>()(
  "StockReserved",
  { sku: Schema.String, quantity: Schema.Number, orderId: Schema.String }
) {}

export class StockReleased extends Schema.TaggedClass<StockReleased>()(
  "StockReleased",
  { sku: Schema.String, quantity: Schema.Number, orderId: Schema.String }
) {}

export const InventoryEvent = Schema.Union(StockReserved, StockReleased)
export type InventoryEvent = typeof InventoryEvent.Type

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

export class InsufficientStock extends Schema.TaggedError<InsufficientStock>()(
  "InsufficientStock",
  { sku: Schema.String, requested: Schema.Number, available: Schema.Number }
) {}

export const InventoryErrors = Schema.Union(InsufficientStock)
export type InventoryErrors = typeof InventoryErrors.Type

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

export class InventoryState extends Schema.Class<InventoryState>("InventoryState")({
  sku: Schema.String,
  available: Schema.Number,
  reserved: Schema.Number
}) {}

export const initialInventoryState = new InventoryState({
  sku: "",
  available: 0,
  reserved: 0
})

// ---------------------------------------------------------------------------
// Result
// ---------------------------------------------------------------------------

export class StockResult extends Schema.Class<StockResult>("StockResult")({
  sku: Schema.String,
  available: Schema.Number,
  reserved: Schema.Number
}) {}

// ---------------------------------------------------------------------------
// Commands (Schema.TaggedRequest)
// ---------------------------------------------------------------------------

export class ReserveStock extends Schema.TaggedRequest<ReserveStock>("ReserveStock")(
  "ReserveStock",
  { failure: InsufficientStock, success: StockResult, payload: { sku: Schema.String, quantity: Schema.Number, orderId: Schema.String } }
) {}

export class ReleaseStock extends Schema.TaggedRequest<ReleaseStock>("ReleaseStock")(
  "ReleaseStock",
  { failure: InsufficientStock, success: StockResult, payload: { sku: Schema.String, quantity: Schema.Number, orderId: Schema.String } }
) {}

export const InventoryCommand = Schema.Union(ReserveStock, ReleaseStock)
export type InventoryCommand = typeof InventoryCommand.Type

export const InventoryCommands = {
  ReserveStock,
  ReleaseStock
} as const

// ---------------------------------------------------------------------------
// Entity (derived from command constructors)
// ---------------------------------------------------------------------------

export const InventoryEntity = N2.makeEntity("Inventory", InventoryCommands, {
  primaryKey: (p) => p.sku,
  persisted: true
})
