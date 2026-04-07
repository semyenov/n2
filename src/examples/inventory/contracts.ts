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

const InventoryEvents = N2.Definitions.defineEvents(StockReserved, StockReleased)

export const InventoryEvent = InventoryEvents.schema
export type InventoryEvent = typeof InventoryEvent.Type

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

export class InsufficientStock extends Schema.TaggedError<InsufficientStock>()(
  "InsufficientStock",
  { sku: Schema.String, requested: Schema.Number, available: Schema.Number }
) {}

const InventoryErrorDefinitions = N2.Definitions.defineErrors(InsufficientStock)

export const InventoryErrors = InventoryErrorDefinitions.schema
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

export const InventoryCommands = N2.Definitions.defineCommands(
  ReserveStock,
  ReleaseStock
)

export const InventoryCommand = InventoryCommands.schema
export type InventoryCommand = typeof InventoryCommand.Type

// ---------------------------------------------------------------------------
// Entity (derived from command classes)
// ---------------------------------------------------------------------------

const pk = (p: { sku: string }) => p.sku

export const InventoryEntity = N2.Entities.persistedEntityFromCommands(
  "Inventory",
  pk,
  ReserveStock,
  ReleaseStock
)
