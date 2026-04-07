/**
 * Order aggregate contracts.
 *
 * Commands are Schema.TaggedRequest -- they carry success/failure types
 * and serve as both the command definition AND the RPC schema.
 */
import * as Schema from "effect/Schema"
import * as Option from "effect/Option"
import * as N2 from "../../framework/helpers/index.js"

// ---------------------------------------------------------------------------
// Line Item
// ---------------------------------------------------------------------------

export class LineItem extends Schema.Class<LineItem>("LineItem")({
  sku: Schema.String,
  quantity: Schema.Number,
  price: Schema.Number
}) { }

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

export class OrderCreated extends Schema.TaggedClass<OrderCreated>()(
  "OrderCreated",
  { orderId: Schema.String, customerId: Schema.String, createdAt: Schema.DateTimeUtc }
) { }

export class ItemAdded extends Schema.TaggedClass<ItemAdded>()(
  "ItemAdded",
  { orderId: Schema.String, sku: Schema.String, quantity: Schema.Number, price: Schema.Number }
) { }

export class OrderSubmitted extends Schema.TaggedClass<OrderSubmitted>()(
  "OrderSubmitted",
  { orderId: Schema.String, submittedAt: Schema.DateTimeUtc }
) { }

export class OrderCancelled extends Schema.TaggedClass<OrderCancelled>()(
  "OrderCancelled",
  { orderId: Schema.String, reason: Schema.String, cancelledAt: Schema.DateTimeUtc }
) { }

const OrderEvents = N2.Definitions.defineEvents(
  OrderCreated,
  ItemAdded,
  OrderSubmitted,
  OrderCancelled
)

export const OrderEvent = OrderEvents.schema
export type OrderEvent = typeof OrderEvent.Type

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

export class OrderError extends Schema.TaggedError<OrderError>()(
  "OrderError",
  { message: Schema.String }
) { }

export class OrderNotFound extends Schema.TaggedError<OrderNotFound>()(
  "OrderNotFound",
  { orderId: Schema.String }
) { }

const OrderErrorDefinitions = N2.Definitions.defineErrors(OrderError, OrderNotFound)

export const OrderErrors = OrderErrorDefinitions.schema
export type OrderErrors = typeof OrderErrors.Type

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

export const OrderStatus = Schema.Literal("empty", "draft", "submitted", "cancelled")
export type OrderStatus = typeof OrderStatus.Type

export class OrderState extends Schema.Class<OrderState>("OrderState")({
  status: OrderStatus,
  orderId: Schema.OptionFromSelf(Schema.String),
  customerId: Schema.OptionFromSelf(Schema.String),
  items: Schema.Array(LineItem),
  totalAmount: Schema.Number
}) { }

export const initialOrderState = new OrderState({
  status: "empty",
  orderId: Option.none(),
  customerId: Option.none(),
  items: [],
  totalAmount: 0
})

// ---------------------------------------------------------------------------
// Result
// ---------------------------------------------------------------------------

export class CommandResult extends Schema.Class<CommandResult>("CommandResult")({
  orderId: Schema.String,
  revision: Schema.Number
}) { }

// ---------------------------------------------------------------------------
// Commands (Schema.TaggedRequest -- carry success/failure types)
//
// Each command IS both:
// 1. The domain command (payload + business semantics)
// 2. The RPC schema (success + failure types for wire protocol)
// ---------------------------------------------------------------------------

export class CreateOrder extends Schema.TaggedRequest<CreateOrder>("CreateOrder")(
  "CreateOrder",
  { failure: OrderError, success: CommandResult, payload: { orderId: Schema.String, customerId: Schema.String } }
) { }

export class AddItem extends Schema.TaggedRequest<AddItem>("AddItem")(
  "AddItem",
  { failure: OrderError, success: CommandResult, payload: { orderId: Schema.String, sku: Schema.String, quantity: Schema.Number, price: Schema.Number } }
) { }

export class SubmitOrder extends Schema.TaggedRequest<SubmitOrder>("SubmitOrder")(
  "SubmitOrder",
  { failure: OrderError, success: CommandResult, payload: { orderId: Schema.String } }
) { }

export class CancelOrder extends Schema.TaggedRequest<CancelOrder>("CancelOrder")(
  "CancelOrder",
  { failure: OrderError, success: CommandResult, payload: { orderId: Schema.String, reason: Schema.String } }
) { }

export const OrderCommands = N2.Definitions.defineCommands(
  CreateOrder,
  AddItem,
  SubmitOrder,
  CancelOrder
)

export const OrderCommand = OrderCommands.schema
export type OrderCommand = typeof OrderCommand.Type

// ---------------------------------------------------------------------------
// Entity definition (derived from command classes)
// ---------------------------------------------------------------------------

const pk = (p: { orderId: string }) => p.orderId

export const OrderEntity = N2.Entities.persistedEntityFromCommands(
  "Order",
  pk,
  CreateOrder,
  AddItem,
  SubmitOrder,
  CancelOrder
)

// ---------------------------------------------------------------------------
// RPC group -- derived from Entity protocol (no duplication)
// ---------------------------------------------------------------------------

export const OrderRpcs = OrderEntity.protocol
