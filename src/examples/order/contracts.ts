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

/** Common fields shared by all order events. */
const OrderEventBase = {
  orderId: Schema.String,
  occurredAt: Schema.DateTimeUtc
}

export class OrderCreated extends Schema.TaggedClass<OrderCreated>()(
  "OrderCreated",
  { ...OrderEventBase, customerId: Schema.String }
) { }

export class ItemAdded extends Schema.TaggedClass<ItemAdded>()(
  "ItemAdded",
  { ...OrderEventBase, sku: Schema.String, quantity: Schema.Number, price: Schema.Number }
) { }

export class OrderSubmitted extends Schema.TaggedClass<OrderSubmitted>()(
  "OrderSubmitted",
  { ...OrderEventBase }
) { }

export class OrderCancelled extends Schema.TaggedClass<OrderCancelled>()(
  "OrderCancelled",
  { ...OrderEventBase, reason: Schema.String }
) { }

const OrderEvents = N2.defineEvents(
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

const OrderErrorDefinitions = N2.defineErrors(OrderError, OrderNotFound)

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
  {
    failure: OrderError, success: CommandResult,
    payload: { orderId: Schema.String, customerId: Schema.String }
  }
) { }

export class AddItem extends Schema.TaggedRequest<AddItem>("AddItem")(
  "AddItem",
  {
    failure: OrderError, success: CommandResult,
    payload: { orderId: Schema.String, sku: Schema.String, quantity: Schema.Number, price: Schema.Number }
  }
) { }

export class SubmitOrder extends Schema.TaggedRequest<SubmitOrder>("SubmitOrder")(
  "SubmitOrder",
  {
    failure: OrderError, success: CommandResult,
    payload: { orderId: Schema.String }
  }
) { }

export class CancelOrder extends Schema.TaggedRequest<CancelOrder>("CancelOrder")(
  "CancelOrder",
  {
    failure: OrderError, success: CommandResult,
    payload: { orderId: Schema.String, reason: Schema.String }
  }
) { }

export class GetOrder extends Schema.TaggedRequest<GetOrder>("GetOrder")(
  "GetOrder",
  {
    failure: OrderNotFound, success: OrderState,
    payload: { orderId: Schema.String }
  }
) { }

export class FulfillmentResult extends Schema.Class<FulfillmentResult>("FulfillmentResult")({
  orderId: Schema.String,
  shipped: Schema.Boolean
}) { }

export class FulfillOrder extends Schema.TaggedRequest<FulfillOrder>("FulfillOrder")(
  "FulfillOrder",
  {
    failure: OrderError, success: FulfillmentResult,
    payload: { orderId: Schema.String, sku: Schema.String, quantity: Schema.Number }
  }
) { }

// ---------------------------------------------------------------------------
// Commands collection
// ---------------------------------------------------------------------------

export const OrderCommands = N2.defineCommands(
  CreateOrder,
  AddItem,
  SubmitOrder,
  CancelOrder,
  GetOrder,
  FulfillOrder
)

export const OrderCommand = OrderCommands.schema
export type OrderCommand = typeof OrderCommand.Type

// ---------------------------------------------------------------------------
// Entity definition and RPC group (derived from commands, no duplication)
// ---------------------------------------------------------------------------

export const OrderEntity = OrderCommands.toPersistedEntity("Order", (p) => p.orderId)
export const OrderRpcs = OrderEntity.protocol
