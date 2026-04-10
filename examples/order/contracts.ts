/**
 * Order aggregate contracts — pure schemas, no framework helpers.
 *
 * Commands are Schema.TaggedRequest: they carry success/failure types and
 * serve as both the domain command and the RPC schema. No separate DTOs.
 *
 * The cluster Entity is built directly from Rpc.make + Entity.make, making
 * every layer of the wire protocol explicit.
 */
import * as Schema from "effect/Schema"
import * as Option from "effect/Option"
import { ClusterSchema, Entity } from "@effect/cluster"
import { Rpc } from "@effect/rpc"

// ---------------------------------------------------------------------------
// Line Item
// ---------------------------------------------------------------------------

export class LineItem extends Schema.Class<LineItem>("LineItem")({
  sku: Schema.String,
  quantity: Schema.Number,
  price: Schema.Number
}) {}

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

export class OrderCreated extends Schema.TaggedClass<OrderCreated>()(
  "OrderCreated",
  { orderId: Schema.String, customerId: Schema.String, createdAt: Schema.DateTimeUtc }
) {}

export class ItemAdded extends Schema.TaggedClass<ItemAdded>()(
  "ItemAdded",
  { orderId: Schema.String, sku: Schema.String, quantity: Schema.Number, price: Schema.Number }
) {}

export class OrderSubmitted extends Schema.TaggedClass<OrderSubmitted>()(
  "OrderSubmitted",
  { orderId: Schema.String, submittedAt: Schema.DateTimeUtc }
) {}

// Union used in aggregate signatures and state machine typing.
export const OrderEvent = Schema.Union(OrderCreated, ItemAdded, OrderSubmitted)
export type OrderEvent = typeof OrderEvent.Type

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

export class OrderError extends Schema.TaggedError<OrderError>()(
  "OrderError",
  { message: Schema.String }
) {}

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

export const OrderStatus = Schema.Literal("empty", "draft", "submitted")
export type OrderStatus = typeof OrderStatus.Type

export class OrderState extends Schema.Class<OrderState>("OrderState")({
  status: OrderStatus,
  orderId: Schema.OptionFromSelf(Schema.String),
  customerId: Schema.OptionFromSelf(Schema.String),
  items: Schema.Array(LineItem),
  totalAmount: Schema.Number
}) {}

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
}) {}

// ---------------------------------------------------------------------------
// Commands
//
// Schema.TaggedRequest<T> encodes both:
//   - the domain command (typed payload for aggregate.ts)
//   - the RPC schema (success/failure types for the wire protocol)
// ---------------------------------------------------------------------------

export class CreateOrder extends Schema.TaggedRequest<CreateOrder>("CreateOrder")(
  "CreateOrder",
  {
    failure: OrderError, success: CommandResult,
    payload: { orderId: Schema.String, customerId: Schema.String }
  }
) {}

export class AddItem extends Schema.TaggedRequest<AddItem>("AddItem")(
  "AddItem",
  {
    failure: OrderError, success: CommandResult,
    payload: { orderId: Schema.String, sku: Schema.String, quantity: Schema.Number, price: Schema.Number }
  }
) {}

export class SubmitOrder extends Schema.TaggedRequest<SubmitOrder>("SubmitOrder")(
  "SubmitOrder",
  {
    failure: OrderError, success: CommandResult,
    payload: { orderId: Schema.String }
  }
) {}

// Union used in aggregate `decide` signature.
export const OrderCommandSchema = Schema.Union(CreateOrder, AddItem, SubmitOrder)
export type OrderCommand = typeof OrderCommandSchema.Type

// ---------------------------------------------------------------------------
// Cluster entity definition
//
// Entity.make wires a set of Rpc definitions into a shardable actor type.
// Rpc.make defines one RPC endpoint: payload fields, primary key (shard key),
// success schema, and error schema.
// annotateRpcs(ClusterSchema.Persisted, true) enables event-sourced persistence
// via EventJournal — state survives restarts.
// ---------------------------------------------------------------------------

export const OrderEntity = Entity.make("Order", [
  Rpc.make("CreateOrder", {
    payload: { orderId: Schema.String, customerId: Schema.String },
    primaryKey: (p) => p.orderId,
    success: CommandResult,
    error: OrderError
  }),
  Rpc.make("AddItem", {
    payload: { orderId: Schema.String, sku: Schema.String, quantity: Schema.Number, price: Schema.Number },
    primaryKey: (p) => p.orderId,
    success: CommandResult,
    error: OrderError
  }),
  Rpc.make("SubmitOrder", {
    payload: { orderId: Schema.String },
    primaryKey: (p) => p.orderId,
    success: CommandResult,
    error: OrderError
  })
]).annotateRpcs(ClusterSchema.Persisted, true)

// The RpcGroup derived from the entity — used to build HTTP routes and handlers.
export const OrderRpcs = OrderEntity.protocol
