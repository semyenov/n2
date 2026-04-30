/**
 * Tests for the public helpers in `Definitions.ts` — used by every service to
 * derive cluster entities, EventGroups, and tagged unions from contracts.
 * Bugs here corrupt the wiring between contracts and infrastructure silently.
 */
import { it, expect } from "@effect/vitest"
import * as Context from "effect/Context"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import { ClusterSchema } from "@effect/cluster"
import type * as ExperimentalEvent from "@effect/experimental/Event"
import type * as ExperimentalEventGroup from "@effect/experimental/EventGroup"
import { defineCommands, defineEvents, eventPayloadSchema } from "./Definitions.js"
import { eventGroupEvent, eventGroupEvents } from "./EventGroupAccess.js"

class CreateOrder extends Schema.TaggedRequest<CreateOrder>()("CreateOrder", {
  payload: { orderId: Schema.String, customerId: Schema.String },
  success: Schema.Struct({ revision: Schema.Number }),
  failure: Schema.Struct({ message: Schema.String })
}) {}

class AddItem extends Schema.TaggedRequest<AddItem>()("AddItem", {
  payload: { orderId: Schema.String, sku: Schema.String, quantity: Schema.Number },
  success: Schema.Struct({ revision: Schema.Number }),
  failure: Schema.Struct({ message: Schema.String })
}) {}

class GetOrder extends Schema.TaggedRequest<GetOrder>()("GetOrder", {
  payload: { orderId: Schema.String },
  success: Schema.Struct({ orderId: Schema.String }),
  failure: Schema.Struct({ message: Schema.String })
}) {}

class OrderCreated extends Schema.TaggedClass<OrderCreated>()("OrderCreated", {
  orderId: Schema.String,
  customerId: Schema.String
}) {}

class ItemAdded extends Schema.TaggedClass<ItemAdded>()("ItemAdded", {
  orderId: Schema.String,
  sku: Schema.String,
  quantity: Schema.Number
}) {}

it("eventPayloadSchema strips _tag and yields a Schema.Struct over the remaining fields", () => {
  const schema = eventPayloadSchema(OrderCreated)
  // The Schema.Struct should expose .fields with the original keys minus _tag.
  const fieldKeys = Object.keys((schema as { readonly fields: Record<string, unknown> }).fields)
  expect(fieldKeys.sort()).toEqual(["customerId", "orderId"])
  expect(fieldKeys).not.toContain("_tag")

  // Decoding a payload through the stripped schema should produce the plain object
  // (the discriminant isn't required on the wire).
  const decoded = Schema.decodeUnknownSync(schema)({ orderId: "o-1", customerId: "c-1" })
  expect(decoded).toEqual({ orderId: "o-1", customerId: "c-1" })
})

it("defineCommands.toPersistedEntity annotates every Rpc with ClusterSchema.Persisted = true", () => {
  const Cmds = defineCommands(CreateOrder, AddItem)
  const entity = Cmds.toPersistedEntity("Order", (p) => p.orderId)

  expect(entity.type as string).toBe("Order")
  for (const rpc of entity.protocol.requests.values()) {
    const persisted = Context.getOption(rpc.annotations, ClusterSchema.Persisted)
    expect(Option.isSome(persisted)).toBe(true)
    if (Option.isSome(persisted)) {
      expect(persisted.value).toBe(true)
    }
  }
})

it("defineCommands.toEntity does not flag every Rpc as Persisted=true", () => {
  // toEntity should NOT explicitly mark rpcs as Persisted=true. Either the
  // annotation is absent, or it's set to false (the impl branch uses
  // `persisted ? annotateRpcs(...true) : entity`).
  const Cmds = defineCommands(CreateOrder, AddItem)
  const entity = Cmds.toEntity("OrderEphemeral", (p) => p.orderId)

  expect(entity.type as string).toBe("OrderEphemeral")
  for (const rpc of entity.protocol.requests.values()) {
    const persisted = Context.getOption(rpc.annotations, ClusterSchema.Persisted)
    if (Option.isSome(persisted)) {
      expect(persisted.value).not.toBe(true)
    }
  }
})

it("defineCommands.toEntityWithPersisted annotates only selected Rpc tags", () => {
  const Cmds = defineCommands(CreateOrder, AddItem, GetOrder)
  const entity = Cmds.toEntityWithPersisted("Order", (p) => p.orderId, [
    "CreateOrder",
    "AddItem"
  ])

  const persistedByTag = new Map(
    Array.from(entity.protocol.requests.values(), (rpc) => [
      rpc._tag,
      Context.getOption(rpc.annotations, ClusterSchema.Persisted)
    ])
  )

  expect(persistedByTag.get("CreateOrder")).toEqual(Option.some(true))
  expect(persistedByTag.get("AddItem")).toEqual(Option.some(true))
  const getOrderPersisted = persistedByTag.get("GetOrder")!
  if (Option.isSome(getOrderPersisted)) {
    expect(getOrderPersisted.value).not.toBe(true)
  }
})

it("defineEvents.toEventGroup builds a group with one entry per event and the right payload schemas", () => {
  const Events = defineEvents(OrderCreated, ItemAdded)
  const group = Events.toEventGroup((p) => p.orderId)
  type GroupEvents = ExperimentalEventGroup.EventGroup.Events<typeof group>
  const acceptsKnownTag = <Tag extends ExperimentalEvent.Event.Tag<GroupEvents>>(_tag: Tag) => undefined
  acceptsKnownTag("OrderCreated")
  acceptsKnownTag("ItemAdded")
  // @ts-expect-error derived EventGroup should preserve the concrete event tags
  acceptsKnownTag("UnknownEvent")

  const events = eventGroupEvents(group)
  expect(Object.keys(events).sort()).toEqual(["ItemAdded", "OrderCreated"])

  // The payload for OrderCreated should be the stripped Schema.Struct.
  const orderCreatedEvent = events["OrderCreated"]
  expect(orderCreatedEvent).toBeDefined()
  // payloadMsgPack wraps the user-supplied payload schema; the payload schema's
  // fields should match the event class minus _tag.
  const payloadFields = Object.keys(
    (orderCreatedEvent as unknown as { readonly payload: { readonly fields: Record<string, unknown> } }).payload.fields
  )
  expect(payloadFields.sort()).toEqual(["customerId", "orderId"])
})

it("defineEvents.toEventGroup wires the primaryKey function into each event", () => {
  const Events = defineEvents(OrderCreated, ItemAdded)
  const group = Events.toEventGroup((p) => `pk:${p.orderId}`)

  const orderCreatedEvent = eventGroupEvent<{ readonly orderId: string; readonly customerId: string }>(
    group,
    "OrderCreated"
  )
  expect(orderCreatedEvent).toBeDefined()
  // Event.make stores `primaryKey` as a direct property on the runtime event.
  // Calling it with a sample payload should reflect the user-supplied function.
  expect(orderCreatedEvent?.primaryKey({ orderId: "o-1", customerId: "c-1" })).toBe("pk:o-1")
})
