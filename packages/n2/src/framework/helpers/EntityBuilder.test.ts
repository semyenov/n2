/**
 * Tests for `rpcFromCommand` and `rpcListFromCommandDefinitions` — derives
 * Rpc definitions from `Schema.TaggedRequest` command classes. Bugs here
 * silently corrupt cluster entity wiring.
 */
import { test, expect } from "bun:test"
import * as PrimaryKey from "effect/PrimaryKey"
import * as Schema from "effect/Schema"
import { rpcFromCommand, rpcListFromCommandDefinitions } from "./EntityBuilder.js"

class CreateOrder extends Schema.TaggedRequest<CreateOrder>()("CreateOrder", {
  payload: {
    orderId: Schema.String,
    customerId: Schema.String
  },
  success: Schema.Struct({ revision: Schema.Number }),
  failure: Schema.Struct({ message: Schema.String })
}) {}

class AddItem extends Schema.TaggedRequest<AddItem>()("AddItem", {
  payload: {
    orderId: Schema.String,
    sku: Schema.String,
    quantity: Schema.Number
  },
  success: Schema.Struct({ revision: Schema.Number }),
  failure: Schema.Struct({ message: Schema.String })
}) {}

test("rpcFromCommand derives Rpc with _tag matching the command tag", () => {
  const rpc = rpcFromCommand(CreateOrder, (p) => p.orderId)
  expect(rpc._tag).toBe("CreateOrder")
})

test("rpcFromCommand strips _tag from payload fields", () => {
  const rpc = rpcFromCommand(CreateOrder, (p) => p.orderId)
  // Rpc.make wraps a payload-fields record into a Schema.Class — the resulting
  // payloadSchema exposes `.fields` listing the (stripped) field keys.
  const schema = (rpc as unknown as { readonly payloadSchema: { readonly fields: Record<string, unknown> } }).payloadSchema
  const fieldKeys = Object.keys(schema.fields)
  expect(fieldKeys).toEqual(["orderId", "customerId"])
  expect(fieldKeys).not.toContain("_tag")
})

test("rpcFromCommand threads success and failure schemas unchanged", () => {
  const rpc = rpcFromCommand(CreateOrder, (p) => p.orderId)
  const successSchema = (rpc as unknown as { readonly successSchema: unknown }).successSchema
  const errorSchema = (rpc as unknown as { readonly errorSchema: unknown }).errorSchema
  expect(successSchema).toBe(CreateOrder.success)
  expect(errorSchema).toBe(CreateOrder.failure)
})

test("rpcFromCommand wires primaryKey via the PrimaryKey symbol on the payload class", () => {
  const primaryKey = (p: { orderId: string }) => `pk:${p.orderId}`
  const rpc = rpcFromCommand(CreateOrder, primaryKey)

  // Rpc.make stores primaryKey as a method on the synthetic Payload Schema.Class
  // accessed via PrimaryKey.symbol. Constructing a payload instance and calling
  // PrimaryKey.value(payload) produces the configured key.
  const payloadSchema = (rpc as unknown as { readonly payloadSchema: Schema.Schema<unknown, unknown, never> }).payloadSchema
  const decode = Schema.decodeUnknownSync(payloadSchema)
  const payload = decode({ orderId: "o-1", customerId: "c-1" })
  expect(PrimaryKey.value(payload as never)).toBe("pk:o-1")
})

test("rpcListFromCommandDefinitions builds an array preserving tag order", () => {
  const rpcs = rpcListFromCommandDefinitions(
    (p: { orderId: string }) => p.orderId,
    CreateOrder,
    AddItem
  )
  expect(rpcs.length).toBe(2)
  const tags = rpcs.map((r) => (r as unknown as { readonly _tag: string })._tag)
  expect(tags).toEqual(["CreateOrder", "AddItem"])
})
