import { it, expect } from "@effect/vitest"
import * as PrimaryKey from "effect/PrimaryKey"
import * as Schema from "effect/Schema"
import {
  entityFromCommands,
  persistedEntityFromCommands,
  rpcListFromCommands
} from "./Entities.js"

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
    sku: Schema.String
  },
  success: Schema.Struct({ revision: Schema.Number }),
  failure: Schema.Struct({ message: Schema.String })
}) {}

it("rpcListFromCommands preserves command order and primary keys", () => {
  const rpcs = rpcListFromCommands(
    (payload) => `order:${payload.orderId}`,
    CreateOrder,
    AddItem
  )

  expect(rpcs.map((rpc) => rpc._tag)).toEqual(["CreateOrder", "AddItem"])

  const payloadSchema = rpcs[0].payloadSchema
  const payload = Schema.decodeUnknownSync(payloadSchema)({
    orderId: "o-1",
    customerId: "c-1"
  })

  expect(PrimaryKey.value(payload as never)).toBe("order:o-1")
})

it("entityFromCommands creates a named entity from command definitions", () => {
  const entity = entityFromCommands(
    "Order",
    (payload) => payload.orderId,
    CreateOrder,
    AddItem
  )

  expect(entity.type).toBe("Order")
  expect(typeof entity.protocol).toBe("function")
  expect(typeof entity.getShardId).toBe("function")
})

it("persistedEntityFromCommands preserves the entity identity", () => {
  const entity = persistedEntityFromCommands(
    "Order",
    (payload) => payload.orderId,
    CreateOrder,
    AddItem
  )

  expect(entity.type).toBe("Order")
  expect(typeof entity.protocol).toBe("function")
})
