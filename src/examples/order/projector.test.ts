/**
 * Orders view projector tests.
 */
import { test, expect } from "bun:test"
import * as Effect from "effect/Effect"
import * as HashMap from "effect/HashMap"
import * as DateTime from "effect/DateTime"
import * as Schema from "effect/Schema"
import { EntityId, EntityType } from "@effect/cluster"
import * as ProjectorTestHarness from "../../framework/testing/ProjectorTestHarness.js"
import { OrdersViewProjector, type OrdersViewState, type OrderView } from "./projector.js"
import { EventEnvelope } from "../../framework/contracts/EventEnvelope.js"
import { empty as emptyMetadata } from "../../framework/contracts/Metadata.js"

const harness = ProjectorTestHarness.make(OrdersViewProjector)
const now = DateTime.unsafeMake(0)

const makeEnvelope = (
  revision: number,
  payload: unknown
): EventEnvelope =>
  new EventEnvelope({
    eventId: `evt-${revision}`,
    streamId: "Order-order-1",
    aggregateId: EntityId.make("order-1"),
    aggregateType: Schema.decodeSync(EntityType.EntityType)("Order"),
    revision,
    occurredAt: now,
    metadata: emptyMetadata,
    payload
  })

test("OrderCreated creates a view entry", async () => {
  const state = await Effect.runPromise(
    harness.givenSingle(
      makeEnvelope(1, {
        _tag: "OrderCreated",
        orderId: "order-1",
        customerId: "cust-1",
        createdAt: now
      })
    )
  ) as OrdersViewState

  const view = HashMap.get(state, "order-1")
  expect(view._tag).toBe("Some")
  if (view._tag === "Some") {
    expect(view.value.customerId).toBe("cust-1")
    expect(view.value.status).toBe("draft")
    expect(view.value.itemCount).toBe(0)
    expect(view.value.totalAmount).toBe(0)
  }
})

test("ItemAdded updates view", async () => {
  const state = await Effect.runPromise(
    harness.given([
      makeEnvelope(1, {
        _tag: "OrderCreated",
        orderId: "order-1",
        customerId: "cust-1",
        createdAt: now
      }),
      makeEnvelope(2, {
        _tag: "ItemAdded",
        orderId: "order-1",
        sku: "SKU-001",
        quantity: 2,
        price: 10
      })
    ])
  ) as OrdersViewState

  const view = HashMap.get(state, "order-1")
  expect(view._tag).toBe("Some")
  if (view._tag === "Some") {
    expect(view.value.itemCount).toBe(1)
    expect(view.value.totalAmount).toBe(20)
  }
})

test("OrderSubmitted updates status", async () => {
  const state = await Effect.runPromise(
    harness.given([
      makeEnvelope(1, {
        _tag: "OrderCreated",
        orderId: "order-1",
        customerId: "cust-1",
        createdAt: now
      }),
      makeEnvelope(2, {
        _tag: "OrderSubmitted",
        orderId: "order-1",
        submittedAt: now
      })
    ])
  ) as OrdersViewState

  const view = HashMap.get(state, "order-1")
  expect(view._tag).toBe("Some")
  if (view._tag === "Some") {
    expect(view.value.status).toBe("submitted")
  }
})
