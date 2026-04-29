/**
 * Behavior tests for `makeEventDecoder` — converts EventJournal entries into
 * typed domain events. Critical to event replay; bugs here corrupt CQRS reads.
 */
import { test, expect } from "bun:test"
import * as Cause from "effect/Cause"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Schema from "effect/Schema"
import { EventGroup } from "@effect/experimental"
import * as ExpEventJournal from "@effect/experimental/EventJournal"
import * as MsgPack from "@effect/platform/MsgPack"
import { eventGroupEvents } from "./EventGroupAccess.js"
import { makeEventDecoder } from "./EventDecoder.js"

class OrderCreated extends Schema.TaggedClass<OrderCreated>()("OrderCreated", {
  orderId: Schema.String,
  total: Schema.Number
}) {}

class ItemAdded extends Schema.TaggedClass<ItemAdded>()("ItemAdded", {
  orderId: Schema.String,
  sku: Schema.String,
  quantity: Schema.Number
}) {}

const OrderCreatedPayload = Schema.Struct({
  orderId: Schema.String,
  total: Schema.Number
})
const ItemAddedPayload = Schema.Struct({
  orderId: Schema.String,
  sku: Schema.String,
  quantity: Schema.Number
})

const TestEventGroup = EventGroup.empty
  .add({
    tag: "OrderCreated",
    primaryKey: (p: { orderId: string }) => p.orderId,
    payload: OrderCreatedPayload
  })
  .add({
    tag: "ItemAdded",
    primaryKey: (p: { orderId: string }) => p.orderId,
    payload: ItemAddedPayload
  })

// Build a journal entry with msgpack-encoded payload via the same schema the
// decoder uses, so the round trip is realistic.
const makeEntry = (tag: string, payload: unknown): ExpEventJournal.Entry => {
  const events = eventGroupEvents(TestEventGroup)
  const event = events[tag]
  if (!event) throw new Error(`unknown tag in test setup: ${tag}`)
  const bytes = Schema.encodeSync(event.payloadMsgPack as unknown as Schema.Schema<unknown, Uint8Array, never>)(payload as never)
  return new ExpEventJournal.Entry({
    id: ExpEventJournal.makeEntryId(),
    event: tag,
    primaryKey: "test-pk",
    payload: bytes
  })
}

const makeRawEntry = (tag: string, payload: unknown): ExpEventJournal.Entry => {
  const bytes = Schema.encodeSync(MsgPack.schema(Schema.Unknown))(payload)
  return new ExpEventJournal.Entry({
    id: ExpEventJournal.makeEntryId(),
    event: tag,
    primaryKey: "test-pk",
    payload: bytes
  })
}

test("makeEventDecoder decodes a valid entry to the matching constructor", async () => {
  const decode = makeEventDecoder<OrderCreated | ItemAdded>(TestEventGroup, { OrderCreated, ItemAdded })
  const entry = makeEntry("OrderCreated", { orderId: "o-1", total: 42 })

  const event = await Effect.runPromise(decode(entry))

  expect(event).toBeInstanceOf(OrderCreated)
  expect(event._tag).toBe("OrderCreated")
  expect((event as OrderCreated).orderId).toBe("o-1")
  expect((event as OrderCreated).total).toBe(42)
})

test("makeEventDecoder fails with typed Error on unknown tag", async () => {
  const decode = makeEventDecoder<OrderCreated | ItemAdded>(TestEventGroup, { OrderCreated, ItemAdded })
  const entry = new ExpEventJournal.Entry({
    id: ExpEventJournal.makeEntryId(),
    event: "UnknownTag",
    primaryKey: "test-pk",
    payload: new Uint8Array()
  })

  const exit = await Effect.runPromiseExit(decode(entry))
  expect(Exit.isFailure(exit)).toBe(true)
  if (Exit.isFailure(exit)) {
    const failure = Cause.failureOption(exit.cause)
    expect(failure._tag).toBe("Some")
    if (failure._tag === "Some") {
      expect(failure.value).toBeInstanceOf(Error)
      expect((failure.value as Error).message).toContain("Unsupported event")
    }
  }
})

test("makeEventDecoder fails with typed Error when constructor is missing", async () => {
  // Schema for the tag exists in the EventGroup, but no constructor was supplied.
  const decode = makeEventDecoder(TestEventGroup, { OrderCreated })
  const entry = makeEntry("ItemAdded", { orderId: "o-1", sku: "abc", quantity: 1 })

  const exit = await Effect.runPromiseExit(decode(entry))
  expect(Exit.isFailure(exit)).toBe(true)
  if (Exit.isFailure(exit)) {
    const failure = Cause.failureOption(exit.cause)
    expect(failure._tag).toBe("Some")
    if (failure._tag === "Some") {
      expect((failure.value as Error).message).toContain("No constructor for event")
    }
  }
})

test("makeEventDecoder migrates historical payloads before current schema validation", async () => {
  let received: unknown
  const decode = makeEventDecoder<OrderCreated | ItemAdded>(TestEventGroup, { OrderCreated, ItemAdded }, {
    migrations: {
      OrderCreated: (payload) => {
        received = payload
        const historical = payload as { readonly orderId: string; readonly amount: number }
        return { orderId: historical.orderId, total: historical.amount }
      }
    }
  })
  const entry = makeRawEntry("OrderCreated", { orderId: "o-1", amount: 42 })

  const decoded = await Effect.runPromise(decode(entry))

  expect(decoded).toBeInstanceOf(OrderCreated)
  expect(received).not.toBeInstanceOf(Uint8Array)
  expect((received as { orderId: string }).orderId).toBe("o-1")
  expect((received as { amount: number }).amount).toBe(42)
  expect((decoded as OrderCreated).total).toBe(42)
})

test("makeEventDecoder fails historical payloads without a migration", async () => {
  const decode = makeEventDecoder<OrderCreated | ItemAdded>(TestEventGroup, { OrderCreated, ItemAdded })
  const entry = makeRawEntry("OrderCreated", { orderId: "o-1", amount: 42 })

  const exit = await Effect.runPromiseExit(decode(entry))

  expect(Exit.isFailure(exit)).toBe(true)
  if (Exit.isFailure(exit)) {
    const failure = Cause.failureOption(exit.cause)
    expect(failure._tag).toBe("Some")
    if (failure._tag === "Some") {
      expect((failure.value as Error).message).toContain("Failed to decode OrderCreated")
    }
  }
})

test("makeEventDecoder wraps schema decode failures in a wrapped Error", async () => {
  const decode = makeEventDecoder<OrderCreated | ItemAdded>(TestEventGroup, { OrderCreated, ItemAdded })
  // Random non-msgpack bytes will fail the MsgPack decoder.
  const entry = new ExpEventJournal.Entry({
    id: ExpEventJournal.makeEntryId(),
    event: "OrderCreated",
    primaryKey: "test-pk",
    payload: new Uint8Array([0x99, 0x99, 0x99])
  })

  const exit = await Effect.runPromiseExit(decode(entry))
  expect(Exit.isFailure(exit)).toBe(true)
  if (Exit.isFailure(exit)) {
    const failure = Cause.failureOption(exit.cause)
    expect(failure._tag).toBe("Some")
    if (failure._tag === "Some") {
      expect((failure.value as Error).message).toContain("Failed to decode OrderCreated")
    }
  }
})
