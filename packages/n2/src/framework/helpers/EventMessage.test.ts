import { it, expect } from "@effect/vitest"
import * as Schema from "effect/Schema"
import { makeEventMessage, makeEventMessageFields } from "./EventMessage.js"

class TestEventMessage extends Schema.Class<TestEventMessage>("TestEventMessage")({
  ...makeEventMessageFields("entityId")
}) {}

it("makeEventMessage builds standard publication metadata", () => {
  const message = new TestEventMessage(makeEventMessage({
    topic: "test.events",
    entityIdKey: "entityId",
    entityId: "00000000-0000-4000-8000-000000000001",
    revision: 3,
    eventType: "ThingHappened",
    occurredAt: "2026-01-01T00:00:00.000Z",
    payload: { value: 1 },
    headers: { source: "test" }
  }))

  expect(message).toMatchObject({
    id: "00000000-0000-4000-8000-000000000001:3:ThingHappened",
    topic: "test.events",
    partitionKey: "00000000-0000-4000-8000-000000000001",
    entityId: "00000000-0000-4000-8000-000000000001",
    eventType: "ThingHappened",
    revision: 3,
    occurredAt: "2026-01-01T00:00:00.000Z",
    payload: { value: 1 },
    headers: {
      eventType: "ThingHappened",
      entityId: "00000000-0000-4000-8000-000000000001",
      revision: 3,
      source: "test"
    }
  })
})
