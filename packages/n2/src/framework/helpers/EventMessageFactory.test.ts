import { it, expect } from "@effect/vitest"
import * as Schema from "effect/Schema"
import { makeEventMessageFields } from "./EventMessage.js"
import { makeEventMessageFactory } from "./EventMessageFactory.js"

class ProfileEventMessage extends Schema.Class<ProfileEventMessage>("ProfileEventMessage")({
  ...makeEventMessageFields("profileId")
}) {}

const makeProfileMessage = makeEventMessageFactory({
  topic: "profile-provider.events",
  entityIdKey: "profileId",
  schema: ProfileEventMessage
})

it("makeEventMessageFactory threads entityId through to schema instance", () => {
  const message = makeProfileMessage({
    profileId: "00000000-0000-4000-8000-000000000001",
    revision: 7,
    eventType: "ProfileCreated",
    occurredAt: "2026-04-28T00:00:00.000Z",
    payload: { foo: "bar" }
  })

  expect(message).toBeInstanceOf(ProfileEventMessage)
  expect(message).toMatchObject({
    id: "00000000-0000-4000-8000-000000000001:7:ProfileCreated",
    topic: "profile-provider.events",
    partitionKey: "00000000-0000-4000-8000-000000000001",
    profileId: "00000000-0000-4000-8000-000000000001",
    eventType: "ProfileCreated",
    revision: 7,
    occurredAt: "2026-04-28T00:00:00.000Z",
    payload: { foo: "bar" },
    headers: {
      eventType: "ProfileCreated",
      profileId: "00000000-0000-4000-8000-000000000001",
      revision: 7
    }
  })
})

it("makeEventMessageFactory passes headers through", () => {
  const message = makeProfileMessage({
    profileId: "00000000-0000-4000-8000-000000000001",
    revision: 1,
    eventType: "ProfileCreated",
    occurredAt: "2026-04-28T00:00:00.000Z",
    payload: {},
    headers: { source: "test" }
  })

  expect(message).toMatchObject({ headers: { source: "test" } })
})
