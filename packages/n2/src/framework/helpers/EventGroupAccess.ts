import type * as Schema from "effect/Schema"
import type { Event, EventGroup } from "@effect/experimental"

type EventGroupWithEvents = {
  readonly events: Readonly<Record<string, Event.Event.AnyWithProps | undefined>>
}

const eventGroupWithEvents = (
  eventGroup: EventGroup.EventGroup.Any
): EventGroupWithEvents =>
  // EventGroup.Any intentionally hides implementation details, but replay and
  // decoding need the runtime event map built by @effect/experimental.
  eventGroup as unknown as EventGroupWithEvents

export const eventGroupEvents = (
  eventGroup: EventGroup.EventGroup.Any
): Readonly<Record<string, Event.Event.AnyWithProps | undefined>> =>
  eventGroupWithEvents(eventGroup).events

export const eventGroupPayloadSchemas = (
  eventGroup: EventGroup.EventGroup.Any
): Readonly<Record<string, Schema.Schema.All | undefined>> => {
  const schemas: Record<string, Schema.Schema.All | undefined> = {}
  for (const [tag, event] of Object.entries(eventGroupEvents(eventGroup))) {
    schemas[tag] = event?.payloadMsgPack
  }
  return schemas
}

export const eventGroupPayloadValueSchemas = (
  eventGroup: EventGroup.EventGroup.Any
): Readonly<Record<string, Schema.Schema.All | undefined>> => {
  const schemas: Record<string, Schema.Schema.All | undefined> = {}
  for (const [tag, event] of Object.entries(eventGroupEvents(eventGroup))) {
    schemas[tag] = event?.payload
  }
  return schemas
}
