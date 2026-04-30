import type * as Schema from "effect/Schema"
import type { Event, EventGroup } from "@effect/experimental"

export type EventGroupEventDefinition<Payload = unknown> =
  Event.Event.AnyWithProps & {
    readonly primaryKey: (payload: Payload) => string
    readonly payload: Schema.Schema.All
    readonly payloadMsgPack: Schema.Schema.All
  }

export type EventGroupEventMap = Readonly<Record<string, EventGroupEventDefinition | undefined>>

type EventGroupWithEvents = {
  readonly events: EventGroupEventMap
}

const eventGroupWithEvents = (
  eventGroup: EventGroup.EventGroup.Any
): EventGroupWithEvents =>
  // EventGroup.Any hides the documented runtime `events` map. Keep this
  // experimental metadata boundary here so replay, decoding, and write-through
  // publishing all share the same adapter.
  eventGroup as unknown as EventGroupWithEvents

export const eventGroupEvents = (
  eventGroup: EventGroup.EventGroup.Any
): EventGroupEventMap =>
  eventGroupWithEvents(eventGroup).events

export const eventGroupEvent = <Payload = unknown>(
  eventGroup: EventGroup.EventGroup.Any,
  tag: string
): EventGroupEventDefinition<Payload> | undefined =>
  eventGroupEvents(eventGroup)[tag] as EventGroupEventDefinition<Payload> | undefined

export const eventGroupEventTags = (
  eventGroup: EventGroup.EventGroup.Any
): ReadonlyArray<string> =>
  Object.keys(eventGroupEvents(eventGroup))

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
