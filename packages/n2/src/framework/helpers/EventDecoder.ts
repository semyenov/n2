/**
 * @since 1.0.0
 *
 * Generic event decoder for replaying events from an EventJournal.
 * Builds a decoder function from an EventGroup + event constructors,
 * eliminating the repetitive per-event decoder boilerplate.
 *
 * Supports optional event migrations for schema evolution. When an event
 * payload structure changes, provide a migration function that transforms
 * old payloads into the current shape before decoding.
 *
 * @example
 * ```ts
 * const decodeEvent = makeEventDecoder(MyEventGroup, {
 *   OrderCreated, ItemAdded, OrderSubmitted, OrderCancelled
 * })
 *
 * const event = yield* decodeEvent(journalEntry)
 * ```
 *
 * @example With migrations
 * ```ts
 * const decodeEvent = makeEventDecoder(MyEventGroup, {
 *   OrderCreated, ItemAdded
 * }, {
 *   migrations: {
 *     OrderCreated: (payload) => ({ ...payload, newField: payload.newField ?? "default" })
 *   }
 * })
 * ```
 */
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import type { EventGroup } from "@effect/experimental"
import type * as EventJournalApi from "@effect/experimental/EventJournal"
import { eventGroupPayloadSchemas } from "./EventGroupAccess.js"

type Tagged = { readonly _tag: string }

/** Per-event migration function. Transforms a raw payload before Schema decoding. */
type EventMigration = (payload: unknown) => unknown

/**
 * Creates a decoder function that converts EventJournal entries into typed domain events.
 * Automatically builds decoders from the EventGroup's payloadMsgPack schemas and
 * instantiates the corresponding event class.
 *
 * @param eventGroup - EventGroup defining the payload schemas
 * @param constructors - Map of event tag → constructor class
 * @param options - Optional configuration
 * @param options.migrations - Per-event payload migration functions for schema evolution
 */
export const makeEventDecoder = <Event extends Tagged>(
  eventGroup: EventGroup.EventGroup.Any,
  constructors: { readonly [tag: string]: new (payload: never) => Event },
  options?: {
    /** Per-event migration functions that transform raw payloads before Schema decoding.
     *  Use when event schemas evolve and old journal entries need transformation. */
    readonly migrations?: { readonly [tag: string]: EventMigration }
  }
) => {
  const events = eventGroupPayloadSchemas(eventGroup)
  const decoders = new Map<string, (payload: unknown) => Effect.Effect<unknown, unknown, never>>()
  const migrations = options?.migrations ?? {}

  for (const [tag, schema] of Object.entries(events)) {
    if (schema) {
      decoders.set(
        tag,
        Schema.decodeUnknown(schema as Schema.Schema<unknown, unknown, never>) as (payload: unknown) => Effect.Effect<unknown, unknown, never>
      )
    }
  }

  return (entry: EventJournalApi.Entry): Effect.Effect<Event, Error> => {
    const tag = entry.event
    const decoder = decoders.get(tag)
    if (!decoder) {
      return Effect.fail(new Error(`Unsupported event: ${tag}`))
    }
    const Ctor = constructors[tag]
    if (!Ctor) {
      return Effect.fail(new Error(`No constructor for event: ${tag}`))
    }
    const migrate = migrations[tag]
    const payload = migrate ? migrate(entry.payload) : entry.payload
    return decoder(payload).pipe(
      Effect.map((decoded) => new Ctor(decoded as never)),
      Effect.mapError((e) => new Error(`Failed to decode ${tag}: ${String(e)}`))
    )
  }
}
