/**
 * @since 1.0.0
 *
 * Generic event decoder for replaying events from an EventJournal.
 * Builds a decoder function from an EventGroup + event constructors,
 * eliminating the repetitive per-event decoder boilerplate.
 *
 * @example
 * ```ts
 * const decodeEvent = makeEventDecoder(MyEventGroup, {
 *   OrderCreated, ItemAdded, OrderSubmitted, OrderCancelled
 * })
 *
 * const event = yield* decodeEvent(journalEntry)
 * ```
 */
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import type { EventGroup } from "@effect/experimental"
import type * as EventJournalApi from "@effect/experimental/EventJournal"

type Tagged = { readonly _tag: string }

/**
 * Creates a decoder function that converts EventJournal entries into typed domain events.
 * Automatically builds decoders from the EventGroup's payloadMsgPack schemas and
 * instantiates the corresponding event class.
 */
export const makeEventDecoder = <Event extends Tagged>(
  eventGroup: EventGroup.EventGroup.Any,
  constructors: { readonly [tag: string]: new (payload: never) => Event }
) => {
  const events = (eventGroup as unknown as { events: Record<string, { payloadMsgPack: Schema.Schema.All } | undefined> }).events
  const decoders = new Map<string, (payload: unknown) => Effect.Effect<unknown>>()

  for (const [tag, schema] of Object.entries(events)) {
    if (schema) {
      decoders.set(tag, Schema.decode(schema.payloadMsgPack as never) as (u: unknown) => Effect.Effect<unknown>)
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
    return decoder(entry.payload).pipe(
      Effect.map((payload) => new Ctor(payload as never)),
      Effect.mapError((e) => new Error(`Failed to decode ${tag}: ${String(e)}`))
    )
  }
}
