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
 * // Migration runs after raw MsgPack decoding and before current-schema
 * // validation. The migration receives the stored historical payload and must
 * // return a value accepted by the current event payload schema.
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
import * as MsgPack from "@effect/platform/MsgPack"
import {
  eventGroupPayloadSchemas,
  eventGroupPayloadValueSchemas
} from "./EventGroupAccess.js"

type Tagged = { readonly _tag: string }

/** Per-event migration function. Receives the raw unpacked historical payload
 *  and returns a payload that must validate against the current event schema. */
type EventMigration = (payload: unknown) => unknown
type DecodePayload = (payload: unknown) => Effect.Effect<unknown, unknown, never>
type EventDecoders = {
  readonly msgPack: DecodePayload
  readonly payload: DecodePayload
}

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
  const msgPackPayloadSchemas = eventGroupPayloadSchemas(eventGroup)
  const payloadValueSchemas = eventGroupPayloadValueSchemas(eventGroup)
  const decoders = new Map<string, EventDecoders>()
  const decodeRawPayload = Schema.decodeUnknown(MsgPack.schema(Schema.Unknown))
  const migrations = options?.migrations ?? {}

  for (const [tag, msgPackSchema] of Object.entries(msgPackPayloadSchemas)) {
    const payloadSchema = payloadValueSchemas[tag]
    if (msgPackSchema && payloadSchema) {
      decoders.set(
        tag,
        {
          msgPack: Schema.decodeUnknown(msgPackSchema as Schema.Schema<unknown, unknown, never>) as DecodePayload,
          payload: Schema.decodeUnknown(payloadSchema as Schema.Schema<unknown, unknown, never>) as DecodePayload
        }
      )
    }
  }

  return (entry: EventJournalApi.Entry): Effect.Effect<Event, Error> => {
    const tag = entry.event
    const decoderSet = decoders.get(tag)
    if (!decoderSet) {
      return Effect.fail(new Error(`Unsupported event: ${tag}`))
    }
    const Ctor = constructors[tag]
    if (!Ctor) {
      return Effect.fail(new Error(`No constructor for event: ${tag}`))
    }
    const migrate = migrations[tag]
    const decodePayload = migrate
      ? decodeRawPayload(entry.payload).pipe(
        Effect.map(migrate),
        Effect.flatMap(decoderSet.payload)
      )
      : decoderSet.msgPack(entry.payload)

    return decodePayload.pipe(
      Effect.map((payload) => new Ctor(payload as never)),
      Effect.mapError((e) => new Error(`Failed to decode ${tag}: ${String(e)}`))
    )
  }
}
