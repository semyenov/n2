/**
 * @since 1.0.0
 * @module EventEnvelope
 *
 * Schema-driven event envelope for wrapping domain events with metadata.
 * Uses @effect/cluster EntityId and EntityType for branded aggregate identity.
 */
import * as Schema from "effect/Schema"
import { EntityId, EntityType } from "@effect/cluster"
import { Metadata } from "./Metadata.js"

/**
 * @since 1.0.0
 * @category schemas
 */
export class EventEnvelope extends Schema.Class<EventEnvelope>("n2/EventEnvelope")({
  eventId: Schema.String,
  streamId: Schema.String,
  aggregateId: EntityId.EntityId,
  aggregateType: EntityType.EntityType,
  revision: Schema.Number,
  occurredAt: Schema.DateTimeUtc,
  metadata: Metadata,
  payload: Schema.Unknown
}) {}

/**
 * Creates a typed event envelope schema for a specific payload type.
 *
 * @since 1.0.0
 * @category constructors
 */
export const makeTyped = <A extends Schema.Schema.Any>(payloadSchema: A) =>
  Schema.Struct({
    eventId: Schema.String,
    streamId: Schema.String,
    aggregateId: EntityId.EntityId,
    aggregateType: EntityType.EntityType,
    revision: Schema.Number,
    occurredAt: Schema.DateTimeUtc,
    metadata: Metadata,
    payload: payloadSchema
  })

/**
 * @since 1.0.0
 * @category type-level
 */
export type TypedEventEnvelope<A extends Schema.Schema.Any> = Schema.Schema.Type<
  ReturnType<typeof makeTyped<A>>
>
