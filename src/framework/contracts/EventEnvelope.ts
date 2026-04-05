/**
 * @since 1.0.0
 * @module EventEnvelope
 *
 * Schema-driven event envelope for wrapping domain events with metadata.
 * Uses native Effect tracing (traceId/spanId) instead of custom Metadata.
 * Uses @effect/cluster EntityId and EntityType for branded aggregate identity.
 */
import * as Schema from "effect/Schema"
import { EntityId, EntityType } from "@effect/cluster"

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
  version: Schema.optionalWith(Schema.Number, { default: () => 1 }),
  occurredAt: Schema.DateTimeUtc,
  traceId: Schema.optional(Schema.String),
  spanId: Schema.optional(Schema.String),
  actorId: Schema.optional(Schema.String),
  tenantId: Schema.optional(Schema.String),
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
    version: Schema.optionalWith(Schema.Number, { default: () => 1 }),
    occurredAt: Schema.DateTimeUtc,
    traceId: Schema.optional(Schema.String),
    spanId: Schema.optional(Schema.String),
    actorId: Schema.optional(Schema.String),
    tenantId: Schema.optional(Schema.String),
    payload: payloadSchema
  })

/**
 * @since 1.0.0
 * @category type-level
 */
export type TypedEventEnvelope<A extends Schema.Schema.Any> = Schema.Schema.Type<
  ReturnType<typeof makeTyped<A>>
>
