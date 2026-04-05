/**
 * @since 1.0.0
 * @deprecated Use native Effect tracing (Effect.withSpan + cluster Envelope headers).
 * EventEnvelope now carries traceId/spanId directly.
 * @module Metadata
 *
 * Request metadata for correlation and causation.
 *
 * In clustered mode, prefer Effect's native tracing:
 * - `Effect.withSpan("name")` for spans
 * - `Effect.annotateCurrentSpan("key", "value")` for attributes
 * - `@effect/cluster` Envelope carries traceId/spanId over the wire automatically
 *
 * This module is kept for backward compatibility and for
 * non-cluster contexts (e.g. Kafka message headers).
 */
import * as Schema from "effect/Schema"
import * as DateTime from "effect/DateTime"
import * as Effect from "effect/Effect"

/**
 * @since 1.0.0
 * @category schemas
 */
export class Metadata extends Schema.Class<Metadata>("n2/Metadata")({
  correlationId: Schema.String,
  causationId: Schema.String,
  tenantId: Schema.optional(Schema.String),
  actorId: Schema.String,
  timestamp: Schema.DateTimeUtc
}) {}

/**
 * @since 1.0.0
 * @category constructors
 */
export const make = (params: {
  readonly correlationId?: string
  readonly causationId?: string
  readonly tenantId?: string
  readonly actorId: string
}): Effect.Effect<Metadata> =>
  Effect.gen(function*() {
    const now = yield* DateTime.now
    return new Metadata({
      correlationId: params.correlationId ?? crypto.randomUUID(),
      causationId: params.causationId ?? crypto.randomUUID(),
      tenantId: params.tenantId,
      actorId: params.actorId,
      timestamp: now
    })
  })

/**
 * @since 1.0.0
 * @category constructors
 */
export const empty: Metadata = new Metadata({
  correlationId: "00000000-0000-0000-0000-000000000000",
  causationId: "00000000-0000-0000-0000-000000000000",
  actorId: "system",
  timestamp: DateTime.unsafeMake(0)
})
