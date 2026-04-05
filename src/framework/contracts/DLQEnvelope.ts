/**
 * @since 1.0.0
 * @module DLQEnvelope
 *
 * Dead-letter queue envelope for failed event processing.
 */
import * as Schema from "effect/Schema"
import { EventEnvelope } from "./EventEnvelope.js"

/**
 * @since 1.0.0
 * @category schemas
 */
export class DLQEnvelope extends Schema.Class<DLQEnvelope>("n2/DLQEnvelope")({
  originalEnvelope: EventEnvelope,
  error: Schema.String,
  failedAt: Schema.DateTimeUtc,
  retryCount: Schema.Number,
  projectorName: Schema.String
}) {}
