/**
 * @since 1.0.0
 * @module Revision
 *
 * Revision (version) types for optimistic concurrency on event streams.
 */
import * as Schema from "effect/Schema"

/**
 * @since 1.0.0
 * @category schemas
 */
export const RevisionSchema = Schema.Number.pipe(
  Schema.int(),
  Schema.nonNegative(),
  Schema.brand("Revision")
)

/**
 * @since 1.0.0
 * @category models
 */
export type Revision = typeof RevisionSchema.Type

/**
 * Construct a Revision from a number. Validates at runtime.
 *
 * @since 1.0.0
 * @category constructors
 */
export const make = Schema.decodeSync(RevisionSchema)

/**
 * @since 1.0.0
 * @category constants
 */
export const InitialRevision: Revision = make(0)

/**
 * Increment a revision by 1.
 *
 * @since 1.0.0
 * @category combinators
 */
export const next = (revision: Revision): Revision => make(revision + 1)

/**
 * @since 1.0.0
 * @category errors
 */
export class ConcurrencyError extends Schema.TaggedError<ConcurrencyError>()(
  "ConcurrencyError",
  {
    expected: RevisionSchema,
    actual: RevisionSchema,
    streamId: Schema.String
  }
) {
  override get message(): string {
    return `Concurrency conflict on stream "${this.streamId}": expected revision ${this.expected}, got ${this.actual}`
  }
}
