/**
 * @since 1.0.0
 * @module BrandedId
 *
 * Branded ID utilities. Re-exports @effect/cluster EntityId and EntityType
 * for aggregate identifiers. Keeps makeBrandedId for domain-specific IDs.
 */
import * as Schema from "effect/Schema"
import { EntityId, EntityType } from "@effect/cluster"

/**
 * Re-export @effect/cluster EntityId.
 *
 * - Schema: `EntityId.EntityId` (NonEmptyTrimmedString & Brand<"EntityId">)
 * - Constructor: `EntityId.make("abc")` -> EntityId
 * - Type: `EntityId.EntityId`
 *
 * @since 1.0.0
 * @category re-exports
 */
export { EntityId, EntityType }

/**
 * Creates a branded string schema for domain-specific IDs (not entity routing IDs).
 * Use this for things like CustomerId, Sku, etc.
 * For aggregate/entity IDs, use EntityId from @effect/cluster directly.
 *
 * @since 1.0.0
 * @category constructors
 */
export const makeBrandedId = <const Name extends string>(name: Name) =>
  Schema.String.pipe(Schema.brand(name))
