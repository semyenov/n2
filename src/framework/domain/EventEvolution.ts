/**
 * @since 1.0.0
 * @module EventEvolution
 *
 * Event schema versioning and upcasting.
 * Apply upcasters to transform old event formats to current versions.
 */
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import type * as ParseResult from "effect/ParseResult"

/**
 * @since 1.0.0
 * @category models
 */
export interface Upcaster<From, To> {
  readonly fromVersion: number
  readonly toVersion: number
  readonly transform: (event: From) => To
}

/**
 * @since 1.0.0
 * @category constructors
 */
export const makeUpcaster = <From, To>(
  fromVersion: number,
  toVersion: number,
  transform: (event: From) => To
): Upcaster<From, To> => ({ fromVersion, toVersion, transform })

/**
 * An ordered chain of upcasters that transform events from any old
 * version to the current version.
 *
 * @since 1.0.0
 * @category models
 */
export interface UpcasterChain {
  readonly currentVersion: number
  readonly upcasters: ReadonlyArray<Upcaster<unknown, unknown>>
}

/**
 * @since 1.0.0
 * @category constructors
 */
export const makeChain = (
  currentVersion: number,
  upcasters: ReadonlyArray<Upcaster<unknown, unknown>>
): UpcasterChain => ({
  currentVersion,
  upcasters: [...upcasters].sort((a, b) => a.fromVersion - b.fromVersion)
})

/**
 * Applies upcasters to transform an event from `eventVersion` to
 * the chain's `currentVersion`.
 *
 * @since 1.0.0
 * @category combinators
 */
export const upcast = (
  chain: UpcasterChain,
  event: unknown,
  eventVersion: number
): unknown => {
  if (eventVersion >= chain.currentVersion) return event
  let current = event
  let version = eventVersion
  for (const upcaster of chain.upcasters) {
    if (upcaster.fromVersion === version) {
      current = upcaster.transform(current)
      version = upcaster.toVersion
    }
  }
  return current
}

/**
 * Wraps a schema with version-aware decoding.
 * Applies upcasters before decoding if the event version is older.
 *
 * @since 1.0.0
 * @category combinators
 */
export const decodeVersioned = <A, I, R>(
  schema: Schema.Schema<A, I, R>,
  chain: UpcasterChain
) => {
  const decode = Schema.decodeUnknown(schema)
  return (event: unknown, version: number): Effect.Effect<A, ParseResult.ParseError, R> => {
    const upcasted = upcast(chain, event, version)
    return decode(upcasted)
  }
}
