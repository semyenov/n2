/**
 * @since 1.0.0
 * @module Clock
 *
 * Framework clock. Uses Effect's built-in DateTime.now.
 * Kept as a thin Context.Tag for testability.
 */
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as DateTime from "effect/DateTime"

/**
 * @since 1.0.0
 * @category models
 */
export interface N2ClockService {
  readonly now: Effect.Effect<DateTime.Utc>
}

/**
 * @since 1.0.0
 * @category tags
 */
export class N2Clock extends Context.Tag("n2/Clock")<
  N2Clock,
  N2ClockService
>() {}

/**
 * Live clock using Effect's DateTime.now.
 *
 * @since 1.0.0
 * @category layers
 */
export const N2ClockLive: Layer.Layer<N2Clock> = Layer.succeed(
  N2Clock,
  { now: DateTime.now }
)
