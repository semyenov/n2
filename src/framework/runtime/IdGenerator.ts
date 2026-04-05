/**
 * @since 1.0.0
 * @module IdGenerator
 *
 * ID generation using @effect/cluster Snowflake.
 * Snowflake IDs are time-ordered, machine-aware distributed IDs.
 */
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import { Snowflake } from "@effect/cluster"

/**
 * @since 1.0.0
 * @category models
 */
export interface IdGeneratorService {
  readonly generate: Effect.Effect<string>
}

/**
 * @since 1.0.0
 * @category tags
 */
export class IdGenerator extends Context.Tag("n2/IdGenerator")<
  IdGenerator,
  IdGeneratorService
>() {}

/**
 * Live ID generator backed by @effect/cluster Snowflake.
 * Produces time-ordered, machine-aware distributed IDs.
 *
 * @since 1.0.0
 * @category layers
 */
export const IdGeneratorLive: Layer.Layer<IdGenerator, never, Snowflake.Generator> =
  Layer.effect(
    IdGenerator,
    Effect.gen(function*() {
      const snowflake = yield* Snowflake.Generator
      return {
        generate: Effect.sync(() => String(snowflake.unsafeNext()))
      }
    })
  )

/**
 * Simple UUID-based ID generator (no cluster dependency).
 *
 * @since 1.0.0
 * @category layers
 */
export const IdGeneratorUuid: Layer.Layer<IdGenerator> = Layer.succeed(
  IdGenerator,
  { generate: Effect.sync(() => crypto.randomUUID()) }
)

/**
 * Re-export Snowflake for direct usage.
 *
 * @since 1.0.0
 * @category re-exports
 */
export { Snowflake }
