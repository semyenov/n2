/**
 * @since 1.0.0
 * @module DeterministicIdGenerator
 *
 * Sequential ID generator for deterministic testing.
 */
import * as Effect from "effect/Effect"
import * as Ref from "effect/Ref"

/**
 * @since 1.0.0
 * @category models
 */
export interface DeterministicIdGenerator {
  readonly generate: Effect.Effect<string>
}

/**
 * @since 1.0.0
 * @category constructors
 */
export const make = (prefix: string = "test-id"): Effect.Effect<DeterministicIdGenerator> =>
  Effect.gen(function*() {
    const counter = yield* Ref.make(0)
    return {
      generate: Ref.getAndUpdate(counter, (n) => n + 1).pipe(
        Effect.map((n) => `${prefix}-${n}`)
      )
    }
  })
