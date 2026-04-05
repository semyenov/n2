/**
 * @since 1.0.0
 * @module CircuitBreaker
 *
 * Circuit breaker pattern for external service calls.
 * States: Closed → Open (after threshold failures) → HalfOpen (after timeout) → Closed/Open
 *
 * Also re-exports @effect/experimental RateLimiter for token-bucket rate limiting.
 */
import * as Effect from "effect/Effect"
import * as Ref from "effect/Ref"
import * as Schema from "effect/Schema"
import * as Duration from "effect/Duration"
import * as Clock from "effect/Clock"
import { RateLimiter } from "@effect/experimental"

// ---------------------------------------------------------------------------
// Circuit breaker
// ---------------------------------------------------------------------------

/**
 * @since 1.0.0
 * @category models
 */
export type CircuitState =
  | { readonly _tag: "Closed"; readonly failures: number }
  | { readonly _tag: "Open"; readonly openedAt: number }
  | { readonly _tag: "HalfOpen" }

/**
 * @since 1.0.0
 * @category errors
 */
export class CircuitOpenError extends Schema.TaggedError<CircuitOpenError>()(
  "CircuitOpenError",
  { message: Schema.String }
) {}

/**
 * @since 1.0.0
 * @category models
 */
export interface CircuitBreakerConfig {
  readonly failureThreshold: number
  readonly resetTimeoutMillis: number
}

/**
 * @since 1.0.0
 * @category models
 */
export interface CircuitBreaker {
  readonly protect: <A, E, R>(
    effect: Effect.Effect<A, E, R>
  ) => Effect.Effect<A, E | CircuitOpenError, R>
  readonly state: Effect.Effect<CircuitState>
}

/**
 * Creates a circuit breaker.
 *
 * @since 1.0.0
 * @category constructors
 */
export const make = (config: CircuitBreakerConfig): Effect.Effect<CircuitBreaker> =>
  Effect.gen(function*() {
    const stateRef = yield* Ref.make<CircuitState>({ _tag: "Closed", failures: 0 })

    const protect = <A, E, R>(
      effect: Effect.Effect<A, E, R>
    ): Effect.Effect<A, E | CircuitOpenError, R> =>
      Effect.gen(function*() {
        const current = yield* Ref.get(stateRef)
        const now = yield* Clock.currentTimeMillis

        if (current._tag === "Open") {
          if (now - current.openedAt <= config.resetTimeoutMillis) {
            return yield* new CircuitOpenError({ message: "Circuit is open" })
          }
          yield* Ref.set(stateRef, { _tag: "HalfOpen" } satisfies CircuitState)
        }

        return yield* effect.pipe(
          Effect.tap(() =>
            Ref.set(stateRef, { _tag: "Closed", failures: 0 } satisfies CircuitState)
          ),
          Effect.tapError(() =>
            Ref.modify(stateRef, (s): readonly [void, CircuitState] => {
              const failures = (s._tag === "Closed" ? s.failures : 0) + 1
              if (failures >= config.failureThreshold) {
                return [undefined, { _tag: "Open", openedAt: Date.now() }]
              }
              return [undefined, { _tag: "Closed", failures }]
            })
          )
        )
      })

    return {
      protect,
      state: Ref.get(stateRef)
    } satisfies CircuitBreaker
  })

// ---------------------------------------------------------------------------
// Re-exports
// ---------------------------------------------------------------------------

/**
 * @effect/experimental RateLimiter.
 * Supports token-bucket and fixed-window algorithms.
 *
 * @since 1.0.0
 * @category re-exports
 */
export { RateLimiter }
