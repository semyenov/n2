/**
 * @since 1.0.0
 * @module TestClock
 *
 * Deterministic clock for tests. Wraps a Ref<DateTime.Utc>.
 */
import * as Effect from "effect/Effect"
import * as Ref from "effect/Ref"
import * as DateTime from "effect/DateTime"

/**
 * @since 1.0.0
 * @category models
 */
export interface TestClock {
  readonly now: Effect.Effect<DateTime.Utc>
  readonly set: (date: DateTime.Utc) => Effect.Effect<void>
  readonly advance: (millis: number) => Effect.Effect<void>
}

/**
 * @since 1.0.0
 * @category constructors
 */
export const make = (initial?: DateTime.Utc): Effect.Effect<TestClock> =>
  Effect.gen(function*() {
    const ref = yield* Ref.make<DateTime.Utc>(initial ?? DateTime.unsafeMake(0))
    return {
      now: Ref.get(ref),
      set: (date: DateTime.Utc) => Ref.set(ref, date),
      advance: (millis: number) =>
        Ref.update(ref, (current) =>
          DateTime.mutate(current, (d) => { d.setTime(d.getTime() + millis) })
        )
    }
  })
