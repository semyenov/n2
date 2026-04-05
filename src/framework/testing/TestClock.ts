/**
 * @since 1.0.0
 * @module TestClock
 *
 * Ref-backed test clock for deterministic time in tests.
 */
import * as Effect from "effect/Effect"
import * as Ref from "effect/Ref"
import * as Layer from "effect/Layer"
import * as DateTime from "effect/DateTime"
import { N2Clock, type N2ClockService } from "../runtime/Clock.js"

/**
 * @since 1.0.0
 * @category tags
 */
export class TestClockAccess extends Effect.Tag(
  "n2/testing/TestClockAccess"
)<TestClockAccess, {
  readonly set: (date: DateTime.Utc) => Effect.Effect<void>
  readonly advance: (millis: number) => Effect.Effect<void>
}>() {}

/**
 * @since 1.0.0
 * @category constructors
 */
export const make = (
  initial?: DateTime.Utc
): Effect.Effect<
  N2ClockService & {
    readonly set: (date: DateTime.Utc) => Effect.Effect<void>
    readonly advance: (millis: number) => Effect.Effect<void>
  }
> =>
  Effect.gen(function*() {
    const ref = yield* Ref.make<DateTime.Utc>(
      initial ?? DateTime.unsafeMake(0)
    )

    const now: Effect.Effect<DateTime.Utc> = Ref.get(ref)

    const set = (date: DateTime.Utc): Effect.Effect<void> =>
      Ref.set(ref, date)

    const advance = (millis: number): Effect.Effect<void> =>
      Ref.update(ref, (current) =>
        DateTime.mutate(current, (d) => {
          d.setTime(d.getTime() + millis)
        })
      )

    return { now, set, advance }
  })

/**
 * @since 1.0.0
 * @category layers
 */
export const layer = (initial?: DateTime.Utc): Layer.Layer<N2Clock> =>
  Layer.effect(
    N2Clock,
    make(initial).pipe(Effect.map((svc) => ({ now: svc.now })))
  )
