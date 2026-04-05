/**
 * @since 1.0.0
 * @module InMemoryDeadLetter
 *
 * Ref-backed in-memory dead-letter store for testing.
 */
import * as Effect from "effect/Effect"
import * as Ref from "effect/Ref"
import * as Stream from "effect/Stream"
import * as Layer from "effect/Layer"
import { DeadLetter, type DeadLetterService } from "../projection/DeadLetter.js"
import type { DLQEnvelope } from "../contracts/DLQEnvelope.js"

/**
 * @since 1.0.0
 * @category constructors
 */
export const make: Effect.Effect<
  DeadLetterService & {
    readonly all: Effect.Effect<ReadonlyArray<DLQEnvelope>>
  }
> = Effect.gen(function*() {
  const store = yield* Ref.make<ReadonlyArray<DLQEnvelope>>([])

  const send = (envelope: DLQEnvelope): Effect.Effect<void> =>
    Ref.update(store, (items) => [...items, envelope])

  const consume = (projectorName: string): Stream.Stream<DLQEnvelope> =>
    Stream.fromEffect(Ref.get(store)).pipe(
      Stream.flatMap((items) =>
        Stream.fromIterable(
          items.filter((e) => e.projectorName === projectorName)
        )
      )
    )

  const retry = (_envelope: DLQEnvelope): Effect.Effect<void> => Effect.void

  const all: Effect.Effect<ReadonlyArray<DLQEnvelope>> = Ref.get(store)

  return { send, consume, retry, all }
})

/**
 * @since 1.0.0
 * @category layers
 */
export const layer: Layer.Layer<DeadLetter> = Layer.effect(
  DeadLetter,
  make.pipe(
    Effect.map((svc) => ({
      send: svc.send,
      consume: svc.consume,
      retry: svc.retry
    }))
  )
)
