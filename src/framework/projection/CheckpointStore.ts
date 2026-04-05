/**
 * @since 1.0.0
 * @module CheckpointStore
 *
 * Checkpoint/offset store for projector progress tracking.
 * Backed by @effect/experimental Persistence.
 */
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import { Persistence } from "@effect/experimental"

/**
 * @since 1.0.0
 * @category models
 */
export interface CheckpointStoreService {
  readonly load: (
    projectorName: string,
    partition: number
  ) => Effect.Effect<Option.Option<string>>

  readonly save: (
    projectorName: string,
    partition: number,
    offset: string
  ) => Effect.Effect<void>
}

/**
 * @since 1.0.0
 * @category tags
 */
export class CheckpointStore extends Context.Tag("n2/CheckpointStore")<
  CheckpointStore,
  CheckpointStoreService
>() {}

/**
 * CheckpointStore backed by @effect/experimental BackingPersistence.
 *
 * @since 1.0.0
 * @category layers
 */
export const layerFromPersistence: Layer.Layer<
  CheckpointStore,
  never,
  Persistence.BackingPersistence
> = Layer.scoped(
  CheckpointStore,
  Effect.gen(function*() {
    const backing = yield* Persistence.BackingPersistence
    const store = yield* backing.make("n2/checkpoints")

    const load = (
      projectorName: string,
      partition: number
    ): Effect.Effect<Option.Option<string>> =>
      store.get(`${projectorName}:${partition}`).pipe(
        Effect.map((opt) => Option.map(opt, (v) => String(v))),
        Effect.catchAll(() => Effect.succeed(Option.none()))
      )

    const save = (
      projectorName: string,
      partition: number,
      offset: string
    ): Effect.Effect<void> =>
      store.set(
        `${projectorName}:${partition}`,
        offset,
        Option.none()
      ).pipe(Effect.catchAll(() => Effect.void))

    return { load, save } satisfies CheckpointStoreService
  })
)

/**
 * In-memory CheckpointStore backed by @effect/experimental Persistence.layerMemory.
 *
 * @since 1.0.0
 * @category layers
 */
export const layerMemory: Layer.Layer<CheckpointStore> =
  layerFromPersistence.pipe(Layer.provide(Persistence.layerMemory))
