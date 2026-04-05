/**
 * @since 1.0.0
 * @module SnapshotStore
 *
 * Snapshot store service interface for aggregate state caching.
 * Includes a layer backed by @effect/experimental Persistence.
 */
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Duration from "effect/Duration"
import { Persistence } from "@effect/experimental"
import type { EntityId } from "@effect/cluster/EntityId"
import type { Revision } from "../domain/Revision.js"

/**
 * @since 1.0.0
 * @category models
 */
export interface SnapshotData {
  readonly state: unknown
  readonly revision: Revision
}

/**
 * @since 1.0.0
 * @category models
 */
export interface SnapshotStoreService {
  readonly load: (
    aggregateId: EntityId
  ) => Effect.Effect<Option.Option<SnapshotData>>

  readonly save: (
    aggregateId: EntityId,
    state: unknown,
    revision: Revision
  ) => Effect.Effect<void>
}

/**
 * @since 1.0.0
 * @category tags
 */
export class SnapshotStore extends Context.Tag("n2/SnapshotStore")<
  SnapshotStore,
  SnapshotStoreService
>() {}

/**
 * Creates a SnapshotStore backed by @effect/experimental BackingPersistence.
 *
 * @since 1.0.0
 * @category layers
 */
export const layerFromPersistence: Layer.Layer<
  SnapshotStore,
  never,
  Persistence.BackingPersistence
> = Layer.scoped(
  SnapshotStore,
  Effect.gen(function*() {
    const backing = yield* Persistence.BackingPersistence
    const store = yield* backing.make("n2/snapshots")

    const load = (
      aggregateId: EntityId
    ): Effect.Effect<Option.Option<SnapshotData>> =>
      store.get(aggregateId).pipe(
        Effect.map((opt) =>
          Option.map(opt, (raw) => raw as SnapshotData)
        ),
        Effect.catchAll(() => Effect.succeed(Option.none()))
      )

    const save = (
      aggregateId: EntityId,
      state: unknown,
      revision: Revision
    ): Effect.Effect<void> =>
      store.set(
        aggregateId,
        { state, revision } satisfies SnapshotData,
        Option.none()
      ).pipe(Effect.catchAll(() => Effect.void))

    return { load, save } satisfies SnapshotStoreService
  })
)

/**
 * In-memory SnapshotStore backed by @effect/experimental Persistence.layerMemory.
 *
 * @since 1.0.0
 * @category layers
 */
export const layerMemory: Layer.Layer<SnapshotStore> = layerFromPersistence.pipe(
  Layer.provide(Persistence.layerMemory)
)
