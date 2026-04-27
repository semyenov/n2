/**
 * @since 1.0.0
 *
 * Test utilities for aggregate handler tests. Provides ready-made mock layers
 * for snapshot stores, event logs, and a typed test runner.
 *
 * @example
 * ```ts
 * const { makeTestLayers, runWith } = makeTestAggregate({
 *   eventLogSchema: MyEventLogSchema,
 *   noOpProjection: MyNoOpProjection,
 *   handlersLayer: MyHandlersRaw,
 *   snapshotsTag: MySnapshots
 * })
 *
 * test("my test", async () => {
 *   const { handlersLayer, snapshotStore } = makeTestLayers()
 *   await runWith(handlersLayer, Effect.gen(function* () {
 *     const client = yield* RpcTest.makeClient(MyRpcs)
 *     // ...
 *   }))
 * })
 * ```
 */
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import type * as Context from "effect/Context"
import * as ExpEventJournal from "@effect/experimental/EventJournal"
import { Identity } from "@effect/experimental/EventLog"
import * as EventLogApi from "@effect/experimental/EventLog"
import type { EventGroup } from "@effect/experimental"
import type { SnapshotEntry, SnapshotService } from "./Snapshots.js"

/**
 * Creates test utilities for aggregate handler tests.
 * Provides mock snapshot store, event log, and a typed `runWith` helper.
 */
const runProvided = <A, E, R>(
  handlersLayer: Layer.Layer<never, never, never>,
  program: Effect.Effect<A, E, R>
): Promise<A> =>
  Effect.runPromise(
    // Test helpers intentionally collapse the provided test environment after
    // composing handler, journal, identity, projection, and snapshot layers.
    Effect.provide(Effect.scoped(program), handlersLayer) as unknown as Effect.Effect<A, E, never>
  )

export const makeTestAggregate = <State, SnapshotsI>(config: {
  readonly eventLogSchema: EventLogApi.EventLogSchema<EventGroup.EventGroup.Any>
  readonly noOpProjection: Layer.Layer<never, never, never>
  readonly handlersLayer: Layer.Layer<never, never, never>
  readonly snapshotsTag: Context.Tag<SnapshotsI, SnapshotService<State>>
}) => {
  const testJournalLayer = ExpEventJournal.layerMemory
  const testIdentityLayer = Layer.succeed(Identity, Identity.makeRandom())

  const testEventLogLayer = EventLogApi.layer(config.eventLogSchema).pipe(
    Layer.provide(config.noOpProjection),
    Layer.provide(Layer.merge(testJournalLayer, testIdentityLayer))
  )

  const makeTestLayers = () => {
    const snapshotStore = new Map<string, SnapshotEntry<State>>()
    const snapshotsLayer = Layer.succeed(config.snapshotsTag, {
      load: (entityId: string) => Effect.succeed(Option.fromNullable(snapshotStore.get(entityId))),
      save: (entityId: string, state: State, revision: number) =>
        Effect.sync(() => { snapshotStore.set(entityId, { state, revision }) })
    })

    return {
      snapshotStore,
      handlersLayer: Layer.provide(config.handlersLayer, Layer.mergeAll(
        testJournalLayer,
        testIdentityLayer,
        testEventLogLayer,
        snapshotsLayer
      ))
    }
  }

  const runWith = <A, E, R>(
    handlersLayer: Layer.Layer<never, never, never>,
    program: Effect.Effect<A, E, R>
  ): Promise<A> =>
    runProvided(handlersLayer, program)

  return { makeTestLayers, runWith }
}
